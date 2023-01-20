import cbor2
from dataclasses import dataclass
import hashlib
import json
import requests
import typing
from dask.distributed import Lock
from multiformats import CID
from multiformats import multihash
from py_hamt.hamt import Hamt, create, load


@dataclass
class InlineCodec:
    decoder: typing.Callable[[bytes], typing.Any]
    encoder: typing.Callable[[typing.Any], bytes]


def json_dumps_bytes(obj: typing.Any) -> bytes:
    return json.dumps(obj).encode("utf-8")


json_inline_codec = InlineCodec(json.loads, json_dumps_bytes)


inline_objects = {
    ".zarray": json_inline_codec,
    ".zgroup": json_inline_codec,
    ".zmetadata": json_inline_codec,
    ".zattrs": json_inline_codec,
}

def default_encoder(encoder, value):
    encoder.encode(cbor2.CBORTag(42,  b'\x00' + bytes(value)))

def get_cbor_dag_hash(obj) -> typing.Tuple[CID, bytes]:
    """Generates the IPFS hash and bytes an object would have if it were put to IPFS as dag-cbor,
        without actually making an IPFS call (much faster)

    Args:
        obj: object to generate hash and dag-cbor bytes for
    Returns:
        typing.Tuple[CID, bytes]: IPFS hash and dag-cbor bytes
    """
    obj_cbor = cbor2.dumps(obj, default=default_encoder)
    obj_cbor_hash = multihash.get("sha2-256").digest(obj_cbor)
    return CID("base32", 1, "dag-cbor", obj_cbor_hash), obj_cbor


class HamtIPFSStore:
    """Class implementing methods necessary for a HAMT store. Unlike other stores, does not save objects
    permanently -- instead, stores them in memory and generates an ID without actually persisting the object.
    It is the responsibility of the user to save all the HAMTs keys on their own if they
    wish the HAMT to persist.
    """

    def __init__(self):
        self.mapping = {}

    def save(self, obj):
        cid, obj_cbor = get_cbor_dag_hash(obj)
        self.mapping[cid] = obj_cbor
        return cid

    def load(self, cid):
        if isinstance(cid, cbor2.CBORTag):
            cid = CID.decode(cid.value[1:]).set(base="base32")
        try:
            return cbor2.loads(self.mapping[cid])
        except KeyError:
            res = requests.post(
                "http://localhost:5001/api/v0/block/get", params={"arg": str(cid)}
            )
            res.raise_for_status()
            obj = cbor2.loads(res.content)
            self.mapping[cid] = res.content
            return obj

    def is_equal(self, id1: CID, id2: CID):
        return str(id1) == str(id2)

    def is_link(self, obj: CID):
        if isinstance(obj, cbor2.CBORTag):
            obj = CID.decode(obj.value[1:]).set(base="base32")
        return isinstance(obj, CID) and obj.codec.name == "dag-cbor"


class HamtWrapper:
    SEP = "/"

    def __init__(
        self,
        store=HamtIPFSStore(),
        starting_id: typing.Optional[CID] = None,
        others_dict: dict = None,
    ) -> None:
        """
        Args:
            store (optional): Backing store for underyling HAMT. Defaults to HamtIPFSStore().
            starting_id (typing.Optional[CID], optional):
                CID with which to initialize HAMT. In this implementation, the HAMT will only include
                keys corresponding to zarr chunks, represented by dag-pb CIDs. Defaults to None.
            others_dict (dict, optional):
                Starting dict for keys that aren't going to be put in the HAMT. Defaults to None.
        """

        # Register the sha2-256 hash with HAMT
        Hamt.register_hasher(
            0x12, 32, lambda x: hashlib.sha256(x.encode("utf-8")).digest()
        )

        if starting_id:
            # Load HAMT from store using given id
            self.hamt = load(store, starting_id)
        else:
            # Create HAMT from scratch with sensible default options
            self.hamt = create(
                store, options={"bit_width": 8, "bucket_size": 5, "hash_alg": 0x12}
            )

        self.others_dict = others_dict if others_dict is not None else {}

    def get(self, key_path: typing.List[str]):
        """Get the value located at a `key_path`. First checks the HAMT, and if unable to find the key,
            checks `others_dict`. If not in either, raises `KeyError`

        Args:
            key_path (typing.List[str]): decomposed key for which to find value

        Returns:
            value located at this `key_path`
        """
        try:
            return self.hamt.get(self.SEP.join(key_path))
        except KeyError:
            return get_recursive(self.others_dict, key_path)

    def set(self, key_path: typing.List[str], value) -> None:
        """Sets a `key_path` to a given `value`. If the value is a dag-pb CID, then sets the HAMT.
            Otherwise, sets `others_dict`.

        Args:
            key_path (typing.List[str]): decomposed key for which to set value
            value: value to set
        """
        if isinstance(value, CID) and value.codec.name == "dag-pb":
            # We need to lock the HAMT to prevent multiple threads writing to it at once,
            # as it is not thread safe
            try:
                # lock needs name so dask knows to recognize it across threads
                lock = Lock("hamt-write")
                lock.acquire()
            except ValueError:
                lock = None
            self.hamt = self.hamt.set(self.SEP.join(key_path), value)
            if lock is not None:
                lock.release()
        else:
            set_recursive(self.others_dict, key_path, value)

    def iter_all(self) -> typing.Iterator[str]:
        """Iterates over all keys in both `others_dict` and the HAMT

        Yields:
            str: key
        """
        yield from self._iter_nested("", self.others_dict)
        yield from self.hamt.keys()

    def _iter_nested(self, prefix: str, mapping: dict) -> typing.Iterator[str]:
        """Iterates over all keys in `mapping`, reconstructing the key from decomposed
            version when necessary

        Args:
            prefix (str): parts of key that have been used up intil this point in the recursion
            mapping (dict): dict to pull keys from

        Yields:
            str: key
        """
        for key, value in mapping.items():
            key_parts = key.split(self.SEP)
            if key_parts[-1] in inline_objects:
                yield prefix + key
            elif isinstance(value, dict):
                yield from self._iter_nested(prefix + key + self.SEP, value)
            else:
                yield prefix + key

    def to_dict(self) -> dict:
        """Stores all keys in the HAMT permanently in IPFS, then returns a dict representation
            of the whole data structure

        Returns:
            dict: dict representation of `self`, including both `others_dict` and `hamt`
        """
        for id in self.hamt.ids():
            obj_cbor = self.hamt.store.mapping[id]
            res = requests.post(
                "http://localhost:5001/api/v0/block/put?cid-codec=dag-cbor",
                files={"dummy": obj_cbor},
            )
            res.raise_for_status()
        return {**self.others_dict, "hamt": self.hamt.id}

    @staticmethod
    def from_dict(d: dict) -> "HamtWrapper":
        """Takes a dict generated by `to_dict` and turns it back into a HAMT.

        Args:
            d (dict): generated by `to_dict`

        Returns:
            HamtWrapper: corresponds to this dict `d`
        """
        others_dict = d
        hamt_id = others_dict["hamt"]
        del others_dict["hamt"]
        return HamtWrapper(starting_id=hamt_id, others_dict=others_dict)


def set_recursive(obj, path, value) -> None:
    assert len(path) >= 1
    if len(path) == 1:
        obj[path[0]] = value
    else:
        set_recursive(obj.setdefault(path[0], {}), path[1:], value)  # type: ignore


def get_recursive(obj, path):
    assert len(path) >= 1
    if len(path) == 1:
        return obj[path[0]]
    else:
        return get_recursive(obj[path[0]], path[1:])  # type: ignore
