import cbor2
from dataclasses import dataclass
import hashlib
import json
import psutil
import requests
import typing
from dask.distributed import Lock
from multiformats import CID
from hashlib import sha256
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
    encoder.encode(cbor2.CBORTag(42, b"\x00" + bytes(value)))


def get_cbor_dag_hash(obj) -> typing.Tuple[CID, bytes]:
    """Generates the IPFS hash and bytes an object would have if it were put to IPFS as dag-cbor,
        without actually making an IPFS call (much faster)

    Args:
        obj: object to generate hash and dag-cbor bytes for
    Returns:
        typing.Tuple[CID, bytes]: IPFS hash and dag-cbor bytes
    """
    obj_cbor = cbor2.dumps(obj, default=default_encoder)

    # the multihash format prefixes the raw sha256 digest with two bytes:
    # 18 (the multicodec code for sha256) and 32 (the length of the digest in bytes)
    obj_cbor_multi_hash = bytes([18, 32]) + sha256(obj_cbor).digest()
    return CID("base32", 1, "dag-cbor", obj_cbor_multi_hash), obj_cbor


class HamtMemoryStore:
    """Class implementing methods necessary for a HAMT store. Unlike other stores, does not save objects
    permanently -- instead, stores them in memory and generates an ID without actually persisting the object.
    It is the responsibility of the user to save all the HAMTs keys on their own if they
    wish the HAMT to persist.
    """

    def __init__(self):
        self.mapping: typing.Dict[CID, bytes] = {}
        self.num_bytes_in_mapping = 0

    def save(self, obj) -> CID:
        """Generate an ID for object and store the CID and a dag_cbor representation of the object
            in the store's mapping. Also tracks how large mapping is by adding the length of the dag_cbor
            representation.

        Args:
            obj: object being stored in HAMT

        Returns:
            CID: dag-cbor hash for object
        """
        cid, obj_cbor = get_cbor_dag_hash(obj)
        self.mapping[cid] = obj_cbor
        self.num_bytes_in_mapping += len(obj_cbor)
        return cid

    def garbage_collect_mapping(self, hamt: Hamt):
        """Removes all CIDs in the store's mapping not present in the provided HAMT. Useful because, in general,
            the intermediate states stored in the mapping are not necesary to persist and can consume 100s of GBs
            of RAM when generating large HAMTs.


        Args:
            hamt (Hamt): HAMT whose CIDs will be persisted -- ids that are in the store's mapping but not
                in `hamt` will be deleted from the mapping.
        """
        hamt_ids = set()
        for hamt_id in hamt.ids():
            if isinstance(hamt_id, cbor2.CBORTag):
                hamt_ids.add(CID.decode(hamt_id.value[1:]).set(base="base32"))
            else:
                hamt_ids.add(hamt_id)
        to_delete = [key for key in self.mapping if key not in hamt_ids]
        for key in to_delete:
            self.num_bytes_in_mapping -= len(self.mapping[key])
            del self.mapping[key]

    def load(self, cid: typing.Union[cbor2.CBORTag, CID]):
        """Given a dag-cbor CID, returns the corresponding object. First checks to see if
            CID is present in store's mapping. If not, then checks IFPS, and finally fails if
            the CID can't be found in 30s.

        Args:
            cid typing.Union[cbor2.CBORTag, CID]: CID to load. Might initially be in CBORTag form
                due to the fact that we use cbor2 to encode objects for speed.

        Raises:
            Exception: Raised when CID cannot be located in store's mapping or IPFS.

        Returns: object whose dag-cbor hash is the CID
        """
        if isinstance(cid, cbor2.CBORTag):
            cid = CID.decode(cid.value[1:]).set(base="base32")
        try:
            return cbor2.loads(self.mapping[cid])
        except KeyError:
            try:
                res = requests.post(
                    "http://localhost:5001/api/v0/block/get",
                    params={"arg": str(cid)},
                    timeout=30,
                )
            except requests.exceptions.ReadTimeout:
                raise Exception(f"timed out on {str(cid)}")
            res.raise_for_status()
            obj = cbor2.loads(res.content)
            self.mapping[cid] = res.content
            return obj

    def is_equal(self, id1: CID, id2: CID):
        return str(id1) == str(id2)

    def is_link(self, obj: CID):
        if isinstance(obj, cbor2.CBORTag):
            # dag-cbor tags have 37 bytes in their value:
            # 1 from CBORTag prefix, 1 from version, 34 from multihash digest, 1 from codec
            # dag-pb v0 hashes have fewer.
            return obj.tag == 42 and len(obj.value) == 37
        return isinstance(obj, CID) and obj.codec.name == "dag-cbor"


class HamtWrapper:
    SEP = "/"

    MAX_PERCENT_OF_RAM_FOR_MAPPING = 0.1
    MIN_GC_RATIO_BEFORE_FAILURE = 1.1

    def __init__(
        self,
        starting_id: typing.Optional[CID] = None,
        others_dict: dict = None,
    ) -> None:
        """
        Args:
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
        store = HamtMemoryStore()

        if starting_id:
            # Load HAMT from store using given id
            self.hamt = load(store, starting_id)
        else:
            # Create HAMT from scratch with sensible default options
            self.hamt = create(
                store, options={"bit_width": 8, "bucket_size": 5, "hash_alg": 0x12}
            )

        self.others_dict = others_dict if others_dict is not None else {}
        self._system_ram = psutil.virtual_memory().total

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
            # lock needs name so dask knows to recognize it across threads
            with Lock("hamt-write"):
                self.hamt = self.hamt.set(self.SEP.join(key_path), value)
                # Now check if the percent of system RAM used by the store's mapping exceeds a 10% threshold
                percent_ram_used_by_mapping = (
                    self.hamt.store.num_bytes_in_mapping / self._system_ram
                )
                if percent_ram_used_by_mapping > self.MAX_PERCENT_OF_RAM_FOR_MAPPING:
                    # If it does exceed the threshold, next determine if the store's mapping is larger
                    # than the HAMT by a 10% margin.
                    # here, we use number of IDs in the HAMT as a proxy for the HAMT's size
                    num_ids_in_hamt = sum(1 for _ in self.hamt.ids())
                    if (
                        len(self.hamt.store.mapping)
                        > self.MIN_GC_RATIO_BEFORE_FAILURE * num_ids_in_hamt
                    ):
                        # If both criteria are met, we can save signficant RAM by GCing, so do so.
                        self.hamt.store.garbage_collect_mapping(self.hamt)
                    else:
                        # If the first criterium is met but not the second, then the HAMT itself is too large to comfortably
                        # fit in memory, and we abort the parse.
                        raise RuntimeError(
                            f"HAMT mapping is taking up more than {self.MAX_PERCENT_OF_RAM_FOR_MAPPING} of system RAM and gc won't help much, abort"
                        )
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
            if isinstance(id, cbor2.CBORTag):
                id = CID.decode(id.value[1:]).set(base="base32")
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
