import dag_cbor
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


def get_cbor_dag_hash(obj):
    ob_cbor = dag_cbor.encode(obj)
    ob_cbor_hash = multihash.get("sha2-256").digest(ob_cbor)
    return CID("base32", 1, "dag-cbor", ob_cbor_hash)


class HamtIPFSStore:
    def __init__(self):
        self.mapping = {}

    def save(self, obj):
        cid = get_cbor_dag_hash(obj)
        self.mapping[cid] = obj
        return cid

    def load(self, id):
        try:
            return self.mapping[id]
        except KeyError:
            res = requests.post(
                "http://localhost:5001/api/v0/block/get", params={"arg": str(id)}
            )
            res.raise_for_status()
            obj = dag_cbor.decode(res.content)
            self.mapping[id] = obj
            return obj

    def is_equal(self, id1: CID, id2: CID):
        return str(id1) == str(id2)

    def is_link(self, obj: CID):
        return isinstance(obj, CID) and obj.codec.name == "dag-cbor"


class HamtWrapper:
    SEP = "/"

    def __init__(self, store=HamtIPFSStore(), starting_id=None, others_dict=None):
        Hamt.register_hasher(
            0x12, 32, lambda x: hashlib.sha256(x.encode("utf-8")).digest()
        )

        if starting_id:
            self.hamt = load(store, starting_id)
        else:
            self.hamt = create(
                store, options={"bit_width": 8, "bucket_size": 5, "hash_alg": 0x12}
            )

        self.others_dict = others_dict if others_dict is not None else {}

    def get(self, key_path):
        try:
            return self.hamt.get(self.SEP.join(key_path))
        except KeyError:
            return get_recursive(self.others_dict, key_path)

    def set(self, key_path, value):
        if isinstance(value, CID) and value.codec.name == "dag-pb":
            try:
                lock = Lock("x")
                lock.acquire()
            except ValueError:
                lock = None
            self.hamt = self.hamt.set(self.SEP.join(key_path), value)
            if lock is not None:
                lock.release()
        else:
            set_recursive(self.others_dict, key_path, value)

    def iter_all(self):
        yield from self._iter_nested("", self.others_dict)
        yield from self.hamt.keys()

    def _iter_nested(self, prefix: str, mapping):
        for key, value in mapping.items():
            key_parts = key.split(self.SEP)
            if key_parts[-1] in inline_objects:
                yield prefix + key
            elif isinstance(value, dict):
                yield from self._iter_nested(prefix + key + self.SEP, value)
            else:
                yield prefix + key

    def to_dict(self):
        for id in self.hamt.ids():
            obj = self.hamt.store.mapping[id]
            obj = dag_cbor.encode(obj)
            res = requests.post(
                "http://localhost:5001/api/v0/block/put?cid-codec=dag-cbor",
                files={"dummy": obj},
            )
            res.raise_for_status()
        return {**self.others_dict, "hamt": self.hamt.id}

    @staticmethod
    def from_dict(d):
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
