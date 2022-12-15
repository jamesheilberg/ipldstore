import cbor2
import dag_cbor
import mmh3
import requests
from dask.distributed import Lock
from multiformats import CID
from py_hamt.hamt import Hamt, create, load

inline_objects = [
    ".zarray",
    ".zgroup",
    ".zmetadata",
    ".zattrs",
]


class HamtIPFSStore:
    def save(self, obj):
        obj = dag_cbor.encode(obj)
        res = requests.post(
            "http://localhost:5001/api/v0/dag/put",
            params={"store-codec": "dag-cbor", "input-codec": "dag-cbor", "pin": False},
            files={"dummy": obj},
        )
        res.raise_for_status()
        return CID.decode(res.json()["Cid"]["/"])

    def load(self, id):
        if isinstance(id, cbor2.CBORTag):
            id = CID.decode(id.value[1:])
        res = requests.post(
            "http://localhost:5001/api/v0/block/get", params={"arg": str(id)}
        )
        res.raise_for_status()
        return dag_cbor.decode(res.content)

    def is_equal(self, id1: CID, id2: CID):
        return str(id1) == str(id2)

    def is_link(self, obj: CID):
        return isinstance(obj, CID) and obj.codec.name == "dag-cbor"


class HamtWrapper:
    SEP = "/"

    def __init__(self, store=HamtIPFSStore(), starting_id=None, others_dict=None):
        Hamt.register_hasher(
            0x23, 4, lambda x: mmh3.hash(x, signed=False).to_bytes(4, byteorder="big")
        )
        if starting_id:
            self.hamt = load(store, starting_id)
        else:
            self.hamt = create(store)

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
        return {**self.others_dict, "hamt": self.hamt.id}

    @staticmethod
    def from_dict(d):
        others_dict = d
        hamt_id = others_dict["hamt"]
        del others_dict["hamt"]
        return HamtWrapper(
            HamtIPFSStore(), starting_id=hamt_id, others_dict=others_dict
        )


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
