"""
Implementation of a MutableMapping based on IPLD data structures.
"""



from io import BufferedIOBase
from collections.abc import MutableMapping
import sys
from dataclasses import dataclass
from typing import Optional, Callable, Any, TypeVar, Union, Iterator, overload, List, Dict
import json

from multiformats import CID
import dag_cbor
from cbor2 import CBORTag
from numcodecs.compat import ensure_bytes  # type: ignore

from .contentstore import ContentAddressableStore, IPFSStore, MappingCAStore
from .utils import StreamLike

if sys.version_info >= (3, 9):
    MutableMappingT = MutableMapping
    MutableMappingSB = MutableMapping[str, bytes]
else:
    from typing import MutableMapping as MutableMappingT
    MutableMappingSB = MutableMapping

@dataclass
class InlineCodec:
    decoder: Callable[[bytes], Any]
    encoder: Callable[[Any], bytes]


def json_dumps_bytes(obj: Any) -> bytes:
    return json.dumps(obj).encode("utf-8")


json_inline_codec = InlineCodec(json.loads, json_dumps_bytes)

inline_objects = {
    ".zarray": json_inline_codec,
    ".zgroup": json_inline_codec,
    ".zmetadata": json_inline_codec,
    ".zattrs": json_inline_codec,
}


class IPLDStore(MutableMappingSB):
    def __init__(self, castore: Optional[ContentAddressableStore] = None, sep: str = "/", should_async_get: bool = True):
        self._mapping: Dict[str, Union[bytes, dag_cbor.encoding.EncodableType]] = {}
        self._store = castore or MappingCAStore()
        if isinstance(self._store, IPFSStore) and should_async_get:
            # Monkey patch zarr to use the async get of multiple chunks
            def storage_getitems(kv_self, keys, on_error="omit"):
                return kv_self._mutable_mapping.getitems(keys)
            import zarr
            zarr.KVStore.getitems = storage_getitems
        self.sep = sep
        self.root_cid: Optional[CID] = None

    def getitems(self, keys: List[str]) -> Dict[str, bytes]:
        if not isinstance(self._store, IPFSStore):
            raise NotImplementedError("Multiget of keys only supported for IPFSStore")
        cid_to_key_map = {}
        key_to_bytes_map = {}
        to_async_get = []
        for key in keys:
            key_parts = key.split(self.sep)
            get_value = get_recursive(self._mapping, key_parts)
            try:
                # First see if this is a special key that doesn't need to be handled by the store
                inline_codec = inline_objects[key_parts[-1]]
                key_to_bytes_map[key] = inline_codec.encoder(get_value)
            except KeyError:
                # If it isn't, the key is an IPFS CID and needs to be passed to the store to be handled asynchronously
                if isinstance(get_value, CBORTag):
                    get_value = CID.decode(get_value.value[1:])
                assert isinstance(get_value, CID)
                cid_to_key_map[get_value] = key
                to_async_get.append(get_value)
        # Get the bytes for all CIDs asynchronously
        cid_to_bytes_map = self._store.getitems(to_async_get)
        for cid, key in cid_to_key_map.items():
            key_to_bytes_map[key] = cid_to_bytes_map[cid]
        return key_to_bytes_map

    def __getitem__(self, key: str) -> bytes:
        key_parts = key.split(self.sep)
        get_value = get_recursive(self._mapping, key_parts)
        try:
            inline_codec = inline_objects[key_parts[-1]]
        except KeyError:
            if isinstance(get_value, CBORTag):
                get_value = CID.decode(get_value.value[1:])
            assert isinstance(get_value, CID)
            res = self._store.get(get_value)
            assert isinstance(res, bytes)
            return res
        else:
            return inline_codec.encoder(get_value)

    def __setitem__(self, key: str, value: bytes) -> None:
        value = ensure_bytes(value)
        key_parts = key.split(self.sep)

        try:
            inline_codec = inline_objects[key_parts[-1]]
        except KeyError:
            cid = self._store.put(value)
            set_value = cid
        else:
            set_value = inline_codec.decoder(value)

        self.root_cid = None
        set_recursive(self._mapping, key_parts, set_value)

    def __delitem__(self, key: str) -> None:
        key_parts = key.split(self.sep)
        del_recursive(self._mapping, key_parts)

    def __iter__(self) -> Iterator[str]:
        return self._iter_nested("", self._mapping)

    def _iter_nested(self, prefix: str, mapping: Dict[str, Union[bytes, dag_cbor.encoding.EncodableType]]) -> Iterator[str]:
        for key, value in mapping.items():
            key_parts = key.split(self.sep)
            if key_parts[-1] in inline_objects:
                yield prefix + key
            elif isinstance(value, dict):
                yield from self._iter_nested(prefix + key + self.sep, value)
            else:
                yield prefix + key

    def __len__(self) -> int:
        return len(list(iter(self)))

    def freeze(self) -> CID:
        """
            Store current version and return the corresponding root cid.
        """
        if self.root_cid is None:
            self.root_cid = self._store.put(self._mapping)
        return self.root_cid

    def clear(self) -> None:
        self.root_cid = None
        self._mapping = {}

    @overload
    def to_car(self, stream: BufferedIOBase) -> int:
        ...

    @overload
    def to_car(self, stream: None = None) -> bytes:
        ...

    def to_car(self, stream: Optional[BufferedIOBase] = None) -> Union[int, bytes]:
        return self._store.to_car(self.freeze(), stream)

    def import_car(self, stream: StreamLike) -> None:
        roots = self._store.import_car(stream)
        if len(roots) != 1:
            raise ValueError(f"CAR must have a single root, the given CAR has {len(roots)} roots!")
        self.set_root(roots[0])

    @classmethod
    def from_car(cls, stream: StreamLike) -> "IPLDStore":
        instance = cls()
        instance.import_car(stream)
        return instance

    def set_root(self, cid: CID) -> None:
        if isinstance(cid, str):
            cid = CID.decode(cid)
        assert cid in self._store
        self.root_cid = cid
        self._mapping = self._store.get(cid)  # type: ignore


_T = TypeVar("_T")
_V = TypeVar("_V")

RecursiveMapping = MutableMappingT[_T, Union[_V, "RecursiveMapping"]]  # type: ignore


def set_recursive(obj: RecursiveMapping[_T, _V], path: List[_T], value: _V) -> None:
    assert len(path) >= 1
    if len(path) == 1:
        obj[path[0]] = value
    else:
        set_recursive(obj.setdefault(path[0], {}), path[1:], value)  # type: ignore


def get_recursive(obj: RecursiveMapping[_T, _V], path: List[_T]) -> _V:
    assert len(path) >= 1
    if len(path) == 1:
        return obj[path[0]]
    else:
        return get_recursive(obj[path[0]], path[1:])  # type: ignore


def del_recursive(obj: MutableMappingT[_T, Any], path: List[_T]) -> None:
    assert len(path) >= 1
    if len(path) == 1:
        del obj[path[0]]
    else:
        del_recursive(obj[path[0]], path[1:])
        if len(obj[path[0]]) == 0:
            del obj[path[0]]
