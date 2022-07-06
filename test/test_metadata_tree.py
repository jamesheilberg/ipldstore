from ipldstore import get_ipfs_mapper
import ipfshttpclient

def tree_satisfies_limits(cid_or_dict, limit):
    if isinstance(cid_or_dict, str):
        ipfs = ipfshttpclient.connect()
        d = ipfs.dag.get(cid_or_dict).as_json()
    else:
        d = cid_or_dict
    if len(d) > limit:
        return False
    for k in d:
        if isinstance(d[k], dict):
            if "/" in d[k] and not tree_satisfies_limits(d[k]["/"], limit):
                return False
            elif not tree_satisfies_limits(d[k], limit):
                return False
    return True


ORIGINAL_DICT = {
    '0': 0,
    '1': 1,
    '2': 2,
    '3': 3,
    '4': 4,
    '5': {
        '10': 10,
        '11': 11,
        '12': 12,
        '13': 13,
        '14': 14,
        '15': 15,
        '16': 16,
        '17': 17,
        "18": 18,
        "19": 19},
    '6': 6,
    '7': 7}

LIMIT = 3

def test_make_tree():
    m = get_ipfs_mapper(max_nodes_per_level=LIMIT)
    m._mapping = ORIGINAL_DICT

    h = str(m.freeze())
    assert tree_satisfies_limits(h, LIMIT)

    m.clear()
    m.set_root(h)
    assert m._mapping == ORIGINAL_DICT
