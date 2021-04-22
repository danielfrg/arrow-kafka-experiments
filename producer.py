# %%

import pyarrow as pa

# %%

# my_schema = pa.schema([("field0", pa.int32()), ("field1", t2), ("field2", t4), ("field3", t6)])

data = [
    pa.array([1, 2, 3, 4]),
    pa.array(["foo", "bar", "baz", None]),
    pa.array([True, None, False, True]),
]
