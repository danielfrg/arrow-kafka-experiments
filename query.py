# %%

import pyarrow as pa
import datafusion

F = datafusion.functions

# %%

ctx = datafusion.ExecutionContext()

# %%

ctx.register_parquet("flights", "./data/sample.parquet")

# %%

query = ctx.sql("SELECT SUM(dep_delay) FROM flights")

df = query.collect()
# print(df)

# %%

table = pa.Table.from_batches(df)
print(table.to_pandas())

# %%
