import pandas as pd

def group_iter(groupby):
    for g in groupby:
        yield g[1]
def mapply(groupby, func, not_indexed_same=False, num_procs=-1, drop_keys=False):
    if num_procs<1:
        import multiprocessing
        num_procs=multiprocessing.cpu_count()
    with ProcessPoolExecutor(num_procs) as p:
        out= list(p.map(func, group_iter(groupby)))
    if drop_keys:
        out=pd.concat(out, sort=False).reset_index(drop=True)
    else:
        keys = groupby.grouper._get_group_keys()
        out=groupby._wrap_applied_output(keys, out, not_indexed_same=not_indexed_same)
    return out

pd.core.groupby.generic.DataFrameGroupBy.mapply = mapply


