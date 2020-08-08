# it is probably best to just use threadpoolctl: https://github.com/joblib/threadpoolctl
def set_num_threads(n):
    try: 
        import mkl
        mkl.set_num_threads(n)
    except:
        pass
    
    n=str(n)
    import os
    os.environ['OPENBLAS_NUM_THREADS'] = n
    os.environ['NUMEXPR_NUM_THREADS'] = n
    os.environ['OMP_NUM_THREADS'] = n
    os.environ['MKL_NUM_THREADS'] = n

def reset_num_threads():
    try:
        import mkl
        mkl.set_num_threads(0)
    except:
        pass
    import os
    del os.environ['OPENBLAS_NUM_THREADS']
    del os.environ['NUMEXPR_NUM_THREADS']
    del os.environ['OMP_NUM_THREADS']
    del os.environ['MKL_NUM_THREADS']
