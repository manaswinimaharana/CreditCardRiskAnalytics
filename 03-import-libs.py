# Let's introduce you to a few Standard Python Libs

"""`NumPy` is the fundamental package for scientific computing with Python. It contains among other things:

* a powerful N-dimensional array object
* sophisticated (broadcasting) functions
* tools for integrating C/C++ and Fortran code
* useful linear algebra, Fourier transform, and random number capabilities
"""
import numpy as np

from numpy import sqrt

def my_sqrt(x):
    return np.sqrt(x)

print(np.random.randn(6,4))

#Sample matrix multiplication 
a = [[1, 0], [0, 1]]
b = [[4, 1], [2, 2]]

np.matmul(a, b)


"""pandas is an open source, BSD-licensed 
library providing high-performance,
easy-to-use data structures and data analysis
tools for the Python programming language."""

import pandas as pd 
dates = pd.date_range('20130101', periods=6)
df = pd.DataFrame(np.random.randn(6,4), index=dates, columns=list('ABCD'))
df.head
df.describe()