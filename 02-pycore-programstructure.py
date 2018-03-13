# Named Functions 

#Simple Functions
#Define a function 
def say_hello():
  print ("hello")

#calling the function 
say_hello()

#Define the same function with a input param
def say_hello(name):
  print ("Hello " + name)
  print("hi")
say_hello("Fang")

#Function with Return Type 
def mySquare(number):
    return  number*number
print(mySquare(10))

#Pass by reference
def doit(somefunction, input):
  return somefunction(input)


print(doit(say_hello,"fang"))
print(doit(mySquare,10))

#Single notation functions 
def mul(val): return val*2 

# Anonymous functions (Lambda)
# name-less, created when needed 
# usually used in conjunction with filter(), map() and reduce()
# syntax is very simple 
# lambda argument_list: expression 
#e.g. 

f = lambda x,y,z,t: x + y
print(f(1,1,1,1))

# Map Function example 
# syntax : map (func,seq)

input=[36.5,37.5,39]

#using loop
input=[36.5,37.5,39]
for value in input: 
  print(mySquare(value))

#Same using map - just different way to acheive it  
F = map(mySquare,input)
for t in F:
  print (t)

# Map with lamdba function 
input1 = [(36.5,786),
         (37.5,67),
         (39,78)]
input12 = [(36.5,786),
         (37.5,67),
         (39,78)]
SquareVal = map(lambda x,y: x[0]*y[1], input,input12)

for value in list(SquareVal):
  print value

#Filter with lambda
#The function filter(function, list) offers an elegant way 
#to filter out all the elements of a list,
#for which the function function returns True. 

fib = [1,2,3,4,13,1,3,34]
result = filter(lambda x: x>3, fib)
print(list(result))

# reduce() had been dropped from the core of Python
# when migrating to Python 3
#The function reduce(func, seq) 
#continually applies the function func() to the sequence seq. 
#It returns a single value. 

import functools
#reduce with lambda
#The function reduce(func, seq) continually applies the 
#function func() to the sequence seq.
#It returns a single value. 
A=functools.reduce(lambda x,y: x+y, [47,11,42,13])
print (A)