# class MyClass():
# 	def __init__(self):
# 	   self._internal_var = 10
#
# 	def _internal_method(self):
# 	   print("Internal Mthod")
#
# obj = MyClass()
# print(obj._internal_var)  #output is 10
# obj._internal_method()


class MyClass():
	def __init__(self):
	   self.__mangled_var = 10

	def __mangled_method(self):
	   print("Mangled Method")

obj = MyClass()
print(obj._MyClass__mangled_var)  #output  10
obj._MyClass__mangled_method()   #output  Mangled Method
