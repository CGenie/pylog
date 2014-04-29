class LazyWrapper(object):
    def __init__(self, instance, methods=[]):
        self.__instance = instance

        for method_name in methods:
            setattr(self, method_name, self.__lazy_call(method_name))

    def __lazy_call(self, method_name):
        def call(*args, **kwargs):
            return lambda: getattr(self.__instance, method_name)(*args, **kwargs)

        return call
