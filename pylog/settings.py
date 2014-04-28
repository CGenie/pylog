# settings object for pylog


# these settings have the same format as amqp.connection call
# http://amqp.readthedocs.org/en/latest/reference/amqp.connection.html
DEFAULT_AMQP_SETTINGS = {
    'host': 'localhost',
    'port': 5672,
    'userid': 'guest',
    'password': 'guest',
    'virtual_host': '/',
}

DEFAULT_ES_SETTINGS = {
    'host': 'localhost',
    'port': 9200,
    'index': 'pylogger',
}


class Settings(object):
    def __init__(self, AMQP=DEFAULT_AMQP_SETTINGS, ES=DEFAULT_ES_SETTINGS):
        self.AMQP = AMQP
        self.ES = ES


settings = Settings()