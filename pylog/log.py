import amqp
import json
import pyes

from .settings import settings


class PyLog(object):
    def __init__(self, log_name):
        self.log_name = log_name

        self.es_setup()
        self.amqp_setup()

    def info(self, msg):
        self.log('INFO', msg)

    def error(self, msg):
        self.log('ERROR', msg)

    def log(self, severity, msg):
        index_data = json.dumps({
            'index': {
                '_index': settings.ES['index'],
                '_type': self.log_name,
            }
        })

        msg = json.dumps({
            self.log_name: {
                'severity': severity,
                'msg': msg,
            },
        })

        self.amqp_channel.basic_publish(
            amqp.Message(
                '\n'.join([index_data, msg, ''])),  # trailing newline added
            exchange=self.amqp_exchange_name)

    # RabbitMQ-specific code
    @property
    def amqp_exchange_name(self):
        return 'pylog.%s' % self.log_name

    @property
    def amqp_queue_name(self):
        return 'pylog.%s' % self.log_name

    amqp_exchange_properties = {
        'durable': True,
        'type': 'direct',
        'auto_delete': False
    }
    amqp_queue_properties = {
        'durable': True,
        'auto_delete': True,
    }

    def amqp_setup(self):
        self.amqp_conn = amqp.connection.Connection(**settings.AMQP)
        self.amqp_channel = amqp.Channel(self.amqp_conn)
        self.amqp_exchange = self.amqp_channel.exchange_declare(
            exchange=self.amqp_exchange_name,
            type=self.amqp_exchange_properties['type'],
            durable=self.amqp_exchange_properties['durable'],
            auto_delete=self.amqp_exchange_properties['auto_delete'])
        self.amqp_queue = self.amqp_channel.queue_declare(
            self.amqp_queue_name,
            durable=self.amqp_queue_properties['durable'],
            auto_delete=self.amqp_queue_properties['auto_delete']
        )
        self.amqp_channel.queue_bind(
            self.amqp_queue_name,
            exchange=self.amqp_exchange_name)

    # ElasticSearch-specific code
    @property
    def mapping(self):
        """
        Mapping for ElasticSearch index.
        """
        return {
            'log_name': {
                'index': 'analyzed',
                'store': 'yes',
                'type': 'string',
            },
            'severity': {
                'index': 'analyzed',
                'store': 'yes',
                'type': 'string',
            },
            '_timestamp': {
                'enabled': True,
            }
        }

    @property
    def river_name(self):
        return 'pylog_%s' % self.log_name

    @property
    def river_data(self):
        return {
            'host': settings.AMQP['host'],
            'port': settings.AMQP['port'],

            'user': settings.AMQP['userid'],
            'password': settings.AMQP['password'],

            'vhost': settings.AMQP['virtual_host'],

            'exchange': self.amqp_exchange_name,
            'exchange_type': self.amqp_exchange_properties['type'],
            'exchange_durable': self.amqp_exchange_properties['durable'],

            'routing_key': '',

            'queue': self.amqp_queue_name,
            'queue_durable': self.amqp_queue_properties['durable'],
            'queue_auto_delete': self.amqp_queue_properties['auto_delete'],
        }

    @property
    def es(self):
        return pyes.ES('%s:%s' % (settings.ES['host'], settings.ES['port']))

    def es_setup(self):
        es = self.es

        try:
            es.indices.create_index(settings.ES['index'])
        except pyes.exceptions.IndexAlreadyExistsException:
            pass

        es.indices.put_mapping(
            self.log_name,
            {'properties': self.mapping},
            settings.ES['index'])

        es.create_river(
            pyes.RabbitMQRiver(**self.river_data),
            river_name=self.river_name)
