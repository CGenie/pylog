import amqp
import datetime
from dateutil import tz as dateutil_tz
import json
import pyes

from .lazy import LazyWrapper
from .settings import settings


UTC = dateutil_tz.gettz('UTC')


# Log querying functionality
# This is closely bound to the PyLogBase.log method
class PyLogQuery(object):
    def __init__(self, index, document_type, es):
        self.index = index
        self.document_type = document_type
        self.es = es

    def query(self,
              sort='timestamp',
              start=0,
              size=20,
              severity=None,
              timestamp_from=None,
              timestamp_till=None):
        fltr = []

        if severity is not None:
            fltr.append(
                pyes.TermFilter(field='severity', value=severity)
            )

        if timestamp_from is not None:
            if isinstance(timestamp_from, datetime.datetime):
                timestamp_from = timestamp_from.isoformat()

            fltr.append(
                pyes.RangeFilter(
                    pyes.ESRangeOp(
                        'timestamp', 'gte', timestamp_from
                    )
                )
            )

        if timestamp_till is not None:
            if isinstance(timestamp_till, datetime.datetime):
                timestamp_till = timestamp_till.isoformat()

            fltr.append(
                pyes.RangeFilter(
                    pyes.ESRangeOp(
                        'timestamp', 'lte', timestamp_till
                    )
                )
            )

        f = None
        if fltr:
            f = pyes.ANDFilter(fltr)
        q = pyes.MatchAllQuery()

        s = pyes.Search(
            query=q,
            filter=f,
            start=start,
            size=size)

        return self.es.search(
            s,
            indices=[self.index],
            doc_types=[self.document_type])


class PyLog(object):
    def __init__(self, log_name):
        self.log_name = log_name

        self.es_setup()
        self.amqp_setup()

        self.query = PyLogQuery(
            self.es_index,
            self.es_document_type,
            self.es)

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
                'severity': severity.upper(),
                'msg': msg,
                'timestamp': datetime.datetime.now(tz=UTC).isoformat(),
                },
            })

        # TODO: this should be some generic method so that ES/RabbitMQ can
        #       be replaced later with something else
        bulk_msg = '\n'.join([index_data, msg, ''])  # trailing newline added

        print bulk_msg

        self.amqp_channel.basic_publish(
            amqp.Message(bulk_msg),
            exchange=self.amqp_exchange_name)

    # RabbitMQ-specific code
    @property
    def amqp_exchange_name(self):
        return 'pylog.%s' % self.es_document_type

    @property
    def amqp_queue_name(self):
        return 'pylog.%s' % self.es_document_type

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
        # TODO: initialize these variables in __init__
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
            'timestamp': {
                'index': 'analyzed',
                'store': 'yes',
                'type': 'date',
                'format': 'date_time',
            }
        }

    @property
    def river_name(self):
        return 'pylog_%s' % self.es_document_type

    @property
    def river_data(self):
        return {
            'name': self.river_name,
            'index_name': self.es_index,
            'index_type': self.es_document_type,

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

    @property
    def es_index(self):
        return settings.ES['index']

    @property
    def es_document_type(self):
        return self.log_name

    def es_setup(self):
        es = self.es

        try:
            es.indices.create_index(self.es_index)
        except pyes.exceptions.IndexAlreadyExistsException:
            pass

        es.indices.put_mapping(
            self.es_document_type,
            {'properties': self.mapping},
            self.es_index)

        es.create_river(
            pyes.RabbitMQRiver(**self.river_data),
            river_name=self.river_name)


class PyLogWithListCommit(PyLog):
    """
    This class collects log messages and creates a message upon
     calling commit() or lazy.commit().
    """
    def __init__(self, *args, **kwargs):
        super(PyLogWithListCommit, self).__init__(*args, **kwargs)

        self.log_messages = []

        self.lazy = LazyWrapper(self, ['commit', 'log', 'log_with_commit'])

    def info(self, msg):
        raise NotImplementedError

    def error(self, msg):
        raise NotImplementedError

    def log(self, msg):
        self.log_messages.append(msg)


    def commit(self, severity):
        super(PyLogWithListCommit, self).log(severity, self.log_messages)

        self.log_messages = []

    def log_with_commit(self, severity, msg):
        self.log(msg)
        self.commit(severity)


class PyLogWithDictCommit(PyLog):
    """
    Same as PyLogWithListCommit but creates a dict.
    """
    def __init__(self, *args, **kwargs):
        super(PyLogWithDictCommit, self).__init__(*args, **kwargs)

        self.log_messages = {}

        self.lazy = LazyWrapper(self, ['commit', 'log', 'log_with_commit'])

    def info(self, msg):
        raise NotImplementedError

    def error(self, msg):
        raise NotImplementedError

    def log(self, key, msg):
        self.log_messages[key] = msg

    def commit(self, severity):
        super(PyLogWithDictCommit, self).log(severity, self.log_messages)

        self.log_messages = {}

    def log_with_commit(self, severity, key, msg):
        self.log(key, msg)
        self.commit(severity)
