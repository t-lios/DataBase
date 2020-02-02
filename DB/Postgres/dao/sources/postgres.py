import psycopg2
import configparser

class Database:

    def __init__(self, url=None):
        print('init')
        self.conn = None
        self.cursor = None

        if url:
            print('postgres init 00')
            self.open(url)
        else:
            print('postgres init 01')
            self.url = self.loadConfig()
            self.open(self.url)

    def open(self, url):

        self.connInfo = 'host={} port={} dbname={} user={} password={}'
        self.connInfo = self.connInfo.format(self.url['host'],self.url['port'],self.url['dbname'],self.url['user'],self.url['password'])

        self.conn = psycopg2.connect(self.connInfo)
        self.cursor = self.conn.cursor()

        print('postgres connected')
        print('postgres pid:' + str(self.conn.get_backend_pid()))

    def close(self):
        if self.conn:
            self.conn.commit()
            self.cursor.close()
            self.conn.close()

    def commit(self):
        if self.conn:
            self.conn.commit()

    def rollback(self):
        if self.conn:
            self.conn.rollback()

    def query(self, sql):
        print('SQL:' + sql)
        self.cursor.execute(sql)
        rows = self.cursor.fetchall()

        return rows

    def loadConfig(self):
        configPath = '../config/pgconfig.cfg'

        config = configparser.ConfigParser()
        config.read(configPath)

        self.url = {}
        for section in config.sections():
            for key in config[section]:
                # print('key:' + key + ' value:' + config[section][key])
                self.url[key] = config[section][key]

        print(self.url)
        return self.url

    def state(self):
        print('<----- state ----->')
        print(self.url)
        print(self.conn)
        print('postgres pid:' + str(self.conn.get_backend_pid()))
        print(self.cursor)
