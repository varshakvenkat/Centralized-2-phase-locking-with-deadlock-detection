import Pyro4
import sqlite3
import argparse
import threading

updates = dict()

@Pyro4.expose
class DataManager(object):
    
    def read(self, data_item, transaction_id):
        ''' Read the data item from the database '''
        global site_name
        connection = sqlite3.connect(site_name+'.db')
        cursor = connection.execute("SELECT val from DDB WHERE data_item='"+data_item+"'")
        connection.commit()
        print('Operation: READ \t Site: ', site_name, ' \t Transaction ID: ', transaction_id, '\t Data item: ', data_item, )
        return cursor.fetchone()[0]

    def write(self, data_item, value, site, transaction_id):
        ''' Write the value in temporary memory to allow rollback '''
        global updates
        if site not in updates:
            updates[site] = dict()
        updates[site][data_item] = value
        print('Operation: WRITE \t Site: ', site, ' \t Transaction ID: ', transaction_id, '\t Data item: ', data_item, )

    def commit(self, site, transaction_id):
        ''' Write the value from temporary memory to database '''
        global updates, site_name
        print('Operation: COMMIT \t Site: ', site, ' \t Transaction ID: ', transaction_id)
        connection = sqlite3.connect(site_name+'.db')
        if site in updates:
            for data_item in list(updates[site]):
                value = updates[site][data_item]
                connection.execute("UPDATE DDB set val ="+str(value)+" WHERE data_item='"+data_item+"'")
        connection.commit()
        return

    def abort(self, site, transaction_id):
        ''' Clear temporary memory '''
        global updates
        print('Operation: ABORT \t Site: ', site, ' \t Transaction ID: ', transaction_id)
        if site in updates:
            del updates[site]

def start_server():
    global site_name
    daemon = Pyro4.Daemon()
    ns = Pyro4.locateNS()
    uri = daemon.register(DataManager)
    ns.register('data_manager'+site_name, str(uri))
    
    #Run the daemon as a thread to avoid blocking
    threading.Thread(target=daemon.requestLoop).start()

def setup_db():
    global site_name
    
    connection = sqlite3.connect(site_name+'.db')
    connection.execute('CREATE TABLE IF NOT EXISTS DDB (data_item TEXT, val INTEGER)')
    connection.execute('DELETE FROM DDB')

    #Populate initial values in the database
    for i in range(1000):
        connection.execute("INSERT INTO DDB values('x"+ str(i) +"', 10)")
    connection.commit()


if __name__ == '__main__':
    global site_name
    parser = argparse.ArgumentParser(description="Data Manager for a distributed database",
                                    formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument("-n", "--sitename", help="site name")
    args = parser.parse_args()
    site_name = args.sitename
    
    setup_db()
    start_server()
exit