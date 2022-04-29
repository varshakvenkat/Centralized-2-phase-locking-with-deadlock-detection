import time
import Pyro4
import argparse
import threading

class Transaction:
    actions = []
    current_action = 0
    variables = dict()
    locks = dict()

    def __init__(self, actions, id):
        self.actions = [x.split() for x in actions]
        self.id = id
        self.current_action = 0
        self.variables = dict()
        self.locks = dict()

    def add_lock(self, data_item, lock_type):
        self.locks[data_item] = lock_type

    def restart_transaction(self):
        self.current_action = 0
        self.variables.clear()
        self.locks.clear()

    def get_action_type(self):
        action = self.actions[self.current_action]
        if action[0] in ['READ', 'WRITE', 'COMMIT', 'DELAY']:
            return action[0]
        return 'OPERATION'

    def get_action(self):
        return self.actions[self.current_action]

    def lock_present(self, data_item, ltype):
        if ltype == 'SHARED':
            if data_item in self.locks:
                return True
            return False
        else:
            if data_item not in self.locks or self.locks[data_item] == 'SHARED':
                return False
            return True

    def action_completed(self):
        self.current_action += 1

    def perform_operation(self):
        action = self.get_action()
        target = action[0]
        op1 = self.variables[action[2]]
        op2 = int(action[4])
        if action[3] == '+':
            self.variables[target] = op1 + op2
        else:
            self.variables[target] = op1 - op2

    def get_value(self, data_item):
        return self.variables[data_item]

    def set_value(self, data_item, value):
        self.variables[data_item] = value

def fetchTransaction():
    global file_object, no_of_transactions
    no_of_transactions += 1
    line = file_object.readline().replace('\n','')  
    actions = []
    while line != '' and line!='TRANSACTION':
        actions.append(line)
        line = file_object.readline().replace('\n','')  
    if len(actions) > 0:
        return Transaction(actions, no_of_transactions)
    else:
        return None

def perform_read(transaction):
    global lock_manager, site_name, lvalue
    action = transaction.get_action()
    data_item = action[1]
    if not transaction.lock_present(data_item, 'SHARED'):
        lvalue = 'requested'
        lock_manager.request_lock(data_item, 'SHARED', site_name)
        print('Requested lock for ', data_item)
        while lvalue == 'requested':
            pass
        if lvalue == 'abort':
            return 'abort'
        else:
            print('Lock granted for ', data_item)
            transaction.add_lock(data_item, 'SHARED')
    value = data_manager[site_name].read(data_item, transaction.id)
    
    transaction.set_value(data_item, value)
    return 'success'
        
def perform_write(transaction):
    global lock_manager, site_name, lvalue
    action = transaction.get_action()
    data_item = action[1]
    value = transaction.get_value(data_item)
    if not transaction.lock_present(data_item, 'EXCLUSIVE'):
        lvalue = 'requested'
        lock_manager.request_lock(data_item, 'EXCLUSIVE', site_name)
        print('Requested lock for ', data_item)
        while lvalue == 'requested':
            pass
        if lvalue == 'abort':
            return 'abort'
        else:
            print('Lock granted for ', data_item)
            transaction.add_lock(data_item, 'EXCLUSIVE')
    for site in sites:
        data_manager[site].write(data_item, value, site_name, transaction.id)
    return 'success'

def perform_operation(transaction):
    transaction.perform_operation()
    return 'success'

def perform_commit(transaction):
    ''' Commit at all locations '''
    global lock_manager, site_name
    for site in sites:
        data_manager[site].commit(site_name, transaction.id)
    lock_manager.release_lock(transaction.locks, site_name)
    print('Commited transaction ', transaction.id)
    return 'success'

def perform_abort(transaction):
    print('Aborting transaction ', transaction.id)
    global lock_manager, sites
    lock_manager.release_lock(transaction.locks, site_name)
    for site in sites:
        data_manager[site].abort(site_name, transaction.id)
    transaction.restart_transaction()
    time.sleep(0.5)

def runTransaction(transaction):
    print('Running transaction ', transaction.id)
    while True:
        action_type = transaction.get_action_type()
        if action_type == 'READ':
           status = perform_read(transaction)
        elif action_type == 'WRITE':
            status = perform_write(transaction)
        elif action_type == 'OPERATION':
            status = perform_operation(transaction)
        elif action_type == 'DELAY':
            time.sleep(0.2)
        else:
            status = perform_commit(transaction)
            return status
        if status == 'abort':
            return status
        else:
            transaction.action_completed()

def executeTransactions():
    global end_of_transactions, site_name
    transaction = fetchTransaction()
    while transaction:
        status = runTransaction(transaction)
        if status == 'abort':
            perform_abort(transaction)
        else:
            transaction = fetchTransaction()
    end_of_transactions = True

def is_eot():
    ''' Check if all transactions have been processed '''
    global end_of_transactions
    return not end_of_transactions


@Pyro4.expose
class Server(object):
    @Pyro4.oneway
    def lock_response(self, response):
        ''' Receive response from lock manager '''
        global lvalue
        lvalue = response


def start_server():
    global site_name
    daemon = Pyro4.Daemon()
    ns = Pyro4.locateNS()
    uri = daemon.register(Server)
    ns.register('transaction_manager'+site_name, str(uri))

    #Run the daemon as a thread to avoid blocking
    threading.Thread(target=daemon.requestLoop, args=(is_eot,)).start()


if __name__ == '__main__':
    global site_name, sites, file_object, no_of_transactions, data_manager, end_of_transactions, lock_manager
    parser = argparse.ArgumentParser(description="Transaction Manager for a distributed database",
                                    formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument("-n", "--sitename", help="site name")
    parser.add_argument("-l", "--transaction_location", help="transaction location")
    parser.add_argument("-ns", "--no_sites", help="no. of sites")
    args = parser.parse_args()

    site_name = args.sitename
    no_sites = int(args.no_sites)
    sites = ['site'+str(i) for i in range(1, no_sites+1)]
    file_object = open('Transactions/' + args.transaction_location + '/' + site_name + '.txt', "r")
    file_object.readline()
    no_of_transactions = 0
    end_of_transactions = False
    data_manager = dict()
    start_server()
    lock_manager = Pyro4.Proxy(f"PYRONAME:lock_manager")
    for site in sites:
        data_manager[site] = Pyro4.Proxy(f"PYRONAME:data_manager"+site)

    executeTransactions()