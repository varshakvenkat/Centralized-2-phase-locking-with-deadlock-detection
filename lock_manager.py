import Pyro4
import threading
from queue import Queue
import sys

class Lock:
    def __init__(self):
        self.lock_type = None
        self.owners = []
        self.queue = []


data_items = ['x'+str(i) for i in range(1000)]
locks = dict()

for data_item in data_items:
    locks[data_item] = Lock()

@Pyro4.expose
class Lock_manager(object):
    @Pyro4.oneway
    def request_lock(self, data_item, action_type, site):
        print('Recieved request for data item:', data_item, '\t action: ' , action_type,'\t site: ', site)
        global locks, transaction_manager, no_rollbacks
        lock = locks[data_item]
        if lock.lock_type == None:
            lock.lock_type = action_type
            lock.owners.append(site)
            print('Granting lock to ', site)
            transaction_manager[site].lock_response('granted')
        elif lock.lock_type == 'SHARED' and action_type == 'SHARED':
            lock.owners.append(site)
            transaction_manager[site].lock_response('granted')
            print('Granting lock to ', site)
        elif lock.lock_type == 'SHARED' and lock.owners == [site]:
            lock.lock_type = action_type
            transaction_manager[site].lock_response('granted')
            print('Granting lock to ', site)
        else:
            lock.queue.append([action_type, site])
            if self.deadlock_possible(site):
                print('Deadlock detected. Aborting site: ', site)
                del lock.queue[-1]
                transaction_manager[site].lock_response('abort')
                no_rollbacks+=1
                print('Abort count:', no_rollbacks)
            else:
                print('Queued request for data item:', data_item, '\t action: ' , action_type,'\t site: ', site)


    def deadlock_possible(self, start_site):
        global locks, sites
        to_visit = Queue()
        to_visit.put(start_site)

        visited = dict()
        for site in sites:
            visited[site] = False
        visited[start_site] = True

        while not to_visit.empty():
            site = to_visit.get()
            for data_item in locks:
                lock = locks[data_item]
                if site in lock.owners:
                    for neighbor in lock.queue:
                        if neighbor[1] == site:
                            continue
                        if visited[neighbor[1]]:
                            return True
                        visited[neighbor[1]] = True
                        to_visit.put(neighbor[1])
        return False


    def distribute_lock(self, data_item, lock):
        global transaction_manager

        if len(lock.queue) == 0:
            return

        lock.lock_type = lock.queue[0][0]
        lock.owners = [lock.queue[0][1]]
        del lock.queue[0]
        print('Granting lock to ', lock.owners[0])
        transaction_manager[lock.owners[0]].lock_response('granted')

        exclusive_queue = []
        if lock.lock_type == 'SHARED':
            for i in range(len(lock.queue)):
                if lock.queue[i][0] == 'SHARED':
                    print('Granting lock to ', lock.queue[i][1])
                    transaction_manager[lock.queue[i][1]].lock_response('granted')
                    lock.owners.append(lock.queue[i][1])
                else:
                    exclusive_queue.append(lock.queue[i])
        lock.queue = exclusive_queue


    def release_lock(self, locks_held, site):
        print('Locks released by ', site)
        global locks, transaction_manager
        for data_item in locks_held:
            lock = locks[data_item]
            lock.owners.remove(site)
            lock.queue = list(filter(lambda x: x[1]!=site, lock.queue))

            if len(lock.owners) == 0:
                lock.lock_type = None
                self.distribute_lock(data_item, lock)
            elif len(lock.owners) == 1:
                for (i,each) in enumerate(lock.queue):
                    if each[1] ==  lock.owners[0]:
                        lock.lock_type = each[0]
                        transaction_manager[lock.queue[i][1]].lock_response('granted')
                        del lock.queue[i]
                        break


def start_server():
    daemon = Pyro4.Daemon()
    ns = Pyro4.locateNS()
    uri = daemon.register(Lock_manager)
    ns.register('lock_manager', str(uri))

    #Run the daemon as a thread to avoid blocking
    threading.Thread(target=daemon.requestLoop).start()


if __name__ == '__main__':
    global no_sites, sites, transaction_manager, no_rollbacks
    no_sites = int(sys.argv[1])
    sites = ['site'+str(i) for i in range(1, no_sites+1)]
    transaction_manager = dict()
    no_rollbacks = 0
    for site in sites:
        transaction_manager[site] = Pyro4.Proxy(f"PYRONAME:transaction_manager"+site)
    start_server()