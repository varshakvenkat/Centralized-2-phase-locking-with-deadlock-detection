import random
import sys
import os

no_transactions = 100

for no_variables in [10, 50, 100, 500, 1000]:
    os.mkdir('Transactions/'+str(no_variables))
    for site_name in ['site1', 'site2', 'site3', 'site4', 'site5']:
        file_object = open('Transactions/'+str(no_variables)+ '/' + site_name + '.txt', "w")
        for t in range(no_transactions):
            file_object.write('TRANSACTION\n')
            read = []
            unread = ['x'+str(i) for i in range(no_variables)]
            idx = random.randrange(len(unread))
            file_object.write('READ '+unread[idx]+'\n')
            read.append(unread[idx])
            del unread[idx]
            while True:
                choice = random.randrange(10)
                if choice < 3:
                    if len(unread) == 0:
                        continue
                    idx = random.randrange(len(unread))
                    file_object.write('READ '+unread[idx]+'\n')
                    read.append(unread[idx])
                    del unread[idx]
                elif choice < 6:
                    idx = random.randrange(len(read))
                    file_object.write('WRITE '+read[idx]+'\n')
                elif choice < 9:
                    idx1 = random.randrange(len(read))
                    idx2 = random.randrange(len(read))
                    val = random.randrange(10)
                    if random.randrange(2) == 0:
                        op = '+'
                    else:
                        op = '-'
                    file_object.write(read[idx1]+' = '+read[idx2]+' '+op+' '+str(val)+'\n')
                else:
                    file_object.write('COMMIT\n')
                    break
