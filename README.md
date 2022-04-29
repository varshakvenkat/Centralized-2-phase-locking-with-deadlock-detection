# Centralized-2-phase-locking-with-deadlock-detection
This project is an implementation of centralized rigorous 2 phase locking to perform concurrency control and resolve deadlocks through transaction rollback. All locks are managed at a central site and all other sites contact the central site to acquire or release the locks. The scope of the project is restricted to handling read, write, add and subtract operations on SQLite database. The data is completely replicated at all sites.

## Steps to run on macOS:

### To install requirements:
```
make setup
```
### To run:
```
make run-2pl folder=<folder name> no_sites=<no. of sites>
```
Folder name should refer to a folder inside the Transactions directory. For example,
```
make run-2pl folder=deadlock no_sites=2
```

### To clean:
```
make clean
```


This project was developerd on macOS with Python version 3.9.10
