# dsync

"dsync" is a simple CLI applicatation which synchronises directories onver network.

Usage:
dsync.py [-h] [-c PATH DST_IP | -s PATH]

arguments:

  -h, --help                           | Show this help message and exit
  
  -c PATH DST_IP, --client PATH DST_IP | The Directory path to sync and destination IP
  
  -s PATH, --server PATH               | The Directory path to store. Defailt path is './'
  
  
 ## TODO List:
  1. Synchronise the file attributes.
  2. Make block size variable. Current size is fixed and = 64K. It will allow making deduplication more efficient.
  3. Optimize compression level depending on network bandwidth.
  4. Parallelize read and write operations for better performance.
  5. Optimize delta synch with big delta (currently it is not as fast as initial synch). For example when a user moved heavy directory to the synchronized tree during the delta sync. The side effect of this operation that delta sync will handle each file independently what slows down overall performance.
