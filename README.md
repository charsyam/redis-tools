# redis-tools
redis tools

## Usage
python redis-checker.py --src 127.0.0.1:6379

### Result
===================================================
Host: 127.0.0.1:6379
===================================================
Memory
Used Memory in Redis: 11MB
Real Memory in OS   : 12MB
Ratio               : 1.09
Server Mem          : 16GB
===================================================
Client Setting
CHECK: MaxClients is too small. current 4064, recommend: 50000
===================================================
RDB: 2
DANGER: save option is set by default: 3600 1 300 100 60 10000
WARNING: stop-writes-on-bgsave-error is yes
===================================================
ETC
Info: commands per sec : [5, 1, 1, 1]
===================================================
