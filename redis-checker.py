import redis
import sys
import argparse
import time

class Checker(object):
    def __init__(self):
        self.funcs = []

    def register(self, **options):
        def decorator(f):
            name = options.pop("name", f.__name__)
            self.add_checker(name, f, **options)
            return f
        return decorator

    def add_checker(self, name, f, **options):
        self.funcs.append((f, options))

    def call(self, r, info, reportFunc):
        for func in self.funcs:
            t = func
            f = t[0]
            if "title" in t[1]:
                title = t[1]["title"]
            else:
                title = f.__name__

            reasons = f(r, info)
            if len(reasons) > 0:
                reportFunc(title, reasons)

checker = Checker()

KEYS_GAP = 0
CONN_GAP = 100
MAXCLIENTS = 10000

INFO = 0
CHECK = 1
WARNING = 2
DANGER = 3

LEVELS = [
  "INFO",
  "CHECK",
  "WARNING",
  "DANGER"
]

def toStr(i):
    return LEVELS[i]

def fail(msg):
    print >> sys.stderr, msg
    exit(1)

def redisHost(r):
    return r.connection_pool.connection_kwargs['host']

def redisPort(r):
    return r.connection_pool.connection_kwargs['port']

def redisPassword(r):
    return r.connection_pool.connection_kwargs['password']

def compareVersion(va, vb):
    for vaPart,vbPart in zip([int(x) for x in va.split('.')], [int(x) for x in vb.split('.')]):
        if vaPart > vbPart:
            return 1
        elif vaPart < vbPart:
            return -1
    return 0

def valOrNA(x):
    return x if x != None else 'N/A'

def bytesToStr(bytes):
    if bytes < 1024:
        return '%dB'%bytes
    if bytes < 1024*1024:
        return '%dKB'%(bytes/1024)
    if bytes < 1024*1024*1024:
        return '%dMB'%(bytes/(1024*1024))
    return '%dGB'%(bytes/(1024*1024*1024))

def getRedisConn(url):
    parts = url.split(':')
    port = 6379
    passwd = None
    if len(parts) > 1:
        port = int(parts[1])
    if len(parts) > 2:
        passwd = parts[2]

    hostname = parts[0]
    r = redis.Redis(host=hostname, port=port, password=passwd, socket_timeout=2)
    try:
        ver = r.info()['redis_version']
        r.ver = ver
    except redis.ConnectionError as e:
        fail('Failed connecting (%s) to %s, aborting'%(e,url))

    return r

@checker.register(title="Memory Status")
def checkMemory(r, info):
    mem = bytesToStr(int(info['used_memory']))
    rss = bytesToStr(int(info['used_memory_rss']))
    ratio = info['mem_fragmentation_ratio']
    if compareVersion(r.ver, "3.2") in [0, 1]:
        total = bytesToStr(int(info['total_system_memory']))
    else:
        total = "N/A"

    reasons = []
    reasons.append((INFO, "Used Memory in Redis: %s"%(mem)))
    reasons.append((INFO, "Real Memory in OS   : %s"%(rss)))
    reasons.append((INFO, "Fragmentation Ratio : %s"%(ratio)))
    reasons.append((INFO, "Server Memory       : %s"%(total)))
    
    return reasons

@checker.register(title="RDB Status")
def checkRDB(r, info):
    reasons = []
    default = ['3600', '1', '300', '100', '60', '10000']
    save = r.config_get("save")["save"]
    parts = save.split()
    if default == parts:
        reasons.append((DANGER, "save option is set by default: %s"%(save)))

    if len(parts) > 0:
        if info["rdb_last_bgsave_status"] != "ok":
            reasons.append((DANGER, "rdb_last_bgsave_status is bad"))

        bgsave = r.config_get("stop-writes-on-bgsave-error")["stop-writes-on-bgsave-error"]
        if bgsave == "yes":
            reasons.append((WARNING, "stop-writes-on-bgsave-error is yes"))

    return reasons

@checker.register(title="AOF Status")
def checkAOF(r, info):
    #"auto-aof-rewrite-percentage"
    #"100"
    #"auto-aof-rewrite-min-size"
    #"67108864"

    reasons = []
    appendonly = r.config_get("appendonly")["appendonly"]
    if appendonly == 'no':
        return reasons

    appendfsync = r.config_get("appendfsync")["appendfsync"]
    if appendfsync == "always":
        reasons.append((CHECK, "appendfsync is always. It can cause performance issue"))
    
    per = int(r.config_get("auto-aof-rewrite-percentage")["auto-aof-rewrite-percentage"])
    size = bytesToStr(int(r.config_get("auto-aof-rewrite-min-size")["auto-aof-rewrite-min-size"]))

    if per != 0:
        reasons.append((WARNING, "AOF can be auto generated. per: %s, size: %s"%(per, size)))

    return reasons

def checkClients(r, info):
    reasons = []
    reasons.extend(checkMaxClients(r, info))
    reasons.extend(checkOutputBufferLimites(r, info))
    return reasons

@checker.register(title="MAX Clients Status")
def checkMaxClients(r, info):
    reasons = []
    maxclients = int(r.config_get("maxclients")["maxclients"])
    if maxclients < MAXCLIENTS:
        reasons.append((CHECK, "MaxClients is too small. current %s, recommend: 50000"%(maxclients)))

    return reasons

@checker.register(title="Replication outputBufferLimits Status")
def checkOutputBufferLimites(r, info):
    '''
    normal 0 0 0 slave 268435456 67108864 60 pubsub 33554432 8388608 60
    '''

    mem = int(info['used_memory'])
    rss = int(info['used_memory_rss'])

    reasons = []
    ret = r.config_get("client-output-buffer-limit")["client-output-buffer-limit"]
    parts = ret.split()
    if parts[3] == 'slave':
        hard = int(parts[4])
        soft = int(parts[5])

        if mem > (9 * 1024 * 1024 * 1024):
            if hard < (512 * 1024 * 1024):
                reasons.append(CHECK, "client-output-buffer-limit is small for slave. up it to least 512MB")

    return reasons

def checkDangerCommands(r, info):
    #cmdstat_setex:calls=1486470,usec=3265266,usec_per_call=2.20
    ret = 0
    if "cmdstat_keys" in info:
        ret = int(info['cmdstat_keys']['calls'])
         
    return ret

def checkConnectedClients(r, info):
    return (int(info['connected_clients'])) 

def checkCommands(r, info):
    return (int(info['total_commands_processed'])) 

def arrayGap(t):
    return [t[i+1]-t[i] for i in range(len(t)-1)]

def overGap(t, gap):
    if len(t) == 0:
        return False

    v = t[0]
    for i in t:
        if abs(i - v) > gap:
            return True

    return False

def reportFunc(title, reasons):
    print("===================================================")
    print(title)
    for t in reasons:
        print("%s: %s"%(toStr(t[0]), t[1]))
        
  
@checker.register(title="ETC: Timed Information")
def redisTimedCheck(r, info):
    reasons = []
    info = r.info('all')

    n_conns = []
    n_commands = []
    n_keys = []
    for i in range(CHECK_SECONDS):
        n_keys.append(checkDangerCommands(r, info))
        n_conns.append(checkConnectedClients(r, info))
        n_commands.append(checkCommands(r, info))
        time.sleep(1)
        info = r.info('all')

    keysGap = arrayGap(n_keys)
    if overGap(keysGap, KEYS_GAP):
        reasons.append((DANGER, "Don't use Keys Command: %s"%(keysGap)))

    connGap = arrayGap(n_conns)
    if overGap(connGap, CONN_GAP):
        reasons.append((CHECK, "Connections are frequently changed : %s"%(connGap)))

    commGap = arrayGap(n_commands)
    reasons.append((INFO, "Commands per sec : %s"%(commGap)))
    return reasons


if __name__ == '__main__':
    global CHECK_SECONDS
    parser = argparse.ArgumentParser(description='Interactively migrate a bunch of redis servers to another bunch of redis servers.')
    parser.add_argument('--src', metavar='src_url', required=True, help='source redis to sync from')
    parser.add_argument('--seconds', metavar='seconds', required=False, help='check seconds')
    
    args = parser.parse_args()
    if (args.seconds != None): 
        CHECK_SECONDS = int(args.seconds)
        if CHECK_SECONDS < 2:
            CHECK_SECONDS = 2
    else:
        CHECK_SECONDS = 5
    
    r = getRedisConn(args.src)
    info = r.info()
    checker.call(r, info, reportFunc) 
