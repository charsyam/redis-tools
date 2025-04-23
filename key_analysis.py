from collections import defaultdict
import redis
import argparse
import heapq


def extract_prefix(key, delim):
    if delim in key:
        return key.split(delim)[0]
    return "(no-prefix)"


def get_key_size_safe(r, key, ktype):
    try:
        if ktype == b'string':
            return r.strlen(key)
        elif ktype == b'list':
            return r.llen(key)
        elif ktype == b'set':
            return r.scard(key)
        elif ktype == b'zset':
            return r.zcard(key)
        elif ktype == b'hash':
            return r.hlen(key)
        else:
            return -1
    except Exception:
        return -1


def main():
    parser = argparse.ArgumentParser(description="Redis Key Length/Size")
    parser.add_argument("--host", default="localhost", help="Redis Host")
    parser.add_argument("--port", type=int, default=6379, help="Redis Port")
    parser.add_argument("--pattern", default="*", help="Key Pattern")
    parser.add_argument("--top", type=int, default=0, help="Top N")
    parser.add_argument("--db", type=int, default=0, help="Key DB")
    parser.add_argument("--prefix", default=":", help="Prefix Delimiter(ex: ':', '_')")
    args = parser.parse_args()

    r = redis.Redis(host=args.host, port=args.port)
    r.select(args.db)
    cursor = 0
    total_keys = 0

    print(f"{'Key':40} {'Type':10} {'Size (len/num)':>15}")
    print("-" * 70)

    top_keys = defaultdict(list)
    prefix_counts = defaultdict(int)

    while True:
        cursor, keys = r.scan(cursor=cursor, match=args.pattern, count=100)
        for key in keys:
            try:
                ktype = r.type(key)
                size = get_key_size_safe(r, key, ktype)

                ktype_str = ktype.decode()

                key_str = key.decode()
                prefix = extract_prefix(key_str, args.prefix)
                prefix_counts[prefix] += 1

                heap = top_keys[ktype_str]
                heapq.heappush(heap, (size, key_str))
                if len(heap) > args.top:
                    heapq.heappop(heap)

                if args.top == 0:
                    print(f"{key.decode():40} {ktype.decode():10} {size:>15}")
            except Exception as e:
                print(e)
                continue
            total_keys += 1
        if cursor == 0:
            break

    print("-" * 70)
    print(f"🔍 Total Keys: {total_keys}")

    if args.top > 0:
        for dtype, heap in top_keys.items():
            print(f"📦 Collection : {dtype.upper()} (Top {args.top})")
            print(f"{'Key':40} {'Size':>10}")
            print("-" * 60)

            for size, key in sorted(heap, key=lambda x: -x[0]):
                print(f"{key:40} {size:>10}")

            print()

    print(f"{'Prefix':20} {'Count':>10}")
    print("-" * 50)

    for prefix, count in sorted(prefix_counts.items(), key=lambda x: -x[1])[:args.top]:
        print(f"{prefix:20} {count:>10}")

if __name__ == "__main__":
    main()

