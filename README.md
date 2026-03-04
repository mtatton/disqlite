distsqlite

[Donate](https://www.paypal.com/ncp/payment/KDNN5CHG4C3RN)


distributed sqlite example:

Run 3 nodes like this:

# follower 1
python disqlite.py \
  --node-id n2 \
  --role follower \
  --port 8002 \
  --db data/n2.db

# follower 2
python disqlite.py \
  --node-id n3 \
  --role follower \
  --port 8003 \
  --db data/n3.db

# leader
python disqlite.py \
  --node-id n1 \
  --role leader \
  --port 8001 \
  --db data/n1.db \
  --peers http://127.0.0.1:8002,http://127.0.0.1:8003

# write to leader
curl -X PUT http://127.0.0.1:8001/kv/name \
  -H "Content-Type: application/json" \
  -d '{"value":"alice"}'

# read from leader
curl http://127.0.0.1:8001/kv/name

# read from follower
curl http://127.0.0.1:8002/kv/name

# list all keys
curl http://127.0.0.1:8001/kv

# delete from leader
curl -X DELETE http://127.0.0.1:8001/kv/name
