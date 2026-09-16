# pa2 - leader election

implementation of the LCR (asynchronous ring) leader election algorithm.
each process gets a random uuid, connects to two neighbors to form a ring,
and the messages circulate until everyone agrees on which uuid is the
biggest (that process becomes leader).

## files

- `myleprocess.py` - the process itself
- `config.txt` - default config (server addr on line 1, client/neighbor addr on line 2)
- `config1.txt`, `config2.txt`, `config3.txt` - configs used for the local 3-node demo
- `log1.txt`, `log2.txt`, `log3.txt` - logs from a local run of the demo

## how to run

each node needs its own config file. the first line is the ip/port this
node will listen on (as a server), the second line is the ip/port of the
neighbor it should connect out to (as a client). exchange these with your
neighbor ahead of time.

```
python myleprocess.py <config_file> <log_file>
```

for the 3-node local demo (run each in its own terminal, in any order,
within a few seconds of each other since the client side sleeps briefly
before trying to connect):

```
python myleprocess.py config1.txt log1.txt
python myleprocess.py config2.txt log2.txt
python myleprocess.py config3.txt log3.txt
```

the ring for the demo config is: node1 -> node2 -> node3 -> node1
(node1's client connects to node2's server, etc)

## example run

three terminals running the three configs above, output logged to
log1.txt / log2.txt / log3.txt. this is the tail end of log2.txt after
a full election (f6fea6ea... won since it had the largest uuid):

```
[23:40:26.411] Received: uuid=f6fea6ea-cc27-4f0f-b59d-ad2b98ab31a7, flag=0, same, 0
[23:40:26.411] Leader is decided to f6fea6ea-cc27-4f0f-b59d-ad2b98ab31a7.
[23:40:26.413] Sent: uuid=f6fea6ea-cc27-4f0f-b59d-ad2b98ab31a7, flag=1
[23:40:26.414] Received: uuid=f6fea6ea-cc27-4f0f-b59d-ad2b98ab31a7, flag=1, same, 1, leader_id=f6fea6ea-cc27-4f0f-b59d-ad2b98ab31a7
[23:40:26.414] process terminated
```

log1.txt and log3.txt end up agreeing on the same leader_id, so
termination, uniqueness, and agreement all hold.

## notes

- each node runs the server accept and the client connect on separate
  threads, since accept() blocks and every node in the ring would
  deadlock waiting on it if it ran before connect() on the same thread
- once both connections are up, everything happens on the main thread
  (just blocking recv -> compare -> forward)
- messages are Message objects (uuid + flag) serialized to json before
  being sent over the socket
