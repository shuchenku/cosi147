package kvpaxos

import "net"
import "fmt"
import "net/rpc"
import "log"
import "paxos"
import "sync"
import "os"
import "syscall"
import "encoding/gob"
import "math/rand"
import "time"
// import "strconv"

const Debug=0

func DPrintf(format string, a ...interface{}) (n int, err error) {
  if Debug > 0 {
    log.Printf(format, a...)
  }
  return
}


type Op struct {
  // Your definitions here.
  // Field names must start with capital letters,
  // otherwise RPC will break.
  Key string
  Value string
  Type string
  Id string
  DoHash bool
}

type KVPaxos struct {
  mu sync.Mutex
  l net.Listener
  me int
  dead bool // for testing
  unreliable bool // for testing
  px *paxos.Paxos

  // Your definitions here.
  kvdb map[string]string
  maxseq int
  replies map[string]Op
}

func (kv *KVPaxos) wait(seq int) Op {
  to := 10 * time.Millisecond
  for {
    decided, tmp := kv.px.Status(seq)
    if decided {
      return tmp.(Op)
    }
    // kv.maxseq++
    time.Sleep(to)
    if to < 10 * time.Second {
      to *= 2
    }
  }
}

func (kv *KVPaxos) Get(args *GetArgs, reply *GetReply) error {
  kv.mu.Lock()
  defer kv.mu.Unlock()
  opargs := Op{
    Key: args.Key,
    Type: Get,
    Id: args.Id,
  }
  var value Op
  for {
    prev,ok := kv.replies[args.Id]
    if ok {
      reply.Value = prev.Value
      return nil
    }

    decided, tmp := kv.px.Status(kv.maxseq+1)
    if decided {
      value = tmp.(Op)
    } else {
      // kv.maxseq++
      kv.px.Start(kv.maxseq+1,opargs)
      value = kv.wait(kv.maxseq+1)

    }
    // log.Printf("kvpaxos: %v, getting: %v, supposed to be: %v, got: %v",kv.me,args.Key, args.Id, value.Id)
    if value.Id == args.Id {
      value.Value = kv.kvdb[value.Key]
      kv.replies[value.Id] = value
      // log.Printf("kvpaxos: %v, get reply: Key %v, Value %v, Type %v",kv.me,value.Key,value.Value,value.Type)
      _,ex := kv.kvdb[args.Key]
      if ex { 
        reply.Value = kv.kvdb[args.Key]
        // log.Printf("kvpaxos: %v, key: %v, value: %v, get successful", kv.me, args.Key, val)
      }
      kv.maxseq++ 
      kv.px.Done(kv.maxseq) 
      break
    } else {
    rep := kv.kvdb[value.Key]
    if value.Type==Put {
        if value.DoHash {
          kv.kvdb[value.Key] = NextValue(kv.kvdb[value.Key], value.Value)
        } else {
          kv.kvdb[value.Key] = value.Value
        }      
        // log.Printf("kvpaxos: %v, put: catching up on seq: %v",kv.me,kv.maxseq)
      } 
      value.Value = rep
      kv.replies[value.Id] = value
    }
    // kv.px.Done(kv.maxseq)
    kv.maxseq++
    kv.px.Done(kv.maxseq)
    // log.Printf("get outer")    
  }
  return nil
}

func (kv *KVPaxos) Put(args *PutArgs, reply *PutReply) error {
  // Your code here.
  kv.mu.Lock()
  defer kv.mu.Unlock()
  opargs := Op{
    Key: args.Key,
    Value: args.Value,
    Type: Put,
    Id: args.Id,
    DoHash: args.DoHash,
  }
  var value Op
  for {
    prev,ok := kv.replies[args.Id]
    if ok {
      reply.PreviousValue = prev.Value
      return nil
    }

    decided, tmp := kv.px.Status(kv.maxseq+1)
    if decided {
      value = tmp.(Op)
    } else {
      // kv.maxseq++
      kv.px.Start(kv.maxseq+1,opargs)
      value = kv.wait(kv.maxseq+1)
    }

    // log.Printf("kvpaxos: %v, putting: key %v, value: %v ",kv.me,args.Key,args.Value)
    if value.Id == args.Id {
      reply.PreviousValue = kv.kvdb[args.Key]      
      // val,ex := kv.kvdb[args.Key]
      if args.DoHash {
        // log.Printf("Has no key for %v, val: %v, test: %v",args.Key,val, val=="")
        // log.Printf("kvpaxos: %v, key: %v, value: %v, put successful, overwritting", kv.me, args.Key, val)
        kv.kvdb[args.Key] = NextValue(kv.kvdb[args.Key], value.Value)
      } else {
        kv.kvdb[args.Key] = args.Value
      }
      value.Value = reply.PreviousValue
      kv.replies[value.Id] = value
      kv.maxseq++ 
      kv.px.Done(kv.maxseq) 
      break
    } else {
      rep := kv.kvdb[value.Key]
      if value.Type==Put {
        if value.DoHash {
          kv.kvdb[value.Key] = NextValue(kv.kvdb[value.Key], value.Value)
        } else {
          kv.kvdb[value.Key] = value.Value
        }      
        // log.Printf("kvpaxos: %v, put: catching up on seq: %v",kv.me,kv.maxseq)
      } 
      value.Value = rep
      kv.replies[value.Id] = value
    }
    kv.maxseq++
    kv.px.Done(kv.maxseq)
  }
  return nil
}

// func NextValue(hprev string, val string) string {
//   h := hash(hprev + val)
//   return strconv.Itoa(int(h))
// }

// tell the server to shut itself down.
// please do not change this function.
func (kv *KVPaxos) kill() {
  DPrintf("Kill(%d): die\n", kv.me)
  kv.dead = true
  kv.l.Close()
  kv.px.Kill()
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// 
func StartServer(servers []string, me int) *KVPaxos {
  // this call is all that's needed to persuade
  // Go's RPC library to marshall/unmarshall
  // struct Op.
  gob.Register(Op{})

  kv := new(KVPaxos)
  kv.me = me

  // Your initialization code here.

  kv.replies = map[string]Op{}
  kv.kvdb = map[string]string{}
  kv.maxseq = 0


  rpcs := rpc.NewServer()
  rpcs.Register(kv)

  kv.px = paxos.Make(servers, me, rpcs)

  os.Remove(servers[me])
  l, e := net.Listen("unix", servers[me]);
  if e != nil {
    log.Fatal("listen error: ", e);
  }
  kv.l = l


  // please do not change any of the following code,
  // or do anything to subvert it.

  go func() {
    for kv.dead == false {
      conn, err := kv.l.Accept()
      if err == nil && kv.dead == false {
        if kv.unreliable && (rand.Int63() % 1000) < 100 {
          // discard the request.
          conn.Close()
        } else if kv.unreliable && (rand.Int63() % 1000) < 200 {
          // process the request but force discard of reply.
          c1 := conn.(*net.UnixConn)
          f, _ := c1.File()
          err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
          if err != nil {
            fmt.Printf("shutdown: %v\n", err)
          }
          go rpcs.ServeConn(conn)
        } else {
          go rpcs.ServeConn(conn)
        }
      } else if err == nil {
        conn.Close()
      }
      if err != nil && kv.dead == false {
        fmt.Printf("KVPaxos(%v) accept: %v\n", me, err.Error())
        kv.kill()
      }
    }
  }()

  return kv
}


