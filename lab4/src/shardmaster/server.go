package shardmaster

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

type ShardMaster struct {
  mu sync.Mutex
  l net.Listener
  me int
  dead bool // for testing
  unreliable bool // for testing
  px *paxos.Paxos

  configs []Config // indexed by config num
  configNum int
  seqNum int

}


type Op struct {
  // Your data here.
  Type string
  GID int64
  Servers []string
  Shard int
  Num int
  Uid int64
}

const (
  Join = "Join"
  Leave = "Leave"
  Move = "Move"
  Query = "Query"
)

func (sm *ShardMaster) wait(seq int) Op {
  to := 10 * time.Millisecond
  for {
    decided, tmp := sm.px.Status(seq)
    if decided {
      return tmp.(Op)
    }
    time.Sleep(to)
    if to < 10 * time.Second {
      to *= 2
    }
  }
}


func getMostLoaded(config Config) int64 {
  maxid := int64(0)
  maxload := 0
  for gid,_ := range config.Groups {
    load := 0
    for _,sgid := range config.Shards {
      if gid == sgid {
        load++
      }
    }
    if load > maxload {
      maxload = load
      maxid = gid
    }
  }
  return maxid
}

func getLeastLoaded(config Config) int64 {
  minid := int64(0)
  minload := NShards+1
  for gid,_ := range config.Groups {
    load := 0
    for _,sgid := range config.Shards {
      if gid == sgid {
        load++
      }
    }
    if load < minload {
      minload = load
      minid = gid
    }
  }
  return minid
}

func getShard(config Config, gid int64) int {
  for id,val := range config.Shards {
    if val == gid { return id }
  }
  return -1
}

func makeNewConfig(prev Config) Config {
  newConfig := Config{}
  newConfig.Groups = map[int64][]string{}
  newConfig.Num = prev.Num+1
  copy(newConfig.Shards[:],prev.Shards[:])
  for k,v := range prev.Groups {
    newConfig.Groups[k] = v
  }
  return newConfig
}

func (sm *ShardMaster) DoJoin(args Op) {
  newConfig := makeNewConfig(sm.configs[sm.configNum])
  if _,ex := newConfig.Groups[args.GID]; !ex {
    newConfig.Groups[args.GID] = args.Servers
    avg := NShards/len(newConfig.Groups)
    fromGroup := getMostLoaded(newConfig)
    count := 0
    for {
      if (count == avg) { break }
      newConfig.Shards[getShard(newConfig,fromGroup)] = args.GID
      count++
    }
  }
  // log.Printf("current Group size: %v, Num: %v",len(newConfig.Groups),newConfig.Num)
  // log.Printf("previous Group size: %v, Num: %v",len(sm.configs[sm.configNum].Groups),sm.configs[sm.configNum].Num)
  sm.configs = append(sm.configs,newConfig)
  sm.configNum = newConfig.Num
  sm.px.Done(sm.seqNum) 
  sm.seqNum++
}

func (sm *ShardMaster) DoLeave(args Op) {
  newConfig := makeNewConfig(sm.configs[sm.configNum])
  if _,ex := newConfig.Groups[args.GID]; ex {
    delete(newConfig.Groups,args.GID)
    toGroup := getLeastLoaded(newConfig)
    for {
      shid := getShard(newConfig,args.GID)
      if shid == -1 { break }
      newConfig.Shards[shid] = toGroup
    }
  }
  sm.configs = append(sm.configs,newConfig)
  sm.configNum = newConfig.Num
  sm.px.Done(sm.seqNum)
  sm.seqNum++ 
}

func (sm *ShardMaster) DoMove(args Op) {
  newConfig := makeNewConfig(sm.configs[sm.configNum])
  newConfig.Shards[args.Shard] = args.GID 
  sm.configs = append(sm.configs,newConfig)
  sm.configNum = newConfig.Num
  sm.px.Done(sm.seqNum)
  sm.seqNum++
}

func (sm *ShardMaster) DoQuery(args Op) Config {
  var reply Config
  if args.Num == -1 {
    reply = sm.configs[sm.configNum]
  } else {
    reply = sm.configs[args.Num]
  }
  sm.px.Done(sm.seqNum)
  sm.seqNum++
  return reply
}

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) error {
  // Your code here.
  sm.mu.Lock()
  defer sm.mu.Unlock()
  opargs := Op{
    Type: Join,
    GID: args.GID,
    Servers: args.Servers,
    Uid: rand.Int63(),
  }
  var value Op
  for {
    decided, tmp := sm.px.Status(sm.seqNum+1)
    if decided {
      value = tmp.(Op)
    } else {
      sm.px.Start(sm.seqNum+1,opargs)
      value = sm.wait(sm.seqNum+1)
    }
    // log.Printf("stuck in Join")
    if value.Uid == opargs.Uid {
      sm.DoJoin(opargs)
      break
    }
    sm.DoOp(value)
  }
  return nil
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) error {
  // Your code here.
  sm.mu.Lock()
  defer sm.mu.Unlock()
  opargs := Op{
    Type: Leave,
    GID: args.GID,
    Uid: rand.Int63(),
  }
  var value Op
  for {
    decided, tmp := sm.px.Status(sm.seqNum+1)
    if decided {
      value = tmp.(Op)
    } else {
      sm.px.Start(sm.seqNum+1,opargs)
      value = sm.wait(sm.seqNum+1)
    }
    // log.Printf("stuck in Join")
    if value.Uid == opargs.Uid {
      sm.DoLeave(opargs)
      break
    }
    sm.DoOp(value)
  }
  return nil
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) error {
  // Your code here.
  sm.mu.Lock()
  defer sm.mu.Unlock()
  opargs := Op{
    Type: Move,
    GID: args.GID,
    Shard: args.Shard,
    Uid: rand.Int63(),
  }
  var value Op
  for {
    decided, tmp := sm.px.Status(sm.seqNum+1)
    if decided {
      value = tmp.(Op)
    } else {
      sm.px.Start(sm.seqNum+1,opargs)
      value = sm.wait(sm.seqNum+1)
    }

    if value.Uid == opargs.Uid {
      sm.DoMove(opargs)
      break
    }
    sm.DoOp(value)
  }
  return nil
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) error {
  // Your code here.
  sm.mu.Lock()
  defer sm.mu.Unlock()
  opargs := Op{
    Type: Query,
    Num: args.Num,
    Uid: rand.Int63(),
  }
  var value Op
  for {
    decided, tmp := sm.px.Status(sm.seqNum+1)
    if decided {
      value = tmp.(Op)
    } else {
      sm.px.Start(sm.seqNum+1,opargs)
      value = sm.wait(sm.seqNum+1)
    }
    // log.Printf("stuck in Query")
    if value.Uid == opargs.Uid {
      reply.Config = sm.DoQuery(opargs)
      break
    }
    sm.DoOp(value)
  }
  return nil
}

func (sm *ShardMaster) DoOp(args Op) {
  switch args.Type {
  case Join:
    sm.DoJoin(args)
  case Leave:
    sm.DoLeave(args)
  case Move:
    sm.DoMove(args)
  case Query:
    sm.DoQuery(args)
  }
}

// please don't change this function.
func (sm *ShardMaster) Kill() {
  sm.dead = true
  sm.l.Close()
  sm.px.Kill()
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
// 
func StartServer(servers []string, me int) *ShardMaster {
  gob.Register(Op{})

  sm := new(ShardMaster)
  sm.me = me

  sm.configs = make([]Config, 1)
  sm.configs[0].Groups = map[int64][]string{}

  sm.configNum = 0;
  sm.seqNum = 0

  rpcs := rpc.NewServer()
  rpcs.Register(sm)

  sm.px = paxos.Make(servers, me, rpcs)

  os.Remove(servers[me])
  l, e := net.Listen("unix", servers[me]);
  if e != nil {
    log.Fatal("listen error: ", e);
  }
  sm.l = l

  // please do not change any of the following code,
  // or do anything to subvert it.

  go func() {
    for sm.dead == false {
      conn, err := sm.l.Accept()
      if err == nil && sm.dead == false {
        if sm.unreliable && (rand.Int63() % 1000) < 100 {
          // discard the request.
          conn.Close()
        } else if sm.unreliable && (rand.Int63() % 1000) < 200 {
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
      if err != nil && sm.dead == false {
        fmt.Printf("ShardMaster(%v) accept: %v\n", me, err.Error())
        sm.Kill()
      }
    }
    }()

    return sm
  }
