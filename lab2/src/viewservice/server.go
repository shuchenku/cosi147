package viewservice

import "net"
import "net/rpc"
import "log"
import "time"
import "sync"
import "fmt"
import "os"

type ViewServer struct {
  mu sync.Mutex
  l net.Listener
  dead bool
  me string

  // Your declarations here.
  lastPings map[string]time.Time
  currentView View
  acked bool
  restarted bool
}

//
// server Ping RPC handler.
//
func (vs *ViewServer) Ping(args *PingArgs, reply *PingReply) error {
  // Your code here.

  vs.mu.Lock()
  defer vs.mu.Unlock()

  vs.lastPings[args.Me] = time.Now();
  if (vs.currentView.Primary == args.Me) {
    vs.acked = args.Viewnum == vs.currentView.Viewnum
    vs.restarted = (args.Viewnum == 0 && vs.currentView.Viewnum != 1)
  } 
  reply.View = vs.currentView
  return nil
}

// 
// server Get() RPC handler.
//
func (vs *ViewServer) Get(args *GetArgs, reply *GetReply) error {
  // Your code here.
  vs.mu.Lock()
  defer vs.mu.Unlock()

  reply.View = vs.currentView
  return nil
}


//
// tick() is called once per PingInterval; it should notice
// if servers have died or recovered, and change the view
// accordingly.
//
func (vs *ViewServer) tick() {
  // Your code here.

  if (vs.currentView.Viewnum == 0) {
    luckyIdleServer := vs.pickAnIdle()
    if (luckyIdleServer != "") {
      vs.currentView.Primary = luckyIdleServer
      vs.currentView.Viewnum++
    }
    return
  }

  if (((vs.currentView.Viewnum != 0 && vs.isDead(vs.currentView.Primary)) || vs.restarted) && vs.acked) {
    vs.currentView.Primary = vs.currentView.Backup
    vs.currentView.Backup = ""
    vs.currentView.Viewnum++
    vs.acked = false
    return
  } 

  if ((vs.currentView.Backup == "" || vs.isDead(vs.currentView.Backup)) && vs.acked) {
    luckyIdleServer := vs.pickAnIdle()
    if (!(luckyIdleServer == "" && vs.currentView.Backup == "")) {

      vs.currentView.Backup = luckyIdleServer
      vs.currentView.Viewnum++
      vs.acked = false
    }

    return
  }
}

func (vs *ViewServer) pickAnIdle() string {
  for i := range vs.lastPings {
    if (i != vs.currentView.Primary && i !=vs.currentView.Backup && !vs.isDead(i)) {
      return i
    }
  }
  return ""
}

func (vs *ViewServer) isDead(serverName string) bool {
  if (time.Since(vs.lastPings[serverName]) > PingInterval * DeadPings) {
    return true
  }
  return false
}

//
// tell the server to shut itself down.
// for testing.
// please don't change this function.
//
func (vs *ViewServer) Kill() {
  vs.dead = true
  vs.l.Close()
}

func StartServer(me string) *ViewServer {
  vs := new(ViewServer)
  vs.me = me
  // Your vs.* initializations here.
  vs.currentView = View{}
  vs.currentView.Viewnum = 0
  vs.lastPings = make(map[string]time.Time)

  // tell net/rpc about our RPC server and handlers.
  rpcs := rpc.NewServer()
  rpcs.Register(vs)

  // prepare to receive connections from clients.
  // change "unix" to "tcp" to use over a network.
  os.Remove(vs.me) // only needed for "unix"
  l, e := net.Listen("unix", vs.me);
  if e != nil {
    log.Fatal("listen error: ", e);
  }
  vs.l = l

  // please don't change any of the following code,
  // or do anything to subvert it.

  // create a thread to accept RPC connections from clients.
  go func() {
    for vs.dead == false {
      conn, err := vs.l.Accept()
      if err == nil && vs.dead == false {
        go rpcs.ServeConn(conn)
      } else if err == nil {
        conn.Close()
      }
      if err != nil && vs.dead == false {
        fmt.Printf("ViewServer(%v) accept: %v\n", me, err.Error())
        vs.Kill()
      }
    }
  }()

  // create a thread to call tick() periodically.
  go func() {
    for vs.dead == false {
      vs.tick()
      time.Sleep(PingInterval)
    }
  }()

  return vs
}
