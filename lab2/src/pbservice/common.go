package pbservice

const (
  OK = "OK"
  ErrNoKey = "ErrNoKey"
  ErrWrongServer = "ErrWrongServer"
)
type Err string

type PutArgs struct {
  FromPrimary bool
  Key string
  Value string
}

type PutReply struct {
  Err Err
}

type GetArgs struct {
  FromPrimary bool
  Key string
}

type GetReply struct {
  Err Err
  Value string
}

// Your RPC definitions here.

type TransferArgs struct {
  State map[string]string
}

type  TransferReply struct {
  Err Err
}