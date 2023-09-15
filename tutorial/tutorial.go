package main

import (
	"log"
	"math"
	"math/rand"
	"time"

	"example.com/util"
)

type Server struct {
	DB        []uint64
	DBSize    uint64
	ChunkSize uint64
	ChunkNum  uint64
}

type Client struct {
	DBSize    uint64
	ChunkSize uint64
	ChunkNum  uint64

	Q  uint64
	M1 uint64
	M2 uint64

	primaryHints []LocalHint
	backupHints  []LocalHint
}

func (s Server) possibleParities(offsetVec []uint64) []uint64 {
	// Run by the server. Given a punctured offset, it first guesses the position of the punctured entry,
	// then it computes the possible parities.
	parities := make([]uint64, s.ChunkNum)
	parities[0] = 0
	for i := uint64(0); i < s.ChunkNum-1; i++ {
		xi := (i+1)*s.ChunkSize + offsetVec[i]
		parities[0] ^= s.DB[xi]
	}
	for i := uint64(0); i < s.ChunkNum-1; i++ {
		parities[i+1] = parities[i] ^ s.DB[(i+1)*s.ChunkSize+offsetVec[i]] ^ s.DB[i*s.ChunkSize+offsetVec[i]]
	}
	return parities
}

// XXX(caw): this should be extended to support pipelining for unbounded queries
func (s Server) Process(offsetVec []uint64) []uint64 {
	return s.possibleParities(offsetVec)
}

type LocalHint struct {
	key             util.PrfKey
	parity          uint64
	programmedPoint uint64
	isProgrammed    bool
}

// Elem returns the element in the chunkID-th chunk of the hint. It takes care of the case when the hint is programmed.
func (c Client) Elem(hint *LocalHint, chunkId uint64) uint64 {
	if hint.isProgrammed && chunkId == hint.programmedPoint/c.ChunkSize {
		return hint.programmedPoint
	} else {
		return util.PRFEval(&hint.key, chunkId)%c.ChunkSize + chunkId*c.ChunkSize
	}
}

func NewServer(DBSize uint64) Server {
	DB := make([]uint64, DBSize)
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	for i := uint64(0); i < DBSize; i++ {
		DB[i] = rng.Uint64()
	}

	// setup the parameters
	ChunkSize := uint64(math.Sqrt(float64(DBSize)))
	ChunkNum := uint64(math.Ceil(float64(DBSize) / float64(ChunkSize)))

	return Server{
		DB:        DB,
		DBSize:    DBSize,
		ChunkSize: ChunkSize,
		ChunkNum:  ChunkNum,
	}
}

func (s Server) NewClient() Client {
	Q := uint64(math.Sqrt(float64(s.DBSize)) * math.Log(float64(s.DBSize)))
	M1 := 4 * uint64(math.Sqrt(float64(s.DBSize))*math.Log(float64(s.DBSize)))
	M2 := 4 * uint64(math.Log(float64(s.DBSize)))

	return Client{
		DBSize:    s.DBSize,
		ChunkSize: s.ChunkSize,
		ChunkNum:  s.ChunkNum,

		Q:  Q,
		M1: M1,
		M2: M2,
	}
}

func (s Server) Query(index uint64) uint64 {
	return s.DB[index]
}

type ClientState struct {
	config          Client
	rng             *rand.Rand
	primaryHints    []LocalHint
	backupHints     []LocalHint
	localCache      map[uint64]uint64
	consumedHintNum []uint64
}

func (c Client) InitializeState(s Server) *ClientState {
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))

	primaryHints := make([]LocalHint, c.M1)
	backupHints := make([]LocalHint, c.M2*c.ChunkNum)
	for i := uint64(0); i < c.M1; i++ {
		primaryHints[i] = LocalHint{util.RandKey(rng), 0, 0, false}
	}
	for i := uint64(0); i < c.M2*c.ChunkNum; i++ {
		backupHints[i] = LocalHint{util.RandKey(rng), 0, 0, false}
	}

	// Initialize all hints using the randomly sampled values from the database
	for i := uint64(0); i < c.ChunkNum; i++ {
		// suppose the client receives the i-th chunk, DB[i*ChunkSize:(i+1)*ChunkSize]
		for j := uint64(0); j < c.M1; j++ {
			index := c.Elem(&primaryHints[j], i)
			primaryHints[j].parity ^= s.Query(index)
		}
		for j := uint64(0); j < c.M2*c.ChunkNum; j++ {
			if j/c.M2 != i {
				index := c.Elem(&backupHints[j], i)
				backupHints[j].parity ^= s.Query(index)
			}
		}
	}

	localCache := make(map[uint64]uint64)
	consumedHintNum := make([]uint64, c.ChunkNum)

	return &ClientState{
		config:          c,
		rng:             rng,
		primaryHints:    primaryHints,
		backupHints:     backupHints,
		localCache:      localCache,
		consumedHintNum: consumedHintNum,
	}
}

type ClientQuery struct {
	state   *ClientState
	index   uint64
	chunkId uint64
	hitId   uint64
}

func (c ClientQuery) Prepare() []uint64 {
	offsetVec := make([]uint64, c.state.config.ChunkNum)
	for i := uint64(0); i < c.state.config.ChunkNum; i++ {
		offsetVec[i] = c.state.config.Elem(&c.state.primaryHints[c.hitId], i) % c.state.config.ChunkSize
	}
	punctOffsetVec := offsetVec[0:c.chunkId]
	punctOffsetVec = append(punctOffsetVec, offsetVec[c.chunkId+1:]...)

	return punctOffsetVec
}

func (c *ClientState) RandomQuery() ClientQuery {
	x := c.rng.Uint64() % c.config.DBSize

	// make sure x is not in the local cache
	for {
		if _, ok := c.localCache[x]; ok == false {
			break
		}
		x = c.rng.Uint64() % c.config.DBSize
	}

	chunkId := x / c.config.ChunkSize
	hitId := uint64(999999999)
	for i := uint64(0); i < c.config.M1; i++ {
		if c.config.Elem(&c.primaryHints[i], chunkId) == x {
			hitId = i
			break
		}
	}
	if hitId == uint64(999999999) {
		log.Fatalf("Error: cannot find the hitId")
	}

	return ClientQuery{
		state:   c,
		index:   x,
		chunkId: chunkId,
		hitId:   hitId,
	}
}

func (c *ClientState) RecoverAnswer(clientQuery ClientQuery, serverParities []uint64) uint64 {
	answer := serverParities[clientQuery.chunkId] ^ c.primaryHints[clientQuery.hitId].parity
	c.localCache[clientQuery.index] = answer

	if c.consumedHintNum[clientQuery.chunkId] < c.config.M2 {
		c.primaryHints[clientQuery.hitId] = c.backupHints[clientQuery.chunkId*c.config.M2+c.consumedHintNum[clientQuery.chunkId]]
		c.primaryHints[clientQuery.hitId].isProgrammed = true
		c.primaryHints[clientQuery.hitId].programmedPoint = clientQuery.index
		c.primaryHints[clientQuery.hitId].parity ^= answer
		c.consumedHintNum[clientQuery.chunkId]++
	} else {
		// XXX(caw): what is the proper failure condition here?
		log.Fatalf("Not enough backup hints")
	}

	return answer
}

func main() {
	dbSize := uint64(10000)
	server := NewServer(dbSize)
	client := server.NewClient()
	clientState := client.InitializeState(server)

	for q := uint64(0); q < client.Q; q++ {
		query := clientState.RandomQuery()
		preparedQuery := query.Prepare()
		response := server.Process(preparedQuery)
		answer := clientState.RecoverAnswer(query, response)
		if answer != server.Query(query.index) {
			log.Fatalf("Error: answer is not correct")
		}
	}
}
