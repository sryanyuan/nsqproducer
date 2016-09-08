package nsqproducer

import (
	"encoding/json"
	"fmt"

	"github.com/nsqio/go-nsq"
)

/*
NSQProducer is an wrap for go-nsq client producer.
It lookups all available nsqd instance and transpond user message to it
NOTE : not thread safe
*/

//Setting variables
var (
	maxRetryTimes = 1
)

//SetMaxRetryTimes set the retry times if producer connection is break
func SetMaxRetryTimes(tm int) {
	maxRetryTimes = tm
}

//logger interface
type logger interface {
	Output(calldepth int, s string) error
}

//INSQProducer is an interface to export
type INSQProducer interface {
	Stop()
	Publish(topic string, body []byte) error
	SetLogger(l logger, lvl nsq.LogLevel)
}

//Json unmarshal struct
type nsqlookupdNodesProducer struct {
	BroadcastAddress string   `json:"broadcast_address"`
	TCPPort          int      `json:"tcp_port"`
	Topics           []string `json:"topics"`
	//  internal use
	available bool
	inuse     bool
}
type nsqlookupdNodesData struct {
	Producers []nsqlookupdNodesProducer `json:"producers"`
}
type nsqlookupdNodesResult struct {
	StatusCode int                 `json:"status_code"`
	StatusText string              `json:"status_text"`
	Data       nsqlookupdNodesData `json:"data"`
}

type nsqProducer struct {
	lookupdAddr    string
	cfg            *nsq.Config
	availableNodes []nsqlookupdNodesProducer

	currentProducer *nsq.Producer
	inuseNode       *nsqlookupdNodesProducer

	loglvl nsq.LogLevel
	log    logger
}

//Implement interface
func (n *nsqProducer) Stop() {
	if n.currentProducer != nil {
		n.currentProducer.Stop()
	}

	n.currentProducer = nil
	n.inuseNode = nil
	n.availableNodes = nil
}

func (n *nsqProducer) Publish(topic string, body []byte) error {
	//  get available producer
	producer, err := n.getProducerWithRetry()
	if nil != err {
		return err
	}

	//  publish message
	err = producer.Publish(topic, body)
	if nil != err {
		//  get available producer again if publish failed
		producer, err = n.getProducerWithRetry()
		if nil != err {
			return err
		}

		return producer.Publish(topic, body)
	}

	return nil
}

func (n *nsqProducer) SetLogger(l logger, lvl nsq.LogLevel) {
	n.log = l
	n.loglvl = lvl
}

//unexport methods
func (n *nsqProducer) getProducer() (*nsq.Producer, error) {
	//  already has a producer
	if nil != n.currentProducer {
		if err := n.currentProducer.Ping(); nil == err {
			return n.currentProducer, nil
		}
		//  clear the node info
		n.inuseNode.available = false
		n.inuseNode = nil
		n.currentProducer.Stop()
		n.currentProducer = nil
	}
	//  create a new producer
	if nil == n.availableNodes ||
		len(n.availableNodes) == 0 {
		return nil, fmt.Errorf("No available nsqd node : node list is empty")
	}
	//  try to connect
	var producer *nsq.Producer
	var err error
	for i, v := range n.availableNodes {
		if !v.available {
			//continue
		}
		producer, err = nsq.NewProducer(fmt.Sprintf("%s:%d", v.BroadcastAddress, v.TCPPort), n.cfg)
		if nil != err {
			continue
		}
		//	apply logger
		if nil != n.log {
			producer.SetLogger(n.log, n.loglvl)
		}
		//  success, ping it
		err = producer.Ping()
		if nil != err {
			continue
		}
		n.availableNodes[i].inuse = true
		n.inuseNode = &n.availableNodes[i]
		n.currentProducer = producer
		break
	}

	return producer, err
}

//  get the producer, it will get all nsqd nodes and connect to it once get producer failed
func (n *nsqProducer) getProducerWithRetry() (*nsq.Producer, error) {
	producer, err := n.getProducer()
	if err != nil {
		n.Stop()
		for i := 0; i < maxRetryTimes; i++ {
			n.availableNodes, err = getAllAvailableNSQDFromNSQLookupd(n.lookupdAddr)
			if nil != err {
				continue
			}
			producer, err = n.getProducer()
			if nil != err {
				break
			}
		}
		if nil != err {
			return nil, err
		}
	}

	return producer, err
}

//Get all available nodes from nsqlookupd
func getAllAvailableNSQDFromNSQLookupd(nsqlookupdAddr string) ([]nsqlookupdNodesProducer, error) {
	//  search for all available node
	body, err := doGet("http://"+nsqlookupdAddr+"/nodes", nil)
	if err != nil {
		return nil, err
	}

	//  parse json
	var nodesResult nsqlookupdNodesResult
	if err = json.Unmarshal(body, &nodesResult); nil != err {
		return nil, err
	}

	//  check result
	if nodesResult.StatusCode != 200 {
		return nil, fmt.Errorf("Get nodes from nsqd failed , status_code : %d", nodesResult.StatusCode)
	}

	//  get all nodes
	return nodesResult.Data.Producers, nil
}

/*NewNSQProducer return a NSQProducer instance
  @param1 nsqlookupAddr
*/
func NewNSQProducer(nsqlookupdAddr string, config *nsq.Config, l logger, loglvl nsq.LogLevel) (INSQProducer, error) {
	instance := &nsqProducer{
		lookupdAddr: nsqlookupdAddr,
		cfg:         config,
	}

	nodes, err := getAllAvailableNSQDFromNSQLookupd(nsqlookupdAddr)
	if nil != err {
		return nil, err
	}
	instance.availableNodes = nodes
	instance.log = l
	instance.loglvl = loglvl

	//  check get producer
	_, err = instance.getProducer()
	if nil != err {
		return nil, err
	}

	return instance, nil
}
