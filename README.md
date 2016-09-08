# nsqproducer
nsqproducer is a wrap for nsq.Producer but using nsqlookupd to find available endpoint

## purpose

nsq.Producer use one nsqd endpoint to publish message.

nsqproducer is a wrap for nsq.Producer, but using nsqlookupd to find all available endpoint.When current using nsqd is unavailable, it will auto search for the next available one.

Once all endpoint is unavailable, it will pull nsqd node list from nsqlookupd again.

## usage

	nsqlookupdAddrs := []string{"127.0.0.1:4161"}
	producer, err := nsqproducer.NewNSQProducer(nsqlookupdAddrs, cfg, &nsqDebugLogger, nsq.LogLevelDebug)
	if nil != err {
		return err
	}
	producer.Publish(topic, body)