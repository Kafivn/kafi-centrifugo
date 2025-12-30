package consuming

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	g "github.com/tidwall/gjson"
	"github.com/twmb/franz-go/pkg/kgo"
)

type marketDataRecord struct {
	Channel string          `json:"channel"`
	Data    json.RawMessage `json:"data"`
}

func (pc *partitionConsumer) isMarketDataTopic(topic string) bool {
	return strings.HasPrefix(topic, "market.")
}

func (pc *partitionConsumer) processMarketDataRecord(ctx context.Context, record *kgo.Record) error {
	result := g.ParseBytes(record.Value)
	// Get the data object as raw JSON
	dataField := result.Get("data")
	if !dataField.Exists() {
		return fmt.Errorf("'data' field not found in message")
	}
	// Determine channel based on topic type
	key := ""
	channel := record.Topic

	switch record.Topic {
	case "market.quote", "market.bidoffer", "market.quote.oddlot":
		key = "s"
	case "market.dealNotice", "market.advertised":
		key = "m"

	// Logic mdds
	// SendKafka(transactionId, "market.extra", "Update", extraQuote, extraQuote.S)
	// SendRedis("market.bidoffer", extraQuote, transactionId)
	case "market.extra":
		key = "s"
		channel = "market.bidoffer"
	}

	if key != "" {
		f := dataField.Get(key)
		if !f.Exists() {
			return fmt.Errorf("key %s not found in market data", key)
		}
		channel += "." + f.String()
	}

	payload := marketDataRecord{
		Channel: channel,
		Data:    json.RawMessage(dataField.Raw),
	}

	jsonData, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("failed to marshal publish payload: %w", err)
	}

	return pc.dispatcher.DispatchCommand(ctx, "publish", jsonData)
}
