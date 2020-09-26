package worker

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
)

type Config struct {
	EtcdEndpoints         []string `json:"etcdEndpoints"`
	EtcdDialTimeout       int      `json:"etcdDialTimeout"`
	MongodbUri            string   `json:"mongodbUri"`
	MongodbConnectTimeout int      `json:"mongodbConnectTimeout"`
	JobLogBatchSize       int      `json:"jobLogBatchSize"`
	JobLogCommitTimeout   int      `json:"jobLogCommitTimeout"`
}

var (
	G_config *Config
)

func InitConfig(filename string) (err error) {
	var (
		conf    Config
		content []byte
	)

	if content, err = ioutil.ReadFile(filename); err != nil {
		return
	}

	if err = json.Unmarshal(content, &conf); err != nil {
		return
	}

	G_config = &conf

	fmt.Println(conf)

	return

}
