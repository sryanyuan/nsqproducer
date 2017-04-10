package nsqproducer

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"strings"
)

func DoHTTPGet(reqURL string, args map[string]string) ([]byte, error) {
	u, _ := url.Parse(strings.Trim(reqURL, "/"))
	q := u.Query()
	if nil != args {
		for arg, val := range args {
			q.Add(arg, val)
		}
	}

	u.RawQuery = q.Encode()
	res, err := http.Get(u.String())
	if err != nil {
		return nil, err
	}

	if res.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("Http statusCode:%d", res.StatusCode)
	}

	result, err := ioutil.ReadAll(res.Body)
	res.Body.Close()
	if err != nil {
		return nil, err
	}

	return result, nil
}
