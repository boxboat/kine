package kv

import (
	"fmt"
	"github.com/nats-io/nats.go"
	"github.com/shengdoushi/base58"
	"strings"
)

// EtcdCodec turns keys like /this/is/a.test.key into Base58 encoded values split on `/`
type EtcdCodec struct{}

var (
	keyAlphabet = base58.BitcoinAlphabet
)

func (e *EtcdCodec) EncodeRange(keys string) (string, error) {
	ek, err := e.Encode(keys)
	if err != nil {
		return "", err
	}
	if strings.HasSuffix(ek, "."){
		return fmt.Sprintf("%s>", ek), nil
	}
	return ek, nil
}

func (*EtcdCodec) Encode(key string) (retKey string, e error) {
	//defer func() {
	//	logrus.Debugf("encoded %s => %s", key, retKey)
	//}()
	parts := []string{}
	for _, part := range strings.Split(strings.TrimPrefix(key, "/"), "/") {
		if part == ">" || part == "*" {
			parts = append(parts, part)
			continue
		}
		parts = append(parts, base58.Encode([]byte(part), keyAlphabet))
	}

	if len(parts) == 0 {
		return "", nats.ErrInvalidKey
	}

	return strings.Join(parts, "."), nil
}

func (*EtcdCodec) Decode(key string) (retKey string, e error) {
	//defer func() {
	//	logrus.Debugf("decoded %s => %s", key, retKey)
	//}()
	parts := []string{}
	for _, s := range strings.Split(key, ".") {
		decodedPart, err := base58.Decode(s, keyAlphabet)
		if err != nil {
			return "", err
		}
		parts = append(parts, string(decodedPart[:]))
	}
	if len(parts) == 0 {
		return "", nats.ErrInvalidKey
	}
	return fmt.Sprintf("/%s", strings.Join(parts, "/")), nil
}