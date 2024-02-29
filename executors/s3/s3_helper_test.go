package s3_test

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"os"
	"testing"
	"time"

	madmin "github.com/minio/madmin-go/v3"
	mclient "github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	minio "github.com/minio/minio/cmd"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
)

// https://github.com/draganm/miniotest/blob/master/miniotest_test.go

const (
	Username   = "minioadmin"
	Password   = Username
	BucketName = "test"
)

func StartEmbedded() (string, func() error, error) {
	l, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		return "", nil, errors.Wrap(err, "while creating listener")
	}

	addr := l.Addr().String()
	err = l.Close()
	if err != nil {
		return "", nil, errors.Wrap(err, "while closing listener")
	}

	accessKeyID := "minioadmin"
	secretAccessKey := "minioadmin"

	madm, err := madmin.New(addr, accessKeyID, secretAccessKey, false)
	if err != nil {
		return "", nil, errors.Wrap(err, "while creating madimin")
	}

	td, err := os.MkdirTemp("", "")
	if err != nil {
		return "", nil, errors.Wrap(err, "while creating temp dir")
	}

	go minio.Main([]string{"minio", "server", "--quiet", "--address", addr, td})
	time.Sleep(500 * time.Millisecond)

	client, err := mclient.New(addr, &mclient.Options{
		Creds:  credentials.NewStaticV4(accessKeyID, secretAccessKey, ""),
		Secure: false,
	})
	if err != nil {
		return "", nil, errors.Wrap(err, "while creating client")
	}

	err = client.MakeBucket(context.Background(), BucketName, mclient.MakeBucketOptions{})
	if err != nil {
		return "", nil, errors.Wrap(err, "while creating bucket")
	}

	return addr, func() error {
		err := madm.ServiceStop(context.Background())
		if err != nil {
			return errors.Wrap(err, "while stopping service")
		}

		err = os.RemoveAll(td)
		if err != nil {
			return errors.Wrap(err, "while deleting temp dir")
		}

		return nil
	}, nil
}

func Test_PutObject(t *testing.T) {
	mc, err := mclient.New(addr, &mclient.Options{
		Creds:  credentials.NewStaticV4(Username, Password, ""),
		Secure: false,
	})
	require.NoError(t, err)

	data := []byte("test")

	_, err = mc.PutObject(context.Background(), BucketName, "foo/var", bytes.NewReader(data), int64(len(data)), mclient.PutObjectOptions{})
	require.NoError(t, err)
}

var addr string

func TestMain(m *testing.M) {
	var cleanup func() error
	var err error
	addr, cleanup, err = StartEmbedded()
	if err != nil {
		fmt.Fprintf(os.Stderr, "while starting embedded server: %s", err)
		os.Exit(1)
	}

	exitCode := m.Run()

	err = cleanup()
	if err != nil {
		fmt.Fprintf(os.Stderr, "while stopping embedded server: %s", err)
	}

	os.Exit(exitCode)
}
