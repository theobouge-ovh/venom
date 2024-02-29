package s3

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log"

	minio "github.com/minio/minio-go/v7"
	credentials "github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/ovh/venom"
)

// Name for test ssh
const Name = "s3"

const (
	CommandNewBucket    = "create-or-reset-bucket"
	CommandReadObject   = "read-object"
	CommandWriteObject  = "write-object"
	CommandDeleteObject = "delete-object"
)

func AvailableCommands() []string {
	return []string{CommandNewBucket}
}

func New() venom.Executor {
	return &Executor{}
}

type Executor struct {
	Endpoint      string `json:"endpoint,omitempty" yaml:"endpoint,omitempty"`
	Command       string `json:"command,omitempty" yaml:"command,omitempty"`
	User          string `json:"user,omitempty" yaml:"user,omitempty"`
	Password      string `json:"password,omitempty" yaml:"password,omitempty"`
	UseSSL        bool   `json:"use_ssl,omitempty" yaml:"use_ssl,omitempty"`
	BucketName    string `json:"bucket_name,omitempty" yaml:"bucket_name,omitempty"`
	ObjectName    string `json:"object_name,omitempty" yaml:"object_name,omitempty"`
	ObjectContent string `json:"object_content,omitempty" yaml:"object_content,omitempty"`
}

type Result struct {
	ObjectName    string `json:"object_name" yaml:"object_name"`
	ObjectContent string `json:"object_content" yaml:"object_content"`
	ObjectETag    string `json:"object_etag" yaml:"object_etag"`
}

func (e Executor) Run(ctx context.Context, step venom.TestStep) (interface{}, error) {
	switch e.Command {
	case CommandNewBucket:
		return e.CreateOrResetBucket(ctx, step)
	case CommandReadObject:
		return e.ReadObject(ctx, step)
	case CommandWriteObject:
		return e.WriteObject(ctx, step)
	default:
		return nil, fmt.Errorf("%s is not a valid command, available commands are %v", e.Command, AvailableCommands())
	}
}

func (e Executor) CreateOrResetBucket(ctx context.Context, step venom.TestStep) (*Result, error) {
	minioClient, err := e.Connect(ctx, step)
	if err != nil {
		return nil, fmt.Errorf("error creating minio client: %v", err)
	}

	isAlreadyThere, err := minioClient.BucketExists(ctx, e.BucketName)
	if err != nil {
		return nil, fmt.Errorf("error checking if bucket %s exists: %v", e.BucketName, err)
	}
	if !isAlreadyThere {
		err = minioClient.MakeBucket(ctx, e.BucketName, minio.MakeBucketOptions{})
		if err != nil {
			return nil, fmt.Errorf("error creating bucket %s: %v", e.BucketName, err)
		}
		return &Result{}, nil
	}

	objectsCh := make(chan minio.ObjectInfo)
	go func() {
		defer close(objectsCh)
		for object := range minioClient.ListObjects(ctx, e.BucketName, minio.ListObjectsOptions{Recursive: true}) {
			if object.Err != nil {
				log.Fatalln(object.Err)
			}
			objectsCh <- object
		}
	}()
	errorCh := minioClient.RemoveObjects(ctx, e.BucketName, objectsCh, minio.RemoveObjectsOptions{})
	for err := range errorCh {
		return nil, fmt.Errorf("error removing object %s from bucket %s: %v", e.ObjectName, e.BucketName, err)
	}
	return &Result{}, nil
}

func (e Executor) Connect(_ context.Context, _ venom.TestStep) (*minio.Client, error) {
	return minio.New(e.Endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(e.User, e.Password, ""),
		Secure: e.UseSSL,
	})
}

func (e Executor) ReadObject(ctx context.Context, step venom.TestStep) (*Result, error) {
	minioClient, err := e.Connect(ctx, step)
	if err != nil {
		return nil, fmt.Errorf("error creating minio client: %v", err)
	}

	report, err := minioClient.GetObject(ctx, e.BucketName, e.ObjectName, minio.GetObjectOptions{})
	if err != nil {
		return nil, fmt.Errorf("error getting object %s from bucket %s: %v", e.ObjectName, e.BucketName, err)
	}
	defer report.Close()

	data, err := io.ReadAll(report)
	if err != nil {
		return nil, fmt.Errorf("error reading object %s from bucket %s: %v", e.ObjectName, e.BucketName, err)
	}
	return &Result{ObjectContent: string(data), ObjectName: e.ObjectName}, nil
}

func (e Executor) WriteObject(ctx context.Context, step venom.TestStep) (*Result, error) {
	minioClient, err := e.Connect(ctx, step)
	if err != nil {
		return nil, fmt.Errorf("error creating minio client: %v", err)
	}

	reader := bytes.NewReader([]byte(e.ObjectContent))
	length := int64(len(e.ObjectContent))
	report, err := minioClient.PutObject(ctx, e.BucketName, e.ObjectName, reader, length, minio.PutObjectOptions{})
	if err != nil {
		return nil, fmt.Errorf("error writing object %s to bucket %s: %v", e.ObjectName, e.BucketName, err)
	}
	return &Result{ObjectContent: e.ObjectContent, ObjectName: e.ObjectName, ObjectETag: report.ETag}, nil
}

func (e Executor) DeleteObject(ctx context.Context, step venom.TestStep) (*Result, error) {
	minioClient, err := e.Connect(ctx, step)
	if err != nil {
		return nil, fmt.Errorf("error creating minio client: %v", err)
	}

	err = minioClient.RemoveObject(ctx, e.BucketName, e.ObjectName, minio.RemoveObjectOptions{})
	if err != nil {
		return nil, fmt.Errorf("error deleting object %s from bucket %s: %v", e.ObjectName, e.BucketName, err)
	}
	return &Result{}, nil
}
