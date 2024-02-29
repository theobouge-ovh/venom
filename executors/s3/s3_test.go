package s3_test

import (
	"context"
	"testing"

	"github.com/ovh/venom"
	"github.com/ovh/venom/executors/s3"
	"github.com/tj/assert"
)

func TestExcutor_Connect(t *testing.T) {
	executor := s3.Executor{
		Endpoint:   addr,
		User:       Username,
		Password:   Password,
		BucketName: BucketName,
	}

	_, err := executor.Connect(context.Background(), venom.TestStep{})
	if err != nil {
		panic(err)
	}
}

func TestExcutor_ResetBucket(t *testing.T) {
	executor := s3.Executor{
		Endpoint:   addr,
		User:       Username,
		Password:   Password,
		BucketName: BucketName,
	}

	_, err := executor.CreateOrResetBucket(context.Background(), venom.TestStep{})
	assert.Nil(t, err)
}

func TestExcutor_CreateBucket(t *testing.T) {
	executor := s3.Executor{
		Endpoint:   addr,
		User:       Username,
		Password:   Password,
		BucketName: BucketName + "newbucket",
	}

	_, err := executor.CreateOrResetBucket(context.Background(), venom.TestStep{})
	assert.Nil(t, err)
}

func TestExcutor_WriteObject(t *testing.T) {
	executor := s3.Executor{
		Endpoint:      addr,
		User:          Username,
		Password:      Password,
		BucketName:    BucketName,
		ObjectName:    "test",
		ObjectContent: "Hello World!",
	}

	_, err := executor.WriteObject(context.Background(), venom.TestStep{})
	assert.Nil(t, err)
}

func TestExecutor_ReadObject(t *testing.T) {
	executor := s3.Executor{
		Endpoint:      addr,
		User:          Username,
		Password:      Password,
		BucketName:    BucketName,
		ObjectName:    "test",
		ObjectContent: "Hello World!",
	}

	_, err := executor.WriteObject(context.Background(), venom.TestStep{})
	assert.Nil(t, err)

	result, err := executor.ReadObject(context.Background(), venom.TestStep{})
	assert.Nil(t, err)

	assert.EqualValues(t, result, &s3.Result{
		ObjectName:    "test",
		ObjectContent: "Hello World!",
	})
}

func TestExecutor_DeleteObject(t *testing.T) {
	executor := s3.Executor{
		Endpoint:      addr,
		User:          Username,
		Password:      Password,
		BucketName:    BucketName,
		ObjectName:    "test",
		ObjectContent: "Hello World!",
	}

	_, err := executor.WriteObject(context.Background(), venom.TestStep{})
	assert.Nil(t, err)

	_, err = executor.DeleteObject(context.Background(), venom.TestStep{})
	assert.Nil(t, err)
}
