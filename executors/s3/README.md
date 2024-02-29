# Venom - Executor S3

Manage you S3 via venom

## Input

### Create Bucket

```yaml
- type: s3
  command: create-or-reset-bucket
  endpoint: https://endpoint/
  user: user
  password: password
  use_ssl: false
  bucket_name: mybucket
```

### Read Object

```yaml
- type: s3
  command: read-object
  endpoint: https://endpoint/
  user: user
  password: password
  use_ssl: false
  bucket_name: mybucket
  object_name: myobject.txt
  assertions:
    - result.object_content ShouldEqual foo
```

### Write Object

```yaml
- type: s3
  command: write-object
  endpoint: https://endpoint/
  user: user
  password: password
  use_ssl: false
  bucket_name: mybucket
  object_name: myobject.txt
  obeject_content: foo
```

### Delete Object

```yaml
- type: s3
  command: delete-object
  endpoint: https://endpoint/
  user: user
  password: password
  use_ssl: false
  bucket_name: mybucket
  object_name: myobject.txt
```
