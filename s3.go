package storager

import (
	"context"
	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	smithyendpoints "github.com/aws/smithy-go/endpoints"
	"github.com/csturiale/logger"
	"io"
	"net/url"
	"strings"
)

type s3Storage struct {
	endpoint  string
	accessKey string
	secretKey string
	bucket    string
	domain    string
	client    *s3.Client
}
type resolverV2 struct{}

func (*resolverV2) ResolveEndpoint(ctx context.Context, params s3.EndpointParameters) (
	smithyendpoints.Endpoint, error,
) {
	// s3.Options.BaseEndpoint is accessible here:
	logger.Infof("The endpoint provided in config is %s\n", *params.Endpoint)

	// fallback to default
	return s3.NewDefaultEndpointResolverV2().ResolveEndpoint(ctx, params)
}

func NewS3Storager(storage string, domain string) (Storage, error) {
	r := strings.Split(storage, "://")
	if len(r) != 2 {
		logger.Fatal("Invalid S3 storage config")
	}
	details := strings.Split(r[1], ":")
	if len(details) != 4 {
		logger.Fatal("Invalid S3 storage config")
	}
	endpoint := details[0]
	accessKey := details[1]
	secretKey := details[2]
	bucket := details[3]
	u, err := url.Parse(endpoint)
	if err != nil {
		return nil, err
	}
	if u.Scheme == "" {
		u.Scheme = "https"
	}
	cfg, err := config.LoadDefaultConfig(context.Background())
	if err != nil {
		logger.Fatal(err)
	}
	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(u.String())
		o.EndpointResolverV2 = &resolverV2{}
	})

	return &s3Storage{
		endpoint:  endpoint,
		accessKey: accessKey,
		secretKey: secretKey,
		bucket:    bucket,
		domain:    domain,
		client:    client,
	}, nil
}

func (s *s3Storage) Save(name string, reader io.Reader) error {
	r := io.NopCloser(reader) // avoid oss SDK to close reader
	_, err := s.client.PutObject(context.Background(), &s3.PutObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(name),
		Body:   r,
	}, s3.WithAPIOptions(
		v4.SwapComputePayloadSHA256ForUnsignedPayloadMiddleware,
	))
	return err
}

func (s *s3Storage) OpenFile(name string) (io.ReadCloser, error) {
	out, err := s.client.GetObject(context.Background(), &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(name),
	})
	return out.Body, err
}

func (s *s3Storage) Delete(name string) error {
	_, err := s.client.DeleteObject(context.Background(), &s3.DeleteObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(name),
	})
	return err
}

func (s *s3Storage) Move(src, dest string) error {
	_, err := s.client.CopyObject(context.Background(), &s3.CopyObjectInput{
		Bucket:     aws.String(s.bucket),
		CopySource: aws.String(s.bucket + "/" + src),
		Key:        aws.String(dest),
	})
	if err != nil {
		return err
	}
	return s.Delete(src)
}

func (s *s3Storage) GetFile(name string) (io.Reader, error) {
	out, err := s.client.GetObject(context.Background(), &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(name),
	})
	return out.Body, err
}
