package artifact_storage

import (
	"io"
)

type Storage interface {
	Save(name string, reader io.Reader) error
	OpenFile(name string) (io.ReadCloser, error)
	Delete(name string) error
	Move(src, dest string) error
	GetFile(name string) (io.Reader, error)
}
