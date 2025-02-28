package artifact_storage

import (
	"bufio"
	"github.com/spf13/afero"
	"io"
	"path/filepath"
)

type oferoStorage struct {
	fs afero.Fs
}

const (
	oferoStoragerDirPerm = 0755
	WRITER_BUFFER_SIZE   = 1024 * 1024 * 2 // 1M
)

var _ Storage = (*oferoStorage)(nil)

func NewAferoStorager(fs afero.Fs) Storage {
	return &oferoStorage{fs: fs}
}

func NewOsFileStorager(basepath string) Storage {
	return NewAferoStorager(afero.NewBasePathFs(afero.NewOsFs(), basepath))
}

func NewMemStorager() Storage {
	return NewAferoStorager(afero.NewMemMapFs())
}

func (f *oferoStorage) Save(name string, reader io.Reader) error {
	dir := filepath.Dir(name)
	if err := f.fs.MkdirAll(dir, oferoStoragerDirPerm); err != nil {
		return err
	}
	fi, err := f.fs.Create(name)
	defer func() {
		_ = fi.Close()
	}()
	if err != nil {
		return err
	}

	// write with buffer
	w := bufio.NewWriterSize(fi, WRITER_BUFFER_SIZE)
	_, err = io.Copy(w, reader)
	if err != nil {
		return err
	}

	err = w.Flush()
	if err != nil {
		return err
	}

	return err
}

func (f *oferoStorage) OpenFile(name string) (io.ReadCloser, error) {
	return f.fs.Open(name)
}

func (f *oferoStorage) Delete(name string) error {
	err := f.fs.Remove(name)
	if err != nil {
		return err
	}
	// auto delete empty dir
	err = f.deleteEmptyDir(filepath.Dir(name))
	if err != nil {
		// NOTE: ignore error
	}
	return nil
}

func (f *oferoStorage) deleteEmptyDir(name string) error {
	name = filepath.Clean(name)
	if name == "." {
		return nil
	}

	err := f.fs.Remove(name)
	if err != nil {
		return err
	}

	return f.deleteEmptyDir(filepath.Dir(name))
}

func (f *oferoStorage) Move(src, dest string) error {
	err := f.fs.MkdirAll(filepath.Dir(dest), oferoStoragerDirPerm)
	if err != nil {
		return err
	}
	return f.fs.Rename(src, dest)
}

func (f *oferoStorage) GetFile(name string) (io.Reader, error) {
	file, e := f.fs.Open(name)
	if e != nil {
		return nil, e
	}
	return bufio.NewReader(file), nil
}
