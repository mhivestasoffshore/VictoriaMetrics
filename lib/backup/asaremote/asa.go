package asaremote

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/url"
	"strings"

	"github.com/Azure/azure-storage-blob-go/azblob"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/backup/common"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/backup/fscommon"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
)

// FS represents file system for backup in Azure Storage Account
type FS struct {
	// Azure storage account access key
	AccessKey string

	// StorageAccount name
	StorageAccount string

	// Storage account blob container
	Container string

	credentials  azblob.StorageAccountCredential
	containerURL azblob.ContainerURL
}

// Init initialize the fs structure.
func (fs *FS) Init() error {
	credential, err := azblob.NewSharedKeyCredential(fs.StorageAccount, fs.AccessKey)
	if err != nil {
		return fmt.Errorf("Unable to create credential: %s", err)
	}
	fs.credentials = credential

	url, err := url.Parse(fmt.Sprintf("https://%s.blob.core.windows.net", fs.StorageAccount))
	if err != nil {
		return fmt.Errorf("Invalid blob url: %s", err)
	}
	pip := azblob.NewPipeline(credential, azblob.PipelineOptions{})
	serviceURL := azblob.NewServiceURL(*url, pip)
	fs.containerURL = serviceURL.NewContainerURL(fs.Container)
	return nil
}

// String returns human-readable description for fs.
func (fs *FS) String() string {
	return fmt.Sprintf("ASA{sa: %q, container: %q}", fs.StorageAccount, fs.Container)
}

// ListParts must return all the parts for the RemoteFS.
func (fs *FS) ListParts() ([]common.Part, error) {
	var parts []common.Part
	ctx := context.Background()
	for marker := (azblob.Marker{}); marker.NotDone(); {
		listBlob, err := fs.containerURL.ListBlobsFlatSegment(ctx, marker, azblob.ListBlobsSegmentOptions{})
		if err != nil {
			return nil, fmt.Errorf("Unable to list blobs: %s", err)
		}
		marker = listBlob.NextMarker
		for _, b := range listBlob.Segment.BlobItems {
			var p common.Part
			if fscommon.IgnorePath(b.Name) {
				continue
			}
			if !p.ParseFromRemotePath(b.Name) {
				logger.Infof("skipping unknown object %q", b.Name)
				continue
			}
			p.ActualSize = uint64(*b.Properties.ContentLength)
			parts = append(parts, p)
		}
	}
	return parts, nil
}

// DeletePart must delete part p from RemoteFS.
func (fs *FS) DeletePart(p common.Part) error {
	path := fs.path(p)
	return fs.DeleteFile(path)
}

// RemoveEmptyDirs recursively removes empty dirs in fs.
func (fs *FS) RemoveEmptyDirs() error {
	// N/A to Azure Storage Account blob storage
	// RemoveEmptyDirs must recursively remove empty directories in RemoteFS.
	return nil
}

// CopyPart must copy part p from srcFS to RemoteFS.
func (fs *FS) CopyPart(srcFS common.OriginFS, p common.Part) error {
	src, ok := srcFS.(*FS)
	if !ok {
		return fmt.Errorf("cannot perform server-side copying from %s to %s: both of them must be Azure Storage", srcFS, fs)
	}
	srcPath := src.path(p)
	srcUrl := src.containerURL.NewBlobURL(srcPath).URL()
	sas := azblob.BlobSASSignatureValues{}
	sasqp, err := sas.NewSASQueryParameters(fs.credentials)
	srcUrl.RawQuery = sasqp.Encode()

	dstPath := fs.path(p)
	ctx := context.Background()
	_, err = fs.containerURL.NewBlobURL(dstPath).StartCopyFromURL(ctx, srcUrl, azblob.Metadata{}, azblob.ModifiedAccessConditions{}, azblob.BlobAccessConditions{})
	if err != nil {
		return fmt.Errorf("cannot copy %q from %s to %s: %s", p.Path, srcPath, dstPath, err)
	}
	return nil
}

// DownloadPart must download part p from RemoteFS to w.
func (fs *FS) DownloadPart(p common.Part, w io.Writer) error {
	path := fs.path(p)
	ctx := context.Background()
	resp, err := fs.containerURL.NewBlockBlobURL(path).Download(ctx, 0, 0, azblob.BlobAccessConditions{}, false)
	if err != nil {
		return fmt.Errorf("cannot download blob %q: %s", path, err)
	}
	r := resp.Body(azblob.RetryReaderOptions{})
	n, err := io.Copy(w, r)
	if err1 := r.Close(); err1 != nil && err == nil {
		err = err1
	}
	if err != nil {
		return fmt.Errorf("cannot download %q from at %s (remote path %q): %s", p.Path, fs, path, err)
	}
	if uint64(n) != p.Size {
		return fmt.Errorf("wrong data size downloaded from %q at %s; got %d bytes; want %d bytes", p.Path, fs, n, p.Size)
	}
	return nil
}

// UploadPart must upload part p from r to RemoteFS.
func (fs *FS) UploadPart(p common.Part, r io.Reader) error {
	path := fs.path(p)
	sr := &statReader{
		r: r,
	}
	ctx := context.Background()
	blob := fs.containerURL.NewBlockBlobURL(path)
	_, err := azblob.UploadStreamToBlockBlob(ctx, sr, blob, azblob.UploadStreamToBlockBlobOptions{})
	if err != nil {
		return fmt.Errorf("cannot upoad data to %q at %s (remote path %q): %s", p.Path, fs, path, err)
	}
	if uint64(sr.size) != p.Size {
		return fmt.Errorf("wrong data size uploaded to %q at %s; got %d bytes; want %d bytes", p.Path, fs, sr.size, p.Size)
	}
	return nil
}

// DeleteFile deletes filePath at RemoteFS
func (fs *FS) DeleteFile(filePath string) error {
	ctx := context.Background()
	_, err := fs.containerURL.NewBlobURL(filePath).Delete(ctx, azblob.DeleteSnapshotsOptionNone, azblob.BlobAccessConditions{})
	if err != nil {
		return fmt.Errorf("cannot delete %q: %s", filePath, err)
	}
	return nil
}

// CreateFile creates filePath at RemoteFS and puts data into it.
func (fs *FS) CreateFile(filePath string, data []byte) error {
	sr := &statReader{
		r: bytes.NewReader(data),
	}
	ctx := context.Background()
	blob := fs.containerURL.NewBlockBlobURL(filePath)
	_, err := azblob.UploadStreamToBlockBlob(ctx, sr, blob, azblob.UploadStreamToBlockBlobOptions{})
	if err != nil {
		return fmt.Errorf("cannot upoad data to %q at %s: %s", filePath, fs, err)
	}
	l := int64(len(data))
	if sr.size != l {
		return fmt.Errorf("wrong data size uploaded to %q at %s; got %d bytes; want %d bytes", filePath, fs, sr.size, l)
	}
	return nil
}

// HasFile returns true if filePath exists at RemoteFS.
func (fs *FS) HasFile(filePath string) (bool, error) {
	ctx := context.Background()
	p, err := fs.containerURL.NewBlobURL(filePath).GetProperties(ctx, azblob.BlobAccessConditions{})
	if err == nil && p.StatusCode() != 404 {
		return true, nil
	}
	return false, nil
}

func (fs *FS) path(p common.Part) string {
	path := p.RemotePath("")
	for strings.HasPrefix(path, "/") {
		path = path[1:]
	}
	return path
}

type statReader struct {
	r    io.Reader
	size int64
}

func (sr *statReader) Read(p []byte) (int, error) {
	n, err := sr.r.Read(p)
	sr.size += int64(n)
	return n, err
}
