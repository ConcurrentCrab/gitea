// Copyright 2024 The Gitea Authors. All rights reserved.
// SPDX-License-Identifier: MIT

package backend

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"
	"time"

	"code.gitea.io/gitea/modules/httplib"
	"code.gitea.io/gitea/modules/lfs"
	"code.gitea.io/gitea/modules/lfstransfer/transfer"
	"code.gitea.io/gitea/modules/proxyprotocol"
	"code.gitea.io/gitea/modules/setting"
)

// HTTP headers
const (
	headerAccept        = "Accept"
	headerAuthorisation = "Authorization"
	headerContentType   = "Content-Type"
	headerContentLength = "Content-Length"
)

// MIME types
const (
	mimeGitLFS      = "application/vnd.git-lfs+json"
	mimeOctetStream = "application/octet-stream"
)

// SSH protocol argument keys
const (
	argExpiresAt = "expires-at"
	argID        = "id"
	argRefname   = "refname"
	argToken     = "token"
	argTransfer  = "transfer"
)

// Operations enum
const (
	opNone = iota
	opDownload
	opUpload
)

var opMap = map[string]int{
	"download": opDownload,
	"upload":   opUpload,
}

var (
	ErrMissingID = fmt.Errorf("%w: missing id arg", transfer.ErrMissingData)
)

// Version is the git-lfs-transfer protocol version number.
const Version = "1"

// Capabilities is a list of Git LFS capabilities supported by this package.
var Capabilities = []string{
	"version=" + Version,
	// "locking", // no support yet in gitea backend
}

var _ transfer.Backend = &GiteaBackend{}

// GiteaBackend is an adapter between git-lfs-transfer library and Gitea's internal LFS API
type GiteaBackend struct {
	ctx    context.Context
	server string
	op     string
	token  string
}

func New(ctx context.Context, token string, repo string, op string) transfer.Backend {
	// runServ guarantees repo will be in form [owner]/[name].git
	server := setting.LocalURL + "/" + repo + "/info/lfs"
	return &GiteaBackend{ctx: ctx, server: server, op: op, token: token}
}

// Batch implements transfer.Backend
func (g *GiteaBackend) Batch(_ string, pointers []transfer.BatchItem, args transfer.Args) ([]transfer.BatchItem, error) {
	reqBody := lfs.BatchRequest{Operation: g.op}
	if transfer, ok := args[argTransfer]; ok {
		reqBody.Transfers = []string{transfer}
	}
	if ref, ok := args[argRefname]; ok {
		reqBody.Ref = &lfs.Reference{Name: ref}
	}
	reqBody.Objects = make([]lfs.Pointer, len(pointers))
	for i := range pointers {
		reqBody.Objects[i].Oid = pointers[i].Oid
		reqBody.Objects[i].Size = pointers[i].Size
	}

	bodyBytes, err := json.Marshal(reqBody)
	if err != nil {
		return nil, err
	}
	url := g.server + "/objects/batch"
	headers := map[string]string{
		headerAuthorisation: g.token,
		headerAccept:        mimeGitLFS,
		headerContentType:   mimeGitLFS,
	}
	req := newInternalRequest(g.ctx, url, http.MethodPost, headers, bodyBytes)
	resp, err := req.Response()
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		return nil, statusCodeToErr(resp.StatusCode)
	}
	defer resp.Body.Close()
	respBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	var respBody lfs.BatchResponse
	json.Unmarshal(respBytes, &respBody)

	// rebuild slice, we can't rely on order in resp being the same as req
	pointers = pointers[:0]
	opNum := opMap[g.op]
	for _, obj := range respBody.Objects {
		item := transfer.BatchItem{}
		item.Oid = obj.Pointer.Oid
		item.Size = obj.Pointer.Size
		switch opNum {
		case opDownload:
			if action, ok := obj.Actions["download"]; ok {
				item.Present = true
				item.Args[argID] = action.Href
				item.Args[argToken] = action.Header[headerAuthorisation]
				item.Args[argExpiresAt] = action.ExpiresAt.String()
			} else {
				// must be an error, but the SSH protocol can't propagate individual errors
				item.Present = false
			}
		case opUpload:
			if action, ok := obj.Actions["upload"]; ok {
				item.Present = false
				item.Args[argID] = action.Href
				item.Args[argToken] = action.Header[headerAuthorisation]
				item.Args[argExpiresAt] = action.ExpiresAt.String()
			} else {
				item.Present = true
			}
		}
		pointers = append(pointers, item)
	}
	return pointers, nil
}

// Download implements transfer.Backend. The returned reader must be closed by the
// caller.
func (g *GiteaBackend) Download(oid string, args transfer.Args) (io.ReadCloser, int64, error) {
	url, exists := args[argID]
	if !exists {
		return nil, 0, ErrMissingID
	}
	headers := map[string]string{
		headerAuthorisation: g.token,
		headerAccept:        mimeOctetStream,
	}
	req := newInternalRequest(g.ctx, url, http.MethodGet, headers, nil)
	resp, err := req.Response()
	if err != nil {
		return nil, 0, err
	}
	if resp.StatusCode != http.StatusOK {
		return nil, 0, statusCodeToErr(resp.StatusCode)
	}
	defer resp.Body.Close()
	respBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, 0, err
	}
	respSize := int64(len(respBytes))
	respBuf := io.NopCloser(bytes.NewBuffer(respBytes))
	return respBuf, respSize, nil
}

// StartUpload implements transfer.Backend.
func (g *GiteaBackend) Upload(oid string, size int64, r io.Reader, args transfer.Args) error {
	url, exists := args[argID]
	if !exists {
		return ErrMissingID
	}
	headers := map[string]string{
		headerAuthorisation: g.token,
		headerContentType:   mimeOctetStream,
		headerContentLength: strconv.FormatInt(size, 10),
	}
	reqBytes, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	req := newInternalRequest(g.ctx, url, http.MethodPut, headers, reqBytes)
	resp, err := req.Response()
	if err != nil {
		return err
	}
	if resp.StatusCode != http.StatusOK {
		return statusCodeToErr(resp.StatusCode)
	}
	return nil
}

// Verify implements transfer.Backend.
func (g *GiteaBackend) Verify(oid string, size int64, args transfer.Args) (transfer.Status, error) {
	reqBody := lfs.Pointer{Oid: oid, Size: size}

	bodyBytes, err := json.Marshal(reqBody)
	if err != nil {
		return transfer.NewStatus(transfer.StatusInternalServerError), err
	}
	url, exists := args[argID]
	if !exists {
		return transfer.NewStatus(transfer.StatusBadRequest, "missing argument: id"), ErrMissingID
	}
	headers := map[string]string{
		headerAuthorisation: g.token,
		headerAccept:        mimeGitLFS,
		headerContentType:   mimeGitLFS,
	}
	req := newInternalRequest(g.ctx, url, http.MethodPost, headers, bodyBytes)
	resp, err := req.Response()
	if err != nil {
		return transfer.NewStatus(transfer.StatusInternalServerError), err
	}
	if resp.StatusCode != http.StatusOK {
		return transfer.NewStatus(uint32(resp.StatusCode), http.StatusText(resp.StatusCode)), statusCodeToErr(resp.StatusCode)
	}
	return transfer.SuccessStatus(), nil
}

// LockBackend implements transfer.Backend.
func (g *GiteaBackend) LockBackend(_ transfer.Args) transfer.LockBackend {
	// Gitea doesn't support the locking API
	// this should never be called as we don't advertise the capability
	return (transfer.LockBackend)(nil)
}

func newInternalRequest(ctx context.Context, url, method string, headers map[string]string, body []byte) *httplib.Request {
	req := httplib.NewRequest(url, method).
		SetContext(ctx).
		SetTimeout(10*time.Second, 60*time.Second).
		SetTLSClientConfig(&tls.Config{
			InsecureSkipVerify: true,
			ServerName:         setting.Domain,
		})

	if setting.Protocol == setting.HTTPUnix {
		req.SetTransport(&http.Transport{
			DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
				var d net.Dialer
				conn, err := d.DialContext(ctx, "unix", setting.HTTPAddr)
				if err != nil {
					return conn, err
				}
				if setting.LocalUseProxyProtocol {
					if err = proxyprotocol.WriteLocalHeader(conn); err != nil {
						_ = conn.Close()
						return nil, err
					}
				}
				return conn, err
			},
		})
	} else if setting.LocalUseProxyProtocol {
		req.SetTransport(&http.Transport{
			DialContext: func(ctx context.Context, network, address string) (net.Conn, error) {
				var d net.Dialer
				conn, err := d.DialContext(ctx, network, address)
				if err != nil {
					return conn, err
				}
				if err = proxyprotocol.WriteLocalHeader(conn); err != nil {
					_ = conn.Close()
					return nil, err
				}
				return conn, err
			},
		})
	}

	for k, v := range headers {
		req.Header(k, v)
	}

	req.Body(body)

	return req
}

func statusCodeToErr(code int) error {
	switch code {
	case http.StatusBadRequest:
		return transfer.ErrParseError
	case http.StatusConflict:
		return transfer.ErrConflict
	case http.StatusForbidden:
		return transfer.ErrForbidden
	case http.StatusNotFound:
		return transfer.ErrNotFound
	case http.StatusUnauthorized:
		return transfer.ErrUnauthorized
	default:
		return fmt.Errorf("server returned status %v: %v", code, http.StatusText(code))
	}
}
