// Copyright 2024 The Gitea Authors. All rights reserved.
// SPDX-License-Identifier: MIT

package backend

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"

	git_model "code.gitea.io/gitea/models/git"
	repo_model "code.gitea.io/gitea/models/repo"
	"code.gitea.io/gitea/modules/lfs"
	"code.gitea.io/gitea/modules/lfstransfer/transfer"
)

// Version is the git-lfs-transfer protocol version number.
const Version = "1"

// Capabilities is a list of Git LFS capabilities supported by this package.
var Capabilities = []string{
	"version=" + Version,
	// "locking", // no support yet in gitea backend
}

// GiteaBackend is an adapter between git-lfs-transfer library and Gitea's internal LFS API
type GiteaBackend struct {
	ctx   context.Context
	repo  *repo_model.Repository
	store *lfs.ContentStore
}

var _ transfer.Backend = &GiteaBackend{}

// Batch implements transfer.Backend
func (g *GiteaBackend) Batch(_ string, pointers []transfer.BatchItem, _ transfer.Args) ([]transfer.BatchItem, error) {
	for i := range pointers {
		pointers[i].Present = false
		pointer := lfs.Pointer{Oid: pointers[i].Oid, Size: pointers[i].Size}
		exists, err := g.store.Verify(pointer)
		if err != nil || !exists {
			continue
		}
		accessible, err := g.repoHasAccess(pointers[i].Oid)
		if err != nil || !accessible {
			continue
		}
		pointers[i].Present = true
	}
	return pointers, nil
}

// Download implements transfer.Backend. The returned reader must be closed by the
// caller.
func (g *GiteaBackend) Download(oid string, _ transfer.Args) (io.ReadCloser, int64, error) {
	pointer := lfs.Pointer{Oid: oid}
	pointer, err := g.store.GetMeta(pointer)
	if errors.Is(err, lfs.ErrObjectNotInStore) {
		return nil, 0, transfer.ErrNotFound
	}
	if err != nil {
		return nil, 0, err
	}
	obj, err := g.store.Get(pointer)
	if err != nil {
		return nil, 0, err
	}
	accessible, err := g.repoHasAccess(oid)
	if err != nil {
		return nil, 0, err
	}
	if !accessible {
		return nil, 0, transfer.ErrNotFound
	}
	return obj, pointer.Size, nil
}

// StartUpload implements transfer.Backend.
func (g *GiteaBackend) Upload(oid string, size int64, r io.Reader, _ transfer.Args) error {
	if r == nil {
		return fmt.Errorf("%w: received null data", transfer.ErrMissingData)
	}
	pointer := lfs.Pointer{Oid: oid, Size: size}
	exists, err := g.store.Verify(pointer)
	if err != nil {
		return err
	}
	if exists {
		accessible, err := g.repoHasAccess(oid)
		if err != nil {
			return err
		}
		if accessible {
			// we already have this object in the store and metadata
			return nil
		}
		// we have this object in the store but not accessible
		// so verify hash and size, and add it to metadata
		hash := sha256.New()
		written, err := io.Copy(hash, r)
		if err != nil {
			return fmt.Errorf("error hashing data: %w", err)
		}
		recvOid := hex.EncodeToString(hash.Sum(nil))
		if written != size {
			return fmt.Errorf("%w: size mismatch: expected %v", transfer.ErrCorruptData, size)
		}
		if recvOid != oid {
			return fmt.Errorf("%w: OID mismatch: expected %v", transfer.ErrCorruptData, oid)
		}
	} else {
		err = g.store.Put(pointer, r)
		if errors.Is(err, lfs.ErrSizeMismatch) {
			return fmt.Errorf("%w: size mismatch: expected %v", transfer.ErrCorruptData, size)
		}
		if errors.Is(err, lfs.ErrHashMismatch) {
			return fmt.Errorf("%w: OID mismatch: expected %v", transfer.ErrCorruptData, oid)
		}
		if err != nil {
			return err
		}
	}
	_, err = git_model.NewLFSMetaObject(g.ctx, g.repo.ID, pointer)
	if err != nil {
		return fmt.Errorf("could not create LFS Meta Object: %w", err)
	}
	return nil
}

// Verify implements transfer.Backend.
func (g *GiteaBackend) Verify(oid string, size int64, args transfer.Args) (transfer.Status, error) {
	pointer := lfs.Pointer{Oid: oid, Size: size}
	exists, err := g.store.Verify(pointer)
	if err != nil {
		return transfer.NewStatus(transfer.StatusNotFound, "not found"), err
	}
	if !exists {
		return transfer.NewStatus(transfer.StatusNotFound, "not found"), transfer.ErrNotFound
	}
	accessible, err := g.repoHasAccess(oid)
	if err != nil {
		return transfer.NewStatus(transfer.StatusNotFound, "not found"), err
	}
	if !accessible {
		return transfer.NewStatus(transfer.StatusNotFound, "not found"), transfer.ErrNotFound
	}
	return transfer.SuccessStatus(), nil
}

// LockBackend implements transfer.Backend.
func (g *GiteaBackend) LockBackend(_ transfer.Args) transfer.LockBackend {
	// Gitea doesn't support the locking API
	// this should never be called as we don't advertise the capability
	return (transfer.LockBackend)(nil)
}

// repoHasAccess checks if the repo already has the object with OID stored
func (g *GiteaBackend) repoHasAccess(oid string) (bool, error) {
	// check if OID is in global LFS store
	exists, err := g.store.Exists(lfs.Pointer{Oid: oid})
	if err != nil || !exists {
		return false, err
	}
	// check if OID is in repo LFS store
	metaObj, err := git_model.GetLFSMetaObjectByOid(g.ctx, g.repo.ID, oid)
	if err != nil || metaObj == nil {
		return false, err
	}
	return true, nil
}

func New(ctx context.Context, r *repo_model.Repository, s *lfs.ContentStore) transfer.Backend {
	return &GiteaBackend{ctx: ctx, repo: r, store: s}
}
