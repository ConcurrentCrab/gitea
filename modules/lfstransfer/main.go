// Copyright 2024 The Gitea Authors. All rights reserved.
// SPDX-License-Identifier: MIT

package lfstransfer

import (
	"context"
	"fmt"
	"io"
	"os"
	"runtime/debug"

	"code.gitea.io/gitea/modules/lfstransfer/backend"
	"github.com/charmbracelet/git-lfs-transfer/transfer"
)

func Main(ctx context.Context, repo string, verb string, token string) error {
	f, _ := os.OpenFile("/tmp/lfs.log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	defer f.Close()
	logger := newLogger(fmt.Sprintf("PID [%05d]", os.Getpid()), f)
	t, _ := os.OpenFile(fmt.Sprintf("/tmp/lfs/%v.log", os.Getpid()), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	defer t.Close()
	in := io.TeeReader(os.Stdin, t)
	out := io.MultiWriter(os.Stdout, t)
	pktline := transfer.NewPktline(in, out, logger)
	giteaBackend, err := backend.New(ctx, repo, verb, token, logger)
	if err != nil {
		return err
	}

	defer func() {
		if err := recover(); err != nil {
			logger.Log("got panic", err)
			logger.Log(string(debug.Stack()))
			panic(err)
		}
	}()

	for _, cap := range backend.Capabilities {
		if err := pktline.WritePacketText(cap); err != nil {
			logger.Log("error sending capability due to error:", err)
		}
	}
	if err := pktline.WriteFlush(); err != nil {
		logger.Log("error flushing capabilities:", err)
	}
	p := transfer.NewProcessor(pktline, giteaBackend, logger)
	defer logger.Log("done processing commands")
	switch verb {
	case "upload":
		return p.ProcessCommands(transfer.UploadOperation)
	case "download":
		return p.ProcessCommands(transfer.DownloadOperation)
	default:
		return fmt.Errorf("unknown operation %q", verb)
	}
}
