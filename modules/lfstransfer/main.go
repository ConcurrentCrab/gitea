// Copyright 2024 The Gitea Authors. All rights reserved.
// SPDX-License-Identifier: MIT

package lfstransfer

import (
	"context"
	"fmt"
	"os"

	"code.gitea.io/gitea/modules/lfstransfer/backend"
	"code.gitea.io/gitea/modules/lfstransfer/transfer"
)

func Main(ctx context.Context, token string, repo string, verb string) error {
	logger := newLogger()
	pktline := transfer.NewPktline(os.Stdin, os.Stdout, logger)
	giteaBackend := backend.New(ctx, token, repo, verb)

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
