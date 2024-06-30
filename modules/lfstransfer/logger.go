// Copyright 2024 The Gitea Authors. All rights reserved.
// SPDX-License-Identifier: MIT

package lfstransfer

import (
	"fmt"
	"io"

	"github.com/charmbracelet/git-lfs-transfer/transfer"
)

var _ transfer.Logger = (*GiteaLogger)(nil)

// noop logger for passing into transfer
type GiteaLogger struct {
	p string
	w io.Writer
}

func newLogger(p string, w io.Writer) transfer.Logger {
	return &GiteaLogger{p: p + ": ", w: w}
}

// Log implements transfer.Logger
func (g *GiteaLogger) Log(msg string, itms ...interface{}) {
	fmt.Fprintln(g.w, g.p, msg, itms)
}
