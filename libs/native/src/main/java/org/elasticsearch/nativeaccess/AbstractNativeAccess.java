/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.nativeaccess;

import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.nativeaccess.lib.NativeLibraryProvider;
import org.elasticsearch.nativeaccess.lib.ZstdLibrary;

abstract class AbstractNativeAccess implements NativeAccess {

    protected static final Logger logger = LogManager.getLogger(NativeAccess.class);

    private final String name;
    private final Zstd zstd;
    protected boolean isMemoryLocked = false;
    protected ExecSandboxState execSandboxState = ExecSandboxState.NONE;

    protected AbstractNativeAccess(String name, NativeLibraryProvider libraryProvider) {
        this.name = name;
        this.zstd = new Zstd(libraryProvider.getLibrary(ZstdLibrary.class));
    }

    String getName() {
        return name;
    }

    @Override
    public Systemd systemd() {
        return null;
    }

    @Override
    public Zstd getZstd() {
        return zstd;
    }

    @Override
    public boolean isMemoryLocked() {
        return isMemoryLocked;
    }

    @Override
    public ExecSandboxState getExecSandboxState() {
        return execSandboxState;
    }
}
