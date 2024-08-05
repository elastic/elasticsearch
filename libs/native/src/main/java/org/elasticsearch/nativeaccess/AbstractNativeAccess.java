/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.nativeaccess;

import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.nativeaccess.lib.JavaLibrary;
import org.elasticsearch.nativeaccess.lib.NativeLibraryProvider;
import org.elasticsearch.nativeaccess.lib.ZstdLibrary;

abstract class AbstractNativeAccess implements NativeAccess {

    protected static final Logger logger = LogManager.getLogger(NativeAccess.class);

    private final String name;
    private final JavaLibrary javaLib;
    private final Zstd zstd;
    protected boolean isMemoryLocked = false;
    protected ExecSandboxState execSandboxState = ExecSandboxState.NONE;

    protected AbstractNativeAccess(String name, NativeLibraryProvider libraryProvider) {
        this.name = name;
        this.javaLib = libraryProvider.getLibrary(JavaLibrary.class);
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
    public CloseableByteBuffer newBuffer(int len) {
        assert len > 0;
        return javaLib.newBuffer(len);
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
