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

abstract class AbstractNativeAccess implements NativeAccess {

    protected static final Logger logger = LogManager.getLogger(NativeAccess.class);

    protected boolean memoryLocked = false;
    protected long maxVirtualMemorySize = Long.MIN_VALUE;
    protected long maxFileSize = Long.MIN_VALUE;
    // the maximum number of threads that can be created for
    // the user ID that owns the running Elasticsearch process
    protected long maxNumberOfThreads = -1;
    protected boolean systemCallFilterInstalled = false;

    @Override
    public boolean isMemoryLocked() {
        return memoryLocked;
    }

    @Override
    public long getMaxVirtualMemorySize() {
        return maxVirtualMemorySize;
    }

    @Override
    public long getMaxFileSize() {
        return maxFileSize;
    }

    @Override
    public long getMaxNumberOfThreads() {
        return maxNumberOfThreads;
    }

    @Override
    public boolean isSystemCallFilterInstalled() {
        return systemCallFilterInstalled;
    }
}
