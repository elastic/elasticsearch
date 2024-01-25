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

class NoopNativeAccess extends AbstractNativeAccess {
    private static final Logger logger = LogManager.getLogger(NativeAccess.class);

    @Override
    public boolean definitelyRunningAsRoot() {
        logger.warn("Cannot check if running as root because native access is not available");
        return false;
    }

    @Override
    public void tryLockMemory() {
        logger.warn("Cannot lock memory because native access is not available");
    }

    public void trySetMaxNumberOfThreads() {
        logger.warn("Cannot set max number of threads because native access is not available");
    }

    public void trySetMaxVirtualMemorySize() {
        logger.warn("Cannot set max size of virtual memory because native access is not available");
    }

    public void trySetMaxFileSize() {
        logger.warn("Cannot set max file size because native access is not available");
    }
}
