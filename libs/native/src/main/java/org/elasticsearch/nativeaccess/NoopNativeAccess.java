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

class NoopNativeAccess implements NativeAccess {

    private static final Logger logger = LogManager.getLogger(NativeAccess.class);

    NoopNativeAccess() {}

    @Override
    public boolean definitelyRunningAsRoot() {
        logger.warn("Cannot check if running as root because native access is not available");
        return false;
    }

    @Override
    public Systemd systemd() {
        logger.warn("Cannot get systemd access because native access is not available");
        return null;
    }

    @Override
    public Zstd getZstd() {
        logger.warn("cannot compress with zstd because native access is not available");
        return null;
    }

    @Override
    public CloseableByteBuffer newBuffer(int len) {
        logger.warn("cannot allocate buffer because native access is not available");
        return null;
    }
}
