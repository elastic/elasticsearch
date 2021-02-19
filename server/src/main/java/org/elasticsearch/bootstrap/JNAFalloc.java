/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.bootstrap;

import com.sun.jna.Native;
import com.sun.jna.Platform;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.Constants;
import org.elasticsearch.common.Nullable;

/**
 * System specific wrappers of the fallocate system call via JNA for Linux.
 * TODO: look into adding native implementations for falloc equivalents for other platforms (Windows) to improve performance.
 */
abstract class JNAFalloc {

    private static final Logger logger = LogManager.getLogger(JNAFalloc.class);

    public abstract int fallocate(int fd, long offset, long length);

    @Nullable
    public static JNAFalloc falloc() {
        try {
            if (Constants.LINUX) {
                return Linux.INSTANCE;
            }
        } catch (Throwable t) {
            logger.warn("unable to link C library. native (falloc) will be disabled.", t);
        }
        return null;
    }

    private static class Linux extends JNAFalloc {

        static final Linux INSTANCE = new Linux();

        static {
            Native.register(Platform.C_LIBRARY_NAME);
        }

        @Override
        public int fallocate(int fd, long offset, long length) {
            final int res = fallocate(fd, 0, offset, length);
            return res == 0 ? 0 : Native.getLastError();
        }

        private static native int fallocate(int fd, int mode, long offset, long length);
    }

}
