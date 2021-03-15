/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.searchablesnapshots.preallocate;

import com.sun.jna.Native;
import com.sun.jna.Platform;

import java.security.AccessController;
import java.security.PrivilegedAction;

final class LinuxPreallocator implements Preallocator {

    @Override
    public boolean available() {
        return Natives.NATIVES_AVAILABLE;
    }

    @Override
    public int preallocate(final int fd, final long currentSize, final long fileSize) {
        final int rc = Natives.fallocate(fd, 0, currentSize, fileSize - currentSize);
        return rc == 0 ? 0 : Native.getLastError();
    }

    @Override
    public String error(int errno) {
        return Natives.strerror(errno);
    }

    private static class Natives {

        public static final boolean NATIVES_AVAILABLE;

        static {
            NATIVES_AVAILABLE = AccessController.doPrivileged((PrivilegedAction<Boolean>) () -> {
                try {
                    Native.register(Natives.class, Platform.C_LIBRARY_NAME);
                } catch (final UnsatisfiedLinkError e) {
                    return false;
                }
                return true;
            });
        }

        static native int fallocate(int fd, int mode, long offset, long length);

        static native String strerror(int errno);

    }

}
