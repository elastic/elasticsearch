/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.searchablesnapshots.preallocate;

import com.sun.jna.FromNativeContext;
import com.sun.jna.Native;
import com.sun.jna.Pointer;
import com.sun.jna.ptr.PointerByReference;

import java.security.AccessController;
import java.security.PrivilegedAction;

public class WindowsPreallocator implements Preallocator {

    @Override
    public boolean available() {
        return false;
    }

    @Override
    public int preallocate(final int fd, final long currentSize, final long fileSize) {
        return 0;
    }

    @Override
    public String error(int errno) {
        return null;
    }

    private static class Natives {

        static boolean NATIVES_AVAILABLE;

        static {
            NATIVES_AVAILABLE = AccessController.doPrivileged((PrivilegedAction<Boolean>) () -> {
                try {
                    Native.register(WindowsPreallocator.Natives.class, "kernel32");
                } catch (final UnsatisfiedLinkError e) {
                    return false;
                }
                return true;
            });

            FromNativeContext
        }



    }

}
