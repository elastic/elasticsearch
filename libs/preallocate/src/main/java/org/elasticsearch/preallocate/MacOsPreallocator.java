/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.preallocate;

import com.sun.jna.Library;
import com.sun.jna.Memory;
import com.sun.jna.Native;
import com.sun.jna.NativeLong;
import com.sun.jna.Platform;
import com.sun.jna.Pointer;

import java.lang.invoke.MethodHandles;
import java.security.AccessController;
import java.security.PrivilegedAction;

final class MacOsPreallocator extends AbstractPosixPreallocator {

    static {
        try {
            MethodHandles.lookup().ensureInitialized(Natives.class);
            logger.info("Initialized macos natives: " + Natives.NATIVES_AVAILABLE);
        } catch (IllegalAccessException unexpected) {
            throw new AssertionError(unexpected);
        }
    }

    MacOsPreallocator() {
        super(new PosixConstants(144, 96, 512));
    }

    @Override
    public boolean useNative() {
        return Natives.NATIVES_AVAILABLE && super.useNative();
    }

    @Override
    public int preallocate(final int fd, final long currentSize /* unused */ , final long fileSize) {
        // the Structure.ByReference constructor requires access to declared members
        final Natives.Fcntl.FStore fst = new Natives.Fcntl.FStore();
        fst.setFlags(Natives.Fcntl.F_ALLOCATECONTIG);
        fst.setPosmode(Natives.Fcntl.F_PEOFPOSMODE);
        fst.setOffset(0);
        fst.setLength(fileSize);
        // first, try allocating contiguously
        logger.info("Calling fcntl for preallocate");
        if (Natives.functions.fcntl(fd, Natives.Fcntl.F_PREALLOCATE, fst.memory) != 0) {
            logger.warn("Failed to get contiguous preallocate, trying non-contiguous");
            // that failed, so let us try allocating non-contiguously
            fst.setFlags(Natives.Fcntl.F_ALLOCATEALL);
            if (Natives.functions.fcntl(fd, Natives.Fcntl.F_PREALLOCATE, fst.memory) != 0) {
                logger.warn("Failed to get non-continugous preallocate");
                // i'm afraid captain dale had to bail
                return Native.getLastError();
            }
        }
        if (Natives.functions.ftruncate(fd, new NativeLong(fileSize)) != 0) {
            logger.warn("Failed to ftruncate");
            return Native.getLastError();
        }
        return 0;
    }

    private static class Natives {

        static final boolean NATIVES_AVAILABLE;
        static final NativeFunctions functions;

        static {
            NativeFunctions nativeFunctions = AccessController.doPrivileged((PrivilegedAction<NativeFunctions>) () -> {
                try {
                    return Native.load(Platform.C_LIBRARY_NAME, NativeFunctions.class);
                } catch (final UnsatisfiedLinkError e) {
                    logger.warn("Failed to load macos native preallocate functions");
                    return null;
                }
            });
            functions = nativeFunctions;
            NATIVES_AVAILABLE = nativeFunctions != null;
        }

        static class Fcntl {
            private static final int F_PREALLOCATE = 42;

            // allocate flags; these might be unused, but are here for reference
            @SuppressWarnings("unused")
            private static final int F_ALLOCATECONTIG = 0x00000002; // allocate contiguous space
            private static final int F_ALLOCATEALL = 0x00000004; // allocate all the requested space or no space at all

            // position modes; these might be unused, but are here for reference
            private static final int F_PEOFPOSMODE = 3; // allocate from the physical end of the file
            @SuppressWarnings("unused")
            private static final int F_VOLPOSMODE = 4; // allocate from the volume offset

            public static final class FStore {
                final Memory memory = new Memory(32);

                public void setFlags(int flags) {
                    memory.setInt(0, flags);
                }

                public void setPosmode(int posmode) {
                    memory.setInt(4, posmode);
                }

                public void setOffset(long offset) {
                    memory.setLong(8, offset);
                }

                public void setLength(long length) {
                    memory.setLong(16, length);
                }

                public void getBytesalloc() {
                    memory.getLong(24);
                }

            }
        }

        private interface NativeFunctions extends Library {
            int fcntl(int fd, int cmd, Object... args);

            int ftruncate(int fd, NativeLong length);
        }
    }

}
