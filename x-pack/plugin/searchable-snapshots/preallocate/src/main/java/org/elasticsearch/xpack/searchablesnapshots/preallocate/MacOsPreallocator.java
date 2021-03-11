/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.searchablesnapshots.preallocate;

import com.sun.jna.Native;
import com.sun.jna.NativeLong;
import com.sun.jna.Platform;
import com.sun.jna.Structure;

import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Arrays;
import java.util.List;

final class MacOsPreallocator implements Preallocator {

    @Override
    public boolean available() {
        return Natives.NATIVES_AVAILABLE;
    }

    @Override
    public int preallocate(final int fd, final long currentSize /* unused */ , final long fileSize) {
        // the Structure.ByReference constructor requires access to declared members
        final Natives.Fcntl.FStore fst =
            AccessController.doPrivileged((PrivilegedAction<Natives.Fcntl.FStore>) Natives.Fcntl.FStore::new);
        fst.fst_flags = Natives.Fcntl.F_ALLOCATEALL;
        fst.fst_posmode = Natives.Fcntl.F_PEOFPOSMODE;
        fst.fst_offset = new NativeLong(0);
        fst.fst_length = new NativeLong(fileSize);
        if (Natives.fcntl(fd, Natives.Fcntl.F_PREALLOCATE, fst) != 0) {
            return Native.getLastError();
        }
        if (Natives.ftruncate(fd, new NativeLong(fileSize)) != 0) {
            return Native.getLastError();
        }
        return 0;
    }

    @Override
    public String error(final int errno) {
        return Natives.strerror(errno);
    }

    private static class Natives {

        static boolean NATIVES_AVAILABLE;

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

            public static final class FStore extends Structure implements Structure.ByReference {
                public int fst_flags = 0;
                public int fst_posmode = 0;
                public NativeLong fst_offset = new NativeLong(0);
                public NativeLong fst_length = new NativeLong(0);
                @SuppressWarnings("unused")
                public NativeLong fst_bytesalloc = new NativeLong(0);

                @Override
                protected List<String> getFieldOrder() {
                    return Arrays.asList("fst_flags", "fst_posmode", "fst_offset", "fst_length", "fst_bytesalloc");
                }

            }
        }

        public static native int fcntl(final int fd, final int cmd, final Fcntl.FStore fst);
        public static native int ftruncate(final int fd, final NativeLong length);
        public static native String strerror(final int errno);

    }

}
