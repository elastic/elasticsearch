/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.preallocate;

import com.sun.jna.FunctionMapper;
import com.sun.jna.Library;
import com.sun.jna.Native;
import com.sun.jna.NativeLibrary;
import com.sun.jna.NativeLong;
import com.sun.jna.Platform;
import com.sun.jna.Structure;

import java.io.IOException;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Locale;
import java.util.Map;

abstract class AbstractPosixPreallocator implements Preallocator {

    private static final int O_WRONLY = 1;

    protected final int SIZEOF_STAT;
    protected final int STAT_ST_SIZE_OFFSET;
    protected final int O_CREAT;

    AbstractPosixPreallocator(int SIZEOF_STAT, int STAT_ST_SIZE_OFFSET, int O_CREAT) {
        this.SIZEOF_STAT = SIZEOF_STAT;
        this.STAT_ST_SIZE_OFFSET = STAT_ST_SIZE_OFFSET;
        this.O_CREAT = O_CREAT;
    }

    static final class Stat64 extends Structure implements Structure.ByReference {
        public byte[] _ignore1;
        public NativeLong st_size = new NativeLong(0);
        public byte[] _ignore2;

        Stat64(int sizeof, int stSizeOffset) {
            this._ignore1 = new byte[stSizeOffset];
            this._ignore2 = new byte[sizeof - stSizeOffset - 8];
        }
    }

    private static class NativeFunctions {
        static native String strerror(int errno);

        static native int open(String filename, int flags, Object... mode);

        static native int close(int fd);
    }

    private static class FStat64Function {
        static native int fstat64(int fd, Stat64 stat);
    }

    public static final boolean NATIVES_AVAILABLE;

    static {
        NATIVES_AVAILABLE = AccessController.doPrivileged((PrivilegedAction<Boolean>) () -> loadFstat() && loadNatives());
    }

    private static boolean loadFstat() {
        try {
            Native.register(FStat64Function.class, Platform.C_LIBRARY_NAME);
        } catch (final UnsatisfiedLinkError e) {
            try {
                // on Linux fstat64 isn't available as a symbol, but instead uses a special __ name
                var options = Map.of(Library.OPTION_FUNCTION_MAPPER, (FunctionMapper) (lib, method) -> "__fxstat64");
                Native.register(FStat64Function.class, NativeLibrary.getInstance(Platform.C_LIBRARY_NAME, options));
            } catch (UnsatisfiedLinkError e2) {
                return false;
            }
        }
        return true;
    }

    private static boolean loadNatives() {
        try {
            Native.register(NativeFunctions.class, Platform.C_LIBRARY_NAME);
        } catch (final UnsatisfiedLinkError e) {
            return false;
        }
        return true;
    }

    private class PosixNativeFileHandle implements NativeFileHandle {

        private final int fd;

        PosixNativeFileHandle(int fd) {
            this.fd = fd;
        }

        @Override
        public int fd() {
            return fd;
        }

        @Override
        public long getSize() throws IOException {
            var stat = new Stat64(SIZEOF_STAT, STAT_ST_SIZE_OFFSET);
            if (FStat64Function.fstat64(fd, stat) == -1) {
                throw newIOException("Could not get size of file");
            }
            return stat.st_size.longValue();
        }

        @Override
        public void close() throws IOException {
            if (NativeFunctions.close(fd) != 0) {
                throw newIOException("Could not close file");
            }
        }
    }

    @Override
    public boolean useNative() {
        return false;
    }

    @Override
    public NativeFileHandle open(String path) throws IOException {
        int fd = NativeFunctions.open(path, O_WRONLY, O_CREAT);
        if (fd < 0) {
            throw newIOException(String.format(Locale.ROOT, "Could not open file [%s] for preallocation", path));
        }
        return new PosixNativeFileHandle(fd);
    }

    @Override
    public String error(int errno) {
        return NativeFunctions.strerror(errno);
    }

    private static IOException newIOException(String prefix) {
        int errno = Native.getLastError();
        return new IOException(String.format(Locale.ROOT, "%s(errno=%d): %s", prefix, errno, NativeFunctions.strerror(errno)));
    }
}
