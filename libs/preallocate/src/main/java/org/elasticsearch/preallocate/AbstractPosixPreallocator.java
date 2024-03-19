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
import com.sun.jna.NativeLong;
import com.sun.jna.Platform;
import com.sun.jna.Structure;

import java.io.IOException;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Locale;
import java.util.Map;

abstract class AbstractPosixPreallocator implements Preallocator {

    /**
     * Constants relating to posix libc.
     *
     * @param SIZEOF_STAT The size of the stat64 structure, ie sizeof(stat64_t), found by importing sys/stat.h
     * @param STAT_ST_SIZE_OFFSET The offsite into stat64 at which st_size exists, ie offsetof(stat64_t, st_size),
     *                           found by importing sys/stat.h
     * @param O_CREAT The file mode for creating a file upon opening, found by importing fcntl.h
     */
    protected record PosixConstants(int SIZEOF_STAT, int STAT_ST_SIZE_OFFSET, int O_CREAT) {}

    private static final int O_WRONLY = 1;

    static final class Stat64 extends Structure implements Structure.ByReference {
        public byte[] _ignore1;
        public NativeLong st_size = new NativeLong(0);
        public byte[] _ignore2;

        Stat64(int sizeof, int stSizeOffset) {
            this._ignore1 = new byte[stSizeOffset];
            this._ignore2 = new byte[sizeof - stSizeOffset - 8];
        }
    }

    private interface NativeFunctions extends Library {
        String strerror(int errno);

        int open(String filename, int flags, Object... mode);

        int close(int fd);
    }

    private interface FStat64Function extends Library {
        int fstat64(int fd, Stat64 stat);
    }

    public static final boolean NATIVES_AVAILABLE;
    private static final NativeFunctions functions;
    private static final FStat64Function fstat64;

    static {
        functions = AccessController.doPrivileged((PrivilegedAction<NativeFunctions>) () -> {
            try {
                return Native.load(Platform.C_LIBRARY_NAME, NativeFunctions.class);
            } catch (final UnsatisfiedLinkError e) {
                return null;
            }
        });
        fstat64 = AccessController.doPrivileged((PrivilegedAction<FStat64Function>) () -> {
            try {
                return Native.load(Platform.C_LIBRARY_NAME, FStat64Function.class);
            } catch (final UnsatisfiedLinkError e) {
                try {
                    // on Linux fstat64 isn't available as a symbol, but instead uses a special __ name
                    var options = Map.of(Library.OPTION_FUNCTION_MAPPER, (FunctionMapper) (lib, method) -> "__fxstat64");
                    return Native.load(Platform.C_LIBRARY_NAME, FStat64Function.class, options);
                } catch (UnsatisfiedLinkError e2) {
                    return null;
                }
            }
        });
        NATIVES_AVAILABLE = functions != null && fstat64 != null;
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
            var stat = new Stat64(constants.SIZEOF_STAT, constants.STAT_ST_SIZE_OFFSET);
            if (fstat64.fstat64(fd, stat) == -1) {
                throw newIOException("Could not get size of file");
            }
            return stat.st_size.longValue();
        }

        @Override
        public void close() throws IOException {
            if (functions.close(fd) != 0) {
                throw newIOException("Could not close file");
            }
        }
    }

    protected final PosixConstants constants;

    AbstractPosixPreallocator(PosixConstants constants) {
        this.constants = constants;
    }

    @Override
    public boolean useNative() {
        return false;
    }

    @Override
    public NativeFileHandle open(String path) throws IOException {
        int fd = functions.open(path, O_WRONLY, constants.O_CREAT);
        if (fd < 0) {
            throw newIOException(String.format(Locale.ROOT, "Could not open file [%s] for preallocation", path));
        }
        return new PosixNativeFileHandle(fd);
    }

    @Override
    public String error(int errno) {
        return functions.strerror(errno);
    }

    private static IOException newIOException(String prefix) {
        int errno = Native.getLastError();
        return new IOException(String.format(Locale.ROOT, "%s(errno=%d): %s", prefix, errno, functions.strerror(errno)));
    }
}
