/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.preallocate;

import com.sun.jna.Library;
import com.sun.jna.Native;
import com.sun.jna.NativeLibrary;
import com.sun.jna.NativeLong;
import com.sun.jna.Platform;
import com.sun.jna.Structure;

import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;

import java.io.IOException;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;

abstract class AbstractPosixPreallocator implements Preallocator {

    static final Logger logger = LogManager.getLogger(AbstractPosixPreallocator.class);

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

    public static final class Stat64 extends Structure implements Structure.ByReference {
        public byte[] _ignore1;
        public NativeLong st_size = new NativeLong(0);
        public byte[] _ignore2;

        Stat64(int sizeof, int stSizeOffset) {
            this._ignore1 = new byte[stSizeOffset];
            this._ignore2 = new byte[sizeof - stSizeOffset - 8];
        }

        @Override
        protected List<String> getFieldOrder() {
            return Arrays.asList("_ignore1", "st_size", "_ignore2");
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

    private interface FXStatFunction extends Library {
        int __fxstat(int version, int fd, Stat64 stat);
    }

    public static final boolean NATIVES_AVAILABLE;
    private static final NativeFunctions functions;
    private static final FStat64Function fstat64;

    static {
        functions = AccessController.doPrivileged((PrivilegedAction<NativeFunctions>) () -> {
            try {
                return Native.load(Platform.C_LIBRARY_NAME, NativeFunctions.class);
            } catch (final UnsatisfiedLinkError e) {
                logger.warn("Failed to load posix functions for preallocate");
                return null;
            }
        });
        fstat64 = AccessController.doPrivileged((PrivilegedAction<FStat64Function>) () -> {
            try {
                // JNA lazily finds symbols, so even though we try to bind two different functions below, if fstat64
                // isn't found, we won't know until runtime when calling the function. To force resolution of the
                // symbol we get a function object directly from the native library. We don't use it, we just want to
                // see if it will throw UnsatisfiedLinkError
                NativeLibrary.getInstance(Platform.C_LIBRARY_NAME).getFunction("fstat64");
                return Native.load(Platform.C_LIBRARY_NAME, FStat64Function.class);
            } catch (final UnsatisfiedLinkError e) {
                // fstat has a long history in linux from the 32-bit architecture days. On some modern linux systems,
                // fstat64 doesn't exist as a symbol in glibc. Instead, the compiler replaces fstat64 calls with
                // the internal __fxstat method. Here we fall back to __fxstat, and statically bind the special
                // "version" argument so that the call site looks the same as that of fstat64
                try {
                    var fxstat = Native.load(Platform.C_LIBRARY_NAME, FXStatFunction.class);
                    int version = System.getProperty("os.arch").equals("aarch64") ? 0 : 1;
                    return (fd, stat) -> fxstat.__fxstat(version, fd, stat);
                } catch (UnsatisfiedLinkError e2) {
                    logger.warn("Failed to load __fxstat for preallocate");
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
        return NATIVES_AVAILABLE;
    }

    @Override
    public NativeFileHandle open(String path) throws IOException {
        // We pass down O_CREAT, so open will create the file if it does not exist.
        // From the open man page (https://www.man7.org/linux/man-pages/man2/open.2.html):
        // - The mode parameter is needed when specifying O_CREAT
        // - The effective mode is modified by the process's umask: in the absence of a default ACL, the mode of the created file is
        // (mode & ~umask).
        // We choose to pass down 0666 (r/w permission for user/group/others) to mimic what the JDK does for its open operations;
        // see for example the fileOpen implementation in libjava:
        // https://github.com/openjdk/jdk/blob/98562166e4a4c8921709014423c6cbc993aa0d97/src/java.base/unix/native/libjava/io_util_md.c#L105
        int fd = functions.open(path, O_WRONLY | constants.O_CREAT, 0666);
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
