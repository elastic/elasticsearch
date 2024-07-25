/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.nativeaccess.jna;

import com.sun.jna.Library;
import com.sun.jna.Memory;
import com.sun.jna.Native;
import com.sun.jna.NativeLibrary;
import com.sun.jna.NativeLong;
import com.sun.jna.Pointer;
import com.sun.jna.Structure;

import org.elasticsearch.nativeaccess.lib.PosixCLibrary;

import java.util.Arrays;
import java.util.List;

class JnaPosixCLibrary implements PosixCLibrary {

    /** corresponds to struct rlimit */
    public static final class JnaRLimit extends Structure implements Structure.ByReference, RLimit {
        public NativeLong rlim_cur = new NativeLong(0);
        public NativeLong rlim_max = new NativeLong(0);

        @Override
        protected List<String> getFieldOrder() {
            return Arrays.asList("rlim_cur", "rlim_max");
        }

        @Override
        public long rlim_cur() {
            return rlim_cur.longValue();
        }

        @Override
        public long rlim_max() {
            return rlim_max.longValue();
        }

        @Override
        public void rlim_cur(long v) {
            rlim_cur.setValue(v);
        }

        @Override
        public void rlim_max(long v) {
            rlim_max.setValue(v);
        }
    }

    public static final class JnaStat64 implements Stat64 {
        final Memory memory;
        private final int stSizeOffset;
        private final int stBlocksOffset;

        JnaStat64(int sizeof, int stSizeOffset, int stBlocksOffset) {
            this.memory = new Memory(sizeof);
            this.stSizeOffset = stSizeOffset;
            this.stBlocksOffset = stBlocksOffset;
        }

        @Override
        public long st_size() {
            return memory.getLong(stSizeOffset);
        }

        @Override
        public long st_blocks() {
            return memory.getLong(stBlocksOffset);
        }
    }

    public static class JnaFStore implements FStore {
        final Memory memory;

        JnaFStore() {
            this.memory = new Memory(32);
        }

        @Override
        public void set_flags(int flags) {
            memory.setInt(0, flags);
        }

        @Override
        public void set_posmode(int posmode) {
            memory.setInt(4, posmode);
        }

        @Override
        public void set_offset(long offset) {
            memory.setLong(8, offset);
        }

        @Override
        public void set_length(long length) {
            memory.setLong(16, length);
        }

        @Override
        public long bytesalloc() {
            return memory.getLong(24);
        }
    }

    private interface NativeFunctions extends Library {
        int geteuid();

        int getrlimit(int resource, JnaRLimit rlimit);

        int setrlimit(int resource, JnaRLimit rlimit);

        int mlockall(int flags);

        int fcntl(int fd, int cmd, Object... args);

        int ftruncate(int fd, NativeLong length);

        int open(String filename, int flags, Object... mode);

        int close(int fd);

        String strerror(int errno);
    }

    private interface FStat64Function extends Library {
        int fstat64(int fd, Pointer stat);
    }

    private interface FXStatFunction extends Library {
        int __fxstat(int version, int fd, Pointer stat);
    }

    private final NativeFunctions functions;
    private final FStat64Function fstat64;

    JnaPosixCLibrary() {
        this.functions = Native.load("c", NativeFunctions.class);
        FStat64Function fstat64;
        try {
            // JNA lazily finds symbols, so even though we try to bind two different functions below, if fstat64
            // isn't found, we won't know until runtime when calling the function. To force resolution of the
            // symbol we get a function object directly from the native library. We don't use it, we just want to
            // see if it will throw UnsatisfiedLinkError
            NativeLibrary.getInstance("c").getFunction("fstat64");
            fstat64 = Native.load("c", FStat64Function.class);
        } catch (UnsatisfiedLinkError e) {
            // fstat has a long history in linux from the 32-bit architecture days. On some modern linux systems,
            // fstat64 doesn't exist as a symbol in glibc. Instead, the compiler replaces fstat64 calls with
            // the internal __fxstat method. Here we fall back to __fxstat, and staticall bind the special
            // "version" argument so that the call site looks the same as that of fstat64
            var fxstat = Native.load("c", FXStatFunction.class);
            int version = System.getProperty("os.arch").equals("aarch64") ? 0 : 1;
            fstat64 = (fd, stat) -> fxstat.__fxstat(version, fd, stat);
        }
        this.fstat64 = fstat64;
    }

    @Override
    public int geteuid() {
        return functions.geteuid();
    }

    @Override
    public RLimit newRLimit() {
        return new JnaRLimit();
    }

    @Override
    public Stat64 newStat64(int sizeof, int stSizeOffset, int stBlocksOffset) {
        return new JnaStat64(sizeof, stSizeOffset, stBlocksOffset);
    }

    @Override
    public int getrlimit(int resource, RLimit rlimit) {
        assert rlimit instanceof JnaRLimit;
        var jnaRlimit = (JnaRLimit) rlimit;
        return functions.getrlimit(resource, jnaRlimit);
    }

    @Override
    public int setrlimit(int resource, RLimit rlimit) {
        assert rlimit instanceof JnaRLimit;
        var jnaRlimit = (JnaRLimit) rlimit;
        return functions.setrlimit(resource, jnaRlimit);
    }

    @Override
    public int mlockall(int flags) {
        return functions.mlockall(flags);
    }

    @Override
    public FStore newFStore() {
        return new JnaFStore();
    }

    @Override
    public int fcntl(int fd, int cmd, FStore fst) {
        assert fst instanceof JnaFStore;
        var jnaFst = (JnaFStore) fst;
        return functions.fcntl(fd, cmd, jnaFst.memory);
    }

    @Override
    public int ftruncate(int fd, long length) {
        return functions.ftruncate(fd, new NativeLong(length));
    }

    @Override
    public int open(String pathname, int flags) {
        return functions.open(pathname, flags);
    }

    @Override
    public int open(String pathname, int flags, int mode) {
        return functions.open(pathname, flags, mode);
    }

    @Override
    public int close(int fd) {
        return functions.close(fd);
    }

    @Override
    public int fstat64(int fd, Stat64 stats) {
        assert stats instanceof JnaStat64;
        var jnaStats = (JnaStat64) stats;
        return fstat64.fstat64(fd, jnaStats.memory);
    }

    @Override
    public String strerror(int errno) {
        return functions.strerror(errno);
    }

    @Override
    public int errno() {
        return Native.getLastError();
    }
}
