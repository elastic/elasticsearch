/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.nativeaccess.jna;

import com.sun.jna.FunctionMapper;
import com.sun.jna.Library;
import com.sun.jna.Native;
import com.sun.jna.NativeLong;
import com.sun.jna.Structure;

import org.elasticsearch.nativeaccess.lib.PosixCLibrary;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

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

    public static final class JnaStat64 extends Structure implements Structure.ByReference, Stat64 {
        public byte[] _ignore1;
        public NativeLong st_size = new NativeLong(0);
        public byte[] _ignore2;
        public NativeLong st_blocks = new NativeLong(0);
        public byte[] _ignore3;

        JnaStat64(int sizeof, int stSizeOffset, int stBlocksOffset) {
            this._ignore1 = new byte[stSizeOffset];
            this._ignore2 = new byte[stBlocksOffset - stSizeOffset - 8];
            this._ignore3 = new byte[sizeof - stBlocksOffset - 8];
        }

        @Override
        public long st_size() {
            return st_size.longValue();
        }

        @Override
        public long st_blocks() {
            return st_blocks.longValue();
        }
    }

    public static class JnaFStore extends Structure implements Structure.ByReference, FStore {

        public int fst_flags = 0;
        public int fst_posmode = 0;
        public NativeLong fst_offset = new NativeLong(0);
        public NativeLong fst_length = new NativeLong(0);
        public NativeLong fst_bytesalloc = new NativeLong(0);

        @Override
        public void set_flags(int flags) {
            this.fst_flags = flags;
        }

        @Override
        public void set_posmode(int posmode) {
            this.fst_posmode = posmode;
        }

        @Override
        public void set_offset(long offset) {
            fst_offset.setValue(offset);
        }

        @Override
        public void set_length(long length) {
            fst_length.setValue(length);
        }

        @Override
        public long bytesalloc() {
            return fst_bytesalloc.longValue();
        }
    }

    private interface NativeFunctions extends Library {
        int geteuid();

        int getrlimit(int resource, JnaRLimit rlimit);

        int setrlimit(int resource, JnaRLimit rlimit);

        int mlockall(int flags);

        int fcntl(int fd, int cmd, JnaFStore fst);

        int ftruncate(int fd, NativeLong length);

        int open(String filename, int flags, Object... mode);

        int close(int fd);

        String strerror(int errno);
    }

    private interface FStat64Function extends Library {
        int fstat64(int fd, JnaStat64 stat);
    }

    private final NativeFunctions functions;
    private final FStat64Function fstat64;

    JnaPosixCLibrary() {
        this.functions = Native.load("c", NativeFunctions.class);
        FStat64Function fstat64;
        try {
            fstat64 = Native.load("c", FStat64Function.class);
        } catch (UnsatisfiedLinkError e) {
            // TODO: explain
            fstat64 = Native.load(
                "c",
                FStat64Function.class,
                Map.of(Library.OPTION_FUNCTION_MAPPER, (FunctionMapper) (lib, method) -> "__fxstat64")
            );
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
        return functions.fcntl(fd, cmd, jnaFst);
    }

    @Override
    public int ftruncate(int fd, long length) {
        return functions.ftruncate(fd, new NativeLong(length));
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
        return fstat64.fstat64(fd, jnaStats);
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
