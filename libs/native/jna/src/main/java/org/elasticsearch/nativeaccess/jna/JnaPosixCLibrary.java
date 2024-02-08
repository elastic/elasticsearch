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

        JnaStat64(int sizeof, int stSizeOffset) {
            this._ignore1 = new byte[stSizeOffset];
            this._ignore2 = new byte[sizeof - stSizeOffset - 8];
        }

        @Override
        public long st_size() {
            return st_size.longValue();
        }
    }

    private interface NativeFunctions extends Library {
        int geteuid();

        int mlockall(int flags);

        int getrlimit(int resource, JnaRLimit rlimit);

        int setrlimit(int resource, JnaRLimit rlimit);

        String strerror(int errno);

        int open(String filename, int flags, Object... mode);

        int close(int fd);
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
    public int mlockall(int flags) {
        return functions.mlockall(flags);
    }

    @Override
    public RLimit newRLimit() {
        return new JnaRLimit();
    }

    @Override
    public Stat64 newStat64(int sizeof, int stSizeOffset) {
        return new JnaStat64(sizeof, stSizeOffset);
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
