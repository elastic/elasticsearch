/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.nativeaccess.jna;

import com.sun.jna.Library;
import com.sun.jna.Native;
import com.sun.jna.NativeLong;
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

        String strerror(int errno);
    }

    private final NativeFunctions functions;

    JnaPosixCLibrary() {
        this.functions = Native.load("c", NativeFunctions.class);
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
    public String strerror(int errno) {
        return functions.strerror(errno);
    }

    @Override
    public int errno() {
        return Native.getLastError();
    }
}
