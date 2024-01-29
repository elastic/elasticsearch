/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.nativeaccess.jna;

import com.sun.jna.Native;
import com.sun.jna.NativeLong;
import com.sun.jna.Structure;

import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.nativeaccess.lib.PosixCLibrary.RLimit;
import org.elasticsearch.nativeaccess.lib.PosixCLibrary.Stat;

import java.util.Arrays;
import java.util.List;

/**
 * java mapping to some libc functions
 */
final class JnaStaticPosixCLibrary {

    private static final Logger logger = LogManager.getLogger(JnaStaticPosixCLibrary.class);

    public static final boolean loaded;

    static {
        boolean success = false;
        try {
            Native.register("c");
            success = true;
        } catch (UnsatisfiedLinkError e) {
            logger.warn("unable to link C library. native methods (mlockall) will be disabled.", e);
        }
        loaded = success;
    }

    static native int mlockall(int flags);

    static native int geteuid();

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

    public static final class JnaStat extends Structure implements Structure.ByReference, Stat {
        public byte[] _ignore1;
        public NativeLong st_size = new NativeLong(0);
        public byte[] _ignore2;

        JnaStat(int sizeof, int stSizeOffset) {
            this._ignore1 = new byte[stSizeOffset];
            this._ignore2 = new byte[sizeof - stSizeOffset - 8];
        }

        @Override
        public long st_size() {
            return st_size.longValue();
        }
    }

    static native int getrlimit(int resource, JnaRLimit rlimit);

    static native int setrlimit(int resource, JnaRLimit rlimit);

    static native String strerror(int errno);

    static native int open(String filename, int flags, int mode);

    static native int close(int fd);

    static native int fstat(int fd, Stat stat);

    private JnaStaticPosixCLibrary() {}
}
