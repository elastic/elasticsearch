/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.bootstrap;

import com.sun.jna.Native;
import com.sun.jna.NativeLong;
import com.sun.jna.Structure;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

/**
 * java mapping to some libc functions
 */
final class JNACLibrary implements CLibrary {

    private static final Logger logger = LogManager.getLogger(JNACLibrary.class);

    private static final JNACLibrary INSTANCE = instanceOrNull();

    private static JNACLibrary instanceOrNull() {
        try {
            Native.register("c");
            return new JNACLibrary();
        } catch (UnsatisfiedLinkError e) {
            logger.warn("unable to link C library. native methods (mlockall) will be disabled.", e);
        }
        return null;
    }

    static Optional<CLibrary> instance() {
        return Optional.ofNullable(INSTANCE);
    }

    @Override
    public native int mlockall(int flags);

    @Override
    public int getLastError() {
        return Native.getLastError();
    }

    @Override
    public native int geteuid();

    /** corresponds to struct rlimit */
    public static final class JNARlimit extends Structure implements Structure.ByReference, CLibrary.Rlimit {
        private NativeLong rlim_cur = new NativeLong(0);
        private NativeLong rlim_max = new NativeLong(0);

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
        public void setrlim_cur(long value) {
            rlim_cur.setValue(value);
        }

        @Override
        public void setrlim_max(long value) {
            rlim_max.setValue(value);
        }

        @Override
        public void close() {}
    }

    @Override
    public int getrlimit(int resource, Rlimit rlimit) {
        return getrlimit(resource, (JNARlimit) rlimit);
    }

    private native int getrlimit(int resource, JNARlimit rlimit);

    @Override
    public int setrlimit(int resource, Rlimit rlimit) {
        return setrlimit(resource, (JNARlimit) rlimit);
    }

    private native int setrlimit(int resource, JNARlimit rlimit);

    @Override
    public native String strerror(int errno);

    @Override
    public Rlimit newRlimit() {
        return new JNARlimit();
    }

    private JNACLibrary() {}
}
