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
import org.apache.lucene.util.Constants;

import java.util.Arrays;
import java.util.List;

/**
 * java mapping to some libc functions
 */
final class JNACLibrary {

    private static final Logger logger = LogManager.getLogger(JNACLibrary.class);

    public static final int MCL_CURRENT = 1;
    public static final int ENOMEM = 12;
    public static final int RLIMIT_MEMLOCK = Constants.MAC_OS_X ? 6 : 8;
    public static final int RLIMIT_AS = Constants.MAC_OS_X ? 5 : 9;
    public static final int RLIMIT_FSIZE = Constants.MAC_OS_X ? 1 : 1;
    public static final long RLIM_INFINITY = Constants.MAC_OS_X ? 9223372036854775807L : -1L;

    static {
        try {
            Native.register("c");
        } catch (UnsatisfiedLinkError e) {
            logger.warn("unable to link C library. native methods (mlockall) will be disabled.", e);
        }
    }

    static native int mlockall(int flags);

    static native int geteuid();

    /** corresponds to struct rlimit */
    public static final class Rlimit extends Structure implements Structure.ByReference {
        public NativeLong rlim_cur = new NativeLong(0);
        public NativeLong rlim_max = new NativeLong(0);

        @Override
        protected List<String> getFieldOrder() {
            return Arrays.asList("rlim_cur", "rlim_max");
        }
    }

    static native int getrlimit(int resource, Rlimit rlimit);
    static native int setrlimit(int resource, Rlimit rlimit);

    static native String strerror(int errno);

    private JNACLibrary() {
    }
}
