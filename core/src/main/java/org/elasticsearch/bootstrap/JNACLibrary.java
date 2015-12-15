/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.bootstrap;

import com.sun.jna.Native;
import com.sun.jna.NativeLong;
import com.sun.jna.Structure;

import org.apache.lucene.util.Constants;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;

import java.util.Arrays;
import java.util.List;

/**
 * java mapping to some libc functions
 */
final class JNACLibrary {

    private static final ESLogger logger = Loggers.getLogger(JNACLibrary.class);

    public static final int MCL_CURRENT = 1;
    public static final int ENOMEM = 12;
    public static final int RLIMIT_MEMLOCK = Constants.MAC_OS_X ? 6 : 8;
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
            return Arrays.asList(new String[] { "rlim_cur", "rlim_max" });
        }
    }
    
    static native int getrlimit(int resource, Rlimit rlimit);
    static native int setrlimit(int resource, Rlimit rlimit);
    
    static native String strerror(int errno);

    private JNACLibrary() {
    }
}
