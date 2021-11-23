/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.bootstrap;

import org.apache.lucene.util.Constants;

import java.util.Optional;

public interface CLibrary {

    int MCL_CURRENT = 1;
    int ENOMEM = 12;
    int RLIMIT_MEMLOCK = Constants.MAC_OS_X ? 6 : 8;
    int RLIMIT_AS = Constants.MAC_OS_X ? 5 : 9;
    int RLIMIT_FSIZE = Constants.MAC_OS_X ? 1 : 1;
    long RLIM_INFINITY = Constants.MAC_OS_X ? 9223372036854775807L : -1L;

    CLibrary INSTANCE = instanceOrNull();

    static CLibrary instanceOrNull() {
        // prefer panama if available, fall back to JNA
        return PanamaCLibrary.instance().orElseGet(() -> JNACLibrary.instance().orElse(null));
    }

    /** Returns the JNA instance or null. */
    static Optional<CLibrary> getInstance() {
        return Optional.ofNullable(INSTANCE);
    }

    int mlockall(int flags);

    int getLastError();

    String strerror(int errno);

    Rlimit newRlimit();

    int getrlimit(int resource, Rlimit rlimit);

    int setrlimit(int resource, Rlimit rlimit);

    /** corresponds to struct rlimit */
    interface Rlimit extends AutoCloseable {
        long rlim_cur();

        long rlim_max();

        void setrlim_cur(long value);

        void setrlim_max(long value);

        @Override
        void close(); // drop the declared exception
    }

    int geteuid();
}
