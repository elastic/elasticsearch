/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.nativeaccess.lib;

public interface PosixCLibrary {

    int mlockall(int flags);

    int geteuid();

    /** corresponds to struct rlimit */
    class RLimit {
        public long rlim_cur;
        public long rlim_max;

        public RLimit() {
            this(0, 0);
        }

        public RLimit(long rlim_cur, long rlim_max) {
            this.rlim_cur = rlim_cur;
            this.rlim_max = rlim_max;
        }
    }

    int getrlimit(int resource, RLimit rlimit);

    int setrlimit(int resource, RLimit rlimit);

    String strerror(int errno);

    int errno();
}
