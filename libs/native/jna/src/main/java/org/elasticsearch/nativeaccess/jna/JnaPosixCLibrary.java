/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.nativeaccess.jna;

import com.sun.jna.Native;

import org.elasticsearch.nativeaccess.jna.JnaStaticPosixCLibrary.JnaRLimit;
import org.elasticsearch.nativeaccess.lib.PosixCLibrary;

public class JnaPosixCLibrary implements PosixCLibrary {
    @Override
    public int mlockall(int flags) {
        return JnaStaticPosixCLibrary.mlockall(flags);
    }

    @Override
    public int geteuid() {
        return JnaStaticPosixCLibrary.geteuid();
    }

    @Override
    public int getrlimit(int resource, RLimit rlimit) {
        JnaRLimit jnaRlimit = new JnaRLimit();
        int ret = JnaStaticPosixCLibrary.getrlimit(resource, jnaRlimit);
        rlimit.rlim_cur = jnaRlimit.rlim_cur.longValue();
        rlimit.rlim_max = jnaRlimit.rlim_max.longValue();
        return ret;
    }

    @Override
    public int setrlimit(int resource, RLimit rlimit) {
        JnaRLimit jnaRlimit = new JnaRLimit();
        jnaRlimit.rlim_cur.setValue(rlimit.rlim_cur);
        jnaRlimit.rlim_max.setValue(rlimit.rlim_max);
        return JnaStaticPosixCLibrary.setrlimit(resource, jnaRlimit);
    }

    @Override
    public String strerror(int errno) {
        return JnaStaticPosixCLibrary.strerror(errno);
    }

    @Override
    public int errno() {
        return Native.getLastError();
    }
}
