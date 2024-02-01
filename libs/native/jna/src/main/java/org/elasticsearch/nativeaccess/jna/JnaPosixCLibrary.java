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
import org.elasticsearch.nativeaccess.jna.JnaStaticPosixCLibrary.JnaStat64;
import org.elasticsearch.nativeaccess.lib.PosixCLibrary;

import java.lang.invoke.MethodHandles;

class JnaPosixCLibrary implements PosixCLibrary {

    static {
        try {
            MethodHandles.lookup().ensureInitialized(JnaStaticPosixCLibrary.class);
        } catch (IllegalAccessException unexpected) {
            throw new AssertionError(unexpected);
        }
    }

    @Override
    public int mlockall(int flags) {
        return JnaStaticPosixCLibrary.mlockall(flags);
    }

    @Override
    public int geteuid() {
        return JnaStaticPosixCLibrary.geteuid();
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
        return JnaStaticPosixCLibrary.getrlimit(resource, jnaRlimit);
    }

    @Override
    public int setrlimit(int resource, RLimit rlimit) {
        assert rlimit instanceof JnaRLimit;
        var jnaRlimit = (JnaRLimit) rlimit;
        return JnaStaticPosixCLibrary.setrlimit(resource, jnaRlimit);
    }

    @Override
    public int open(String pathname, int flags, int mode) {
        return JnaStaticPosixCLibrary.open(pathname, flags, mode);
    }

    @Override
    public int close(int fd) {
        return JnaStaticPosixCLibrary.close(fd);
    }

    @Override
    public int fstat64(int fd, Stat64 stats) {
        assert stats instanceof JnaStat64;
        var jnaStats = (JnaStat64) stats;
        return JnaStaticPosixCLibrary.fstat64(fd, jnaStats);
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
