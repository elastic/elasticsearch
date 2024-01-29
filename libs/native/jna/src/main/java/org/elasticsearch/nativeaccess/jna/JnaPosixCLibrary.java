/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.nativeaccess.jna;

import com.sun.jna.Native;

import org.elasticsearch.jdk.JdkUtils;
import org.elasticsearch.nativeaccess.jna.JnaStaticPosixCLibrary.JnaRLimit;
import org.elasticsearch.nativeaccess.jna.JnaStaticPosixCLibrary.JnaStat;
import org.elasticsearch.nativeaccess.lib.PosixCLibrary;

class JnaPosixCLibrary implements PosixCLibrary {

    static {
        JdkUtils.ensureInitialized(JnaStaticPosixCLibrary.class);
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
    public Stat newStat(int sizeof, int stSizeOffset) {
        return new JnaStat(sizeof, stSizeOffset);
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
    public int fstat(int fd, Stat stats) {
        return JnaStaticPosixCLibrary.fstat(fd, stats);
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
