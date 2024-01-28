/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.nativeaccess.jna;

import com.sun.jna.NativeLong;

import org.elasticsearch.nativeaccess.jna.JnaStaticLinuxCLibrary.JnaSockFProg;
import org.elasticsearch.nativeaccess.jna.JnaStaticLinuxCLibrary.JnaStatx;
import org.elasticsearch.nativeaccess.lib.LinuxCLibrary;

class JnaLinuxCLibrary implements LinuxCLibrary {
    @Override
    public statx newStatx() {
        return new JnaStatx();
    }

    @Override
    public int statx(int dirfd, String pathname, int flags, int mask, statx statxbuf) {
        assert statxbuf instanceof JnaStatx;
        var jnaStatxbuf = (JnaStatx) statxbuf;
        return JnaStaticLinuxCLibrary.statx(dirfd, pathname, flags, mask, jnaStatxbuf);
    }

    @Override
    public SockFProg newSockFProg(SockFilter[] filters) {
        var prog = new JnaSockFProg(filters);
        prog.write();
        return prog;
    }

    @Override
    public int prctl(int option, long arg2, long arg3, long arg4, long arg5) {
        return JnaStaticLinuxCLibrary.prctl(option, new NativeLong(arg2), new NativeLong(arg3), new NativeLong(arg4), new NativeLong(arg5));
    }

    @Override
    public long syscall(long number, int operation, int flags, long address) {
        return JnaStaticLinuxCLibrary.syscall(new NativeLong(number), operation, flags, address).longValue();
    }
}
