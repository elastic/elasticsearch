/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.nativeaccess.jna;

import com.sun.jna.Library;
import com.sun.jna.Memory;
import com.sun.jna.Native;
import com.sun.jna.NativeLong;
import com.sun.jna.Pointer;
import com.sun.jna.Structure;

import org.elasticsearch.nativeaccess.lib.LinuxCLibrary;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

class JnaLinuxCLibrary implements LinuxCLibrary {

    /** corresponds to struct statx */
    @Structure.FieldOrder({ "_ignore1", "stx_blocks", "_ignore2" })
    public static final class JnaStatx extends Structure implements Structure.ByReference, statx {
        // ignoring bytes up to stx_blocks, including 2 bytes padding after stx_mode for alignment
        public byte[] _ignore1 = new byte[48];
        public long stx_blocks = 0;
        // ignoring bytes after stx_blocks. statx_timestamp is 16 bytes each for alignment
        public byte[] _ignore2 = new byte[104];

        @Override
        public long stx_blocks() {
            return stx_blocks;
        }
    }

    @Structure.FieldOrder({ "len", "filter" })
    public static final class JnaSockFProg extends Structure implements Structure.ByReference, SockFProg {
        public short len;           // number of filters
        public Pointer filter;        // filters

        JnaSockFProg(SockFilter filters[]) {
            len = (short) filters.length;
            // serialize struct sock_filter * explicitly, its less confusing than the JNA magic we would need
            Memory filter = new Memory(len * 8);
            ByteBuffer bbuf = filter.getByteBuffer(0, len * 8);
            bbuf.order(ByteOrder.nativeOrder()); // little endian
            for (SockFilter f : filters) {
                bbuf.putShort(f.code());
                bbuf.put(f.jt());
                bbuf.put(f.jf());
                bbuf.putInt(f.k());
            }
            this.filter = filter;
        }

        @Override
        public long address() {
            return Pointer.nativeValue(getPointer());
        }
    }

    private interface NativeFunctions extends Library {
        int statx(int dirfd, String pathname, int flags, int mask, Pointer statxbuf);

        /**
         * maps to prctl(2)
         */
        int prctl(int option, NativeLong arg2, NativeLong arg3, NativeLong arg4, NativeLong arg5);

        /**
         * used to call seccomp(2), its too new...
         * this is the only way, DON'T use it on some other architecture unless you know wtf you are doing
         */
        NativeLong syscall(NativeLong number, Object... args);

        int fallocate(int fd, int mode, long offset, long length);
    }

    private final NativeFunctions functions;

    JnaLinuxCLibrary() {
        this.functions = Native.load("c", NativeFunctions.class);
    }

    @Override
    public statx newStatx() {
        return new JnaStatx();
    }

    @Override
    public int statx(int dirfd, String pathname, int flags, int mask, statx statxbuf) {
        assert statxbuf instanceof JnaStatx;
        var jnaStatxbuf = (JnaStatx) statxbuf;
        return functions.statx(dirfd, pathname, flags, mask, jnaStatxbuf.getPointer());
    }

    @Override
    public SockFProg newSockFProg(SockFilter[] filters) {
        var prog = new JnaSockFProg(filters);
        prog.write();
        return prog;
    }

    @Override
    public int prctl(int option, long arg2, long arg3, long arg4, long arg5) {
        return functions.prctl(option, new NativeLong(arg2), new NativeLong(arg3), new NativeLong(arg4), new NativeLong(arg5));
    }

    @Override
    public long syscall(long number, int operation, int flags, long address) {
        return functions.syscall(new NativeLong(number), operation, flags, address).longValue();
    }

    @Override
    public int fallocate(int fd, int mode, long offset, long length) {
        return functions.fallocate(fd, mode, offset, length);
    }
}
