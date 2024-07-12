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
        try {
            this.functions = Native.load("c", NativeFunctions.class);
        } catch (UnsatisfiedLinkError e) {
            throw new UnsupportedOperationException(
                "seccomp unavailable: could not link methods. requires kernel 3.5+ "
                    + "with CONFIG_SECCOMP and CONFIG_SECCOMP_FILTER compiled in"
            );
        }
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
