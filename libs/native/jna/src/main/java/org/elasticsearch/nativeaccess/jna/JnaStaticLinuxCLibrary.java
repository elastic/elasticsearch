/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.nativeaccess.jna;

import com.sun.jna.Memory;
import com.sun.jna.Native;
import com.sun.jna.NativeLong;
import com.sun.jna.Pointer;
import com.sun.jna.Structure;

import org.elasticsearch.nativeaccess.lib.LinuxCLibrary;
import org.elasticsearch.nativeaccess.lib.LinuxCLibrary.SockFProg;
import org.elasticsearch.nativeaccess.lib.LinuxCLibrary.SockFilter;
import org.elasticsearch.nativeaccess.lib.LinuxCLibrary.statx;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.List;

class JnaStaticLinuxCLibrary {

    static {
        Native.register("c");
    }

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

    static final class JnaSockFProg extends Structure implements Structure.ByReference, SockFProg {
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
        protected List<String> getFieldOrder() {
            return Arrays.asList("len", "filter");
        }

        @Override
        public long address() {
            return Pointer.nativeValue(getPointer());
        }
    }

    static native int statx(int dirfd, String pathname, int flags, int mask, JnaStatx statxbuf);

    /**
     * maps to prctl(2)
     */
    static native int prctl(int option, NativeLong arg2, NativeLong arg3, NativeLong arg4, NativeLong arg5);

    /**
     * used to call seccomp(2), its too new...
     * this is the only way, DON'T use it on some other architecture unless you know wtf you are doing
     */
    static native NativeLong syscall(NativeLong number, Object... args);

    static native int fallocate(int fd, int mode, long offset, long length);
}
