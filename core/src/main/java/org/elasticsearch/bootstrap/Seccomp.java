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

import com.sun.jna.Memory;
import com.sun.jna.Native;
import com.sun.jna.Pointer;
import com.sun.jna.Structure;

import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

/** 
 * installs system call filter, to block some dangerous system calls like execve
 * only supported on linux/amd64
 * not an example of how to write code!!!
 */
final class Seccomp {
    private static final ESLogger logger = Loggers.getLogger(Seccomp.class);

    static {
        try {
            Native.register("c");
        } catch (UnsatisfiedLinkError e) {
            logger.warn("unable to link C library. native methods (seccomp) will be disabled.", e);
        }
    }
    
    // TODO: support new seccomp(2) syscall, to specify SECCOMP_FILTER_FLAG_TSYNC
    
    static final int PR_SET_NO_NEW_PRIVS = 38;  // since Linux 3.5
    static final int PR_SET_SECCOMP = 22;       // since Linux 2.6.23
    static final int SECCOMP_MODE_FILTER = 2;   // since Linux Linux 3.5
    static native int prctl(int option, long arg2, long arg3, long arg4, long arg5);
    static native int prctl(int option, long arg2, SockFProg arg3, long arg4, long arg5);
    
    /** corresponds to struct sock_filter */
    static final class SockFilter {
        short code; // insn
        byte jt;    // number of insn to jump if true
        byte jf;    // number of insn to jump if false
        int k;      // additional data

        SockFilter(short code, byte jt, byte jf, int k) {
            this.code = code;
            this.jt = jt;
            this.jf = jf;
            this.k = k;
        }
    }
    
    /** corresponds to struct sock_fprog */
    public static final class SockFProg extends Structure implements Structure.ByReference {
        public short   len;           // number of filters
        public Pointer filter;       // filters
        
        public SockFProg(SockFilter filters[]) {
            len = (short) filters.length;
            Memory filter = new Memory(len * 8);
            ByteBuffer bbuf = filter.getByteBuffer(0, len * 8);
            for (SockFilter f : filters) {
                bbuf.putShort(f.code);
                bbuf.put(f.jt);
                bbuf.put(f.jf);
                bbuf.putInt(f.k);
            }
            this.filter = filter;
        }
        
        @Override
        protected List<String> getFieldOrder() {
            return Arrays.asList(new String[] { "len", "filter" });
        }
    }
    
    // BPF "macros" and constants
    static final int BPF_LD  = 0x00;
    static final int BPF_JMP = 0x05;
    static final int BPF_RET = 0x06;
    static final int BPF_W   = 0x00;
    static final int BPF_ABS = 0x20;
    static final int BPF_JEQ = 0x10;
    static final int BPF_K   = 0x0;
    
    static SockFilter BPF_STMT(int code, int k) {
        return new SockFilter((short) code, (byte) 0, (byte) 0, k);
    }
    
    static SockFilter BPF_JUMP(int code, int k, int jt, int jf) {
        return new SockFilter((short) code, (byte) jt, (byte) jf, k);
    }
    
    static final int AUDIT_ARCH_X86_64 = 0xC000003E;
    static final int SECCOMP_RET_ERRNO = 0x00050000;
    static final int SECCOMP_RET_DATA  = 0x0000FFFF;
    static final int SECCOMP_RET_ALLOW = 0x7FFF0000;
    static final int EACCES = 0xD;
    
    static final int SECCOMP_DATA_ARCH_OFFSET = 0x04;
    static final int SECCOMP_DATA_NR_OFFSET   = 0x0;
    
    // TODO: probably block some more syscalls, including clone/fork/vfork to get a better exception message maybe
    static final int SYSCALL_EXECVE = 59;
    
    /** try to install our filters */
    static void installFilter() {
        // first set PR_SET_NO_NEW_PRIVS, needed to be able to set a seccomp filter as ordinary user
        if (prctl(PR_SET_NO_NEW_PRIVS, 1, 0, 0, 0) < 0) {
            throw new UnsupportedOperationException("prctl(PR_SET_NO_NEW_PRIVS): " + JNACLibrary.strerror(Native.getLastError()));
        }
        
        SockFilter insns[] = {
          BPF_STMT(BPF_LD  + BPF_W   + BPF_ABS, SECCOMP_DATA_ARCH_OFFSET),
          BPF_JUMP(BPF_JMP + BPF_JEQ + BPF_K,   AUDIT_ARCH_X86_64, 0, 3),
          BPF_STMT(BPF_LD  + BPF_W   + BPF_ABS, SECCOMP_DATA_NR_OFFSET),
          BPF_JUMP(BPF_JMP + BPF_JEQ + BPF_K,   SYSCALL_EXECVE, 0, 1),
          BPF_STMT(BPF_RET + BPF_K, SECCOMP_RET_ERRNO | (EACCES & SECCOMP_RET_DATA)),
          BPF_STMT(BPF_RET + BPF_K, SECCOMP_RET_ALLOW)      
        };
        
        SockFProg prog = new SockFProg(insns);

        if (prctl(PR_SET_SECCOMP, SECCOMP_MODE_FILTER, prog, 0, 0) < 0) {
            throw new UnsupportedOperationException("prctl(PR_SET_SECCOMP): " + JNACLibrary.strerror(Native.getLastError()));
        }
    }
}
