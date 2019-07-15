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

import com.sun.jna.Library;
import com.sun.jna.Memory;
import com.sun.jna.Native;
import com.sun.jna.NativeLong;
import com.sun.jna.Pointer;
import com.sun.jna.Structure;
import com.sun.jna.ptr.PointerByReference;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.Constants;
import org.elasticsearch.core.internal.io.IOUtils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Installs a system call filter to block process execution.
 * <p>
 * This is supported on Linux, Solaris, FreeBSD, OpenBSD, Mac OS X, and Windows.
 * <p>
 * On Linux it currently supports amd64 and i386 architectures, requires Linux kernel 3.5 or above, and requires
 * {@code CONFIG_SECCOMP} and {@code CONFIG_SECCOMP_FILTER} compiled into the kernel.
 * <p>
 * On Linux BPF Filters are installed using either {@code seccomp(2)} (3.17+) or {@code prctl(2)} (3.5+). {@code seccomp(2)}
 * is preferred, as it allows filters to be applied to any existing threads in the process, and one motivation
 * here is to protect against bugs in the JVM. Otherwise, code will fall back to the {@code prctl(2)} method
 * which will at least protect elasticsearch application threads.
 * <p>
 * Linux BPF filters will return {@code EACCES} (Access Denied) for the following system calls:
 * <ul>
 *   <li>{@code execve}</li>
 *   <li>{@code fork}</li>
 *   <li>{@code vfork}</li>
 *   <li>{@code execveat}</li>
 * </ul>
 * <p>
 * On Solaris 10 or higher, the following privileges are dropped with {@code priv_set(3C)}:
 * <ul>
 *   <li>{@code PRIV_PROC_FORK}</li>
 *   <li>{@code PRIV_PROC_EXEC}</li>
 * </ul>
 * <p>
 * On BSD systems, process creation is restricted with {@code setrlimit(RLIMIT_NPROC)}.
 * <p>
 * On Mac OS X Leopard or above, a custom {@code sandbox(7)} ("Seatbelt") profile is installed that
 * denies the following rules:
 * <ul>
 *   <li>{@code process-fork}</li>
 *   <li>{@code process-exec}</li>
 * </ul>
 * <p>
 * On Windows, process creation is restricted with {@code SetInformationJobObject/ActiveProcessLimit}.
 * <p>
 * This is not intended as a sandbox. It is another level of security, mostly intended to annoy
 * security researchers and make their lives more difficult in achieving "remote execution" exploits.
 * @see <a href="http://www.kernel.org/doc/Documentation/prctl/seccomp_filter.txt">
 *      http://www.kernel.org/doc/Documentation/prctl/seccomp_filter.txt</a>
 * @see <a href="https://reverse.put.as/wp-content/uploads/2011/06/The-Apple-Sandbox-BHDC2011-Paper.pdf">
 *      https://reverse.put.as/wp-content/uploads/2011/06/The-Apple-Sandbox-BHDC2011-Paper.pdf</a>
 * @see <a href="https://docs.oracle.com/cd/E23824_01/html/821-1456/prbac-2.html">
 *      https://docs.oracle.com/cd/E23824_01/html/821-1456/prbac-2.html</a>
 */
// not an example of how to write code!!!
final class SystemCallFilter {
    private static final Logger logger = LogManager.getLogger(SystemCallFilter.class);

    // Linux implementation, based on seccomp(2) or prctl(2) with bpf filtering

    /** Access to non-standard Linux libc methods */
    interface LinuxLibrary extends Library {
        /**
         * maps to prctl(2)
         */
        int prctl(int option, NativeLong arg2, NativeLong arg3, NativeLong arg4, NativeLong arg5);
        /**
         * used to call seccomp(2), its too new...
         * this is the only way, DON'T use it on some other architecture unless you know wtf you are doing
         */
        NativeLong syscall(NativeLong number, Object... args);
    }

    // null if unavailable or something goes wrong.
    private static final LinuxLibrary linux_libc;

    static {
        LinuxLibrary lib = null;
        if (Constants.LINUX) {
            try {
                lib = (LinuxLibrary) Native.loadLibrary("c", LinuxLibrary.class);
            } catch (UnsatisfiedLinkError e) {
                logger.warn("unable to link C library. native methods (seccomp) will be disabled.", e);
            }
        }
        linux_libc = lib;
    }

    /** the preferred method is seccomp(2), since we can apply to all threads of the process */
    static final int SECCOMP_SET_MODE_FILTER   =   1;   // since Linux 3.17
    static final int SECCOMP_FILTER_FLAG_TSYNC =   1;   // since Linux 3.17

    /** otherwise, we can use prctl(2), which will at least protect ES application threads */
    static final int PR_GET_NO_NEW_PRIVS       =  39;   // since Linux 3.5
    static final int PR_SET_NO_NEW_PRIVS       =  38;   // since Linux 3.5
    static final int PR_GET_SECCOMP            =  21;   // since Linux 2.6.23
    static final int PR_SET_SECCOMP            =  22;   // since Linux 2.6.23
    static final long SECCOMP_MODE_FILTER      =   2;   // since Linux Linux 3.5

    /** corresponds to struct sock_filter */
    static final class SockFilter {
        short code; // insn
        byte jt;    // number of insn to jump (skip) if true
        byte jf;    // number of insn to jump (skip) if false
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
        public Pointer filter;        // filters

        SockFProg(SockFilter filters[]) {
            len = (short) filters.length;
            // serialize struct sock_filter * explicitly, its less confusing than the JNA magic we would need
            Memory filter = new Memory(len * 8);
            ByteBuffer bbuf = filter.getByteBuffer(0, len * 8);
            bbuf.order(ByteOrder.nativeOrder()); // little endian
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
            return Arrays.asList("len", "filter");
        }
    }

    // BPF "macros" and constants
    static final int BPF_LD  = 0x00;
    static final int BPF_W   = 0x00;
    static final int BPF_ABS = 0x20;
    static final int BPF_JMP = 0x05;
    static final int BPF_JEQ = 0x10;
    static final int BPF_JGE = 0x30;
    static final int BPF_JGT = 0x20;
    static final int BPF_RET = 0x06;
    static final int BPF_K   = 0x00;

    static SockFilter BPF_STMT(int code, int k) {
        return new SockFilter((short) code, (byte) 0, (byte) 0, k);
    }

    static SockFilter BPF_JUMP(int code, int k, int jt, int jf) {
        return new SockFilter((short) code, (byte) jt, (byte) jf, k);
    }

    static final int SECCOMP_RET_ERRNO = 0x00050000;
    static final int SECCOMP_RET_DATA  = 0x0000FFFF;
    static final int SECCOMP_RET_ALLOW = 0x7FFF0000;

    // some errno constants for error checking/handling
    static final int EACCES = 0x0D;
    static final int EFAULT = 0x0E;
    static final int EINVAL = 0x16;
    static final int ENOSYS = 0x26;

    // offsets that our BPF checks
    // check with offsetof() when adding a new arch, move to Arch if different.
    static final int SECCOMP_DATA_NR_OFFSET   = 0x00;
    static final int SECCOMP_DATA_ARCH_OFFSET = 0x04;

    static class Arch {
        /** AUDIT_ARCH_XXX constant from linux/audit.h */
        final int audit;
        /** syscall limit (necessary for blacklisting on amd64, to ban 32-bit syscalls) */
        final int limit;
        /** __NR_fork */
        final int fork;
        /** __NR_vfork */
        final int vfork;
        /** __NR_execve */
        final int execve;
        /**  __NR_execveat */
        final int execveat;
        /** __NR_seccomp */
        final int seccomp;

        Arch(int audit, int limit, int fork, int vfork, int execve, int execveat, int seccomp) {
            this.audit = audit;
            this.limit = limit;
            this.fork = fork;
            this.vfork = vfork;
            this.execve = execve;
            this.execveat = execveat;
            this.seccomp = seccomp;
        }
    }

    /** supported architectures map keyed by os.arch */
    private static final Map<String,Arch> ARCHITECTURES;
    static {
        ARCHITECTURES = Map.of(
                "amd64", new Arch(0xC000003E, 0x3FFFFFFF, 57, 58, 59, 322, 317),
                "aarch64", new Arch(0xC00000B7, 0xFFFFFFFF, 1079, 1071, 221, 281, 277));
    }

    /** invokes prctl() from linux libc library */
    private static int linux_prctl(int option, long arg2, long arg3, long arg4, long arg5) {
        return linux_libc.prctl(option, new NativeLong(arg2), new NativeLong(arg3), new NativeLong(arg4), new NativeLong(arg5));
    }

    /** invokes syscall() from linux libc library */
    private static long linux_syscall(long number, Object... args) {
        return linux_libc.syscall(new NativeLong(number), args).longValue();
    }

    /** try to install our BPF filters via seccomp() or prctl() to block execution */
    private static int linuxImpl() {
        // first be defensive: we can give nice errors this way, at the very least.
        // also, some of these security features get backported to old versions, checking kernel version here is a big no-no!
        final Arch arch = ARCHITECTURES.get(Constants.OS_ARCH);
        boolean supported = Constants.LINUX && arch != null;
        if (supported == false) {
            throw new UnsupportedOperationException("seccomp unavailable: '" + Constants.OS_ARCH + "' architecture unsupported");
        }

        // we couldn't link methods, could be some really ancient kernel (e.g. < 2.1.57) or some bug
        if (linux_libc == null) {
            throw new UnsupportedOperationException("seccomp unavailable: could not link methods. requires kernel 3.5+ " +
                "with CONFIG_SECCOMP and CONFIG_SECCOMP_FILTER compiled in");
        }

        // try to check system calls really are who they claim
        // you never know (e.g. https://chromium.googlesource.com/chromium/src.git/+/master/sandbox/linux/seccomp-bpf/sandbox_bpf.cc#57)
        final int bogusArg = 0xf7a46a5c;

        // test seccomp(BOGUS)
        long ret = linux_syscall(arch.seccomp, bogusArg);
        if (ret != -1) {
            throw new UnsupportedOperationException("seccomp unavailable: seccomp(BOGUS_OPERATION) returned " + ret);
        } else {
            int errno = Native.getLastError();
            switch (errno) {
                case ENOSYS: break; // ok
                case EINVAL: break; // ok
                default: throw new UnsupportedOperationException("seccomp(BOGUS_OPERATION): " + JNACLibrary.strerror(errno));
            }
        }

        // test seccomp(VALID, BOGUS)
        ret = linux_syscall(arch.seccomp, SECCOMP_SET_MODE_FILTER, bogusArg);
        if (ret != -1) {
            throw new UnsupportedOperationException("seccomp unavailable: seccomp(SECCOMP_SET_MODE_FILTER, BOGUS_FLAG) returned " + ret);
        } else {
            int errno = Native.getLastError();
            switch (errno) {
                case ENOSYS: break; // ok
                case EINVAL: break; // ok
                default: throw new UnsupportedOperationException("seccomp(SECCOMP_SET_MODE_FILTER, BOGUS_FLAG): "
                                                                 + JNACLibrary.strerror(errno));
            }
        }

        // test prctl(BOGUS)
        ret = linux_prctl(bogusArg, 0, 0, 0, 0);
        if (ret != -1) {
            throw new UnsupportedOperationException("seccomp unavailable: prctl(BOGUS_OPTION) returned " + ret);
        } else {
            int errno = Native.getLastError();
            switch (errno) {
                case ENOSYS: break; // ok
                case EINVAL: break; // ok
                default: throw new UnsupportedOperationException("prctl(BOGUS_OPTION): " + JNACLibrary.strerror(errno));
            }
        }

        // now just normal defensive checks

        // check for GET_NO_NEW_PRIVS
        switch (linux_prctl(PR_GET_NO_NEW_PRIVS, 0, 0, 0, 0)) {
            case 0: break; // not yet set
            case 1: break; // already set by caller
            default:
                int errno = Native.getLastError();
                if (errno == EINVAL) {
                    // friendly error, this will be the typical case for an old kernel
                    throw new UnsupportedOperationException("seccomp unavailable: requires kernel 3.5+ with" +
                                                            " CONFIG_SECCOMP and CONFIG_SECCOMP_FILTER compiled in");
                } else {
                    throw new UnsupportedOperationException("prctl(PR_GET_NO_NEW_PRIVS): " + JNACLibrary.strerror(errno));
                }
        }
        // check for SECCOMP
        switch (linux_prctl(PR_GET_SECCOMP, 0, 0, 0, 0)) {
            case 0: break; // not yet set
            case 2: break; // already in filter mode by caller
            default:
                int errno = Native.getLastError();
                if (errno == EINVAL) {
                    throw new UnsupportedOperationException("seccomp unavailable: CONFIG_SECCOMP not compiled into kernel," +
                                                            " CONFIG_SECCOMP and CONFIG_SECCOMP_FILTER are needed");
                } else {
                    throw new UnsupportedOperationException("prctl(PR_GET_SECCOMP): " + JNACLibrary.strerror(errno));
                }
        }
        // check for SECCOMP_MODE_FILTER
        if (linux_prctl(PR_SET_SECCOMP, SECCOMP_MODE_FILTER, 0, 0, 0) != 0) {
            int errno = Native.getLastError();
            switch (errno) {
                case EFAULT: break; // available
                case EINVAL: throw new UnsupportedOperationException("seccomp unavailable: CONFIG_SECCOMP_FILTER not" +
                    " compiled into kernel, CONFIG_SECCOMP and CONFIG_SECCOMP_FILTER are needed");
                default: throw new UnsupportedOperationException("prctl(PR_SET_SECCOMP): " + JNACLibrary.strerror(errno));
            }
        }

        // ok, now set PR_SET_NO_NEW_PRIVS, needed to be able to set a seccomp filter as ordinary user
        if (linux_prctl(PR_SET_NO_NEW_PRIVS, 1, 0, 0, 0) != 0) {
            throw new UnsupportedOperationException("prctl(PR_SET_NO_NEW_PRIVS): " + JNACLibrary.strerror(Native.getLastError()));
        }

        // check it worked
        if (linux_prctl(PR_GET_NO_NEW_PRIVS, 0, 0, 0, 0) != 1) {
            throw new UnsupportedOperationException("seccomp filter did not really succeed: prctl(PR_GET_NO_NEW_PRIVS): " +
                                                    JNACLibrary.strerror(Native.getLastError()));
        }

        // BPF installed to check arch, limit, then syscall.
        // See https://www.kernel.org/doc/Documentation/prctl/seccomp_filter.txt for details.
        SockFilter insns[] = {
          /* 1  */ BPF_STMT(BPF_LD  + BPF_W   + BPF_ABS, SECCOMP_DATA_ARCH_OFFSET),             //
          /* 2  */ BPF_JUMP(BPF_JMP + BPF_JEQ + BPF_K,   arch.audit,     0, 7),                 // if (arch != audit) goto fail;
          /* 3  */ BPF_STMT(BPF_LD  + BPF_W   + BPF_ABS, SECCOMP_DATA_NR_OFFSET),               //
          /* 4  */ BPF_JUMP(BPF_JMP + BPF_JGT + BPF_K,   arch.limit,     5, 0),                 // if (syscall > LIMIT) goto fail;
          /* 5  */ BPF_JUMP(BPF_JMP + BPF_JEQ + BPF_K,   arch.fork,      4, 0),                 // if (syscall == FORK) goto fail;
          /* 6  */ BPF_JUMP(BPF_JMP + BPF_JEQ + BPF_K,   arch.vfork,     3, 0),                 // if (syscall == VFORK) goto fail;
          /* 7  */ BPF_JUMP(BPF_JMP + BPF_JEQ + BPF_K,   arch.execve,    2, 0),                 // if (syscall == EXECVE) goto fail;
          /* 8  */ BPF_JUMP(BPF_JMP + BPF_JEQ + BPF_K,   arch.execveat,  1, 0),                 // if (syscall == EXECVEAT) goto fail;
          /* 9  */ BPF_STMT(BPF_RET + BPF_K, SECCOMP_RET_ALLOW),                                // pass: return OK;
          /* 10 */ BPF_STMT(BPF_RET + BPF_K, SECCOMP_RET_ERRNO | (EACCES & SECCOMP_RET_DATA)),  // fail: return EACCES;
        };
        // seccomp takes a long, so we pass it one explicitly to keep the JNA simple
        SockFProg prog = new SockFProg(insns);
        prog.write();
        long pointer = Pointer.nativeValue(prog.getPointer());

        int method = 1;
        // install filter, if this works, after this there is no going back!
        // first try it with seccomp(SECCOMP_SET_MODE_FILTER), falling back to prctl()
        if (linux_syscall(arch.seccomp, SECCOMP_SET_MODE_FILTER, SECCOMP_FILTER_FLAG_TSYNC, new NativeLong(pointer)) != 0) {
            method = 0;
            int errno1 = Native.getLastError();
            if (logger.isDebugEnabled()) {
                logger.debug("seccomp(SECCOMP_SET_MODE_FILTER): {}, falling back to prctl(PR_SET_SECCOMP)...",
                             JNACLibrary.strerror(errno1));
            }
            if (linux_prctl(PR_SET_SECCOMP, SECCOMP_MODE_FILTER, pointer, 0, 0) != 0) {
                int errno2 = Native.getLastError();
                throw new UnsupportedOperationException("seccomp(SECCOMP_SET_MODE_FILTER): " + JNACLibrary.strerror(errno1) +
                                                        ", prctl(PR_SET_SECCOMP): " + JNACLibrary.strerror(errno2));
            }
        }

        // now check that the filter was really installed, we should be in filter mode.
        if (linux_prctl(PR_GET_SECCOMP, 0, 0, 0, 0) != 2) {
            throw new UnsupportedOperationException("seccomp filter installation did not really succeed. seccomp(PR_GET_SECCOMP): "
                                                    + JNACLibrary.strerror(Native.getLastError()));
        }

        logger.debug("Linux seccomp filter installation successful, threads: [{}]", method == 1 ? "all" : "app" );
        return method;
    }

    // OS X implementation via sandbox(7)

    /** Access to non-standard OS X libc methods */
    interface MacLibrary extends Library {
        /**
         * maps to sandbox_init(3), since Leopard
         */
        int sandbox_init(String profile, long flags, PointerByReference errorbuf);

        /**
         * releases memory when an error occurs during initialization (e.g. syntax bug)
         */
        void sandbox_free_error(Pointer errorbuf);
    }

    // null if unavailable, or something goes wrong.
    private static final MacLibrary libc_mac;

    static {
        MacLibrary lib = null;
        if (Constants.MAC_OS_X) {
            try {
                lib = (MacLibrary) Native.loadLibrary("c", MacLibrary.class);
            } catch (UnsatisfiedLinkError e) {
                logger.warn("unable to link C library. native methods (seatbelt) will be disabled.", e);
            }
        }
        libc_mac = lib;
    }

    /** The only supported flag... */
    static final int SANDBOX_NAMED = 1;
    /** Allow everything except process fork and execution */
    static final String SANDBOX_RULES = "(version 1) (allow default) (deny process-fork) (deny process-exec)";

    /** try to install our custom rule profile into sandbox_init() to block execution */
    private static void macImpl(Path tmpFile) throws IOException {
        // first be defensive: we can give nice errors this way, at the very least.
        boolean supported = Constants.MAC_OS_X;
        if (supported == false) {
            throw new IllegalStateException("bug: should not be trying to initialize seatbelt for an unsupported OS");
        }

        // we couldn't link methods, could be some really ancient OS X (< Leopard) or some bug
        if (libc_mac == null) {
            throw new UnsupportedOperationException("seatbelt unavailable: could not link methods. requires Leopard or above.");
        }

        // write rules to a temporary file, which will be passed to sandbox_init()
        Path rules = Files.createTempFile(tmpFile, "es", "sb");
        Files.write(rules, Collections.singleton(SANDBOX_RULES));

        boolean success = false;
        try {
            PointerByReference errorRef = new PointerByReference();
            int ret = libc_mac.sandbox_init(rules.toAbsolutePath().toString(), SANDBOX_NAMED, errorRef);
            // if sandbox_init() fails, add the message from the OS (e.g. syntax error) and free the buffer
            if (ret != 0) {
                Pointer errorBuf = errorRef.getValue();
                RuntimeException e = new UnsupportedOperationException("sandbox_init(): " + errorBuf.getString(0));
                libc_mac.sandbox_free_error(errorBuf);
                throw e;
            }
            logger.debug("OS X seatbelt initialization successful");
            success = true;
        } finally {
            if (success) {
                Files.delete(rules);
            } else {
                IOUtils.deleteFilesIgnoringExceptions(rules);
            }
        }
    }

    // Solaris implementation via priv_set(3C)

    /** Access to non-standard Solaris libc methods */
    interface SolarisLibrary extends Library {
        /**
         * see priv_set(3C), a convenience method for setppriv(2).
         */
        int priv_set(int op, String which, String... privs);
    }

    // null if unavailable, or something goes wrong.
    private static final SolarisLibrary libc_solaris;

    static {
        SolarisLibrary lib = null;
        if (Constants.SUN_OS) {
            try {
                lib = (SolarisLibrary) Native.loadLibrary("c", SolarisLibrary.class);
            } catch (UnsatisfiedLinkError e) {
                logger.warn("unable to link C library. native methods (priv_set) will be disabled.", e);
            }
        }
        libc_solaris = lib;
    }

    // constants for priv_set(2)
    static final int PRIV_OFF = 1;
    static final String PRIV_ALLSETS = null;
    // see privileges(5) for complete list of these
    static final String PRIV_PROC_FORK = "proc_fork";
    static final String PRIV_PROC_EXEC = "proc_exec";

    static void solarisImpl() {
        // first be defensive: we can give nice errors this way, at the very least.
        boolean supported = Constants.SUN_OS;
        if (supported == false) {
            throw new IllegalStateException("bug: should not be trying to initialize priv_set for an unsupported OS");
        }

        // we couldn't link methods, could be some really ancient Solaris or some bug
        if (libc_solaris == null) {
            throw new UnsupportedOperationException("priv_set unavailable: could not link methods. requires Solaris 10+");
        }

        // drop a null-terminated list of privileges
        if (libc_solaris.priv_set(PRIV_OFF, PRIV_ALLSETS, PRIV_PROC_FORK, PRIV_PROC_EXEC, null) != 0) {
            throw new UnsupportedOperationException("priv_set unavailable: priv_set(): " + JNACLibrary.strerror(Native.getLastError()));
        }

        logger.debug("Solaris priv_set initialization successful");
    }

    // BSD implementation via setrlimit(2)

    // TODO: add OpenBSD to Lucene Constants
    // TODO: JNA doesn't have netbsd support, but this mechanism should work there too.
    static final boolean OPENBSD = Constants.OS_NAME.startsWith("OpenBSD");

    // not a standard limit, means something different on linux, etc!
    static final int RLIMIT_NPROC = 7;

    static void bsdImpl() {
        boolean supported = Constants.FREE_BSD || OPENBSD || Constants.MAC_OS_X;
        if (supported == false) {
            throw new IllegalStateException("bug: should not be trying to initialize RLIMIT_NPROC for an unsupported OS");
        }

        JNACLibrary.Rlimit limit = new JNACLibrary.Rlimit();
        limit.rlim_cur.setValue(0);
        limit.rlim_max.setValue(0);
        if (JNACLibrary.setrlimit(RLIMIT_NPROC, limit) != 0) {
            throw new UnsupportedOperationException("RLIMIT_NPROC unavailable: " + JNACLibrary.strerror(Native.getLastError()));
        }

        logger.debug("BSD RLIMIT_NPROC initialization successful");
    }

    // windows impl via job ActiveProcessLimit

    static void windowsImpl() {
        if (!Constants.WINDOWS) {
            throw new IllegalStateException("bug: should not be trying to initialize ActiveProcessLimit for an unsupported OS");
        }

        JNAKernel32Library lib = JNAKernel32Library.getInstance();

        // create a new Job
        Pointer job = lib.CreateJobObjectW(null, null);
        if (job == null) {
            throw new UnsupportedOperationException("CreateJobObject: " + Native.getLastError());
        }

        try {
            // retrieve the current basic limits of the job
            int clazz = JNAKernel32Library.JOBOBJECT_BASIC_LIMIT_INFORMATION_CLASS;
            JNAKernel32Library.JOBOBJECT_BASIC_LIMIT_INFORMATION limits = new JNAKernel32Library.JOBOBJECT_BASIC_LIMIT_INFORMATION();
            limits.write();
            if (!lib.QueryInformationJobObject(job, clazz, limits.getPointer(), limits.size(), null)) {
                throw new UnsupportedOperationException("QueryInformationJobObject: " + Native.getLastError());
            }
            limits.read();
            // modify the number of active processes to be 1 (exactly the one process we will add to the job).
            limits.ActiveProcessLimit = 1;
            limits.LimitFlags = JNAKernel32Library.JOB_OBJECT_LIMIT_ACTIVE_PROCESS;
            limits.write();
            if (!lib.SetInformationJobObject(job, clazz, limits.getPointer(), limits.size())) {
                throw new UnsupportedOperationException("SetInformationJobObject: " + Native.getLastError());
            }
            // assign ourselves to the job
            if (!lib.AssignProcessToJobObject(job, lib.GetCurrentProcess())) {
                throw new UnsupportedOperationException("AssignProcessToJobObject: " + Native.getLastError());
            }
        } finally {
            lib.CloseHandle(job);
        }

        logger.debug("Windows ActiveProcessLimit initialization successful");
    }

    /**
     * Attempt to drop the capability to execute for the process.
     * <p>
     * This is best effort and OS and architecture dependent. It may throw any Throwable.
     * @return 0 if we can do this for application threads, 1 for the entire process
     */
    static int init(Path tmpFile) throws Exception {
        if (Constants.LINUX) {
            return linuxImpl();
        } else if (Constants.MAC_OS_X) {
            // try to enable both mechanisms if possible
            bsdImpl();
            macImpl(tmpFile);
            return 1;
        } else if (Constants.SUN_OS) {
            solarisImpl();
            return 1;
        } else if (Constants.FREE_BSD || OPENBSD) {
            bsdImpl();
            return 1;
        } else if (Constants.WINDOWS) {
            windowsImpl();
            return 1;
        } else {
            throw new UnsupportedOperationException("syscall filtering not supported for OS: '" + Constants.OS_NAME + "'");
        }
    }
}
