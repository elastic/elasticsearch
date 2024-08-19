/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.nativeaccess;

import org.elasticsearch.nativeaccess.lib.LinuxCLibrary;
import org.elasticsearch.nativeaccess.lib.LinuxCLibrary.SockFProg;
import org.elasticsearch.nativeaccess.lib.LinuxCLibrary.SockFilter;
import org.elasticsearch.nativeaccess.lib.NativeLibraryProvider;
import org.elasticsearch.nativeaccess.lib.SystemdLibrary;

import java.util.Map;

class LinuxNativeAccess extends PosixNativeAccess {

    private static final int STATX_BLOCKS = 0x400; /* Want/got stx_blocks */

    /** the preferred method is seccomp(2), since we can apply to all threads of the process */
    static final int SECCOMP_SET_MODE_FILTER = 1;   // since Linux 3.17
    static final int SECCOMP_FILTER_FLAG_TSYNC = 1;   // since Linux 3.17

    /** otherwise, we can use prctl(2), which will at least protect ES application threads */
    static final int PR_GET_NO_NEW_PRIVS = 39;   // since Linux 3.5
    static final int PR_SET_NO_NEW_PRIVS = 38;   // since Linux 3.5
    static final int PR_GET_SECCOMP = 21;   // since Linux 2.6.23
    static final int PR_SET_SECCOMP = 22;   // since Linux 2.6.23
    static final long SECCOMP_MODE_FILTER = 2;   // since Linux Linux 3.5

    // BPF "macros" and constants
    static final int BPF_LD = 0x00;
    static final int BPF_W = 0x00;
    static final int BPF_ABS = 0x20;
    static final int BPF_JMP = 0x05;
    static final int BPF_JEQ = 0x10;
    static final int BPF_JGE = 0x30;
    static final int BPF_JGT = 0x20;
    static final int BPF_RET = 0x06;
    static final int BPF_K = 0x00;

    static SockFilter BPF_STMT(int code, int k) {
        return new SockFilter((short) code, (byte) 0, (byte) 0, k);
    }

    static SockFilter BPF_JUMP(int code, int k, int jt, int jf) {
        return new SockFilter((short) code, (byte) jt, (byte) jf, k);
    }

    static final int SECCOMP_RET_ERRNO = 0x00050000;
    static final int SECCOMP_RET_DATA = 0x0000FFFF;
    static final int SECCOMP_RET_ALLOW = 0x7FFF0000;

    // some errno constants for error checking/handling
    static final int EACCES = 0x0D;
    static final int EFAULT = 0x0E;
    static final int EINVAL = 0x16;
    static final int ENOSYS = 0x26;

    // offsets that our BPF checks
    // check with offsetof() when adding a new arch, move to Arch if different.
    static final int SECCOMP_DATA_NR_OFFSET = 0x00;
    static final int SECCOMP_DATA_ARCH_OFFSET = 0x04;

    record Arch(
        int audit,    // AUDIT_ARCH_XXX constant from linux/audit.h
        int limit,    // syscall limit (necessary for blacklisting on amd64, to ban 32-bit syscalls)
        int fork,     // __NR_fork
        int vfork,    // __NR_vfork
        int execve,   // __NR_execve
        int execveat, // __NR_execveat
        int seccomp   // __NR_seccomp
    ) {}

    /** supported architectures for seccomp keyed by os.arch */
    private static final Map<String, Arch> ARCHITECTURES;
    static {
        ARCHITECTURES = Map.of(
            "amd64",
            new Arch(0xC000003E, 0x3FFFFFFF, 57, 58, 59, 322, 317),
            "aarch64",
            new Arch(0xC00000B7, 0xFFFFFFFF, 1079, 1071, 221, 281, 277)
        );
    }

    private final LinuxCLibrary linuxLibc;
    private final Systemd systemd;

    LinuxNativeAccess(NativeLibraryProvider libraryProvider) {
        super("Linux", libraryProvider, new PosixConstants(-1L, 9, 1, 8, 64, 144, 48, 64));
        this.linuxLibc = libraryProvider.getLibrary(LinuxCLibrary.class);
        this.systemd = new Systemd(libraryProvider.getLibrary(SystemdLibrary.class));
    }

    @Override
    protected long getMaxThreads() {
        // this is only valid on Linux and the value *is* different on OS X
        // see /usr/include/sys/resource.h on OS X
        // on Linux the resource RLIMIT_NPROC means *the number of threads*
        // this is in opposition to BSD-derived OSes
        final int rlimit_nproc = 6;
        return getRLimit(rlimit_nproc, "max number of threads");
    }

    @Override
    public Systemd systemd() {
        return systemd;
    }

    @Override
    protected void logMemoryLimitInstructions() {
        // give specific instructions for the linux case to make it easy
        String user = System.getProperty("user.name");
        logger.warn("""
            These can be adjusted by modifying /etc/security/limits.conf, for example:
            \t# allow user '{}' mlockall
            \t{} soft memlock unlimited
            \t{} hard memlock unlimited""", user, user, user);
        logger.warn("If you are logged in interactively, you will have to re-login for the new limits to take effect.");
    }

    @Override
    protected boolean nativePreallocate(int fd, long currentSize, long newSize) {
        final int rc = linuxLibc.fallocate(fd, 0, currentSize, newSize - currentSize);
        if (rc != 0) {
            logger.warn("fallocate failed: " + libc.strerror(libc.errno()));
            return false;
        }
        return true;
    }

    /**
     * Installs exec system call filtering for Linux.
     * <p>
     * On Linux exec system call filtering currently supports amd64 and aarch64 architectures.
     * It requires Linux kernel 3.5 or above, and {@code CONFIG_SECCOMP} and {@code CONFIG_SECCOMP_FILTER}
     * compiled into the kernel.
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
     * @see <a href="http://www.kernel.org/doc/Documentation/prctl/seccomp_filter.txt">
     *  *      http://www.kernel.org/doc/Documentation/prctl/seccomp_filter.txt</a>
     */
    @Override
    public void tryInstallExecSandbox() {
        // first be defensive: we can give nice errors this way, at the very least.
        // also, some of these security features get backported to old versions, checking kernel version here is a big no-no!
        String archId = System.getProperty("os.arch");
        final Arch arch = ARCHITECTURES.get(archId);
        if (arch == null) {
            throw new UnsupportedOperationException("seccomp unavailable: '" + archId + "' architecture unsupported");
        }

        // try to check system calls really are who they claim
        // you never know (e.g. https://chromium.googlesource.com/chromium/src.git/+/master/sandbox/linux/seccomp-bpf/sandbox_bpf.cc#57)
        final int bogusArg = 0xf7a46a5c;

        // test seccomp(BOGUS)
        long ret = linuxLibc.syscall(arch.seccomp, bogusArg, 0, 0);
        if (ret != -1) {
            throw new UnsupportedOperationException("seccomp unavailable: seccomp(BOGUS_OPERATION) returned " + ret);
        } else {
            int errno = libc.errno();
            switch (errno) {
                case ENOSYS:
                    break; // ok
                case EINVAL:
                    break; // ok
                default:
                    throw new UnsupportedOperationException("seccomp(BOGUS_OPERATION): " + libc.strerror(errno));
            }
        }

        // test seccomp(VALID, BOGUS)
        ret = linuxLibc.syscall(arch.seccomp, SECCOMP_SET_MODE_FILTER, bogusArg, 0);
        if (ret != -1) {
            throw new UnsupportedOperationException("seccomp unavailable: seccomp(SECCOMP_SET_MODE_FILTER, BOGUS_FLAG) returned " + ret);
        } else {
            int errno = libc.errno();
            switch (errno) {
                case ENOSYS:
                    break; // ok
                case EINVAL:
                    break; // ok
                default:
                    throw new UnsupportedOperationException("seccomp(SECCOMP_SET_MODE_FILTER, BOGUS_FLAG): " + libc.strerror(errno));
            }
        }

        // test prctl(BOGUS)
        ret = linuxLibc.prctl(bogusArg, 0, 0, 0, 0);
        if (ret != -1) {
            throw new UnsupportedOperationException("seccomp unavailable: prctl(BOGUS_OPTION) returned " + ret);
        } else {
            int errno = libc.errno();
            switch (errno) {
                case ENOSYS:
                    break; // ok
                case EINVAL:
                    break; // ok
                default:
                    throw new UnsupportedOperationException("prctl(BOGUS_OPTION): " + libc.strerror(errno));
            }
        }

        // now just normal defensive checks

        // check for GET_NO_NEW_PRIVS
        switch (linuxLibc.prctl(PR_GET_NO_NEW_PRIVS, 0, 0, 0, 0)) {
            case 0:
                break; // not yet set
            case 1:
                break; // already set by caller
            default:
                int errno = libc.errno();
                if (errno == EINVAL) {
                    // friendly error, this will be the typical case for an old kernel
                    throw new UnsupportedOperationException(
                        "seccomp unavailable: requires kernel 3.5+ with" + " CONFIG_SECCOMP and CONFIG_SECCOMP_FILTER compiled in"
                    );
                } else {
                    throw new UnsupportedOperationException("prctl(PR_GET_NO_NEW_PRIVS): " + libc.strerror(errno));
                }
        }
        // check for SECCOMP
        switch (linuxLibc.prctl(PR_GET_SECCOMP, 0, 0, 0, 0)) {
            case 0:
                break; // not yet set
            case 2:
                break; // already in filter mode by caller
            default:
                int errno = libc.errno();
                if (errno == EINVAL) {
                    throw new UnsupportedOperationException(
                        "seccomp unavailable: CONFIG_SECCOMP not compiled into kernel,"
                            + " CONFIG_SECCOMP and CONFIG_SECCOMP_FILTER are needed"
                    );
                } else {
                    throw new UnsupportedOperationException("prctl(PR_GET_SECCOMP): " + libc.strerror(errno));
                }
        }
        // check for SECCOMP_MODE_FILTER
        if (linuxLibc.prctl(PR_SET_SECCOMP, SECCOMP_MODE_FILTER, 0, 0, 0) != 0) {
            int errno = libc.errno();
            switch (errno) {
                case EFAULT:
                    break; // available
                case EINVAL:
                    throw new UnsupportedOperationException(
                        "seccomp unavailable: CONFIG_SECCOMP_FILTER not"
                            + " compiled into kernel, CONFIG_SECCOMP and CONFIG_SECCOMP_FILTER are needed"
                    );
                default:
                    throw new UnsupportedOperationException("prctl(PR_SET_SECCOMP): " + libc.strerror(errno));
            }
        }

        // ok, now set PR_SET_NO_NEW_PRIVS, needed to be able to set a seccomp filter as ordinary user
        if (linuxLibc.prctl(PR_SET_NO_NEW_PRIVS, 1, 0, 0, 0) != 0) {
            throw new UnsupportedOperationException("prctl(PR_SET_NO_NEW_PRIVS): " + libc.strerror(libc.errno()));
        }

        // check it worked
        if (linuxLibc.prctl(PR_GET_NO_NEW_PRIVS, 0, 0, 0, 0) != 1) {
            throw new UnsupportedOperationException(
                "seccomp filter did not really succeed: prctl(PR_GET_NO_NEW_PRIVS): " + libc.strerror(libc.errno())
            );
        }

        // BPF installed to check arch, limit, then syscall.
        // See https://www.kernel.org/doc/Documentation/prctl/seccomp_filter.txt for details.
        SockFilter insns[] = {
            /* 1  */ BPF_STMT(BPF_LD + BPF_W + BPF_ABS, SECCOMP_DATA_ARCH_OFFSET),             //
            /* 2  */ BPF_JUMP(BPF_JMP + BPF_JEQ + BPF_K, arch.audit, 0, 7),                 // if (arch != audit) goto fail;
            /* 3  */ BPF_STMT(BPF_LD + BPF_W + BPF_ABS, SECCOMP_DATA_NR_OFFSET),               //
            /* 4  */ BPF_JUMP(BPF_JMP + BPF_JGT + BPF_K, arch.limit, 5, 0),                 // if (syscall > LIMIT) goto fail;
            /* 5  */ BPF_JUMP(BPF_JMP + BPF_JEQ + BPF_K, arch.fork, 4, 0),                 // if (syscall == FORK) goto fail;
            /* 6  */ BPF_JUMP(BPF_JMP + BPF_JEQ + BPF_K, arch.vfork, 3, 0),                 // if (syscall == VFORK) goto fail;
            /* 7  */ BPF_JUMP(BPF_JMP + BPF_JEQ + BPF_K, arch.execve, 2, 0),                 // if (syscall == EXECVE) goto fail;
            /* 8  */ BPF_JUMP(BPF_JMP + BPF_JEQ + BPF_K, arch.execveat, 1, 0),                 // if (syscall == EXECVEAT) goto fail;
            /* 9  */ BPF_STMT(BPF_RET + BPF_K, SECCOMP_RET_ALLOW),                                // pass: return OK;
            /* 10 */ BPF_STMT(BPF_RET + BPF_K, SECCOMP_RET_ERRNO | (EACCES & SECCOMP_RET_DATA)),  // fail: return EACCES;
        };
        // seccomp takes a long, so we pass it one explicitly to keep the JNA simple
        SockFProg prog = linuxLibc.newSockFProg(insns);

        int method = 1;
        // install filter, if this works, after this there is no going back!
        // first try it with seccomp(SECCOMP_SET_MODE_FILTER), falling back to prctl()
        if (linuxLibc.syscall(arch.seccomp, SECCOMP_SET_MODE_FILTER, SECCOMP_FILTER_FLAG_TSYNC, prog.address()) != 0) {
            method = 0;
            int errno1 = libc.errno();
            if (logger.isDebugEnabled()) {
                logger.debug("seccomp(SECCOMP_SET_MODE_FILTER): {}, falling back to prctl(PR_SET_SECCOMP)...", libc.strerror(errno1));
            }
            if (linuxLibc.prctl(PR_SET_SECCOMP, SECCOMP_MODE_FILTER, prog.address(), 0, 0) != 0) {
                int errno2 = libc.errno();
                throw new UnsupportedOperationException(
                    "seccomp(SECCOMP_SET_MODE_FILTER): " + libc.strerror(errno1) + ", prctl(PR_SET_SECCOMP): " + libc.strerror(errno2)
                );
            }
        }

        // now check that the filter was really installed, we should be in filter mode.
        if (linuxLibc.prctl(PR_GET_SECCOMP, 0, 0, 0, 0) != 2) {
            throw new UnsupportedOperationException(
                "seccomp filter installation did not really succeed. seccomp(PR_GET_SECCOMP): " + libc.strerror(libc.errno())
            );
        }

        logger.debug("Linux seccomp filter installation successful, threads: [{}]", method == 1 ? "all" : "app");
        execSandboxState = method == 1 ? ExecSandboxState.ALL_THREADS : ExecSandboxState.EXISTING_THREADS;
    }
}
