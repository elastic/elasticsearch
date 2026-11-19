/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.nativeaccess.lib;

import org.elasticsearch.foreign.CaptureSystemError;
import org.elasticsearch.foreign.Function;
import org.elasticsearch.foreign.InlineStringField;
import org.elasticsearch.foreign.LibrarySpecification;
import org.elasticsearch.foreign.LinkerHelper;
import org.elasticsearch.foreign.Offset;
import org.elasticsearch.foreign.Platform;
import org.elasticsearch.foreign.Sizeof;
import org.elasticsearch.foreign.StructFactory;
import org.elasticsearch.foreign.StructSize;
import org.elasticsearch.foreign.StructSpecification;
import org.elasticsearch.foreign.Variadic;

import java.lang.foreign.MemorySegment;
import java.lang.ref.Reference;
import java.util.Objects;

/**
 * Provides access to methods in libc.so available on POSIX systems.
 */
@LibrarySpecification(
    unavailableOn = { Platform.WINDOWS_X64 },
    symbolResolver = PosixSymbolResolver.class,
    methodHandleResolver = PosixMethodHandleResolver.class
)
public abstract class PosixCLibrary {

    /** socket domain indicating unix file socket */
    public static final short AF_UNIX = 1;

    /** socket type indicating a datagram-oriented socket */
    public static final int SOCK_DGRAM = 2;

    public static final int POSIX_MADV_NORMAL = 0;
    public static final int POSIX_MADV_RANDOM = 1;
    public static final int POSIX_MADV_SEQUENTIAL = 2;
    public static final int POSIX_MADV_WILLNEED = 3;
    public static final int POSIX_MADV_DONTNEED = 4;
    public static final int POSIX_MADV_NOREUSE = 5;

    /**
     * Gets the effective userid of the current process.
     *
     * @return the effective user id
     * @see <a href="https://man7.org/linux/man-pages/man3/geteuid.3p.html">geteuid manpage</a>
     */
    @Function("geteuid")
    public abstract int geteuid();

    /** corresponds to struct rlimit */
    @StructSpecification
    public interface RLimit {
        long rlim_cur();

        void rlim_cur(long v);

        long rlim_max();

        void rlim_max(long v);
    }

    /**
     * Create a new RLimit struct for use by getrlimit.
     */
    @StructFactory
    public abstract RLimit newRLimit();

    /**
     * Retrieve the current rlimit values for the given resource.
     *
     * @return 0 on success, -1 on failure with errno set
     * @see <a href="https://man7.org/linux/man-pages/man2/getrlimit.2.html">getrlimit manpage</a>
     */
    @CaptureSystemError
    @Function("getrlimit")
    public abstract int getrlimit(int resource, RLimit rlimit);

    @CaptureSystemError
    @Function("setrlimit")
    public abstract int setrlimit(int resource, RLimit rlimit);

    /**
     * Lock all the current process's virtual address space into RAM.
     * @param flags flags determining how memory will be locked
     * @return 0 on success, -1 on failure with errno set
     * @see <a href="https://man7.org/linux/man-pages/man2/mlock.2.html">mlockall manpage</a>
     */
    @CaptureSystemError
    @Function("mlockall")
    public abstract int mlockall(int flags);

    /** Raw binding for madvise; use {@link #madvise(MemorySegment, long, long, int)} instead. */
    @CaptureSystemError
    @Function("madvise")
    protected abstract int madvise(long addr, long length, int advice);

    /**
     * Provides advice to the operating system about how a region of memory will be accessed,
     * allowing the kernel to optimize memory management.
     * <p>
     * This method is a thin wrapper around the POSIX {@code madvise(2)} system
     * call. The call is advisory only and does not guarantee any specific behavior.
     *
     * <p><strong>Requirements:</strong>
     * <ul>
     *   <li>The starting address of {@code segment} must be aligned to the system page size.</li>
     *   <li>{@code segment} must represent native (off-heap) memory.
     *       Passing a non-native {@link MemorySegment} will result in an {@link IllegalArgumentException}.</li>
     * </ul>
     *
     * @param segment
     *     the starting memory segment of the region to be advised; must refer to native memory and be page-size aligned
     * @param length
     *     the length in bytes of the memory region starting at {@code segment}
     * @param advice
     *     the access pattern advice (for example {@code MADV_WILLNEED}, {@code MADV_DONTNEED}, {@code MADV_SEQUENTIAL}, etc.)
     * @return
     *     {@code 0} on success, or {@code -1} on failure with {@code errno} set to indicate the error
     *
     * @throws IllegalArgumentException
     *     if {@code segment} does not represent native memory
     *
     * @see <a href="https://man7.org/linux/man-pages/man2/madvise.2.html">madvise manpage</a>
     */
    public int madvise(MemorySegment segment, long offset, long length, int advice) {
        if (segment.isNative() == false) {
            throw new IllegalArgumentException("unexpected non-native segment: " + segment);
        }
        Objects.checkFromIndexSize(offset, length, segment.byteSize());
        long base = segment.address() + offset;
        try {
            return madvise(base, length, advice);
        } catch (Throwable t) {
            throw madviseError(t, segment);
        } finally {
            // protects the segment from being potentially being GC'ed during out downcall
            Reference.reachabilityFence(segment);
        }
    }

    private static Error madviseError(Throwable t, MemorySegment segment) {
        String msg = "madvise failed: segment=" + segment + ", scope=" + segment.scope() + ", isAlive=" + segment.scope().isAlive();
        return new AssertionError(msg, t);
    }

    /** Returns native page size. */
    @Function("getpagesize")
    public abstract int getPageSize();

    /**
     * Corresponds to {@code struct stat64}. Only the two fields needed for allocated-size
     * and block-count accounting ({@code st_size} and {@code st_blocks}) are declared here;
     * the sparse layout means undeclared members cost nothing. Additional fields can be
     * added in the future if other callers need them.
     */
    @StructSpecification(sparse = true)
    @StructSize(144)
    public interface Stat64 {
        @Offset(platforms = { Platform.LINUX_X64, Platform.LINUX_AARCH64 }, value = 48)
        @Offset(platforms = { Platform.DARWIN_X64, Platform.DARWIN_AARCH64 }, value = 96)
        long st_size();

        @Offset(platforms = { Platform.LINUX_X64, Platform.LINUX_AARCH64 }, value = 64)
        @Offset(platforms = { Platform.DARWIN_X64, Platform.DARWIN_AARCH64 }, value = 104)
        long st_blocks();
    }

    @StructFactory
    public abstract Stat64 newStat64();

    @CaptureSystemError
    @Variadic(firstArg = 2)
    @Function("open")
    public abstract int open(String pathname, int flags, int mode);

    @CaptureSystemError
    @Variadic(firstArg = 2)
    @Function("open")
    public abstract int open(String pathname, int flags);

    @CaptureSystemError
    @Function("fstat64")
    public abstract int fstat64(int fd, Stat64 stats);

    @CaptureSystemError
    @Function("ftruncate")
    public abstract int ftruncate(int fd, long length);

    @StructSpecification
    public interface FStore {
        void set_flags(int flags); /* IN: flags word */

        void set_posmode(int posmode); /* IN: indicates offset field */

        void set_offset(long offset); /* IN: start of the region */

        void set_length(long length); /* IN: size of the region */

        long bytesalloc(); /* OUT: number of bytes allocated */
    }

    @StructFactory
    public abstract FStore newFStore();

    @CaptureSystemError
    @Variadic(firstArg = 2)
    @Function("fcntl")
    public abstract int fcntl(int fd, int cmd, FStore fst);

    /**
     * Open a file descriptor to connect to a socket.
     *
     * @param domain The socket protocol family, eg AF_UNIX
     * @param type The socket type, eg SOCK_DGRAM
     * @param protocol The protocol for the given protocl family, normally 0
     * @return an open file descriptor, or -1 on failure with errno set
     * @see <a href="https://man7.org/linux/man-pages/man2/socket.2.html">socket manpage</a>
     */
    @CaptureSystemError
    @Function("socket")
    public abstract int socket(int domain, int type, int protocol);

    /**
     * Struct backing the AF_UNIX sockaddr passed to {@link #connect}. Only ever written by the
     * caller and then handed to {@code connect}, so it exposes setters only.
     */
    @StructSpecification
    public interface SockAddr {
        @Sizeof
        int sizeof();

        void sa_family(short v);

        @InlineStringField(length = 108)
        void sun_path(String v);
    }

    /**
     * Create a sockaddr for the AF_UNIX family.
     */
    @StructFactory
    public abstract SockAddr newSockAddr();

    /** Raw binding for connect; use {@link #connect(int, SockAddr)} instead. */
    @CaptureSystemError
    @Function("connect")
    protected abstract int connect(int sockfd, SockAddr addr, int addrlen);

    /**
     * Connect a socket to an address.
     *
     * @param sockfd An open socket file descriptor
     * @param addr The address to connect to
     * @return 0 on success, -1 on failure with errno set
     */
    public int connect(int sockfd, SockAddr addr) {
        return connect(sockfd, addr, addr.sizeof());
    }

    /**
     * Send a message to a socket.
     *
     * @param sockfd The open socket file descriptor
     * @param buf The starting memory segment of the message bytes to send
     * @param len The number of bytes to send, starting at {@code buf}
     * @param flags Flags that may adjust how the message is sent
     * @return The number of bytes sent, or -1 on failure with errno set
     * @see <a href="https://man7.org/linux/man-pages/man2/sendto.2.html">send manpage</a>
     */
    @CaptureSystemError
    @Function("send")
    public abstract long send(int sockfd, MemorySegment buf, long len, int flags);

    /**
     * Close a file descriptor
     * @param fd The file descriptor to close
     * @return 0 on success, -1 on failure with errno set
     * @see <a href="https://man7.org/linux/man-pages/man2/close.2.html">close manpage</a>
     */
    @CaptureSystemError
    @Function("close")
    public abstract int close(int fd);

    /**
     * Return a string description for an error.
     *
     * @param errno The error number
     * @return a String description for the error
     * @see <a href="https://man7.org/linux/man-pages/man3/strerror.3.html">strerror manpage</a>
     */
    @Function("strerror")
    public abstract String strerror(int errno);

    /**
     * Return the error number from the last failed C library call.
     *
     * @see <a href="https://man7.org/linux/man-pages/man3/errno.3.html">errno manpage</a>
     */
    public int errno() {
        return LinkerHelper.systemError();
    }
}
