/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.nativeaccess.jdk;

import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.nativeaccess.CloseableByteBuffer;
import org.elasticsearch.nativeaccess.lib.PosixCLibrary;

import java.lang.foreign.Arena;
import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.Linker;
import java.lang.foreign.MemoryLayout;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.StructLayout;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;

import static java.lang.foreign.MemoryLayout.PathElement.groupElement;
import static java.lang.foreign.ValueLayout.ADDRESS;
import static java.lang.foreign.ValueLayout.JAVA_BYTE;
import static java.lang.foreign.ValueLayout.JAVA_INT;
import static java.lang.foreign.ValueLayout.JAVA_LONG;
import static java.lang.foreign.ValueLayout.JAVA_SHORT;
import static org.elasticsearch.nativeaccess.jdk.LinkerHelper.downcallHandle;
import static org.elasticsearch.nativeaccess.jdk.MemorySegmentUtil.varHandleWithoutOffset;

class JdkPosixCLibrary implements PosixCLibrary {

    private static final Logger logger = LogManager.getLogger(JdkPosixCLibrary.class);

    // errno can change between system calls, so we capture it
    private static final StructLayout CAPTURE_ERRNO_LAYOUT = Linker.Option.captureStateLayout();
    static final Linker.Option CAPTURE_ERRNO_OPTION = Linker.Option.captureCallState("errno");
    private static final VarHandle errno$vh = varHandleWithoutOffset(CAPTURE_ERRNO_LAYOUT, groupElement("errno"));

    private static final MethodHandle geteuid$mh = downcallHandle("geteuid", FunctionDescriptor.of(JAVA_INT));
    private static final MethodHandle strerror$mh = downcallHandle("strerror", FunctionDescriptor.of(ADDRESS, JAVA_INT));
    private static final MethodHandle getrlimit$mh = downcallHandleWithErrno(
        "getrlimit",
        FunctionDescriptor.of(JAVA_INT, JAVA_INT, ADDRESS)
    );
    private static final MethodHandle setrlimit$mh = downcallHandleWithErrno(
        "setrlimit",
        FunctionDescriptor.of(JAVA_INT, JAVA_INT, ADDRESS)
    );
    private static final MethodHandle mlockall$mh = downcallHandleWithErrno("mlockall", FunctionDescriptor.of(JAVA_INT, JAVA_INT));
    private static final MethodHandle fcntl$mh = downcallHandle(
        "fcntl",
        FunctionDescriptor.of(JAVA_INT, JAVA_INT, JAVA_INT, ADDRESS),
        CAPTURE_ERRNO_OPTION,
        Linker.Option.firstVariadicArg(2)
    );
    private static final MethodHandle ftruncate$mh = downcallHandleWithErrno(
        "ftruncate",
        FunctionDescriptor.of(JAVA_INT, JAVA_INT, JAVA_LONG)
    );
    private static final MethodHandle open$mh = downcallHandle(
        "open",
        FunctionDescriptor.of(JAVA_INT, ADDRESS, JAVA_INT),
        CAPTURE_ERRNO_OPTION,
        Linker.Option.firstVariadicArg(2)
    );
    private static final MethodHandle openWithMode$mh = downcallHandle(
        "open",
        FunctionDescriptor.of(JAVA_INT, ADDRESS, JAVA_INT, JAVA_INT),
        CAPTURE_ERRNO_OPTION,
        Linker.Option.firstVariadicArg(2)
    );
    private static final MethodHandle close$mh = downcallHandleWithErrno("close", FunctionDescriptor.of(JAVA_INT, JAVA_INT));
    private static final MethodHandle fstat$mh;
    static {
        MethodHandle fstat;
        try {
            fstat = downcallHandleWithErrno("fstat64", FunctionDescriptor.of(JAVA_INT, JAVA_INT, ADDRESS));
        } catch (LinkageError e) {
            // Due to different sizes of the stat structure for 32 vs 64 bit machines, on some systems fstat actually points to
            // an internal symbol. So we fall back to looking for that symbol.
            int version = System.getProperty("os.arch").equals("aarch64") ? 0 : 1;
            fstat = MethodHandles.insertArguments(
                downcallHandleWithErrno("__fxstat", FunctionDescriptor.of(JAVA_INT, JAVA_INT, JAVA_INT, ADDRESS)),
                1,
                version
            );
        }
        fstat$mh = fstat;
    }
    private static final MethodHandle socket$mh = downcallHandleWithErrno(
        "socket",
        FunctionDescriptor.of(JAVA_INT, JAVA_INT, JAVA_INT, JAVA_INT)
    );
    private static final MethodHandle connect$mh = downcallHandleWithErrno(
        "connect",
        FunctionDescriptor.of(JAVA_INT, JAVA_INT, ADDRESS, JAVA_INT)
    );
    private static final MethodHandle send$mh = downcallHandleWithErrno(
        "send",
        FunctionDescriptor.of(JAVA_LONG, JAVA_INT, ADDRESS, JAVA_LONG, JAVA_INT)
    );

    static final MemorySegment errnoState = Arena.ofAuto().allocate(CAPTURE_ERRNO_LAYOUT);

    static MethodHandle downcallHandleWithErrno(String function, FunctionDescriptor functionDescriptor) {
        return downcallHandle(function, functionDescriptor, CAPTURE_ERRNO_OPTION);
    }

    @Override
    public int errno() {
        return (int) errno$vh.get(errnoState);
    }

    @Override
    public String strerror(int errno) {
        try {
            MemorySegment str = (MemorySegment) strerror$mh.invokeExact(errno);
            return MemorySegmentUtil.getString(str.reinterpret(Long.MAX_VALUE), 0);
        } catch (Throwable t) {
            throw new AssertionError(t);
        }
    }

    @Override
    public int geteuid() {
        try {
            return (int) geteuid$mh.invokeExact();
        } catch (Throwable t) {
            throw new AssertionError(t);
        }
    }

    @Override
    public RLimit newRLimit() {
        return new JdkRLimit();
    }

    @Override
    public Stat64 newStat64(int sizeof, int stSizeOffset, int stBlocksOffset) {
        return new JdkStat64(sizeof, stSizeOffset, stBlocksOffset);
    }

    @Override
    public FStore newFStore() {
        return new JdkFStore();
    }

    @Override
    public int getrlimit(int resource, RLimit rlimit) {
        assert rlimit instanceof JdkRLimit;
        var jdkRlimit = (JdkRLimit) rlimit;
        try {
            return (int) getrlimit$mh.invokeExact(errnoState, resource, jdkRlimit.segment);
        } catch (Throwable t) {
            throw new AssertionError(t);
        }
    }

    @Override
    public int setrlimit(int resource, RLimit rlimit) {
        assert rlimit instanceof JdkRLimit;
        var jdkRlimit = (JdkRLimit) rlimit;
        try {
            return (int) setrlimit$mh.invokeExact(errnoState, resource, jdkRlimit.segment);
        } catch (Throwable t) {
            throw new AssertionError(t);
        }
    }

    @Override
    public int mlockall(int flags) {
        try {
            return (int) mlockall$mh.invokeExact(errnoState, flags);
        } catch (Throwable t) {
            throw new AssertionError(t);
        }
    }

    @Override
    public int fcntl(int fd, int cmd, FStore fst) {
        assert fst instanceof JdkFStore;
        var jdkFst = (JdkFStore) fst;
        try {
            return (int) fcntl$mh.invokeExact(errnoState, fd, cmd, jdkFst.segment);
        } catch (Throwable t) {
            throw new AssertionError(t);
        }
    }

    @Override
    public int ftruncate(int fd, long length) {
        try {
            return (int) ftruncate$mh.invokeExact(errnoState, fd, length);
        } catch (Throwable t) {
            throw new AssertionError(t);
        }
    }

    @Override
    public int open(String pathname, int flags) {
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment nativePathname = MemorySegmentUtil.allocateString(arena, pathname);
            return (int) open$mh.invokeExact(errnoState, nativePathname, flags);
        } catch (Throwable t) {
            throw new AssertionError(t);
        }
    }

    @Override
    public int open(String pathname, int flags, int mode) {
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment nativePathname = MemorySegmentUtil.allocateString(arena, pathname);
            return (int) openWithMode$mh.invokeExact(errnoState, nativePathname, flags, mode);
        } catch (Throwable t) {
            throw new AssertionError(t);
        }
    }

    @Override
    public int close(int fd) {
        try {
            return (int) close$mh.invokeExact(errnoState, fd);
        } catch (Throwable t) {
            throw new AssertionError(t);
        }
    }

    @Override
    public int fstat64(int fd, Stat64 stat64) {
        assert stat64 instanceof JdkStat64;
        var jdkStat = (JdkStat64) stat64;
        try {
            return (int) fstat$mh.invokeExact(errnoState, fd, jdkStat.segment);
        } catch (Throwable t) {
            throw new AssertionError(t);
        }
    }

    @Override
    public int socket(int domain, int type, int protocol) {
        try {
            return (int) socket$mh.invokeExact(errnoState, domain, type, protocol);
        } catch (Throwable t) {
            throw new AssertionError(t);
        }
    }

    @Override
    public SockAddr newUnixSockAddr(String path) {
        return new JdkSockAddr(path);
    }

    @Override
    public int connect(int sockfd, SockAddr addr) {
        assert addr instanceof JdkSockAddr;
        var jdkAddr = (JdkSockAddr) addr;
        try {
            return (int) connect$mh.invokeExact(errnoState, sockfd, jdkAddr.segment, (int) jdkAddr.segment.byteSize());
        } catch (Throwable t) {
            throw new AssertionError(t);
        }
    }

    @Override
    public long send(int sockfd, CloseableByteBuffer buffer, int flags) {
        assert buffer instanceof JdkCloseableByteBuffer;
        var nativeBuffer = (JdkCloseableByteBuffer) buffer;
        var segment = nativeBuffer.segment;
        try {
            logger.info("Sending {} bytes to socket", buffer.buffer().remaining());
            return (long) send$mh.invokeExact(errnoState, sockfd, segment, (long) buffer.buffer().remaining(), flags);
        } catch (Throwable t) {
            throw new AssertionError(t);
        }
    }

    static class JdkRLimit implements RLimit {
        private static final MemoryLayout layout = MemoryLayout.structLayout(JAVA_LONG, JAVA_LONG);
        private static final VarHandle rlim_cur$vh = varHandleWithoutOffset(layout, groupElement(0));
        private static final VarHandle rlim_max$vh = varHandleWithoutOffset(layout, groupElement(1));

        private final MemorySegment segment;

        JdkRLimit() {
            var arena = Arena.ofAuto();
            this.segment = arena.allocate(layout);
        }

        @Override
        public long rlim_cur() {
            return (long) rlim_cur$vh.get(segment);
        }

        @Override
        public long rlim_max() {
            return (long) rlim_max$vh.get(segment);
        }

        @Override
        public void rlim_cur(long v) {
            rlim_cur$vh.set(segment, v);
        }

        @Override
        public void rlim_max(long v) {
            rlim_max$vh.set(segment, v);
        }

        @Override
        public String toString() {
            return "JdkRLimit[rlim_cur=" + rlim_cur() + ", rlim_max=" + rlim_max();
        }
    }

    private static class JdkStat64 implements Stat64 {

        private final MemorySegment segment;
        private final int stSizeOffset;
        private final int stBlocksOffset;

        JdkStat64(int sizeof, int stSizeOffset, int stBlocksOffset) {
            this.segment = Arena.ofAuto().allocate(sizeof, 8);
            this.stSizeOffset = stSizeOffset;
            this.stBlocksOffset = stBlocksOffset;
        }

        @Override
        public long st_size() {
            return segment.get(JAVA_LONG, stSizeOffset);
        }

        @Override
        public long st_blocks() {
            return segment.get(JAVA_LONG, stBlocksOffset);
        }
    }

    private static class JdkFStore implements FStore {
        private static final MemoryLayout layout = MemoryLayout.structLayout(JAVA_INT, JAVA_INT, JAVA_LONG, JAVA_LONG, JAVA_LONG);
        private static final VarHandle st_flags$vh = varHandleWithoutOffset(layout, groupElement(0));
        private static final VarHandle st_posmode$vh = varHandleWithoutOffset(layout, groupElement(1));
        private static final VarHandle st_offset$vh = varHandleWithoutOffset(layout, groupElement(2));
        private static final VarHandle st_length$vh = varHandleWithoutOffset(layout, groupElement(3));
        private static final VarHandle st_bytesalloc$vh = varHandleWithoutOffset(layout, groupElement(4));

        private final MemorySegment segment;

        JdkFStore() {
            this.segment = Arena.ofAuto().allocate(layout);
        }

        @Override
        public void set_flags(int flags) {
            st_flags$vh.set(segment, flags);
        }

        @Override
        public void set_posmode(int posmode) {
            st_posmode$vh.set(segment, posmode);
        }

        @Override
        public void set_offset(long offset) {
            st_offset$vh.set(segment, offset);
        }

        @Override
        public void set_length(long length) {
            st_length$vh.set(segment, length);
        }

        @Override
        public long bytesalloc() {
            return (long) st_bytesalloc$vh.get(segment);
        }
    }

    private static class JdkSockAddr implements SockAddr {
        private static final MemoryLayout layout = MemoryLayout.structLayout(JAVA_SHORT, MemoryLayout.sequenceLayout(108, JAVA_BYTE));
        final MemorySegment segment;

        JdkSockAddr(String path) {
            segment = Arena.ofAuto().allocate(layout);
            segment.set(JAVA_SHORT, 0, AF_UNIX);
            MemorySegmentUtil.setString(segment, 2, path);
        }
    }
}
