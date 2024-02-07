/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.nativeaccess.jdk;

import org.elasticsearch.nativeaccess.lib.MacCLibrary;

import java.lang.foreign.Arena;
import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.MemoryLayout;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.VarHandle;

import static java.lang.foreign.MemoryLayout.PathElement.groupElement;
import static java.lang.foreign.ValueLayout.ADDRESS;
import static java.lang.foreign.ValueLayout.JAVA_INT;
import static java.lang.foreign.ValueLayout.JAVA_LONG;
import static org.elasticsearch.nativeaccess.jdk.JdkPosixCLibrary.errnoState;
import static org.elasticsearch.nativeaccess.jdk.LinkerHelper.downcallHandle;

class JdkMacCLibrary implements MacCLibrary {

    private static final MethodHandle sandbox_init$mh = downcallHandle(
        "sandbox_init",
        FunctionDescriptor.of(JAVA_INT, ADDRESS, JAVA_LONG, ADDRESS)
    );
    private static final MethodHandle sandbox_free_error$mh = downcallHandle("sandbox_free_error", FunctionDescriptor.ofVoid(ADDRESS));
    private static final MethodHandle fcntl$mh = downcallHandle("fcntl", FunctionDescriptor.of(JAVA_INT, JAVA_INT, JAVA_INT, ADDRESS));
    private static final MethodHandle ftruncate$mh = downcallHandle("ftruncate", FunctionDescriptor.of(JAVA_INT, JAVA_INT, JAVA_LONG));

    private static class JdkErrorReference implements ErrorReference {
        final Arena arena = Arena.ofConfined();
        final MemorySegment segment = arena.allocate(ValueLayout.ADDRESS);

        MemorySegment deref() {
            return segment.get(ADDRESS, 0);
        }

        @Override
        public String toString() {
            return deref().reinterpret(Long.MAX_VALUE).getUtf8String(0);
        }
    }

    @Override
    public ErrorReference newErrorReference() {
        return new JdkErrorReference();
    }

    @Override
    public int sandbox_init(String profile, long flags, ErrorReference errorbuf) {
        assert errorbuf instanceof JdkErrorReference;
        var jdkErrorbuf = (JdkErrorReference) errorbuf;
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment nativeProfile = arena.allocateUtf8String(profile);
            return (int) sandbox_init$mh.invokeExact(nativeProfile, flags, jdkErrorbuf.segment);
        } catch (Throwable t) {
            throw new AssertionError(t);
        }
    }

    @Override
    public void sandbox_free_error(ErrorReference errorbuf) {
        assert errorbuf instanceof JdkErrorReference;
        var jdkErrorbuf = (JdkErrorReference) errorbuf;
        try {
            sandbox_free_error$mh.invokeExact(jdkErrorbuf.deref());
        } catch (Throwable t) {
            throw new AssertionError(t);
        }
    }

    private static class JdkFStore implements FStore {
        private static final MemoryLayout layout = MemoryLayout.structLayout(JAVA_INT, JAVA_INT, JAVA_LONG, JAVA_LONG, JAVA_LONG);
        private static final VarHandle st_flags$vh = layout.varHandle(groupElement(0));
        private static final VarHandle st_posmode$vh = layout.varHandle(groupElement(1));
        private static final VarHandle st_offset$vh = layout.varHandle(groupElement(2));
        private static final VarHandle st_length$vh = layout.varHandle(groupElement(3));
        private static final VarHandle st_bytesalloc$vh = layout.varHandle(groupElement(4));

        private final MemorySegment segment;

        JdkFStore() {
            var arena = Arena.ofAuto();
            this.segment = arena.allocate(layout);
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
            st_offset$vh.get(segment, offset);
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

    @Override
    public FStore newFStore() {
        return new JdkFStore();
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
}
