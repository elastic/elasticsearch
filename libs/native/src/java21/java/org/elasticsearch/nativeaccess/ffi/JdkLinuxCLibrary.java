/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.nativeaccess.ffi;

import org.elasticsearch.nativeaccess.lib.LinuxCLibrary;

import java.lang.foreign.Arena;
import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.MemoryLayout;
import java.lang.foreign.MemorySegment;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.VarHandle;

import static java.lang.foreign.MemoryLayout.PathElement.groupElement;
import static java.lang.foreign.ValueLayout.ADDRESS;
import static java.lang.foreign.ValueLayout.JAVA_INT;
import static java.lang.foreign.ValueLayout.JAVA_LONG;
import static org.elasticsearch.nativeaccess.ffi.JdkPosixCLibrary.downcallHandleWithErrno;
import static org.elasticsearch.nativeaccess.ffi.JdkPosixCLibrary.errnoState;

public class JdkLinuxCLibrary implements LinuxCLibrary {

    private static final MethodHandle statx$mh;

    static {
        statx$mh = downcallHandleWithErrno("statx", FunctionDescriptor.of(JAVA_INT, JAVA_INT, ADDRESS, JAVA_INT, JAVA_INT, ADDRESS));
    }

    private static class JdkStatx implements statx {
        private static final MemoryLayout layout = MemoryLayout.structLayout(
            MemoryLayout.paddingLayout(48),
            JAVA_LONG,
            MemoryLayout.paddingLayout(104));
        private static final VarHandle stx_blocks$vh = layout.varHandle(groupElement(1));

        private final MemorySegment segment;

        JdkStatx() {
            var arena = Arena.ofAuto();
            this.segment = arena.allocate(layout);
        }

        @Override
        public long stx_blocks() {
            return (long)stx_blocks$vh.get(segment);
        }
    }

    @Override
    public statx newStatx() {
        return new JdkStatx();
    }

    @Override
    public int statx(int dirfd, String pathname, int flags, int mask, statx statxbuf) {
        assert statxbuf instanceof JdkStatx;
        var jdkStatxbuf = (JdkStatx)statxbuf;
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment nativePathname = arena.allocateUtf8String(pathname);
            return (int)statx$mh.invokeExact(dirfd, nativePathname, flags, mask, jdkStatxbuf.segment, errnoState);
        } catch (Throwable t) {
            throw new AssertionError(t);
        }
    }
}
