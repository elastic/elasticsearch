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
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandle;

import static java.lang.foreign.ValueLayout.ADDRESS;
import static java.lang.foreign.ValueLayout.JAVA_INT;
import static java.lang.foreign.ValueLayout.JAVA_LONG;
import static org.elasticsearch.nativeaccess.jdk.LinkerHelper.downcallHandle;

class JdkMacCLibrary implements MacCLibrary {

    private static final MethodHandle sandbox_init$mh = downcallHandle(
        "sandbox_init",
        FunctionDescriptor.of(JAVA_INT, ADDRESS, JAVA_LONG, ADDRESS)
    );
    private static final MethodHandle sandbox_free_error$mh = downcallHandle("sandbox_free_error", FunctionDescriptor.ofVoid(ADDRESS));

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
            MemorySegment nativeProfile = MemorySegmentUtil.allocateString(arena, profile);
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
}
