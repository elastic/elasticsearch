/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.foreign;

import java.lang.foreign.Arena;
import java.lang.foreign.Linker;
import java.lang.foreign.MemorySegment;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;

public class LinkerHelperUtil {

    static final Linker.Option[] NONE = new Linker.Option[0];

    /** Returns an empty linker option array, since critical is only available since Java 22. */
    public static Linker.Option[] critical() {
        return NONE;
    }

    /**
     * Wraps a downcall {@link MethodHandle} produced for a {@code @Critical}-annotated method so it tolerates heap
     * {@link MemorySegment}s on JDK 21, where {@code Linker.Option.critical(true)} is unavailable and the raw handle
     * would reject heap segments with {@code IllegalArgumentException: Heap segment not allowed}.
     *
     * <p>The returned handle has the same {@link MethodType} as {@code mh}, so callers can keep using
     * {@code invokeExact} unchanged. On JDK 22+ this is the identity adapter (see the
     * {@code main22} override of this class).
     */
    public static MethodHandle adaptCritical(MethodHandle mh) {
        MethodHandle bound = MethodHandles.insertArguments(STAGING_DISPATCH, 0, mh);
        MethodHandle collected = bound.asCollector(Object[].class, mh.type().parameterCount());
        return collected.asType(mh.type());
    }

    /**
     * Stages every heap {@link MemorySegment} argument into a confined {@link Arena}, invokes {@code target}, then
     * copies the staged segments back over their heap originals. Always copying back is intentional: we cannot tell
     * input segments from output segments at this level. For input-only segments the copy-back is a no-op (the staged
     * copy matches the heap original byte-for-byte); for output segments the staged buffer holds the native callee's
     * writes, which are then mirrored to the heap.
     */
    @SuppressWarnings("unused") // referenced via MethodHandles.lookup() below
    private static Object stagingDispatch(MethodHandle target, Object[] args) throws Throwable {
        try (Arena arena = Arena.ofConfined()) {
            Object[] callArgs = args;
            MemorySegment[] originals = null;
            for (int i = 0; i < args.length; i++) {
                if (args[i] instanceof MemorySegment seg && seg.isNative() == false) {
                    if (originals == null) {
                        callArgs = args.clone();
                        originals = new MemorySegment[args.length];
                    }
                    MemorySegment staged = arena.allocate(seg.byteSize());
                    MemorySegment.copy(seg, 0L, staged, 0L, seg.byteSize());
                    callArgs[i] = staged;
                    originals[i] = seg;
                }
            }
            Object result = target.invokeWithArguments(callArgs);
            if (originals != null) {
                for (int i = 0; i < originals.length; i++) {
                    if (originals[i] != null) {
                        MemorySegment staged = (MemorySegment) callArgs[i];
                        MemorySegment.copy(staged, 0L, originals[i], 0L, originals[i].byteSize());
                    }
                }
            }
            return result;
        }
    }

    private static final MethodHandle STAGING_DISPATCH;
    static {
        try {
            STAGING_DISPATCH = MethodHandles.lookup()
                .findStatic(
                    LinkerHelperUtil.class,
                    "stagingDispatch",
                    MethodType.methodType(Object.class, MethodHandle.class, Object[].class)
                );
        } catch (ReflectiveOperationException e) {
            throw new AssertionError(e);
        }
    }

    private LinkerHelperUtil() {}
}
