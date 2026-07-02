/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.foreign;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marks a {@link Function @Function} binding as <em>critical</em>, asking the FFM linker to use the
 * critical calling convention. Critical calls avoid the safepoint transition that wraps normal native
 * calls, which is faster for short, leaf-style functions but forbids upcalls back into Java and
 * pins any on-heap {@link java.lang.foreign.MemorySegment} arguments for the duration of the call.
 *
 * <p>Use {@code @Critical} for hot, well-bounded native routines such as compression primitives.
 * Pair it with {@link Function @Function} on the same method:
 *
 * <pre>{@code
 * @Function("ZSTD_decompress")
 * @Critical
 * long decompressHeap(MemorySegment dst, long dstCap, MemorySegment src, long srcSize);
 * }</pre>
 *
 * <h2>JDK 21 fallback (optional)</h2>
 *
 * The {@code Linker.Option.critical(boolean)} flag that lets the downcall accept heap segments is
 * only available from JDK 22. On JDK 21 the raw downcall would reject any heap {@link
 * java.lang.foreign.MemorySegment} argument, so {@code @Critical} bindings that receive heap-backed
 * segments must supply a {@link #fallbackAdapter()} that stages the call. The adapter is a class
 * declaring a {@code public static} method whose name matches the annotated method, with parameter
 * list {@code (MethodHandle, …originalParams)} and the same return type as the annotated method. The
 * processor validates the adapter at compile time and, on JDK 21, wraps the raw downcall handle in
 * {@code <clinit>} so the binding routes through the adapter. On JDK 22+ the adapter is never
 * resolved — the linker's {@code critical(true)} option handles heap segments directly.
 *
 * <p>When omitted, the downcall uses the critical calling convention without heap segment support.
 * This is appropriate for functions whose arguments are exclusively off-heap or primitive.
 */
@Retention(RetentionPolicy.SOURCE)
@Target(ElementType.METHOD)
public @interface Critical {

    /**
     * Class providing the JDK 21 fallback adapter. The adapter must declare a {@code public static}
     * method with the same name as the annotated method and a leading {@link java.lang.invoke.MethodHandle}
     * parameter; see the class-level docs for the full contract. When omitted, the binding uses the
     * critical calling convention without heap segment support — appropriate for functions that never
     * receive heap-backed {@link java.lang.foreign.MemorySegment} arguments.
     */
    Class<?> fallbackAdapter() default void.class;
}
