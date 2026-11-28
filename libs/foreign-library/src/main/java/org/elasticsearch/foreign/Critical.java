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
 * <p>Only bind a function this way if it is critical in the sense of the JDK's
 * {@code Linker.Option.critical(boolean)}: it has "an extremely short running time in all cases (similar
 * to calling an empty function)" and never calls back into Java. Linking any other function as critical
 * "is likely to have adverse effects, such as loss of performance or JVM crashes". A function whose cost
 * does not depend on its arguments qualifies, such as libc's {@code fmax} (illustrative only: Java has
 * {@code Math.max}):
 *
 * <pre>{@code
 * @Function("fmax")
 * @Critical(fallbackAdapter = Critical.UnsupportedFallback.class)
 * double fmax(double x, double y);
 * }</pre>
 *
 * <p>So can a function whose cost grows with an argument, if that argument is bounded by something the
 * caller cannot exceed. The pairwise float32 dot product of {@code SimdVecLibrary} touches two vectors, and
 * Elasticsearch limits dense vectors to 4096 dimensions
 * ({@code DenseVectorFieldMapper.MAX_DIMS_COUNT}); the binding itself does not enforce that limit, the
 * index mapping does. A function with no such bound, such as {@code memcmp} or {@code strlen} on arbitrary
 * input, does not qualify, and one that can block or run long, such as {@code fread}, never does.
 *
 * <p>A critical call also has a cost outside its own thread. A thread inside a critical call cannot
 * respond to the JVM-wide handshake that closing a shared {@link java.lang.foreign.Arena} requires, so
 * every such close elsewhere in the process waits until the call returns
 * (see <a href="https://github.com/elastic/elasticsearch/issues/160667">#160667</a>). Prefer passing
 * off-heap segments to a binding without {@code @Critical}. Add a {@code @Critical} overload that takes
 * heap segments only when copying the data off-heap is measurably too expensive, and keep it next to an
 * off-heap twin that has no {@code @Critical}; {@code ZstdLibrary} ({@code compress}/{@code compressHeap})
 * is an example of that pairing.
 *
 * <h2>JDK 21 fallback (required)</h2>
 *
 * The {@code Linker.Option.critical(boolean)} flag that lets the downcall accept heap segments is
 * only available from JDK 22. On JDK 21 the raw downcall would reject any heap {@link
 * java.lang.foreign.MemorySegment} argument, so every {@code @Critical} binding must declare a
 * {@link #fallbackAdapter()}. This can be done in 2 ways:
 *
 * <ul>
 *   <li>A real adapter class that stages the call. The adapter declares a {@code public static}
 *   method whose name matches the annotated method, with parameter list
 *   {@code (MethodHandle, …originalParams)} and the same return type as the annotated method. The
 *   processor validates the adapter at compile time and, on JDK 21, wraps the raw downcall handle in
 *   {@code <clinit>} so the binding routes through the adapter. On JDK 22+ the adapter is never
 *   resolved — the linker's {@code critical(true)} option handles heap segments directly.</li>
 *   <li>The {@link UnsupportedFallback} sentinel, for bindings whose callers guarantee the function
 *   is only invoked on JDK 22+. On JDK 21 the binding is wired to a handle that throws {@link AssertionError}
 *   on any invocation. On JDK 22+ the binding operates as a normal critical call.</li>
 * </ul>
 */
@Retention(RetentionPolicy.SOURCE)
@Target(ElementType.METHOD)
public @interface Critical {

    /**
     * Sentinel value for {@link #fallbackAdapter()} declaring that the binding has no JDK 21
     * fallback, because its callers guarantee it is only invoked on JDK 22+. On JDK 21 the generated
     * binding throws {@link AssertionError} when called; on JDK 22+ it is a normal critical call.
     */
    final class UnsupportedFallback {
        private UnsupportedFallback() {}
    }

    /**
     * Class providing the JDK 21 fallback adapter. The adapter must declare a {@code public static}
     * method with the same name as the annotated method and a leading {@link java.lang.invoke.MethodHandle}
     * parameter; see the class-level docs for the full contract. Required — there is no default, because
     * a {@code @Critical} binding cannot otherwise function on JDK 21 with heap-backed arguments. Pass
     * {@link UnsupportedFallback} instead to declare that the binding is never invoked on JDK 21 and needs
     * no fallback.
     */
    Class<?> fallbackAdapter();
}
