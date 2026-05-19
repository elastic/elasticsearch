/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;

import java.lang.invoke.ConstantCallSite;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.lang.reflect.Field;

/**
 * Allocates uninitialized primitive arrays via {@code jdk.internal.misc.Unsafe#allocateUninitializedArray}.
 * <p>
 * Unlike {@code new T[length]}, the returned arrays are not zeroed by the JVM. Callers must
 * overwrite every element they use before reading it, otherwise they will observe stale memory
 * contents. Use only on the hot path where the zeroing cost is measurable and the array is
 * guaranteed to be fully written before being read.
 * <p>
 * Requires the JVM to be started with
 * {@code --add-opens=java.base/jdk.internal.misc=ALL-UNNAMED} so that reflective access to
 * the singleton {@code Unsafe} instance is permitted. When the reflective bootstrap fails
 * (e.g. the flag is missing or a future JDK changes the internals), this class transparently
 * falls back to zero-initialized {@code new T[length]} allocation after logging a warning.
 * <p>
 * <strong>TLAB-size limit:</strong> the HotSpot {@code _allocateUninitializedArray} intrinsic
 * only skips zeroing on the JIT-inlined TLAB fast path. Allocations that don't fit in the
 * current thread's TLAB (typically arrays larger than a few hundred KB, depending on
 * {@code -XX:TLABSize} and occupancy) fall back to {@code OptoRuntime::new_array_C} →
 * {@code MemAllocator::allocate}, which always zeros the array body before returning it.
 * The intrinsic cannot override that — large heap allocations are zeroed by the GC's
 * allocator regardless of which API requested them. As a result this helper is only a win
 * for <em>small</em> scratch arrays (per-batch decode buffers, tens of KB). For multi-MB
 * buffers, the only way to avoid the zeroing cost is to pool and reuse the buffer instead
 * of allocating a fresh one.
 */
public final class UninitializedArrays {

    private static final Logger logger = LogManager.getLogger(UninitializedArrays.class);

    // Visible for testing
    static final String UNSAFE_DISABLED_MESSAGE = "Could not initialize jdk.internal.misc.Unsafe#allocateUninitializedArray; "
        + "pass --add-opens=java.base/jdk.internal.misc=ALL-UNNAMED to enable the fast path.";

    // Per-primitive-type call sites. Each handle has both the Unsafe receiver and the
    // componentType Class bound, so its type is (int)Object — the int length is the only
    // remaining parameter. Binding the Class as a constant is critical: the HotSpot
    // _allocateUninitializedArray intrinsic requires the component type to be a compile-time
    // constant at the call site. Wrapping the bound handle in a ConstantCallSite lets the JIT
    // treat invokeExact() on getTarget() as a fully constant-folded call, enabling the
    // intrinsic to fire and skip the zeroing memset.
    private static final ConstantCallSite ALLOCATE_BOOLEAN = bindAllocate(boolean.class);
    private static final ConstantCallSite ALLOCATE_BYTE = bindAllocate(byte.class);
    private static final ConstantCallSite ALLOCATE_SHORT = bindAllocate(short.class);
    private static final ConstantCallSite ALLOCATE_CHAR = bindAllocate(char.class);
    private static final ConstantCallSite ALLOCATE_INT = bindAllocate(int.class);
    private static final ConstantCallSite ALLOCATE_LONG = bindAllocate(long.class);
    private static final ConstantCallSite ALLOCATE_FLOAT = bindAllocate(float.class);
    private static final ConstantCallSite ALLOCATE_DOUBLE = bindAllocate(double.class);

    private static final boolean UNSAFE_ENABLED = ALLOCATE_BYTE != null;

    @SuppressForbidden(reason = "need to access jdk.internal.misc.Unsafe")
    static ConstantCallSite bindAllocate(Class<?> componentType) {
        try {
            // We resolve jdk.internal.misc.Unsafe reflectively rather than via a direct
            // import because the ES build compiles with javac's --release flag, which pins
            // the symbol table to the documented JDK API and rejects --add-exports for
            // java.base internal packages ("exporting a package from system module
            // java.base is not allowed with --release"). Reflection sidesteps the
            // compile-time restriction; at runtime, --add-opens=java.base/jdk.internal.misc=ALL-UNNAMED
            // is still required for setAccessible(true) on the theUnsafe field.
            Class<?> unsafeClass = Class.forName("jdk.internal.misc.Unsafe");
            Field theUnsafe = unsafeClass.getDeclaredField("theUnsafe");
            theUnsafe.setAccessible(true);
            Object unsafe = theUnsafe.get(null);
            MethodHandle handle = MethodHandles.lookup()
                .findVirtual(unsafeClass, "allocateUninitializedArray", MethodType.methodType(Object.class, Class.class, int.class))
                .bindTo(unsafe)
                .bindTo(componentType)
                // Refine the return type from Object to the concrete primitive array type
                // (e.g. byte[]) so invokeExact() at the call site matches without a runtime
                // cast and the JIT sees a fully typed (int)T[] signature at the constant
                // call site — a prerequisite for the allocation intrinsic.
                .asType(MethodType.methodType(componentType.arrayType(), int.class));
            return new ConstantCallSite(handle);
        } catch (Throwable e) {
            logger.warn(UNSAFE_DISABLED_MESSAGE + " Falling back to zero-initialized array allocation.", e);
            return null;
        }
    }

    private UninitializedArrays() {}

    /**
     * Returns {@code true} when the reflective bootstrap of
     * {@code jdk.internal.misc.Unsafe#allocateUninitializedArray} succeeded and the
     * {@code newXxxArray} methods will return uninitialized arrays. Returns {@code false}
     * when the JVM is missing {@code --add-opens=java.base/jdk.internal.misc=ALL-UNNAMED}
     * (or the JDK internals changed), in which case the {@code newXxxArray} methods
     * silently fall back to zero-initialized {@code new T[length]}.
     */
    public static boolean isUnsafeEnabled() {
        return UNSAFE_ENABLED;
    }

    /**
     * Throws {@link IllegalStateException} if {@link #isUnsafeEnabled()} is {@code false}.
     * Use from production code paths or tests that require the uninitialized fast path and
     * must fail loudly rather than silently fall back to zero-initialized allocation.
     */
    public static void ensureUnsafeEnabled() {
        if (UNSAFE_ENABLED == false) {
            throw new IllegalStateException(UNSAFE_DISABLED_MESSAGE);
        }
    }

    public static boolean[] newBooleanArray(int length) {
        if (UNSAFE_ENABLED == false) return new boolean[length];
        try {
            return (boolean[]) ALLOCATE_BOOLEAN.getTarget().invokeExact(length);
        } catch (Throwable t) {
            throw rethrow(t);
        }
    }

    public static byte[] newByteArray(int length) {
        if (UNSAFE_ENABLED == false) return new byte[length];
        try {
            return (byte[]) ALLOCATE_BYTE.getTarget().invokeExact(length);
        } catch (Throwable t) {
            throw rethrow(t);
        }
    }

    public static short[] newShortArray(int length) {
        if (UNSAFE_ENABLED == false) return new short[length];
        try {
            return (short[]) ALLOCATE_SHORT.getTarget().invokeExact(length);
        } catch (Throwable t) {
            throw rethrow(t);
        }
    }

    public static char[] newCharArray(int length) {
        if (UNSAFE_ENABLED == false) return new char[length];
        try {
            return (char[]) ALLOCATE_CHAR.getTarget().invokeExact(length);
        } catch (Throwable t) {
            throw rethrow(t);
        }
    }

    public static int[] newIntArray(int length) {
        if (UNSAFE_ENABLED == false) return new int[length];
        try {
            return (int[]) ALLOCATE_INT.getTarget().invokeExact(length);
        } catch (Throwable t) {
            throw rethrow(t);
        }
    }

    public static long[] newLongArray(int length) {
        if (UNSAFE_ENABLED == false) return new long[length];
        try {
            return (long[]) ALLOCATE_LONG.getTarget().invokeExact(length);
        } catch (Throwable t) {
            throw rethrow(t);
        }
    }

    public static float[] newFloatArray(int length) {
        if (UNSAFE_ENABLED == false) return new float[length];
        try {
            return (float[]) ALLOCATE_FLOAT.getTarget().invokeExact(length);
        } catch (Throwable t) {
            throw rethrow(t);
        }
    }

    public static double[] newDoubleArray(int length) {
        if (UNSAFE_ENABLED == false) return new double[length];
        try {
            return (double[]) ALLOCATE_DOUBLE.getTarget().invokeExact(length);
        } catch (Throwable t) {
            throw rethrow(t);
        }
    }

    private static RuntimeException rethrow(Throwable t) {
        if (t instanceof RuntimeException re) return re;
        if (t instanceof Error err) throw err;
        throw new AssertionError(t);
    }
}
