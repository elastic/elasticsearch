/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.lang.reflect.Field;

/**
 * Allocates uninitialized primitive arrays via {@code jdk.internal.misc.Unsafe#allocateUninitializedArray}.
 * <p>
 * Unlike {@code new T[length]}, the returned arrays are not zeroed by the JVM. Callers must
 * fully overwrite every element before reading, otherwise they will observe stale memory
 * contents. Use only on the hot path where the zeroing cost is measurable and the array is
 * guaranteed to be fully written before being read.
 * <p>
 * Requires the JVM to be started with
 * {@code --add-opens=java.base/jdk.internal.misc=ALL-UNNAMED} so that reflective access to
 * the singleton {@code Unsafe} instance is permitted. When the reflective bootstrap fails
 * (e.g. the flag is missing or a future JDK changes the internals), this class transparently
 * falls back to zero-initialized {@code new T[length]} allocation after logging a warning.
 */
public final class UninitializedArrayAllocator {

    private static final Logger logger = LogManager.getLogger(UninitializedArrayAllocator.class);

    private static final String UNSAFE_DISABLED_MESSAGE = "Could not initialize jdk.internal.misc.Unsafe#allocateUninitializedArray; "
        + "falling back to zero-initialized array allocation. "
        + "Pass --add-opens=java.base/jdk.internal.misc=ALL-UNNAMED to enable the fast path.";

    private static final MethodHandle ALLOCATE_UNINITIALIZED_ARRAY = initAllocateHandle();

    private static MethodHandle initAllocateHandle() {
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
                .findVirtual(unsafeClass, "allocateUninitializedArray", MethodType.methodType(Object.class, Class.class, int.class));
            return handle.bindTo(unsafe);
        } catch (ReflectiveOperationException | RuntimeException e) {
            logger.warn(UNSAFE_DISABLED_MESSAGE, e);
            return null;
        }
    }

    private UninitializedArrayAllocator() {}

    /**
     * Returns {@code true} when the reflective bootstrap of
     * {@code jdk.internal.misc.Unsafe#allocateUninitializedArray} succeeded and the
     * {@code newXxxArray} methods will return uninitialized arrays. Returns {@code false}
     * when the JVM is missing {@code --add-opens=java.base/jdk.internal.misc=ALL-UNNAMED}
     * (or the JDK internals changed), in which case the {@code newXxxArray} methods
     * silently fall back to zero-initialized {@code new T[length]}.
     */
    public static boolean isUnsafeEnabled() {
        return ALLOCATE_UNINITIALIZED_ARRAY != null;
    }

    /**
     * Throws {@link IllegalStateException} if {@link #isUnsafeEnabled()} is {@code false}.
     * Use from production code paths or tests that require the uninitialized fast path and
     * must fail loudly rather than silently fall back to zero-initialized allocation.
     */
    public static void ensureUnsafeEnabled() {
        if (ALLOCATE_UNINITIALIZED_ARRAY == null) {
            throw new IllegalStateException(UNSAFE_DISABLED_MESSAGE);
        }
    }

    public static boolean[] newBooleanArray(int length) {
        return ALLOCATE_UNINITIALIZED_ARRAY == null ? new boolean[length] : (boolean[]) allocate(boolean.class, length);
    }

    public static byte[] newByteArray(int length) {
        return ALLOCATE_UNINITIALIZED_ARRAY == null ? new byte[length] : (byte[]) allocate(byte.class, length);
    }

    public static short[] newShortArray(int length) {
        return ALLOCATE_UNINITIALIZED_ARRAY == null ? new short[length] : (short[]) allocate(short.class, length);
    }

    public static char[] newCharArray(int length) {
        return ALLOCATE_UNINITIALIZED_ARRAY == null ? new char[length] : (char[]) allocate(char.class, length);
    }

    public static int[] newIntArray(int length) {
        return ALLOCATE_UNINITIALIZED_ARRAY == null ? new int[length] : (int[]) allocate(int.class, length);
    }

    public static long[] newLongArray(int length) {
        return ALLOCATE_UNINITIALIZED_ARRAY == null ? new long[length] : (long[]) allocate(long.class, length);
    }

    public static float[] newFloatArray(int length) {
        return ALLOCATE_UNINITIALIZED_ARRAY == null ? new float[length] : (float[]) allocate(float.class, length);
    }

    public static double[] newDoubleArray(int length) {
        return ALLOCATE_UNINITIALIZED_ARRAY == null ? new double[length] : (double[]) allocate(double.class, length);
    }

    private static Object allocate(Class<?> componentType, int length) {
        try {
            return ALLOCATE_UNINITIALIZED_ARRAY.invokeExact(componentType, length);
        } catch (Error | RuntimeException e) {
            throw e;
        } catch (Throwable t) {
            throw new AssertionError(t);
        }
    }
}
