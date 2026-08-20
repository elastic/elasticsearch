/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data.arrow;

import org.apache.arrow.memory.AllocationListener;
import org.apache.arrow.memory.AllocationManager;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.ReferenceManager;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.memory.rounding.RoundingPolicy;
import org.apache.arrow.memory.util.MemoryUtil;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.monitor.jvm.JvmInfo;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;

/**
 * Arrow {@link AllocationManager} backed by {@link ByteBuffer#allocateDirect}.
 * <p>
 * That is the only off-heap API HotSpot charges to {@code MaxDirectMemorySize}.
 * {@code UnsafeAllocationManager} mallocs outside that budget (SIGKILL on a
 * cgroup-hard container). Request-breaker accounting stays on
 * {@link CircuitBreakerAllocationListener}. Allocation failure becomes
 * {@link CircuitBreakingException} (429). RootAllocator max is MaxDirect minus
 * {@link #NIO_RESERVE_BYTES}. Arrow's config builder is package-private, so
 * {@link #createRootAllocator} reaches it reflectively.
 */
public final class DirectBufferAllocationManager extends AllocationManager {

    public static final long NIO_RESERVE_BYTES = ByteSizeValue.ofMb(32).getBytes();

    static final RoundingPolicy EXACT_FIT_ROUNDING_POLICY = requestSize -> requestSize;

    private static final MethodHandle INVOKE_CLEANER = bindInvokeCleaner();
    private static final ArrowConfigApi ARROW_CONFIG = bindArrowConfigApi();

    // Process-lifetime 0-size sentinel. UnsafeAllocationManager does the same
    // (allocateMemory(0) + NO_OP). Must not free: allocator.getEmpty() is shared.
    private static final ArrowBuf EMPTY = new ArrowBuf(ReferenceManager.NO_OP, null, 0, MemoryUtil.allocateMemory(0));

    public static final Factory FACTORY = new Factory() {
        @Override
        public AllocationManager create(BufferAllocator accountingAllocator, long size) {
            // Allocate before super(): AllocationManager's ctor associates a BufferLedger.
            return new DirectBufferAllocationManager(accountingAllocator, allocateDirect(accountingAllocator, size));
        }

        @Override
        public ArrowBuf empty() {
            return EMPTY;
        }
    };

    private ByteBuffer buffer;
    private final long size;
    private final long address;

    private DirectBufferAllocationManager(BufferAllocator accountingAllocator, ByteBuffer buffer) {
        super(accountingAllocator);
        this.buffer = buffer;
        this.size = buffer.capacity();
        this.address = MemoryUtil.getByteBufferAddress(buffer);
    }

    public static RootAllocator createRootAllocator(AllocationListener listener, long maxAllocation) {
        return createRootAllocator(listener, maxAllocation, FACTORY);
    }

    // Test-only Factory injection. Production always uses FACTORY. Needed so
    // buffer() can throw CBE without filling MaxDirect (hostile in a unit JVM).
    static RootAllocator createRootAllocator(AllocationListener listener, long maxAllocation, Factory factory) {
        return ARROW_CONFIG.newRootAllocator(listener, maxAllocation, factory);
    }

    public static long arrowDirectMemoryLimit() {
        long maxDirect = JvmInfo.jvmInfo().getMem().getDirectMemoryMax().getBytes();
        // MXBean 0 = unset flag or non-HotSpot; treat as unknown and do not cap here.
        return maxDirect <= 0L ? Long.MAX_VALUE : Math.max(0L, maxDirect - NIO_RESERVE_BYTES);
    }

    static CircuitBreakingException circuitBreakingException(long bytesWanted, long byteLimit, Throwable cause) {
        CircuitBreakingException cbe = new CircuitBreakingException(
            Strings.format("Unable to allocate [%d] bytes of direct memory for Arrow; limit is [%d] bytes", bytesWanted, byteLimit),
            bytesWanted,
            byteLimit,
            CircuitBreaker.Durability.TRANSIENT
        );
        if (cause != null) {
            cbe.initCause(cause);
        }
        return cbe;
    }

    private static ByteBuffer allocateDirect(BufferAllocator accountingAllocator, long size) {
        if (size > Integer.MAX_VALUE) {
            undoPreAllocation(accountingAllocator, size);
            throw circuitBreakingException(size, Integer.MAX_VALUE, null);
        }
        try {
            return ByteBuffer.allocateDirect((int) size);
        } catch (OutOfMemoryError e) {
            throw failedDirectAllocation(accountingAllocator, size, e);
        }
    }

    private static void undoPreAllocation(BufferAllocator accountingAllocator, long size) {
        accountingAllocator.getListener().onRelease(size);
    }

    static CircuitBreakingException failedDirectAllocation(BufferAllocator accountingAllocator, long size, OutOfMemoryError error) {
        undoPreAllocation(accountingAllocator, size);
        return circuitBreakingException(size, arrowDirectMemoryLimit(), error);
    }

    private record ArrowConfigApi(
        Method configBuilder,
        Method setListener,
        Method setMaxAllocation,
        Method setRoundingPolicy,
        Method setFactory,
        Method build,
        Constructor<RootAllocator> ctor
    ) {
        RootAllocator newRootAllocator(AllocationListener listener, long maxAllocation, Factory factory) {
            try {
                Object builder = configBuilder.invoke(null);
                setListener.invoke(builder, listener);
                setMaxAllocation.invoke(builder, maxAllocation);
                setRoundingPolicy.invoke(builder, EXACT_FIT_ROUNDING_POLICY);
                setFactory.invoke(builder, factory);
                return ctor.newInstance(build.invoke(builder));
            } catch (InvocationTargetException e) {
                throw rethrow(e.getCause());
            } catch (ReflectiveOperationException e) {
                throw new AssertionError("Arrow RootAllocator config API is not accessible", e);
            }
        }
    }

    @SuppressForbidden(reason = "Arrow hid RootAllocator config behind package-private BaseAllocator")
    private static ArrowConfigApi bindArrowConfigApi() {
        try {
            Method configBuilder = Class.forName("org.apache.arrow.memory.BaseAllocator").getMethod("configBuilder");
            configBuilder.setAccessible(true);
            Class<?> builderType = configBuilder.invoke(null).getClass();
            Constructor<RootAllocator> ctor = RootAllocator.class.getConstructor(
                Class.forName("org.apache.arrow.memory.BaseAllocator$Config")
            );
            ctor.setAccessible(true);
            return new ArrowConfigApi(
                configBuilder,
                builderType.getMethod("listener", AllocationListener.class),
                builderType.getMethod("maxAllocation", long.class),
                builderType.getMethod("roundingPolicy", RoundingPolicy.class),
                builderType.getMethod("allocationManagerFactory", Factory.class),
                builderType.getMethod("build"),
                ctor
            );
        } catch (ReflectiveOperationException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    @SuppressForbidden(reason = "need jdk.internal.misc.Unsafe.invokeCleaner to free DirectByteBuffer immediately")
    private static MethodHandle bindInvokeCleaner() {
        try {
            Class<?> unsafeClass = Class.forName("jdk.internal.misc.Unsafe");
            Field theUnsafe = unsafeClass.getDeclaredField("theUnsafe");
            theUnsafe.setAccessible(true);
            return MethodHandles.lookup()
                .findVirtual(unsafeClass, "invokeCleaner", MethodType.methodType(void.class, ByteBuffer.class))
                .bindTo(theUnsafe.get(null));
        } catch (Throwable e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    private static void freeDirectBuffer(ByteBuffer buffer) {
        try {
            INVOKE_CLEANER.invokeExact(buffer);
        } catch (Throwable t) {
            throw rethrow(t);
        }
    }

    private static RuntimeException rethrow(Throwable t) {
        if (t instanceof RuntimeException re) {
            return re;
        }
        if (t instanceof Error err) {
            throw err;
        }
        throw new AssertionError(t);
    }

    @Override
    public long getSize() {
        return size;
    }

    @Override
    protected long memoryAddress() {
        return address;
    }

    @Override
    protected void release0() {
        ByteBuffer toFree = buffer;
        if (toFree == null) {
            return;
        }
        buffer = null;
        freeDirectBuffer(toFree);
    }
}
