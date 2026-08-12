/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.unit;

import org.elasticsearch.monitor.jvm.JvmInfo;
import org.elasticsearch.monitor.os.OsProbe;

import java.util.OptionalLong;

/**
 * Calculates the effective memory base for the {@code native_memory} circuit breaker.
 *
 * <p>Two paths are supported, chosen automatically at node startup:
 *
 * <ol>
 *   <li><b>Container path (preferred)</b> — when the JVM is inside a Linux cgroup with a finite
 *       memory limit, the base is {@code min(cgroupLimit, physicalMemory) - heapMax - directMax -
 *       OS_OVERHEAD}. This correctly represents the memory budget that is neither claimed by the
 *       JVM heap nor by NIO direct memory, regardless of whether the native allocator uses
 *       {@code Unsafe.allocateMemory} (like Arrow) or {@code allocateDirect} (like Netty).</li>
 *   <li><b>Fallback path</b> — when no cgroup limit is available (macOS, bare-metal Linux, or a
 *       container without a memory limit set), the base is {@link MemorySizeValue#maxDirectMemory()},
 *       i.e. {@code -XX:MaxDirectMemorySize} when set or the heap max otherwise. This is the only
 *       JVM-visible proxy for a native budget outside a container.</li>
 * </ol>
 *
 * <p>The two bases have different magnitudes — {@code 50%} of the cgroup budget is not the same
 * byte count as {@code 50%} of {@code MaxDirectMemorySize}. Operators who need predictable sizing
 * should prefer an explicit byte value over a percentage.
 */
public final class NativeMemoryLimitCalculator {

    /**
     * Headroom reserved for OS kernel and runtime overhead when computing a cgroup-based budget.
     * Matches the constant used by ML's {@code NativeMemoryCalculator}.
     */
    static final long OS_OVERHEAD = ByteSizeValue.ofMb(200).getBytes();

    /**
     * Floor applied to the cgroup-derived budget to prevent a zero (or negative) limit on
     * mis-configured containers where {@code cgroupLimit <= heapMax + directMax + OS_OVERHEAD}.
     */
    static final long MINIMUM_LIMIT = ByteSizeValue.ofMb(64).getBytes();

    private NativeMemoryLimitCalculator() {}

    /**
     * Returns the effective native memory base for percentage-based limit calculations.
     *
     * <p>Uses the cgroup memory limit when available; otherwise falls back to
     * {@link MemorySizeValue#maxDirectMemory()}.
     *
     * <p><b>Limitation — cgroup limit is read once at call time and not refreshed.</b> This method
     * reads the cgroup memory limit from the filesystem at the moment it is called. If the limit is
     * subsequently changed by the container runtime — for example via Kubernetes In-Place Pod
     * Resource Resize (beta since K8s 1.29) — the circuit breaker limit derived from this value
     * will not update automatically. An operator must explicitly re-apply
     * {@code indices.breaker.native_memory.limit} via the cluster settings API to pick up the new
     * cgroup value. Platforms that restart the node on a memory resize (which is the common
     * approach) are unaffected because the node re-evaluates this at startup.
     */
    public static long nativeMemoryBase() {
        OptionalLong cgroupLimit = OsProbe.getInstance().getCgroupMemoryLimitInBytes();
        if (cgroupLimit.isPresent()) {
            return cgroupNativeMemoryBase(
                cgroupLimit.getAsLong(),
                OsProbe.getInstance().getAdjustedTotalMemorySize(),
                JvmInfo.jvmInfo().getMem().getHeapMax().getBytes(),
                JvmInfo.jvmInfo().getMem().getDirectMemoryMax().getBytes()
            );
        }
        return MemorySizeValue.maxDirectMemory();
    }

    /**
     * Pure calculation for the cgroup path, exposed package-private for unit testing without
     * requiring a live {@link OsProbe} instance.
     *
     * @param cgroupLimit    the cgroup memory limit in bytes
     * @param adjustedTotal  {@link OsProbe#getAdjustedTotalMemorySize()} — physical memory,
     *                       possibly overridden by {@code es.total_memory_bytes}
     * @param heapMax        {@code -Xmx} in bytes
     * @param directMax      {@code -XX:MaxDirectMemorySize} in bytes; {@code 0} means unset,
     *                       in which case {@code heapMax} is used as the effective direct budget
     *                       (the JVM's own fallback when the flag is absent)
     */
    static long cgroupNativeMemoryBase(long cgroupLimit, long adjustedTotal, long heapMax, long directMax) {
        // A cgroupv1 "unlimited" limit is a very large number; min() with physical memory caps it.
        long totalMemory = Math.min(cgroupLimit, adjustedTotal);
        // When -XX:MaxDirectMemorySize is unset (directMax == 0), the JVM still permits NIO direct
        // allocations up to heapMax (the HotSpot default). Use heapMax as the effective direct budget
        // so that the calculation is conservative on nodes that did not set the flag explicitly.
        long effectiveDirectMax = directMax > 0 ? directMax : heapMax;
        long reserved = heapMax + effectiveDirectMax + OS_OVERHEAD;
        return Math.max(totalMemory - reserved, MINIMUM_LIMIT);
    }
}
