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
 *       memory limit, the base is {@code min(cgroupLimit, physicalMemory) - heapMax}: the memory
 *       budget that is not already claimed by the JVM heap.</li>
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
     * Floor applied to the cgroup-derived budget to prevent a zero (or negative) limit on
     * mis-configured containers where {@code cgroupLimit <= heapMax}.
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
     * {@code breaker.native_memory.limit} via the cluster settings API to pick up the new
     * cgroup value. Platforms that restart the node on a memory resize (which is the common
     * approach) are unaffected because the node re-evaluates this at startup.
     */
    public static long nativeMemoryBase() {
        OptionalLong cgroupLimit = OsProbe.getInstance().getCgroupMemoryLimitInBytes();
        if (cgroupLimit.isPresent()) {
            return cgroupNativeMemoryBase(
                cgroupLimit.getAsLong(),
                OsProbe.getInstance().getAdjustedTotalMemorySize(),
                JvmInfo.jvmInfo().getMem().getHeapMax().getBytes()
            );
        }
        return MemorySizeValue.maxDirectMemory();
    }

    /**
     * Pure calculation for the cgroup path, exposed package-private for unit testing without
     * requiring a live {@link OsProbe} instance.
     *
     * @param cgroupLimit   the cgroup memory limit in bytes
     * @param adjustedTotal {@link OsProbe#getAdjustedTotalMemorySize()} — physical memory,
     *                      possibly overridden by {@code es.total_memory_bytes}
     * @param heapMax       {@code -Xmx} in bytes
     */
    static long cgroupNativeMemoryBase(long cgroupLimit, long adjustedTotal, long heapMax) {
        // A cgroupv1 "unlimited" limit is a very large number; min() with physical memory caps it.
        long totalMemory = Math.min(cgroupLimit, adjustedTotal);
        return Math.max(totalMemory - heapMax, MINIMUM_LIMIT);
    }
}
