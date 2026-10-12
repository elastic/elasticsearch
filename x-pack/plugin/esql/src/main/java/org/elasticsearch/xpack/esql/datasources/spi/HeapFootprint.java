/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.spi;

import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.indices.breaker.HierarchyCircuitBreakerService.G1OverLimitStrategy;
import org.elasticsearch.monitor.jvm.JvmInfo;

import java.util.function.LongSupplier;

/**
 * Heap a large {@code byte[]} really occupies, for circuit-breaker charges on the external read path.
 *
 * <p>A breaker charge must never be below what the heap gives up for the buffer, otherwise the ledger under-counts
 * by construction. A {@code byte[]} costs its array header plus object alignment on top of its length, and under G1
 * an array whose size exceeds half a region is "humongous": it occupies a whole number of regions. With
 * Elasticsearch's 4 MiB regions (pinned for heaps below 8 GiB) a 4 MiB array occupies 8 MiB and a 10 MiB array
 * occupies 12 MiB. The region size depends on the heap size and on operator settings, so it is read from the running
 * JVM rather than assumed.
 *
 * <p>Under collectors other than G1 only the header and alignment are added; large-object rules of other collectors
 * (ZGC, Parallel) are not modelled. Under G1 with an unreported region size, the region size is estimated from the
 * heap size as {@link G1OverLimitStrategy#fallbackRegionSize} does for the parent breaker.
 *
 * <p><b>This encodes G1 behaviour, not a JVM contract.</b> The region size and the array header come from the running
 * JVM, but the humongous rule itself (strictly more than half a region, rounded up to whole regions) is hard-coded.
 * It was measured with {@code MemoryMXBean} on JDK 17, 21, 25 and 27, with and without compact object headers, at
 * 1m to 32m regions. If G1 ever packs humongous objects tighter, this over-charges, so queries are refused somewhat
 * early but memory stays bounded; if it ever rounds coarser, this under-charges. {@code HeapFootprintTests} measures
 * the rule against the test JVM's own G1 old generation so a JDK upgrade that changes it fails a test.
 *
 * <p>Only worth calling for buffers that can approach half a region; below that the difference from the raw length
 * is a few bytes.
 */
public final class HeapFootprint {

    /** G1 region size of this JVM, or {@code 0} for "no region rounding" (not G1). */
    private static final long REGION_SIZE = regionSizeFromJvm();

    private HeapFootprint() {}

    /**
     * Heap a {@code byte[]} of {@code length} occupies: header and alignment, rounded up to whole G1 regions when the
     * array is humongous.
     */
    public static long byteArrayBytes(long length) {
        return byteArrayBytes(length, REGION_SIZE);
    }

    /**
     * Largest {@code byte[]} length whose footprint is at most {@code powerOfTwo} bytes. Because the footprint then
     * fits in a power of two, it either fills whole regions exactly or stays at most half a region, at every
     * power-of-two region size; such a buffer wastes no humongous-region tail. Use it for buffer sizes we choose
     * ourselves instead of an exact power of two, which wastes up to a whole region.
     */
    public static int regionFriendlyLength(int powerOfTwo) {
        if (powerOfTwo <= 0 || Integer.bitCount(powerOfTwo) != 1) {
            throw new IllegalArgumentException("expected a positive power of two, got: " + powerOfTwo);
        }
        return (int) lengthFittingIn(powerOfTwo);
    }

    /**
     * Largest {@code byte[]} length whose header-and-alignment size is at most {@code heapBytes}: a few bytes under
     * it. Use it for a buffer size a user configures, so a value they pick as a power of two (as sizes like
     * {@code 4mb} are) gets {@link #regionFriendlyLength(int)}'s waste-free size. Other values only lose the header;
     * if they are humongous they still waste a region tail, and are charged for it by {@link #byteArrayBytes(long)}.
     */
    public static long lengthFittingIn(long heapBytes) {
        long length = heapBytes - RamUsageEstimator.NUM_BYTES_ARRAY_HEADER;
        length -= length % RamUsageEstimator.NUM_BYTES_OBJECT_ALIGNMENT;
        if (length <= 0) {
            throw new IllegalArgumentException("too small for a byte[] header: " + heapBytes);
        }
        return length;
    }

    /** {@link #byteArrayBytes(long)} for an explicit region size; {@code regionSize <= 0} disables region rounding. */
    static long byteArrayBytes(long length, long regionSize) {
        if (length < 0) {
            throw new IllegalArgumentException("length must be non-negative, got: " + length);
        }
        long aligned = RamUsageEstimator.alignObjectSize(RamUsageEstimator.NUM_BYTES_ARRAY_HEADER + length);
        // G1 treats an object as humongous when it is strictly larger than half a region
        if (regionSize <= 0 || aligned <= regionSize / 2) {
            return aligned;
        }
        return ((aligned + regionSize - 1) / regionSize) * regionSize;
    }

    private static long regionSizeFromJvm() {
        JvmInfo info = JvmInfo.jvmInfo();
        return regionSize("true".equals(info.useG1GC()), info.getG1RegionSize(), () -> G1OverLimitStrategy.fallbackRegionSize(info));
    }

    /**
     * Region size to round to: {@code 0} when not running G1, the reported size when known, otherwise the same
     * heap-derived fallback the real-memory parent breaker uses.
     */
    static long regionSize(boolean useG1, long reportedRegionSize, LongSupplier fallback) {
        if (useG1 == false) {
            return 0;
        }
        return reportedRegionSize > 0 ? reportedRegionSize : fallback.getAsLong();
    }
}
