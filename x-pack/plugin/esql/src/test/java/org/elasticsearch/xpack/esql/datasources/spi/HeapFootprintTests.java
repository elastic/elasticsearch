/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.spi;

import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.monitor.jvm.JvmInfo;
import org.elasticsearch.test.ESTestCase;

import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryPoolMXBean;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

/**
 * Footprints at injected G1 region sizes. The boundary cases match what {@code MemoryMXBean} reports on JDK 17 to 27:
 * at 4 MiB regions a {@code byte[]} whose aligned size is exactly 2 MiB is not humongous, one 8 bytes larger is.
 * {@link #testMatchesThisJvmsG1} checks the model against the G1 of the JVM running the test.
 */
public class HeapFootprintTests extends ESTestCase {

    private static final long MB = 1024 * 1024;
    private static final long HEADER = RamUsageEstimator.NUM_BYTES_ARRAY_HEADER;

    public void testFourMegRegions() {
        long region = 4 * MB;
        assertThat(HeapFootprint.byteArrayBytes(2 * MB - HEADER, region), equalTo(2 * MB));
        assertThat(HeapFootprint.byteArrayBytes(2 * MB - HEADER + 8, region), equalTo(4 * MB));
        assertThat(HeapFootprint.byteArrayBytes(4 * MB - HEADER, region), equalTo(4 * MB));
        assertThat(HeapFootprint.byteArrayBytes(4 * MB, region), equalTo(8 * MB));
        assertThat(HeapFootprint.byteArrayBytes(8 * MB, region), equalTo(12 * MB));
        assertThat(HeapFootprint.byteArrayBytes(10 * MB, region), equalTo(12 * MB));
    }

    public void testLargerRegions() {
        assertThat(HeapFootprint.byteArrayBytes(10 * MB, 8 * MB), equalTo(16 * MB));
        assertThat(HeapFootprint.byteArrayBytes(10 * MB, 32 * MB), equalTo(aligned(10 * MB)));
    }

    public void testNoRegionRoundingWithoutRegionSize() {
        long length = randomLongBetween(0, 64 * MB);
        assertThat(HeapFootprint.byteArrayBytes(length, 0), equalTo(aligned(length)));
        assertThat(HeapFootprint.byteArrayBytes(length, -1), equalTo(aligned(length)));
    }

    public void testSmallArraysAreHeaderAndAlignment() {
        assertThat(HeapFootprint.byteArrayBytes(0, 4 * MB), equalTo(aligned(0)));
        assertThat(HeapFootprint.byteArrayBytes(100, 4 * MB), equalTo(aligned(100)));
        assertThat(HeapFootprint.byteArrayBytes(2048, 4 * MB), equalTo(aligned(2048)));
    }

    public void testNeverBelowLength() {
        long length = randomLongBetween(0, Integer.MAX_VALUE);
        long region = randomFrom(-1L, 0L, MB, 2 * MB, 4 * MB, 8 * MB, 16 * MB, 32 * MB);
        assertThat(HeapFootprint.byteArrayBytes(length, region), greaterThan(length));
    }

    public void testRegionSize() {
        assertThat(HeapFootprint.regionSize(false, 4 * MB, () -> { throw new AssertionError("not G1"); }), equalTo(0L));
        assertThat(HeapFootprint.regionSize(true, 4 * MB, () -> { throw new AssertionError("reported"); }), equalTo(4 * MB));
        assertThat(HeapFootprint.regionSize(true, 0, () -> 8 * MB), equalTo(8 * MB));
        assertThat(HeapFootprint.regionSize(true, -1, () -> 2 * MB), equalTo(2 * MB));
    }

    public void testJvmRegionSizeOverload() {
        // whatever region size the test JVM runs with, the footprint is at least the aligned size
        assertThat(HeapFootprint.byteArrayBytes(10 * MB), greaterThan(10 * MB));
        assertThat(HeapFootprint.byteArrayBytes(100), equalTo(aligned(100)));
    }

    public void testRegionFriendlyLengthIsWasteFreeAtEveryRegionSize() {
        for (int shift = 10; shift <= 26; shift++) {
            int pow2 = 1 << shift;
            int length = HeapFootprint.regionFriendlyLength(pow2);
            assertThat(length % RamUsageEstimator.NUM_BYTES_OBJECT_ALIGNMENT, equalTo(0));
            assertThat(aligned(length), lessThanOrEqualTo((long) pow2));
            // the next aligned length would not fit
            assertThat(aligned(length + RamUsageEstimator.NUM_BYTES_OBJECT_ALIGNMENT), greaterThan((long) pow2));
            for (long region = MB; region <= 32 * MB; region <<= 1) {
                assertThat(
                    "pow2=" + pow2 + " region=" + region,
                    HeapFootprint.byteArrayBytes(length, region),
                    lessThanOrEqualTo((long) pow2)
                );
            }
        }
    }

    public void testLengthFittingIn() {
        long heapBytes = randomLongBetween(HEADER + RamUsageEstimator.NUM_BYTES_OBJECT_ALIGNMENT, 64 * MB);
        long length = HeapFootprint.lengthFittingIn(heapBytes);
        assertThat(length % RamUsageEstimator.NUM_BYTES_OBJECT_ALIGNMENT, equalTo(0L));
        assertThat(aligned(length), lessThanOrEqualTo(heapBytes));
        assertThat(aligned(length + RamUsageEstimator.NUM_BYTES_OBJECT_ALIGNMENT), greaterThan(heapBytes));
        int shift = between(10, 26);
        assertThat(HeapFootprint.lengthFittingIn(1L << shift), equalTo((long) HeapFootprint.regionFriendlyLength(1 << shift)));
        expectThrows(IllegalArgumentException.class, () -> HeapFootprint.lengthFittingIn(HEADER));
    }

    public void testRegionFriendlyLengthRejectsNonPowerOfTwo() {
        expectThrows(IllegalArgumentException.class, () -> HeapFootprint.regionFriendlyLength(0));
        expectThrows(IllegalArgumentException.class, () -> HeapFootprint.regionFriendlyLength(-4));
        expectThrows(IllegalArgumentException.class, () -> HeapFootprint.regionFriendlyLength(10 * 1024 * 1024));
        expectThrows(IllegalArgumentException.class, () -> HeapFootprint.regionFriendlyLength(8));
    }

    public void testNegativeLengthRejected() {
        expectThrows(IllegalArgumentException.class, () -> HeapFootprint.byteArrayBytes(-1, 4 * MB));
    }

    /**
     * Allocates humongous arrays in this JVM and compares the growth of the G1 old generation, where G1 places
     * humongous objects, with {@link HeapFootprint#byteArrayBytes(long)}. The model hard-codes G1's humongous rule
     * (more than half a region, rounded up to whole regions), so this fails if a JDK upgrade changes that rule.
     * Only humongous sizes are checked: they land in old gen whole-region-exact, whereas smaller arrays go to eden
     * and only reach old gen through promotion, which says nothing about the rule.
     *
     * <p>The only noise is a collection running between the two usage reads: it reclaims earlier dead arrays (and can
     * promote others), so the delta is meaningless. Such a measurement is discarded by comparing collection counts
     * and retried; a mismatch measured without a collection fails immediately. If every attempt for a size overlaps a
     * collection the test is skipped rather than failed, since it learned nothing.
     */
    public void testMatchesThisJvmsG1() {
        JvmInfo jvm = JvmInfo.jvmInfo();
        assumeTrue("test JVM is not using G1", "true".equals(jvm.useG1GC()));
        long region = jvm.getG1RegionSize();
        assumeTrue("G1 region size unknown", region > 0);
        MemoryPoolMXBean oldGen = null;
        for (MemoryPoolMXBean pool : ManagementFactory.getMemoryPoolMXBeans()) {
            if (pool.getName().equals("G1 Old Gen")) {
                oldGen = pool;
            }
        }
        assumeTrue("no G1 Old Gen memory pool", oldGen != null);

        long[] lengths = {
            region / 2 - HEADER + 8,  // smallest humongous length: one region
            region - HEADER,          // fills one region exactly
            region,                   // header spills into a second region
            2 * region - HEADER,      // fills two regions exactly
            2 * region,               // header spills into a third region
            2 * region + region / 2   // mid-region tail
        };
        int count = 4;
        for (long length : lengths) {
            long expected = HeapFootprint.byteArrayBytes(length);
            assertThat("length " + length + " should be humongous at region " + region, expected % region, equalTo(0L));
            boolean measured = false;
            for (int attempt = 0; attempt < 10 && measured == false; attempt++) {
                byte[][] keep = new byte[count][];
                long collectionsBefore = collectionCount();
                long before = oldGen.getUsage().getUsed();
                for (int i = 0; i < count; i++) {
                    keep[i] = new byte[(int) length];
                }
                long after = oldGen.getUsage().getUsed();
                // keep the arrays reachable until after the measurement
                assertThat(keep[count - 1].length, equalTo((int) length));
                if (collectionCount() != collectionsBefore) {
                    continue;
                }
                assertThat("length " + length + " at region " + region + ": bytes per array", (after - before) / count, equalTo(expected));
                measured = true;
            }
            assumeTrue("every measurement of length " + length + " overlapped a garbage collection", measured);
        }
    }

    private static long collectionCount() {
        long total = 0;
        for (GarbageCollectorMXBean gc : ManagementFactory.getGarbageCollectorMXBeans()) {
            total += Math.max(0, gc.getCollectionCount());
        }
        return total;
    }

    private static long aligned(long length) {
        return RamUsageEstimator.alignObjectSize(HEADER + length);
    }
}
