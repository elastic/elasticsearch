/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.unit;

import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

public class NativeMemoryLimitCalculatorTests extends ESTestCase {

    // -----------------------------------------------------------------------
    // cgroupNativeMemoryBase — pure logic, no OsProbe dependency
    // -----------------------------------------------------------------------

    public void testCgroupPathSubtractsHeapDirectAndOverhead() {
        long cgroupLimit = ByteSizeValue.ofGb(8).getBytes();
        long adjustedTotal = ByteSizeValue.ofGb(8).getBytes();
        long heapMax = ByteSizeValue.ofGb(4).getBytes();
        long directMax = ByteSizeValue.ofGb(1).getBytes();

        long base = NativeMemoryLimitCalculator.cgroupNativeMemoryBase(cgroupLimit, adjustedTotal, heapMax, directMax);

        assertEquals(cgroupLimit - heapMax - directMax - NativeMemoryLimitCalculator.OS_OVERHEAD, base);
    }

    public void testCgroupLimitLargerThanPhysicalMemoryIsCappedAtPhysical() {
        // cgroupv1 "unlimited" is a very large long — larger than any real machine.
        // min(cgroupLimit, adjustedTotal) should reduce it to adjustedTotal.
        // directMax == 0, so effectiveDirectMax falls back to heapMax.
        // Use 16 GB physical / 4 GB heap so that 2*heapMax + OS_OVERHEAD stays under adjustedTotal.
        long adjustedTotal = ByteSizeValue.ofGb(16).getBytes();
        long heapMax = ByteSizeValue.ofGb(4).getBytes();

        long base = NativeMemoryLimitCalculator.cgroupNativeMemoryBase(Long.MAX_VALUE / 2, adjustedTotal, heapMax, 0);

        assertEquals(adjustedTotal - heapMax - heapMax - NativeMemoryLimitCalculator.OS_OVERHEAD, base);
    }

    public void testFloorAppliedWhenBudgetIsNegative() {
        // Misconfigured container: cgroup limit smaller than heap alone.
        long cgroupLimit = ByteSizeValue.ofGb(1).getBytes();
        long heapMax = ByteSizeValue.ofGb(2).getBytes();

        long base = NativeMemoryLimitCalculator.cgroupNativeMemoryBase(cgroupLimit, cgroupLimit, heapMax, 0);

        assertEquals(NativeMemoryLimitCalculator.MINIMUM_LIMIT, base);
    }

    public void testFloorAppliedWhenBudgetIsZero() {
        // Exactly at the boundary: heapMax + OS_OVERHEAD == cgroupLimit.
        long heapMax = ByteSizeValue.ofGb(4).getBytes();
        long cgroupLimit = heapMax + NativeMemoryLimitCalculator.OS_OVERHEAD;

        long base = NativeMemoryLimitCalculator.cgroupNativeMemoryBase(cgroupLimit, cgroupLimit, heapMax, 0);

        assertThat(base, greaterThanOrEqualTo(NativeMemoryLimitCalculator.MINIMUM_LIMIT));
    }

    public void testUnsetDirectMaxFallsBackToHeapMax() {
        // directMax == 0 means -XX:MaxDirectMemorySize was not set; the JVM defaults to heapMax
        // for NIO direct allocations. The conservative choice is to deduct heapMax as the
        // effective direct budget, yielding a smaller base than an explicit small directMax.
        // Use 16 GB cgroup / 4 GB heap so that 2*heapMax + OS_OVERHEAD stays under cgroupLimit.
        long cgroupLimit = ByteSizeValue.ofGb(16).getBytes();
        long heapMax = ByteSizeValue.ofGb(4).getBytes();

        long withNoDirectMax = NativeMemoryLimitCalculator.cgroupNativeMemoryBase(cgroupLimit, cgroupLimit, heapMax, 0);
        long withSmallDirectMax = NativeMemoryLimitCalculator.cgroupNativeMemoryBase(
            cgroupLimit,
            cgroupLimit,
            heapMax,
            ByteSizeValue.ofGb(1).getBytes()
        );

        // No directMax → effectiveDirectMax = heapMax = 4 GB; explicit 1 GB is smaller → larger base
        assertThat(withNoDirectMax, equalTo(cgroupLimit - heapMax - heapMax - NativeMemoryLimitCalculator.OS_OVERHEAD));
        assertThat(withSmallDirectMax, greaterThan(withNoDirectMax));
    }

    // -----------------------------------------------------------------------
    // nativeMemoryBase — end-to-end (exercises the OsProbe fallback path in CI)
    // -----------------------------------------------------------------------

    public void testNativeMemoryBaseIsPositive() {
        // Should always be positive regardless of environment (cgroup or fallback).
        assertThat(NativeMemoryLimitCalculator.nativeMemoryBase(), greaterThan(0L));
    }

    public void testNativeMemoryBaseFallsBackToMaxDirectMemoryWhenNoCgroup() {
        // On macOS, bare-metal Linux, and any environment where OsProbe returns no cgroup limit,
        // nativeMemoryBase() must equal maxDirectMemory(). In CI this path always runs
        // (the test JVM is never inside a memory-limited cgroup). The assertion is a no-op
        // (passes trivially) when a cgroup IS present, which is acceptable — the cgroup path
        // has its own tests above.
        if (org.elasticsearch.monitor.os.OsProbe.getInstance().getCgroupMemoryLimitInBytes().isEmpty()) {
            assertThat(NativeMemoryLimitCalculator.nativeMemoryBase(), equalTo(MemorySizeValue.maxDirectMemory()));
        }
    }
}
