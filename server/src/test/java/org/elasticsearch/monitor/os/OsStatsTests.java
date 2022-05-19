/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.monitor.os;

import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;

public class OsStatsTests extends ESTestCase {

    public void testSerialization() throws IOException {
        int numLoadAverages = randomIntBetween(1, 5);
        double loadAverages[] = new double[numLoadAverages];
        for (int i = 0; i < loadAverages.length; i++) {
            loadAverages[i] = randomDouble();
        }
        OsStats.Cpu cpu = new OsStats.Cpu(randomShort(), loadAverages);
        long memTotal = randomNonNegativeLong();
        OsStats.Mem mem = new OsStats.Mem(memTotal, randomLongBetween(0, memTotal), randomLongBetween(0, memTotal));
        long swapTotal = randomNonNegativeLong();
        OsStats.Swap swap = new OsStats.Swap(swapTotal, randomLongBetween(0, swapTotal));
        OsStats.Cgroup cgroup = new OsStats.Cgroup(
            randomAlphaOfLength(8),
            randomNonNegativeLong(),
            randomAlphaOfLength(8),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            new OsStats.Cgroup.CpuStat(randomNonNegativeLong(), randomNonNegativeLong(), randomNonNegativeLong()),
            randomAlphaOfLength(8),
            Long.toString(randomNonNegativeLong()),
            Long.toString(randomNonNegativeLong())
        );
        OsStats osStats = new OsStats(System.currentTimeMillis(), cpu, mem, swap, cgroup);

        try (BytesStreamOutput out = new BytesStreamOutput()) {
            osStats.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                OsStats deserializedOsStats = new OsStats(in);
                assertEquals(osStats.getTimestamp(), deserializedOsStats.getTimestamp());
                assertEquals(osStats.getCpu().getPercent(), deserializedOsStats.getCpu().getPercent());
                assertArrayEquals(osStats.getCpu().getLoadAverage(), deserializedOsStats.getCpu().getLoadAverage(), 0);
                assertEquals(osStats.getMem().getFree(), deserializedOsStats.getMem().getFree());
                assertEquals(osStats.getMem().getTotal(), deserializedOsStats.getMem().getTotal());
                assertEquals(osStats.getSwap().getFree(), deserializedOsStats.getSwap().getFree());
                assertEquals(osStats.getSwap().getTotal(), deserializedOsStats.getSwap().getTotal());
                assertEquals(osStats.getCgroup().getCpuAcctControlGroup(), deserializedOsStats.getCgroup().getCpuAcctControlGroup());
                assertEquals(osStats.getCgroup().getCpuAcctUsageNanos(), deserializedOsStats.getCgroup().getCpuAcctUsageNanos());
                assertEquals(osStats.getCgroup().getCpuControlGroup(), deserializedOsStats.getCgroup().getCpuControlGroup());
                assertEquals(osStats.getCgroup().getCpuCfsPeriodMicros(), deserializedOsStats.getCgroup().getCpuCfsPeriodMicros());
                assertEquals(osStats.getCgroup().getCpuCfsQuotaMicros(), deserializedOsStats.getCgroup().getCpuCfsQuotaMicros());
                assertEquals(
                    osStats.getCgroup().getCpuStat().getNumberOfElapsedPeriods(),
                    deserializedOsStats.getCgroup().getCpuStat().getNumberOfElapsedPeriods()
                );
                assertEquals(
                    osStats.getCgroup().getCpuStat().getNumberOfTimesThrottled(),
                    deserializedOsStats.getCgroup().getCpuStat().getNumberOfTimesThrottled()
                );
                assertEquals(
                    osStats.getCgroup().getCpuStat().getTimeThrottledNanos(),
                    deserializedOsStats.getCgroup().getCpuStat().getTimeThrottledNanos()
                );
                assertEquals(osStats.getCgroup().getMemoryLimitInBytes(), deserializedOsStats.getCgroup().getMemoryLimitInBytes());
                assertEquals(osStats.getCgroup().getMemoryUsageInBytes(), deserializedOsStats.getCgroup().getMemoryUsageInBytes());
            }
        }
    }

    public void testGetUsedMemoryWithZeroTotal() {
        OsStats.Mem mem = new OsStats.Mem(0, 0, randomNonNegativeLong());
        assertThat(mem.getUsed().getBytes(), equalTo(0L));
    }

    public void testGetUsedSwapWithZeroTotal() {
        OsStats.Swap swap = new OsStats.Swap(0, randomNonNegativeLong());
        assertThat(swap.getUsed().getBytes(), equalTo(0L));
    }
}
