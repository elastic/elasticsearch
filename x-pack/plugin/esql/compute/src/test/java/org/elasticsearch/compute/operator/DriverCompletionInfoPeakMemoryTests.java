/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.search.scheduling.QueryLoad;
import org.elasticsearch.search.scheduling.SearchLoadMetrics;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.TransportVersionUtils;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import static org.hamcrest.Matchers.equalTo;

public class DriverCompletionInfoPeakMemoryTests extends ESTestCase {

    public void testRoundTripsToNodesThatKnowIt() throws IOException {
        DriverCompletionInfo info = peakMemory(4096);
        assertThat(copy(info, TransportVersion.current()).peakMemoryBytes(), equalTo(4096L));

        TransportVersion before = TransportVersionUtils.getPreviousVersion(TransportVersion.fromName("search_peak_memory"));
        DriverCompletionInfo older = copy(info, before);
        assertThat(older.peakMemoryBytes(), equalTo(0L));
        assertThat(older.searchLoad(), equalTo(info.searchLoad()));
    }

    /**
     * Peaks on different nodes need not overlap, so the work of several nodes keeps the highest, in any order.
     */
    public void testAccumulatorsKeepTheHighest() {
        List<DriverCompletionInfo> infos = shuffledList(List.of(peakMemory(300), peakMemory(700), peakMemory(100)));
        DriverCompletionInfo.Accumulator accumulator = new DriverCompletionInfo.Accumulator();
        DriverCompletionInfo.AtomicAccumulator atomic = new DriverCompletionInfo.AtomicAccumulator();
        for (DriverCompletionInfo info : infos) {
            accumulator.accumulate(info);
            atomic.accumulate(info);
        }
        assertThat(accumulator.finish().peakMemoryBytes(), equalTo(700L));
        assertThat(atomic.finish().peakMemoryBytes(), equalTo(700L));
    }

    public void testCopiesKeepThePeak() {
        DriverCompletionInfo info = peakMemory(4096);
        assertThat(info.withAdditionalWarnings(List.of("a warning")).peakMemoryBytes(), equalTo(4096L));
        assertThat(info.withoutApproximationApplied().peakMemoryBytes(), equalTo(4096L));
        DriverCompletionInfo built = DriverCompletionInfo.excludingProfiles(List.of(), 0, new QueryLoad(), 4096, true);
        assertThat(built.withoutApproximationApplied().peakMemoryBytes(), equalTo(4096L));
    }

    private static DriverCompletionInfo peakMemory(long peakMemoryBytes) {
        return new DriverCompletionInfo(
            1,
            2,
            0,
            0,
            0,
            0,
            0,
            7,
            new SearchLoadMetrics(300, 200, 50),
            peakMemoryBytes,
            List.of(),
            List.of(),
            null,
            false,
            false,
            Set.of()
        );
    }

    private static DriverCompletionInfo copy(DriverCompletionInfo info, TransportVersion version) throws IOException {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.setTransportVersion(version);
            info.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                in.setTransportVersion(version);
                return DriverCompletionInfo.readFrom(in, new ThreadContext(Settings.EMPTY));
            }
        }
    }
}
