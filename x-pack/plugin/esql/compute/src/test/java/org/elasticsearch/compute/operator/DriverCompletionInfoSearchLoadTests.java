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
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.TransportVersionUtils;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import static org.hamcrest.Matchers.equalTo;

/**
 * The search load, CPU time and waiting of the work behind a {@link DriverCompletionInfo} add up across computes and nodes.
 */
public class DriverCompletionInfoSearchLoadTests extends ESTestCase {

    public void testRoundTripsToNodesThatKnowIt() throws IOException {
        DriverCompletionInfo info = searchLoad(300, 200, 50);
        DriverCompletionInfo copy = copy(info, TransportVersion.current());
        assertThat(copy.searchLoadNanos(), equalTo(300L));
        assertThat(copy.searchCpuNanos(), equalTo(200L));
        assertThat(copy.searchWaitingNanos(), equalTo(50L));

        TransportVersion before = TransportVersionUtils.getPreviousVersion(TransportVersion.fromName("search_load_report"));
        DriverCompletionInfo older = copy(info, before);
        assertThat(older.searchLoadNanos(), equalTo(0L));
        assertThat(older.searchCpuNanos(), equalTo(0L));
        assertThat(older.searchWaitingNanos(), equalTo(0L));
        assertThat(older.driverNanos(), equalTo(info.driverNanos()));
    }

    public void testAccumulatorsAddThemUp() {
        DriverCompletionInfo.Accumulator accumulator = new DriverCompletionInfo.Accumulator();
        DriverCompletionInfo.AtomicAccumulator atomic = new DriverCompletionInfo.AtomicAccumulator();
        for (DriverCompletionInfo info : List.of(searchLoad(300, 200, 50), searchLoad(100, 10, 5))) {
            accumulator.accumulate(info);
            atomic.accumulate(info);
        }
        for (DriverCompletionInfo sum : List.of(accumulator.finish(), atomic.finish())) {
            assertThat(sum.searchLoadNanos(), equalTo(400L));
            assertThat(sum.searchCpuNanos(), equalTo(210L));
            assertThat(sum.searchWaitingNanos(), equalTo(55L));
        }
    }

    /**
     * What a compute charged outside its drivers, such as its parallel workers, is part of its load.
     */
    public void testIncludesTheLoadOfTheComputeOutsideItsDrivers() {
        QueryLoad computeLoad = new QueryLoad();
        computeLoad.charging((nanos, cpuNanos, finishNanos) -> {}).charge(100, 40, 0);
        DriverCompletionInfo info = DriverCompletionInfo.excludingProfiles(List.of(), 0, computeLoad, false);
        assertThat(info.searchLoadNanos(), equalTo(100L));
        assertThat(info.searchCpuNanos(), equalTo(40L));
    }

    private static DriverCompletionInfo searchLoad(long loadNanos, long cpuNanos, long waitingNanos) {
        return new DriverCompletionInfo(
            1,
            2,
            0,
            0,
            0,
            0,
            0,
            7,
            loadNanos,
            cpuNanos,
            waitingNanos,
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
