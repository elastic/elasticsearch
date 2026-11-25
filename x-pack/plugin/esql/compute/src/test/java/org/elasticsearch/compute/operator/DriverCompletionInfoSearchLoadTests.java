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

/**
 * The search load of {@link DriverCompletionInfo} adds up across computes and nodes.
 */
public class DriverCompletionInfoSearchLoadTests extends ESTestCase {

    public void testRoundTripsToNodesThatKnowIt() throws IOException {
        DriverCompletionInfo info = searchLoad(300, 200, 50);
        DriverCompletionInfo copy = copy(info, TransportVersion.current());
        assertThat(copy.searchLoad(), equalTo(new SearchLoadMetrics(300, 200, 50)));

        TransportVersion before = TransportVersionUtils.getPreviousVersion(TransportVersion.fromName("search_load_report"));
        DriverCompletionInfo older = copy(info, before);
        assertThat(older.searchLoad(), equalTo(SearchLoadMetrics.EMPTY));
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
            assertThat(sum.searchLoad(), equalTo(new SearchLoadMetrics(400, 210, 55)));
        }
    }

    /**
     * What a compute charges outside its drivers is part of its load.
     */
    public void testIncludesTheLoadOfTheComputeOutsideItsDrivers() {
        QueryLoad computeLoad = new QueryLoad();
        computeLoad.charging((nanos, cpuNanos, finishNanos) -> {}).charge(100, 40, 0);
        DriverCompletionInfo info = DriverCompletionInfo.excludingProfiles(List.of(), 0, computeLoad, false);
        assertThat(info.searchLoad().loadNanos(), equalTo(100L));
        assertThat(info.searchLoad().cpuNanos(), equalTo(40L));
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
            new SearchLoadMetrics(loadNanos, cpuNanos, waitingNanos),
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
