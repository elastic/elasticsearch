/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.usage;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.esql.QueryMetricsListener;
import org.elasticsearch.xpack.core.esql.QueryMetricsListener.Outcome;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

public class UsageListenerTests extends ESTestCase {

    /** Records the calls so that the tests can see what the listener forwarded; the real accumulator is another branch's. */
    private static class RecordingAccumulator implements UsageAccumulator {
        final List<Outcome> outcomes = new ArrayList<>();
        final List<Map<String, Long>> metrics = new ArrayList<>();

        @Override
        public void record(Outcome outcome, Map<String, Long> m) {
            outcomes.add(outcome);
            metrics.add(m);
        }

        @Override
        public UsageSnapshot drain(Instant drainTime) {
            return new UsageSnapshot(drainTime, Map.of());
        }
    }

    /** A plugin whose accumulator is replaced, because building the real components needs a whole node. */
    private static EsqlUsageReportingPlugin plugin(UsageAccumulator accumulator, boolean reportExempt) {
        return new EsqlUsageReportingPlugin() {
            @Override
            UsageAccumulator accumulator() {
                return accumulator;
            }

            @Override
            boolean reportExempt() {
                return reportExempt;
            }
        };
    }

    private static final Map<String, Long> METRICS = Map.of(QueryMetricsListener.CPU_NANOS, 5L);

    public void testSuccessIsForwarded() {
        RecordingAccumulator accumulator = new RecordingAccumulator();
        UsageListener listener = new UsageListener(plugin(accumulator, false));
        listener.onQueryCompleted(METRICS);
        listener.onQueryCompleted(Outcome.SUCCESS, METRICS);
        assertThat(accumulator.outcomes, equalTo(List.of(Outcome.SUCCESS, Outcome.SUCCESS)));
        assertThat(accumulator.metrics, equalTo(List.of(METRICS, METRICS)));
    }

    public void testOtherOutcomesAreIgnoredUnlessExemptIsReported() {
        for (Outcome outcome : List.of(Outcome.FAILED, Outcome.CANCELLED)) {
            RecordingAccumulator ignored = new RecordingAccumulator();
            new UsageListener(plugin(ignored, false)).onQueryCompleted(outcome, METRICS);
            assertThat(ignored.outcomes, empty());

            RecordingAccumulator forwarded = new RecordingAccumulator();
            new UsageListener(plugin(forwarded, true)).onQueryCompleted(outcome, METRICS);
            assertThat(forwarded.outcomes, hasSize(1));
            assertThat(forwarded.outcomes.get(0), equalTo(outcome));
        }
    }

    public void testNeverThrows() {
        UsageAccumulator throwing = new RecordingAccumulator() {
            @Override
            public void record(Outcome outcome, Map<String, Long> m) {
                throw new IllegalStateException("boom");
            }
        };
        UsageListener listener = new UsageListener(plugin(throwing, true));
        listener.onQueryCompleted(METRICS);
        listener.onQueryCompleted(Outcome.FAILED, METRICS);
    }

    public void testNoopBeforeComponentsAreBuilt() {
        new UsageListener(new EsqlUsageReportingPlugin()).onQueryCompleted(METRICS);
    }
}
