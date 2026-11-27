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
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

/** Tests of the small value types and of the simple implementations. */
public class UsageTypesTests extends ESTestCase {

    public void testCpuComponentsFromMetricsReadsOnlyCountedKeys() {
        CpuComponents c = CpuComponents.fromMetrics(
            Map.of(
                QueryMetricsListener.PLANNING_CPU_NANOS,
                1L,
                QueryMetricsListener.SPLIT_DISCOVERY_CPU_NANOS,
                2L,
                QueryMetricsListener.CPU_NANOS,
                4L,
                QueryMetricsListener.READ_CPU_NANOS,
                8L,
                QueryMetricsListener.DRIVER_NANOS,
                1000L,
                QueryMetricsListener.BYTES_READ,
                1000L
            )
        );
        assertThat(c, equalTo(new CpuComponents(1, 2, 4, 8)));
        assertThat(c.totalNanos(), equalTo(15L));
    }

    public void testAbsentKeysCountZero() {
        assertThat(CpuComponents.fromMetrics(Map.of()), equalTo(CpuComponents.ZERO));
        assertThat(CpuComponents.ZERO.isZero(), is(true));
    }

    public void testPlusIsAdditive() {
        CpuComponents a = new CpuComponents(1, 2, 3, 4);
        CpuComponents b = new CpuComponents(10, 20, 30, 40);
        assertThat(a.plus(b), equalTo(new CpuComponents(11, 22, 33, 44)));
        assertThat(a.plus(CpuComponents.ZERO), equalTo(a));
    }

    public void testSnapshotIsEmptyWhenNothingIsUsed() {
        Instant now = Instant.now();
        assertThat(new UsageSnapshot(now, Map.of()).isEmpty(), is(true));
        assertThat(new UsageSnapshot(now, Map.of(Outcome.SUCCESS, CpuComponents.ZERO)).isEmpty(), is(true));
        assertThat(new UsageSnapshot(now, Map.of(Outcome.SUCCESS, new CpuComponents(0, 0, 1, 0))).isEmpty(), is(false));
    }

    public void testSourceIdentityCompleteness() {
        assertThat(new SourceIdentity("n", "d", "c", "aws", "r").isComplete(), is(true));
        assertThat(new SourceIdentity("n", "d", "", "aws", "r").isComplete(), is(false));
        assertThat(new SourceIdentity("n", "d", "c", null, "r").isComplete(), is(false));
    }

    public void testUnboundedPendingStore() {
        UnboundedPendingStore store = new UnboundedPendingStore();
        assertThat(store.oldestUsageTimestamp().isPresent(), is(false));
        Instant t0 = Instant.parse("2026-01-01T00:00:00Z");
        UsageRecord r1 = record("1", t0);
        UsageRecord r2 = record("2", t0.plusSeconds(300));
        UsageRecord r3 = record("3", t0.plusSeconds(600));
        store.add(List.of(r1, r2, r3));
        assertThat(store.size(), equalTo(3));
        assertThat(store.next(2), equalTo(List.of(r1, r2)));
        assertThat(store.next(10), equalTo(List.of(r1, r2, r3)));
        assertThat(store.oldestUsageTimestamp().get(), equalTo(t0));
        store.acknowledge(Set.of("1"));
        store.drop(Set.of("2"), "invalid");
        assertThat(store.next(10), equalTo(List.of(r3)));
        store.acknowledge(Set.of("3"));
        assertThat(store.next(10), empty());
    }

    public void testNoopImplementationsDoNothing() {
        Instant now = Instant.now();
        assertThat(NoopUsageAccumulator.INSTANCE.drain(now).isEmpty(), is(true));
        assertThat(NoopUsageRecordBuilder.INSTANCE.build(new UsageSnapshot(now, Map.of()), null, null, now), empty());
        assertThat(NoopUsagePublisher.INSTANCE.publish(List.of()).failures(), empty());
        // A non-empty batch is a retryable failure of the whole batch, so that a reporter does not ask for it again in a loop.
        PublishResult result = NoopUsagePublisher.INSTANCE.publish(List.of(record("1", now)));
        assertThat(result.acceptedIds(), empty());
        assertThat(result.failures().size(), equalTo(1));
        assertThat(result.failures().get(0).failureClass(), equalTo(PublishResult.FailureClass.RETRYABLE));
        assertThat(result.failures().get(0).recordIds(), empty());
    }

    private static UsageRecord record(String id, Instant usageTimestamp) {
        return new UsageRecord(
            id,
            usageTimestamp,
            usageTimestamp,
            UsageTypes.EXTERNAL_SOURCES_CPU,
            1,
            300,
            Map.of(),
            null,
            new SourceIdentity("n", "d", "c", "aws", "r")
        );
    }
}
