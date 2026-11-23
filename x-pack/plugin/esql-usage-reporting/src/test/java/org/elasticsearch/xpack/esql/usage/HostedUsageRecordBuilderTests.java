/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.usage;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.LogEvent;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.MockLog;
import org.elasticsearch.xpack.core.esql.QueryMetricsListener.Outcome;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class HostedUsageRecordBuilderTests extends ESTestCase {

    static final SourceIdentity IDENTITY = new SourceIdentity("node-1", "deployment-1", "cluster-1", "aws", "eu-west-1");
    static final Instant DRAIN_TIME = Instant.parse("2026-11-23T10:05:00.123Z");
    static final Instant CREATION_TIME = Instant.parse("2026-11-23T10:05:00.456Z");
    static final Duration PERIOD = Duration.ofMinutes(5);

    private final HostedUsageRecordBuilder builder = new HostedUsageRecordBuilder();

    private static CpuComponents execution(long nanos) {
        return new CpuComponents(0, 0, nanos, 0);
    }

    private List<UsageRecord> build(Map<Outcome, CpuComponents> byOutcome) {
        return builder.build(new UsageSnapshot(DRAIN_TIME, byOutcome), IDENTITY, PERIOD, CREATION_TIME);
    }

    public void testBilledRecordFields() {
        List<UsageRecord> records = build(Map.of(Outcome.SUCCESS, new CpuComponents(1_000_000, 2_000_000, 3_000_000_000L, 4_000_000)));
        assertThat(records, hasSize(1));
        UsageRecord record = records.get(0);
        assertThat(record.id(), equalTo("es-node-1_es_esql_external_sources_cpu_2026-11-23T10:05:00.123Z"));
        assertThat(record.usageTimestamp(), equalTo(DRAIN_TIME.minus(PERIOD)));
        assertThat(record.creationTimestamp(), equalTo(CREATION_TIME));
        assertThat(record.usageType(), equalTo(UsageTypes.EXTERNAL_SOURCES_CPU));
        assertThat(record.quantityMillis(), equalTo(3007L));
        assertThat(record.periodSeconds(), equalTo(300L));
        assertThat(record.exemption(), nullValue());
        assertThat(record.source(), equalTo(IDENTITY));
    }

    public void testIdIsStableForEqualInputsAndDiffersAfterAnotherDrainTime() {
        Map<Outcome, CpuComponents> usage = Map.of(Outcome.SUCCESS, execution(5_000_000));
        String id = build(usage).get(0).id();
        assertThat(build(usage).get(0).id(), equalTo(id));
        // the creation time is when the record is built, so a retried record does not depend on it
        UsageSnapshot snapshot = new UsageSnapshot(DRAIN_TIME, usage);
        assertThat(builder.build(snapshot, IDENTITY, PERIOD, CREATION_TIME.plusSeconds(60)).get(0).id(), equalTo(id));

        UsageSnapshot later = new UsageSnapshot(DRAIN_TIME.plusSeconds(300), usage);
        String laterId = builder.build(later, IDENTITY, PERIOD, CREATION_TIME).get(0).id();
        assertThat(laterId, not(equalTo(id)));

        // a different node id, as on a node with a new data path
        SourceIdentity restarted = new SourceIdentity("node-2", "deployment-1", "cluster-1", "aws", "eu-west-1");
        assertThat(builder.build(snapshot, restarted, PERIOD, CREATION_TIME).get(0).id(), not(equalTo(id)));
    }

    public void testTimestampsHaveMillisecondPrecision() {
        Instant drainTime = Instant.parse("2026-11-23T10:05:00Z").plusNanos(7_654_321);
        UsageSnapshot snapshot = new UsageSnapshot(drainTime, Map.of(Outcome.SUCCESS, execution(5_000_000)));
        UsageRecord record = builder.build(snapshot, IDENTITY, PERIOD, CREATION_TIME).get(0);
        assertThat(record.id(), equalTo("es-node-1_es_esql_external_sources_cpu_2026-11-23T10:05:00.007Z"));
        assertThat(record.usageTimestamp(), equalTo(Instant.parse("2026-11-23T10:00:00.007Z")));
        // a whole second still has three fractional digits, so that ids are as long as any other
        snapshot = new UsageSnapshot(Instant.parse("2026-11-23T10:05:00Z"), Map.of(Outcome.SUCCESS, execution(5_000_000)));
        assertThat(
            builder.build(snapshot, IDENTITY, PERIOD, CREATION_TIME).get(0).id(),
            equalTo("es-node-1_es_esql_external_sources_cpu_2026-11-23T10:05:00.000Z")
        );
    }

    public void testUsageTimestampIsThePeriodStartSoThatTheLastPeriodOfADayStaysInThatDay() {
        Instant midnight = Instant.parse("2026-11-24T00:00:00Z");
        UsageSnapshot snapshot = new UsageSnapshot(midnight, Map.of(Outcome.SUCCESS, execution(5_000_000)));
        UsageRecord record = builder.build(snapshot, IDENTITY, PERIOD, CREATION_TIME).get(0);
        assertThat(record.usageTimestamp(), equalTo(Instant.parse("2026-11-23T23:55:00Z")));
        // the id is keyed on the drain time, which is the end of the period
        assertThat(record.id(), equalTo("es-node-1_es_esql_external_sources_cpu_2026-11-24T00:00:00.000Z"));
    }

    public void testPartialWindowIsStampedWithItsOwnStartAndLength() {
        // a final drain a minute after a drain at midnight on the first of the month: nothing of it belongs to the previous month
        Instant drainTime = Instant.parse("2026-12-01T00:01:00Z");
        UsageSnapshot snapshot = new UsageSnapshot(drainTime, Map.of(Outcome.SUCCESS, execution(5_000_000)));
        UsageRecord record = builder.build(snapshot, IDENTITY, Duration.ofMinutes(1), CREATION_TIME).get(0);
        assertThat(record.usageTimestamp(), equalTo(Instant.parse("2026-12-01T00:00:00Z")));
        assertThat(record.periodSeconds(), equalTo(60L));
        assertThat(record.id(), equalTo("es-node-1_es_esql_external_sources_cpu_2026-12-01T00:01:00.000Z"));
    }

    public void testQuantityDropsTheRemainder() {
        assertThat(build(Map.of(Outcome.SUCCESS, execution(1_999_999))).get(0).quantityMillis(), equalTo(1L));
        assertThat(build(Map.of(Outcome.SUCCESS, execution(2_000_000))).get(0).quantityMillis(), equalTo(2L));
        // the remainder is dropped from the sum, not from each component
        assertThat(build(Map.of(Outcome.SUCCESS, new CpuComponents(600_000, 600_000, 0, 0))).get(0).quantityMillis(), equalTo(1L));
    }

    public void testZeroQuantityBuildsNothing() {
        assertThat(build(Map.of()), empty());
        assertThat(build(Map.of(Outcome.SUCCESS, CpuComponents.ZERO)), empty());
        assertThat(build(Map.of(Outcome.SUCCESS, execution(999_999))), empty());
        assertThat(build(Map.of(Outcome.FAILED, execution(999_999), Outcome.CANCELLED, CpuComponents.ZERO)), empty());
    }

    public void testNegativeValuesAreNeverReported() {
        assertThat(build(Map.of(Outcome.SUCCESS, execution(-5_000_000))), empty());
        UsageRecord record = build(Map.of(Outcome.SUCCESS, new CpuComponents(-1_000_000, 0, 5_000_000, 0))).get(0);
        assertThat(record.metadata(), equalTo(Map.of("cpu_nanos", "5000000")));
        // the negative component does not reduce what the others counted
        assertThat(record.quantityMillis(), equalTo(5L));
    }

    public void testFactoryBuildsTheHostedBuilder() {
        UsageRecordBuilder built = UsageComponents.newRecordBuilder(null);
        assertThat(built, instanceOf(HostedUsageRecordBuilder.class));
    }

    public void testMetadataHasOnlyTheNonZeroComponents() {
        UsageRecord record = build(Map.of(Outcome.SUCCESS, new CpuComponents(0, 0, 5_000_000, 0))).get(0);
        assertThat(record.metadata(), equalTo(Map.of("cpu_nanos", "5000000")));

        record = build(Map.of(Outcome.SUCCESS, new CpuComponents(1_000_000, 0, 0, 4_000_000))).get(0);
        assertThat(record.metadata(), equalTo(Map.of("planning_cpu_nanos", "1000000", "read_cpu_nanos", "4000000")));

        record = build(Map.of(Outcome.SUCCESS, new CpuComponents(1_000_000, 2_000_000, 3_000_000, 4_000_000))).get(0);
        assertThat(
            record.metadata(),
            equalTo(
                Map.of(
                    "planning_cpu_nanos",
                    "1000000",
                    "split_discovery_cpu_nanos",
                    "2000000",
                    "cpu_nanos",
                    "3000000",
                    "read_cpu_nanos",
                    "4000000"
                )
            )
        );
    }

    public void testOneRecordPerOutcomeAndExemptUsageIsNeverSummedIntoBilledUsage() {
        List<UsageRecord> records = build(
            Map.of(Outcome.SUCCESS, execution(10_000_000), Outcome.FAILED, execution(20_000_000), Outcome.CANCELLED, execution(30_000_000))
        );
        assertThat(records, hasSize(3));
        UsageRecord billed = records.get(0);
        UsageRecord failed = records.get(1);
        UsageRecord cancelled = records.get(2);

        assertThat(billed.exemption(), nullValue());
        assertThat(billed.quantityMillis(), equalTo(10L));
        assertThat(failed.exemption(), equalTo(new UsageRecord.Exemption("failed")));
        assertThat(failed.quantityMillis(), equalTo(20L));
        assertThat(cancelled.exemption(), equalTo(new UsageRecord.Exemption("cancelled")));
        assertThat(cancelled.quantityMillis(), equalTo(30L));

        assertThat(failed.id(), equalTo(billed.id() + "_failed"));
        assertThat(cancelled.id(), equalTo(billed.id() + "_cancelled"));
    }

    public void testOnlyExemptUsageBuildsOnlyExemptRecords() {
        List<UsageRecord> records = build(Map.of(Outcome.FAILED, execution(20_000_000)));
        assertThat(records, hasSize(1));
        assertThat(records.get(0).exemption(), notNullValue());
    }

    public void testRecordOrderDoesNotDependOnTheMap() {
        Map<Outcome, CpuComponents> usage = Map.of(
            Outcome.CANCELLED,
            execution(30_000_000),
            Outcome.FAILED,
            execution(20_000_000),
            Outcome.SUCCESS,
            execution(10_000_000)
        );
        assertThat(build(usage).stream().map(UsageRecord::quantityMillis).toList(), equalTo(List.of(10L, 20L, 30L)));
    }

    public void testIncompleteIdentityBuildsNothingAndLogsOncePerChange() {
        Map<Outcome, CpuComponents> usage = Map.of(Outcome.SUCCESS, execution(5_000_000));
        UsageSnapshot snapshot = new UsageSnapshot(DRAIN_TIME, usage);
        SourceIdentity noCluster = new SourceIdentity("node-1", "deployment-1", null, "aws", "eu-west-1");
        SourceIdentity noClusterNoRegion = new SourceIdentity("node-1", "deployment-1", " ", "aws", null);

        ErrorCounter first = new ErrorCounter("missing: [cluster id]", 1);
        ErrorCounter second = new ErrorCounter("missing: [cluster id, region]", 1);
        try (MockLog mockLog = MockLog.capture(HostedUsageRecordBuilder.class)) {
            mockLog.addExpectation(first);
            mockLog.addExpectation(second);
            for (int i = 0; i < 3; i++) {
                assertThat(builder.build(snapshot, noCluster, PERIOD, CREATION_TIME), empty());
            }
            for (int i = 0; i < 3; i++) {
                assertThat(builder.build(snapshot, noClusterNoRegion, PERIOD, CREATION_TIME), empty());
            }
            mockLog.assertAllExpectationsMatched();
        }
    }

    public void testRecoveredIdentityBuildsAgainAndALaterProblemIsLoggedAgain() {
        UsageSnapshot snapshot = new UsageSnapshot(DRAIN_TIME, Map.of(Outcome.SUCCESS, execution(5_000_000)));
        SourceIdentity incomplete = new SourceIdentity("node-1", "deployment-1", null, "aws", "eu-west-1");

        ErrorCounter errors = new ErrorCounter("missing: [cluster id]", 2);
        try (MockLog mockLog = MockLog.capture(HostedUsageRecordBuilder.class)) {
            mockLog.addExpectation(errors);
            assertThat(builder.build(snapshot, incomplete, PERIOD, CREATION_TIME), empty());
            assertThat(builder.build(snapshot, IDENTITY, PERIOD, CREATION_TIME), hasSize(1));
            assertThat(builder.build(snapshot, incomplete, PERIOD, CREATION_TIME), empty());
            mockLog.assertAllExpectationsMatched();
        }
    }

    /** Counts the error events whose message contains a text: {@link MockLog} can say that an event was seen, not how often. */
    private static class ErrorCounter implements MockLog.LoggingExpectation {
        final AtomicInteger count = new AtomicInteger();
        private final String text;
        private final int expected;

        ErrorCounter(String text, int expected) {
            this.text = text;
            this.expected = expected;
        }

        @Override
        public void match(LogEvent event) {
            if (event.getLevel() == Level.ERROR && event.getMessage().getFormattedMessage().contains(text)) {
                count.incrementAndGet();
            }
        }

        @Override
        public void assertMatched() {
            assertThat("errors containing [" + text + "]", count.get(), equalTo(expected));
        }
    }
}
