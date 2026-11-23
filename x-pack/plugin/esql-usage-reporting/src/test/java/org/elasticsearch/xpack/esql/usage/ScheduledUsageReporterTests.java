/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.usage;

import org.apache.logging.log4j.Level;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.DeterministicTaskQueue;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.MockLog;
import org.elasticsearch.xpack.core.esql.QueryMetricsListener.Outcome;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;

/**
 * Drives {@link ScheduledUsageReporter} with the real {@link UnboundedPendingStore}, a clock backed by a
 * {@link DeterministicTaskQueue}, and small scripted implementations of the accumulator, the builder and the publisher: the real
 * ones are other pieces of the plugin that this class must not depend on.
 */
public class ScheduledUsageReporterTests extends ESTestCase {

    private static final Instant START = Instant.parse("2026-11-23T10:03:20Z");
    private static final SourceIdentity IDENTITY = new SourceIdentity("node", "deployment", "cluster", "aws", "eu-west-1");

    private final DeterministicTaskQueue taskQueue = new DeterministicTaskQueue();
    private final Clock clock = new Clock() {
        @Override
        public ZoneOffset getZone() {
            return ZoneOffset.UTC;
        }

        @Override
        public Clock withZone(ZoneId zone) {
            return this;
        }

        @Override
        public Instant instant() {
            return START.plusMillis(taskQueue.getCurrentTimeMillis() - clockLagMillis);
        }
    };

    /** How far the clock is behind the timers, to make a timer fire before its boundary as seen by the clock. */
    private long clockLagMillis;

    /** Returns a snapshot with usage, or an empty one, and records the time of every drain. */
    private static class ScriptedAccumulator implements UsageAccumulator {
        final List<Instant> drains = new ArrayList<>();
        boolean hasUsage = true;
        /** When set, returned by the next drain instead of the usual snapshot. */
        Map<Outcome, CpuComponents> nextUsage;
        RuntimeException failure;

        @Override
        public void record(Outcome outcome, Map<String, Long> metrics) {
            throw new AssertionError("the reporter never records");
        }

        @Override
        public UsageSnapshot drain(Instant drainTime) {
            drains.add(drainTime);
            if (failure != null) {
                throw failure;
            }
            Map<Outcome, CpuComponents> usage = hasUsage ? Map.of(Outcome.SUCCESS, new CpuComponents(0, 0, 2_000_000, 0)) : Map.of();
            if (nextUsage != null) {
                usage = nextUsage;
                nextUsage = null;
            }
            return new UsageSnapshot(drainTime, usage);
        }
    }

    /**
     * Builds {@link #recordsPerSnapshot} records per snapshot with usage, with ids that depend only on the drain time, like the real
     * builder, which is also the one that decides that a snapshot has no usage. Records the period of every call.
     */
    private static class ScriptedBuilder implements UsageRecordBuilder {
        final List<Duration> periods = new ArrayList<>();
        int recordsPerSnapshot = 1;
        RuntimeException failure;

        @Override
        public List<UsageRecord> build(UsageSnapshot snapshot, SourceIdentity identity, Duration period, Instant creationTime) {
            periods.add(period);
            if (failure != null) {
                throw failure;
            }
            List<UsageRecord> records = new ArrayList<>();
            if (snapshot.byOutcome().isEmpty()) {
                return records;
            }
            for (int i = 0; i < recordsPerSnapshot; i++) {
                records.add(
                    new UsageRecord(
                        "es-" + identity.nodeId() + "_" + snapshot.drainTime() + "_" + i,
                        snapshot.drainTime(),
                        creationTime,
                        UsageTypes.EXTERNAL_SOURCES_CPU,
                        2,
                        period.toSeconds(),
                        Map.of(),
                        null,
                        identity
                    )
                );
            }
            return records;
        }
    }

    /** Answers every batch with the next function of the script, or with an acceptance of the whole batch when there is none. */
    private static class ScriptedPublisher implements UsagePublisher {
        final List<List<String>> batches = new ArrayList<>();
        final List<Function<List<UsageRecord>, PublishResult>> script = new ArrayList<>();
        RuntimeException failure;
        boolean closed;

        @Override
        public PublishResult publish(List<UsageRecord> records) {
            batches.add(records.stream().map(UsageRecord::id).toList());
            if (failure != null) {
                throw failure;
            }
            if (script.isEmpty() == false) {
                return script.remove(0).apply(records);
            }
            return accepted(records);
        }

        @Override
        public void close() {
            closed = true;
        }
    }

    private final ScriptedAccumulator accumulator = new ScriptedAccumulator();
    private final ScriptedBuilder builder = new ScriptedBuilder();
    private final ScriptedPublisher publisher = new ScriptedPublisher();
    private final UnboundedPendingStore store = new UnboundedPendingStore();

    private ScheduledUsageReporter reporter(int batchSize) {
        return reporter(batchSize, "1s");
    }

    private ScheduledUsageReporter reporter(int batchSize, String requestTimeout) {
        Settings settings = Settings.builder()
            .put(UsageReportingSettings.PERIOD.getKey(), "5m")
            .put(UsageReportingSettings.BATCH_SIZE.getKey(), batchSize)
            .put(UsageReportingSettings.REQUEST_TIMEOUT.getKey(), requestTimeout)
            .build();
        return new ScheduledUsageReporter(
            UsageReportingSettings.of(settings),
            accumulator,
            builder,
            store,
            publisher,
            IDENTITY,
            taskQueue.getThreadPool(),
            clock
        );
    }

    /** Puts records in the store as a previous cycle that could not publish them would have. */
    private void addPending(int count) {
        builder.recordsPerSnapshot = count;
        Instant time = at("2026-11-23T10:00:00Z");
        UsageSnapshot snapshot = new UsageSnapshot(time, Map.of(Outcome.SUCCESS, new CpuComponents(0, 0, 2_000_000, 0)));
        store.add(builder.build(snapshot, IDENTITY, Duration.ofMinutes(5), time));
    }

    private static PublishResult accepted(List<UsageRecord> records) {
        return new PublishResult(Set.copyOf(records.stream().map(UsageRecord::id).toList()), List.of());
    }

    private static PublishResult failed(PublishResult.FailureClass failureClass, Set<String> ids) {
        return new PublishResult(Set.of(), List.of(new PublishResult.Failure(ids, failureClass, "failed")));
    }

    private void runUntil(String instant) {
        taskQueue.runTasksUpToTimeInOrder(Instant.parse(instant).toEpochMilli() - START.toEpochMilli());
    }

    private static Instant at(String instant) {
        return Instant.parse(instant);
    }

    public void testNextBoundaryIsStrictlyAfter() {
        Duration period = Duration.ofMinutes(5);
        assertThat(ScheduledUsageReporter.nextBoundary(at("2026-11-23T10:03:20Z"), period), equalTo(at("2026-11-23T10:05:00Z")));
        assertThat(ScheduledUsageReporter.nextBoundary(at("2026-11-23T10:05:00Z"), period), equalTo(at("2026-11-23T10:10:00Z")));
        assertThat(ScheduledUsageReporter.nextBoundary(at("2026-11-23T10:04:59.999Z"), period), equalTo(at("2026-11-23T10:05:00Z")));
        assertThat(ScheduledUsageReporter.nextBoundary(at("2026-11-23T10:59:00Z"), period), equalTo(at("2026-11-23T11:00:00Z")));
    }

    public void testTicksAreAlignedToTheBoundariesOfThePeriod() {
        ScheduledUsageReporter reporter = reporter(10);
        reporter.start();

        runUntil("2026-11-23T10:04:59Z");
        assertThat(accumulator.drains, empty());

        runUntil("2026-11-23T10:20:00Z");
        assertThat(
            accumulator.drains,
            contains(at("2026-11-23T10:05:00Z"), at("2026-11-23T10:10:00Z"), at("2026-11-23T10:15:00Z"), at("2026-11-23T10:20:00Z"))
        );
        assertThat(reporter.recordsSent(), equalTo(4L));
        assertThat(reporter.recordsPending(), equalTo(0));
    }

    public void testThePeriodOfTheRecordsIsTheWindowSinceThePreviousDrain() {
        ScheduledUsageReporter reporter = reporter(10); // created at 10:03:20, when the accumulator starts

        reporter.tick(at("2026-11-23T10:05:00Z"));
        reporter.tick(at("2026-11-23T10:10:00Z"));
        reporter.tick(at("2026-11-23T10:25:00Z")); // the cycles of 10:15 and 10:20 did not run

        assertThat(builder.periods, contains(Duration.ofSeconds(100), Duration.ofMinutes(5), Duration.ofMinutes(15)));
    }

    public void testDrainsAtTheSameInstantOrEarlierGetLaterDrainTimesAndDistinctIds() {
        ScheduledUsageReporter reporter = reporter(10);

        reporter.tick(at("2026-11-23T10:05:00Z"));
        reporter.tick(at("2026-11-23T10:05:00Z")); // the same instant
        reporter.tick(at("2026-11-23T10:04:00Z")); // the clock went back

        assertThat(
            accumulator.drains,
            contains(at("2026-11-23T10:05:00Z"), at("2026-11-23T10:05:00.001Z"), at("2026-11-23T10:05:00.002Z"))
        );
        assertThat(publisher.batches.stream().flatMap(List::stream).distinct().count(), equalTo(3L));
        assertThat(reporter.recordsSent(), equalTo(3L));
        assertThat(builder.periods, contains(Duration.ofSeconds(100), Duration.ofMillis(1), Duration.ofMillis(1)));
    }

    public void testTheWindowDoesNotMoveWhenTheDrainFails() {
        ScheduledUsageReporter reporter = reporter(10);
        accumulator.failure = new IllegalStateException("boom");
        reporter.tick(at("2026-11-23T10:05:00Z"));
        accumulator.failure = null;

        reporter.tick(at("2026-11-23T10:10:00Z"));

        // The failed drain did not reset the sums, so the next one covers both windows.
        assertThat(builder.periods, contains(Duration.ofMinutes(6).plusSeconds(40)));
    }

    public void testTheWindowMovesWhenTheBuilderFails() {
        ScheduledUsageReporter reporter = reporter(10);
        builder.failure = new IllegalStateException("boom");
        reporter.tick(at("2026-11-23T10:05:00Z"));
        builder.failure = null;
        builder.periods.clear();

        reporter.tick(at("2026-11-23T10:10:00Z"));

        // The usage of the failed window was drained and is lost, so the next record must not claim it.
        assertThat(builder.periods, contains(Duration.ofMinutes(5)));
    }

    public void testTheBuilderDecidesWhetherASnapshotHasUsage() {
        ScheduledUsageReporter reporter = reporter(10);
        // The components add up to zero, so the snapshot says it is empty, but a builder that ignores negative components has usage.
        accumulator.nextUsage = Map.of(Outcome.SUCCESS, new CpuComponents(-2_000_000, 0, 2_000_000, 0));

        reporter.tick(at("2026-11-23T10:05:00Z"));

        assertThat(builder.periods.size(), equalTo(1));
        assertThat(reporter.recordsSent(), equalTo(1L));
    }

    public void testAPeriodWithoutUsagePublishesNothing() {
        accumulator.hasUsage = false;
        ScheduledUsageReporter reporter = reporter(10);

        reporter.tick(at("2026-11-23T10:05:00Z"));

        assertThat(accumulator.drains, contains(at("2026-11-23T10:05:00Z")));
        assertThat(publisher.batches, empty());
        assertThat(reporter.recordsSent(), equalTo(0L));
    }

    public void testRecordsArePublishedInBatches() {
        builder.recordsPerSnapshot = 5;
        ScheduledUsageReporter reporter = reporter(2);

        reporter.tick(at("2026-11-23T10:05:00Z"));

        assertThat(publisher.batches.stream().map(List::size).toList(), contains(2, 2, 1));
        assertThat(reporter.recordsSent(), equalTo(5L));
        assertThat(store.size(), equalTo(0));
    }

    public void testOnlyAcceptedRecordsAreAcknowledged() {
        builder.recordsPerSnapshot = 3;
        ScheduledUsageReporter reporter = reporter(10);
        publisher.script.add(records -> {
            Set<String> accepted = Set.of(records.get(0).id());
            Set<String> retry = Set.of(records.get(1).id(), records.get(2).id());
            return new PublishResult(
                accepted,
                List.of(new PublishResult.Failure(retry, PublishResult.FailureClass.RETRYABLE, "unavailable"))
            );
        });

        reporter.tick(at("2026-11-23T10:05:00Z"));

        assertThat(reporter.recordsSent(), equalTo(1L));
        assertThat(
            store.next(10).stream().map(UsageRecord::id).toList(),
            contains("es-node_2026-11-23T10:05:00Z_1", "es-node_2026-11-23T10:05:00Z_2")
        );
    }

    public void testInvalidRecordsAreDroppedAndTheCycleContinues() {
        builder.recordsPerSnapshot = 3;
        ScheduledUsageReporter reporter = reporter(2);
        publisher.script.add(records -> failed(PublishResult.FailureClass.INVALID, Set.of(records.get(0).id())));

        reporter.tick(at("2026-11-23T10:05:00Z"));

        // The first batch [0, 1] loses record 0 for good; record 1 was not answered, so it is in the second batch [1, 2].
        assertThat(publisher.batches.size(), equalTo(2));
        assertThat(store.size(), equalTo(0));
        assertThat(reporter.recordsSent(), equalTo(2L));
    }

    public void testInvalidWithoutIdsDropsTheWholeBatch() {
        builder.recordsPerSnapshot = 2;
        ScheduledUsageReporter reporter = reporter(10);
        publisher.script.add(records -> failed(PublishResult.FailureClass.INVALID, Set.of()));

        reporter.tick(at("2026-11-23T10:05:00Z"));

        assertThat(publisher.batches.size(), equalTo(1));
        assertThat(store.size(), equalTo(0));
        assertThat(reporter.recordsSent(), equalTo(0L));
    }

    public void testRetryableAndAuthFailuresStayPendingAndStopTheCycle() {
        for (PublishResult.FailureClass failureClass : List.of(PublishResult.FailureClass.RETRYABLE, PublishResult.FailureClass.AUTH)) {
            UnboundedPendingStore pending = new UnboundedPendingStore();
            ScriptedPublisher failing = new ScriptedPublisher();
            // The first record is accepted, so the batch makes progress: only the failure can stop the cycle.
            failing.script.add(
                records -> new PublishResult(
                    Set.of(records.get(0).id()),
                    List.of(new PublishResult.Failure(Set.of(), failureClass, "failed"))
                )
            );
            builder.recordsPerSnapshot = 3;
            ScheduledUsageReporter reporter = new ScheduledUsageReporter(
                UsageReportingSettings.of(Settings.builder().put(UsageReportingSettings.BATCH_SIZE.getKey(), 1).build()),
                accumulator,
                builder,
                pending,
                failing,
                IDENTITY,
                taskQueue.getThreadPool(),
                clock
            );

            reporter.tick(at("2026-11-23T10:05:00Z"));

            assertThat(failureClass.name(), failing.batches.size(), equalTo(1));
            assertThat(failureClass.name(), pending.size(), equalTo(2));
        }
    }

    public void testPendingRecordsAreSentAgainWithTheSameIdsOnTheNextTick() {
        ScheduledUsageReporter reporter = reporter(10);
        publisher.script.add(records -> failed(PublishResult.FailureClass.RETRYABLE, Set.of()));

        reporter.tick(at("2026-11-23T10:05:00Z"));
        reporter.tick(at("2026-11-23T10:10:00Z"));

        assertThat(publisher.batches.get(0), contains("es-node_2026-11-23T10:05:00Z_0"));
        assertThat(publisher.batches.get(1), contains("es-node_2026-11-23T10:05:00Z_0", "es-node_2026-11-23T10:10:00Z_0"));
        assertThat(store.size(), equalTo(0));
        assertThat(reporter.recordsSent(), equalTo(2L));
    }

    public void testAPublisherThatAcceptsNothingDoesNotLoop() {
        builder.recordsPerSnapshot = 3;
        ScheduledUsageReporter reporter = reporter(1);
        publisher.script.add(records -> PublishResult.empty());

        reporter.tick(at("2026-11-23T10:05:00Z"));

        assertThat(publisher.batches.size(), equalTo(1));
        assertThat(store.size(), equalTo(3));
    }

    public void testAFailingBuilderDoesNotStopTheScheduleNorThePublicationOfPendingRecords() {
        ScheduledUsageReporter reporter = reporter(10);
        publisher.script.add(records -> failed(PublishResult.FailureClass.RETRYABLE, Set.of()));
        reporter.start();
        runUntil("2026-11-23T10:05:00Z");
        assertThat(store.size(), equalTo(1));

        builder.failure = new IllegalStateException("boom");
        try (MockLog mockLog = MockLog.capture(ScheduledUsageReporter.class)) {
            mockLog.addExpectation(
                new MockLog.SeenEventExpectation(
                    "build failure",
                    ScheduledUsageReporter.class.getCanonicalName(),
                    Level.WARN,
                    "failed to build the usage records of the period ending at [2026-11-23T10:10:00Z]"
                )
            );
            runUntil("2026-11-23T10:10:00Z");
            mockLog.assertAllExpectationsMatched();
        }
        // The record that was pending is published although the builder failed in the same cycle.
        assertThat(store.size(), equalTo(0));

        builder.failure = null;
        runUntil("2026-11-23T10:15:00Z");
        assertThat(accumulator.drains.size(), equalTo(3));
        assertThat(reporter.recordsSent(), equalTo(2L));
    }

    public void testAFailingPublisherOrAccumulatorDoesNotStopTheSchedule() {
        ScheduledUsageReporter reporter = reporter(10);
        reporter.start();

        publisher.failure = new IllegalStateException("publisher boom");
        runUntil("2026-11-23T10:05:00Z");
        assertThat(store.size(), equalTo(1));

        publisher.failure = null;
        accumulator.failure = new IllegalStateException("accumulator boom");
        runUntil("2026-11-23T10:10:00Z");
        assertThat("the pending record is still published", store.size(), equalTo(0));

        accumulator.failure = null;
        runUntil("2026-11-23T10:15:00Z");
        assertThat(accumulator.drains.size(), equalTo(3));
        assertThat(reporter.recordsSent(), equalTo(2L));
    }

    public void testEveryBoundaryIsTickedExactlyOnce() {
        ScheduledUsageReporter reporter = reporter(10);
        reporter.start();

        runUntil("2026-11-23T10:30:00Z");

        assertThat(accumulator.drains.size(), equalTo(6));
        assertThat(accumulator.drains.stream().distinct().count(), equalTo(6L));
    }

    public void testACycleThatIsStillRunningIsNotStartedAgain() {
        ScheduledUsageReporter reporter = reporter(10);
        publisher.script.add(records -> {
            reporter.tick(at("2026-11-23T10:10:00Z"));
            return accepted(records);
        });

        reporter.tick(at("2026-11-23T10:05:00Z"));

        assertThat(reporter.ticksSkipped(), equalTo(1L));
        assertThat(accumulator.drains, contains(at("2026-11-23T10:05:00Z")));
    }

    public void testCloseFlushesWhatIsPendingWithoutDrainingThenStopsTheSchedule() {
        ScheduledUsageReporter reporter = reporter(10);
        reporter.start();
        runUntil("2026-11-23T10:05:00Z");
        assertThat(accumulator.drains.size(), equalTo(1));
        publisher.script.add(records -> failed(PublishResult.FailureClass.RETRYABLE, Set.of()));
        runUntil("2026-11-23T10:10:00Z");
        assertThat(store.size(), equalTo(1));

        try (MockLog mockLog = MockLog.capture(ScheduledUsageReporter.class)) {
            mockLog.addExpectation(
                new MockLog.UnseenEventExpectation("no warning", ScheduledUsageReporter.class.getCanonicalName(), Level.WARN, "*")
            );
            reporter.close();
            mockLog.assertAllExpectationsMatched();
        }

        assertThat("the pending record is flushed", store.size(), equalTo(0));
        assertThat("the usage of the partial period is not drained", accumulator.drains.size(), equalTo(2));

        reporter.close(); // idempotent
        runUntil("2026-11-23T10:30:00Z");
        assertThat(accumulator.drains.size(), equalTo(2));
        assertThat(publisher.closed, equalTo(false));
    }

    public void testATimerThatFiresBeforeItsBoundaryDoesNotRepeatTheBoundary() {
        ScheduledUsageReporter reporter = reporter(10);
        reporter.start();
        clockLagMillis = 1; // the first timer fires 1 ms before its boundary as the clock sees it

        // One second more than the last boundary: the timers that follow the early one run 1 ms late. Without the guard in run(),
        // the early timer would be followed by another one for the same boundary, and the list below would have a repeated time.
        runUntil("2026-11-23T10:20:01Z");

        assertThat(
            accumulator.drains,
            contains(at("2026-11-23T10:05:00Z"), at("2026-11-23T10:10:00Z"), at("2026-11-23T10:15:00Z"), at("2026-11-23T10:20:00Z"))
        );
    }

    public void testAcceptedIdsOutsideTheBatchAreIgnored() {
        builder.recordsPerSnapshot = 3;
        ScheduledUsageReporter reporter = reporter(1);
        publisher.script.add(records -> new PublishResult(Set.of(records.get(0).id(), "es-node_2026-11-23T10:05:00Z_2"), List.of()));
        publisher.script.add(records -> failed(PublishResult.FailureClass.RETRYABLE, Set.of()));

        reporter.tick(at("2026-11-23T10:05:00Z"));

        assertThat(reporter.recordsSent(), equalTo(1L));
        assertThat(
            store.next(10).stream().map(UsageRecord::id).toList(),
            contains("es-node_2026-11-23T10:05:00Z_1", "es-node_2026-11-23T10:05:00Z_2")
        );
    }

    /**
     * The deadline is measured with the clock of the thread pool, which here is the one of the task queue: the first publish moves it
     * past the timeout, so that no real time is involved.
     */
    public void testCloseStopsStartingBatchesAfterTheTimeout() {
        ScheduledUsageReporter reporter = reporter(1, "1s");
        addPending(3);
        publisher.script.add(records -> {
            taskQueue.scheduleAt(taskQueue.getCurrentTimeMillis() + 2000, () -> {});
            taskQueue.advanceTime();
            return accepted(records);
        });

        try (MockLog mockLog = MockLog.capture(ScheduledUsageReporter.class)) {
            mockLog.addExpectation(
                new MockLog.SeenEventExpectation(
                    "usage may be lost",
                    ScheduledUsageReporter.class.getCanonicalName(),
                    Level.WARN,
                    "usage may be lost: [2] records are still pending after the final flush"
                )
            );
            reporter.close();
            mockLog.assertAllExpectationsMatched();
        }
        assertThat(publisher.batches.size(), equalTo(1));
    }

    public void testStartingExactlyOnABoundaryWaitsOneFullPeriod() {
        // Moves the time to 10:05:00 with a task that does nothing: an empty queue does not advance by itself.
        taskQueue.scheduleAt(Duration.ofSeconds(100).toMillis(), () -> {});
        runUntil("2026-11-23T10:05:00Z");
        assertThat(clock.instant(), equalTo(at("2026-11-23T10:05:00Z")));
        ScheduledUsageReporter reporter = reporter(10);
        reporter.start();

        runUntil("2026-11-23T10:09:59Z");
        assertThat(accumulator.drains, empty());
        runUntil("2026-11-23T10:10:00Z");
        assertThat(accumulator.drains, contains(at("2026-11-23T10:10:00Z")));
    }

    public void testATickAfterCloseDoesNothing() {
        ScheduledUsageReporter reporter = reporter(10);
        reporter.close();
        int drains = accumulator.drains.size();
        publisher.batches.clear();

        reporter.tick(at("2026-11-23T10:05:00Z"));

        assertThat(accumulator.drains.size(), equalTo(drains));
        assertThat(publisher.batches, empty());
    }

    public void testAuthFailuresAreLoggedAsWarnings() {
        ScheduledUsageReporter reporter = reporter(10);
        publisher.script.add(records -> failed(PublishResult.FailureClass.AUTH, Set.of()));

        try (MockLog mockLog = MockLog.capture(ScheduledUsageReporter.class)) {
            mockLog.addExpectation(
                new MockLog.SeenEventExpectation(
                    "auth",
                    ScheduledUsageReporter.class.getCanonicalName(),
                    Level.WARN,
                    "the Usage API refused the usage records, which stay pending: failed"
                )
            );
            reporter.tick(at("2026-11-23T10:05:00Z"));
            mockLog.assertAllExpectationsMatched();
        }
    }

    public void testInvalidIdsOfRecordsThatWereNotSentAreIgnored() {
        builder.recordsPerSnapshot = 3;
        ScheduledUsageReporter reporter = reporter(1);
        publisher.script.add(
            records -> failed(PublishResult.FailureClass.INVALID, Set.of(records.get(0).id(), "es-node_2026-11-23T10:05:00Z_2"))
        );
        publisher.script.add(records -> failed(PublishResult.FailureClass.RETRYABLE, Set.of()));

        reporter.tick(at("2026-11-23T10:05:00Z"));

        // Record 0 is dropped; record 2 was not in the batch, so it stays pending together with record 1.
        assertThat(
            store.next(10).stream().map(UsageRecord::id).toList(),
            contains("es-node_2026-11-23T10:05:00Z_1", "es-node_2026-11-23T10:05:00Z_2")
        );
    }

    public void testInvalidIdsOutsideTheBatchDropNothingAndStopTheCycle() {
        builder.recordsPerSnapshot = 3;
        ScheduledUsageReporter reporter = reporter(1);
        publisher.script.add(records -> failed(PublishResult.FailureClass.INVALID, Set.of("es-node_2026-11-23T10:05:00Z_2")));

        reporter.tick(at("2026-11-23T10:05:00Z"));

        assertThat(publisher.batches.size(), equalTo(1));
        assertThat(store.size(), equalTo(3));
    }

    /**
     * The cycle that is running cannot be waited for from the same thread, so a publisher that calls {@code close()} stands for the
     * node stopping while the cycle is stuck in a request.
     */
    public void testCloseWarnsWhenARunningCycleDoesNotFinish() {
        ScheduledUsageReporter reporter = reporter(10, "1ms");
        publisher.script.add(records -> {
            try (MockLog mockLog = MockLog.capture(ScheduledUsageReporter.class)) {
                mockLog.addExpectation(
                    new MockLog.SeenEventExpectation(
                        "flush did not run",
                        ScheduledUsageReporter.class.getCanonicalName(),
                        Level.WARN,
                        "usage may be lost: the cycle that was running did not finish, so the final flush did not run"
                    )
                );
                reporter.close();
                mockLog.assertAllExpectationsMatched();
            }
            return accepted(records);
        });

        reporter.tick(at("2026-11-23T10:05:00Z"));
    }

    /** As above, the node stops while the cycle is in a request: the cycle must not start another batch once the close has begun. */
    public void testACycleStopsStartingBatchesOnceCloseHasBegun() {
        builder.recordsPerSnapshot = 3;
        ScheduledUsageReporter reporter = reporter(1, "1ms");
        publisher.script.add(records -> {
            reporter.close(); // cannot get the permit held by this cycle: it only marks the reporter as closed
            return accepted(records);
        });

        reporter.tick(at("2026-11-23T10:05:00Z"));

        assertThat(publisher.batches.size(), equalTo(1));
        assertThat(store.size(), equalTo(2));
    }

    public void testCloseWarnsWhenTheFinalFlushFails() {
        ScheduledUsageReporter reporter = reporter(10);
        addPending(1);
        publisher.failure = new IllegalStateException("down");

        try (MockLog mockLog = MockLog.capture(ScheduledUsageReporter.class)) {
            mockLog.addExpectation(
                new MockLog.SeenEventExpectation(
                    "usage may be lost",
                    ScheduledUsageReporter.class.getCanonicalName(),
                    Level.WARN,
                    "usage may be lost: [1] records are still pending after the final flush"
                )
            );
            reporter.close();
            mockLog.assertAllExpectationsMatched();
        }
    }
}
