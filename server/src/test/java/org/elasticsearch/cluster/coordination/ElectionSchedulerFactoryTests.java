/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.coordination;

import org.apache.logging.log4j.Level;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.Settings.Builder;
import org.elasticsearch.common.util.concurrent.DeterministicTaskQueue;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.MockLog;
import org.elasticsearch.test.junit.annotations.TestLogging;

import java.util.concurrent.atomic.AtomicBoolean;

import static org.elasticsearch.cluster.coordination.ElectionSchedulerFactory.ELECTION_BACK_OFF_TIME_SETTING;
import static org.elasticsearch.cluster.coordination.ElectionSchedulerFactory.ELECTION_DURATION_SETTING;
import static org.elasticsearch.cluster.coordination.ElectionSchedulerFactory.ELECTION_INITIAL_TIMEOUT_SETTING;
import static org.elasticsearch.cluster.coordination.ElectionSchedulerFactory.ELECTION_MAX_TIMEOUT_SETTING;
import static org.elasticsearch.cluster.coordination.ElectionSchedulerFactory.toPositiveLongAtMost;
import static org.elasticsearch.node.Node.NODE_NAME_SETTING;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;

public class ElectionSchedulerFactoryTests extends ESTestCase {

    private TimeValue randomGracePeriod() {
        return TimeValue.timeValueMillis(randomLongBetween(0, 10000));
    }

    private void assertElectionSchedule(
        final DeterministicTaskQueue deterministicTaskQueue,
        final ElectionSchedulerFactory electionSchedulerFactory,
        final long initialTimeout,
        final long backOffTime,
        final long maxTimeout,
        final long duration
    ) {
        final TimeValue initialGracePeriod = randomGracePeriod();
        final AtomicBoolean electionStarted = new AtomicBoolean();

        try (
            var mockLog = MockLog.capture(ElectionSchedulerFactory.class);
            var ignored1 = electionSchedulerFactory.startElectionScheduler(
                initialGracePeriod,
                () -> assertTrue(electionStarted.compareAndSet(false, true))
            )
        ) {

            mockLog.addExpectation(
                new MockLog.UnseenEventExpectation(
                    "no zero retries message",
                    ElectionSchedulerFactory.class.getName(),
                    Level.INFO,
                    "retrying master election after [0] failed attempts"
                )
            );
            for (int i : new int[] { 10, 20, 990 }) {
                // the test may stop after 1000 attempts, so might not report the 1000th failure; it definitely reports the 990th tho.
                mockLog.addExpectation(
                    new MockLog.SeenEventExpectation(
                        i + " retries message",
                        ElectionSchedulerFactory.class.getName(),
                        Level.INFO,
                        "retrying master election after [" + i + "] failed attempts"
                    )
                );
            }

            long lastElectionFinishTime = deterministicTaskQueue.getCurrentTimeMillis();
            int electionCount = 0;

            while (true) {
                electionCount++;

                while (electionStarted.get() == false) {
                    if (deterministicTaskQueue.hasRunnableTasks() == false) {
                        deterministicTaskQueue.advanceTime();
                    }
                    deterministicTaskQueue.runAllRunnableTasks();
                }
                assertTrue(electionStarted.compareAndSet(true, false));

                final long thisElectionStartTime = deterministicTaskQueue.getCurrentTimeMillis();

                if (electionCount == 1) {
                    final long electionDelay = thisElectionStartTime - lastElectionFinishTime;

                    // Check grace period
                    assertThat(electionDelay, greaterThanOrEqualTo(initialGracePeriod.millis()));

                    // Check upper bound
                    assertThat(electionDelay, lessThanOrEqualTo(initialTimeout + initialGracePeriod.millis()));
                    assertThat(electionDelay, lessThanOrEqualTo(maxTimeout + initialGracePeriod.millis()));

                } else {

                    final long electionDelay = thisElectionStartTime - lastElectionFinishTime;

                    // Check upper bound
                    assertThat(electionDelay, lessThanOrEqualTo(initialTimeout + backOffTime * (electionCount - 1)));
                    assertThat(electionDelay, lessThanOrEqualTo(maxTimeout));

                    // Run until we get a delay close to the maximum to show that backing off does work
                    if (electionCount >= 1000) {
                        if (electionDelay >= maxTimeout * 0.99) {
                            break;
                        }
                    }
                }

                lastElectionFinishTime = thisElectionStartTime + duration;
            }

            mockLog.assertAllExpectationsMatched();
        }
        deterministicTaskQueue.runAllTasks();
        assertFalse(electionStarted.get());
    }

    @TestLogging(reason = "testing logging at INFO level", value = "org.elasticsearch.cluster.coordination.ElectionSchedulerFactory:INFO")
    public void testRetriesOnCorrectSchedule() {
        final Builder settingsBuilder = Settings.builder();

        final long initialTimeout;
        if (randomBoolean()) {
            initialTimeout = randomLongBetween(1, 10000);
            settingsBuilder.put(ELECTION_INITIAL_TIMEOUT_SETTING.getKey(), initialTimeout + "ms");
        } else {
            initialTimeout = ELECTION_INITIAL_TIMEOUT_SETTING.get(Settings.EMPTY).millis();
        }

        final long backOffTime;
        if (randomBoolean()) {
            backOffTime = randomLongBetween(1, 60000);
            settingsBuilder.put(ELECTION_BACK_OFF_TIME_SETTING.getKey(), backOffTime + "ms");
        } else {
            backOffTime = ELECTION_BACK_OFF_TIME_SETTING.get(Settings.EMPTY).millis();
        }

        final long maxTimeout;
        if (ELECTION_MAX_TIMEOUT_SETTING.get(Settings.EMPTY).millis() < initialTimeout || randomBoolean()) {
            maxTimeout = randomLongBetween(Math.max(200, initialTimeout), 180000);
            settingsBuilder.put(ELECTION_MAX_TIMEOUT_SETTING.getKey(), maxTimeout + "ms");
        } else {
            maxTimeout = ELECTION_MAX_TIMEOUT_SETTING.get(Settings.EMPTY).millis();
        }

        final long duration;
        if (randomBoolean()) {
            duration = randomLongBetween(1, 300000);
            settingsBuilder.put(ELECTION_DURATION_SETTING.getKey(), duration + "ms");
        } else {
            duration = ELECTION_DURATION_SETTING.get(Settings.EMPTY).millis();
        }

        final DeterministicTaskQueue deterministicTaskQueue = new DeterministicTaskQueue();
        final ElectionSchedulerFactory electionSchedulerFactory = new ElectionSchedulerFactory(
            settingsBuilder.put(NODE_NAME_SETTING.getKey(), "node").build(),
            random(),
            deterministicTaskQueue.getThreadPool()
        );

        assertElectionSchedule(deterministicTaskQueue, electionSchedulerFactory, initialTimeout, backOffTime, maxTimeout, duration);

        // do it again to show that the max is reset when the scheduler is restarted
        assertElectionSchedule(deterministicTaskQueue, electionSchedulerFactory, initialTimeout, backOffTime, maxTimeout, duration);
    }

    public void testSettingsValidation() {
        {
            final Settings settings = Settings.builder().put(ELECTION_INITIAL_TIMEOUT_SETTING.getKey(), "0s").build();
            IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> ELECTION_INITIAL_TIMEOUT_SETTING.get(settings));
            assertThat(e.getMessage(), is("failed to parse value [0s] for setting [cluster.election.initial_timeout], must be >= [1ms]"));
        }

        {
            final Settings settings = Settings.builder().put(ELECTION_INITIAL_TIMEOUT_SETTING.getKey(), "10001ms").build();
            IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> ELECTION_INITIAL_TIMEOUT_SETTING.get(settings));
            assertThat(
                e.getMessage(),
                is("failed to parse value [10001ms] for setting [cluster.election.initial_timeout], must be <= [10s]")
            );
        }

        {
            final Settings settings = Settings.builder().put(ELECTION_BACK_OFF_TIME_SETTING.getKey(), "0s").build();
            IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> ELECTION_BACK_OFF_TIME_SETTING.get(settings));
            assertThat(e.getMessage(), is("failed to parse value [0s] for setting [cluster.election.back_off_time], must be >= [1ms]"));
        }

        {
            final Settings settings = Settings.builder().put(ELECTION_BACK_OFF_TIME_SETTING.getKey(), "60001ms").build();
            IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> ELECTION_BACK_OFF_TIME_SETTING.get(settings));
            assertThat(
                e.getMessage(),
                is("failed to parse value [60001ms] for setting [cluster.election.back_off_time], must be <= [60s]")
            );
        }

        {
            final Settings settings = Settings.builder().put(ELECTION_MAX_TIMEOUT_SETTING.getKey(), "199ms").build();
            IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> ELECTION_MAX_TIMEOUT_SETTING.get(settings));
            assertThat(e.getMessage(), is("failed to parse value [199ms] for setting [cluster.election.max_timeout], must be >= [200ms]"));
        }

        {
            final Settings settings = Settings.builder().put(ELECTION_MAX_TIMEOUT_SETTING.getKey(), "601s").build();
            IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> ELECTION_MAX_TIMEOUT_SETTING.get(settings));
            assertThat(e.getMessage(), is("failed to parse value [601s] for setting [cluster.election.max_timeout], must be <= [600s]"));
        }

        final var threadPool = new DeterministicTaskQueue().getThreadPool();

        {
            final long initialTimeoutMillis = randomLongBetween(1, 10000);
            final long backOffMillis = randomLongBetween(1, 60000);
            final long maxTimeoutMillis = randomLongBetween(Math.max(200, initialTimeoutMillis), 180000);

            final Settings settings = Settings.builder()
                .put(ELECTION_INITIAL_TIMEOUT_SETTING.getKey(), initialTimeoutMillis + "ms")
                .put(ELECTION_BACK_OFF_TIME_SETTING.getKey(), backOffMillis + "ms")
                .put(ELECTION_MAX_TIMEOUT_SETTING.getKey(), maxTimeoutMillis + "ms")
                .build();

            assertThat(ELECTION_INITIAL_TIMEOUT_SETTING.get(settings), is(TimeValue.timeValueMillis(initialTimeoutMillis)));
            assertThat(ELECTION_BACK_OFF_TIME_SETTING.get(settings), is(TimeValue.timeValueMillis(backOffMillis)));
            assertThat(ELECTION_MAX_TIMEOUT_SETTING.get(settings), is(TimeValue.timeValueMillis(maxTimeoutMillis)));

            assertThat(new ElectionSchedulerFactory(settings, random(), threadPool), not(nullValue())); // doesn't throw an IAE
        }

        {
            final long initialTimeoutMillis = randomLongBetween(201, 10000);
            final long maxTimeoutMillis = randomLongBetween(200, initialTimeoutMillis - 1);

            final Settings settings = Settings.builder()
                .put(ELECTION_INITIAL_TIMEOUT_SETTING.getKey(), initialTimeoutMillis + "ms")
                .put(ELECTION_MAX_TIMEOUT_SETTING.getKey(), maxTimeoutMillis + "ms")
                .build();

            IllegalArgumentException e = expectThrows(
                IllegalArgumentException.class,
                () -> new ElectionSchedulerFactory(settings, random(), threadPool)
            );
            assertThat(
                e.getMessage(),
                equalTo(
                    "[cluster.election.max_timeout] is ["
                        + TimeValue.timeValueMillis(maxTimeoutMillis)
                        + "], but must be at least [cluster.election.initial_timeout] which is ["
                        + TimeValue.timeValueMillis(initialTimeoutMillis)
                        + "]"
                )
            );
        }
    }

    public void testRandomPositiveLongLessThan() {
        for (long input : new long[] { 0, 1, -1, Long.MIN_VALUE, Long.MAX_VALUE, randomLong() }) {
            for (long upperBound : new long[] { 1, 2, 3, 100, Long.MAX_VALUE }) {
                long l = toPositiveLongAtMost(input, upperBound);
                assertThat(l, greaterThan(0L));
                assertThat(l, lessThanOrEqualTo(upperBound));
            }
        }
    }
}
