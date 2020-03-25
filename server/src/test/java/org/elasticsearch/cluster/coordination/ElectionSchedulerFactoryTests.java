/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.cluster.coordination;

import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.Settings.Builder;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.test.ESTestCase;

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

    private void assertElectionSchedule(final DeterministicTaskQueue deterministicTaskQueue,
                                        final ElectionSchedulerFactory electionSchedulerFactory,
                                        final long initialTimeout, final long backOffTime, final long maxTimeout, final long duration) {

        final TimeValue initialGracePeriod = randomGracePeriod();
        final AtomicBoolean electionStarted = new AtomicBoolean();

        try (Releasable ignored = electionSchedulerFactory.startElectionScheduler(initialGracePeriod,
            () -> assertTrue(electionStarted.compareAndSet(false, true)))) {

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
        }
        deterministicTaskQueue.runAllTasks();
        assertFalse(electionStarted.get());
    }

    public void testRetriesOnCorrectSchedule() {
        final Builder settingsBuilder = Settings.builder();

        final long initialTimeoutMillis;
        if (randomBoolean()) {
            initialTimeoutMillis = randomLongBetween(1, 10000);
            settingsBuilder.put(ELECTION_INITIAL_TIMEOUT_SETTING.getKey(), initialTimeoutMillis + "ms");
        } else {
            initialTimeoutMillis = ELECTION_INITIAL_TIMEOUT_SETTING.get(Settings.EMPTY).millis();
        }

        if (randomBoolean()) {
            settingsBuilder.put(ELECTION_BACK_OFF_TIME_SETTING.getKey(), randomLongBetween(1, 60000) + "ms");
        }

        if (ELECTION_MAX_TIMEOUT_SETTING.get(Settings.EMPTY).millis() < initialTimeoutMillis || randomBoolean()) {
            settingsBuilder.put(ELECTION_MAX_TIMEOUT_SETTING.getKey(),
                randomLongBetween(Math.max(200, initialTimeoutMillis), 180000) + "ms");
        }

        final long electionDurationMillis;
        if (randomBoolean()) {
            electionDurationMillis = randomLongBetween(1, 300000);
            settingsBuilder.put(ELECTION_DURATION_SETTING.getKey(), electionDurationMillis + "ms");
        } else {
            electionDurationMillis = ELECTION_DURATION_SETTING.get(Settings.EMPTY).millis();
        }

        final Settings settings = settingsBuilder.put(NODE_NAME_SETTING.getKey(), "node").build();
        final long initialTimeout = ELECTION_INITIAL_TIMEOUT_SETTING.get(settings).millis();
        final long backOffTime = ELECTION_BACK_OFF_TIME_SETTING.get(settings).millis();
        final long maxTimeout = ELECTION_MAX_TIMEOUT_SETTING.get(settings).millis();
        final long duration = ELECTION_DURATION_SETTING.get(settings).millis();

        final DeterministicTaskQueue deterministicTaskQueue = new DeterministicTaskQueue(settings, random());
        final ElectionSchedulerFactory electionSchedulerFactory
            = new ElectionSchedulerFactory(settings, random(), deterministicTaskQueue.getThreadPool());

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
            assertThat(e.getMessage(),
                is("failed to parse value [10001ms] for setting [cluster.election.initial_timeout], must be <= [10s]"));
        }

        {
            final Settings settings = Settings.builder().put(ELECTION_BACK_OFF_TIME_SETTING.getKey(), "0s").build();
            IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> ELECTION_BACK_OFF_TIME_SETTING.get(settings));
            assertThat(e.getMessage(), is("failed to parse value [0s] for setting [cluster.election.back_off_time], must be >= [1ms]"));
        }

        {
            final Settings settings = Settings.builder().put(ELECTION_BACK_OFF_TIME_SETTING.getKey(), "60001ms").build();
            IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> ELECTION_BACK_OFF_TIME_SETTING.get(settings));
            assertThat(e.getMessage(),
                is("failed to parse value [60001ms] for setting [cluster.election.back_off_time], must be <= [60s]"));
        }

        {
            final Settings settings = Settings.builder().put(ELECTION_MAX_TIMEOUT_SETTING.getKey(), "199ms").build();
            IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> ELECTION_MAX_TIMEOUT_SETTING.get(settings));
            assertThat(e.getMessage(), is("failed to parse value [199ms] for setting [cluster.election.max_timeout], must be >= [200ms]"));
        }

        {
            final Settings settings = Settings.builder().put(ELECTION_MAX_TIMEOUT_SETTING.getKey(), "301s").build();
            IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> ELECTION_MAX_TIMEOUT_SETTING.get(settings));
            assertThat(e.getMessage(), is("failed to parse value [301s] for setting [cluster.election.max_timeout], must be <= [300s]"));
        }

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

            assertThat(new ElectionSchedulerFactory(settings, random(), null), not(nullValue())); // doesn't throw an IAE
        }

        {
            final long initialTimeoutMillis = randomLongBetween(201, 10000);
            final long maxTimeoutMillis = randomLongBetween(200, initialTimeoutMillis - 1);

            final Settings settings = Settings.builder()
                .put(ELECTION_INITIAL_TIMEOUT_SETTING.getKey(), initialTimeoutMillis + "ms")
                .put(ELECTION_MAX_TIMEOUT_SETTING.getKey(), maxTimeoutMillis + "ms")
                .build();

            IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
                () -> new ElectionSchedulerFactory(settings, random(), null));
            assertThat(e.getMessage(), equalTo("[cluster.election.max_timeout] is ["
                + TimeValue.timeValueMillis(maxTimeoutMillis)
                + "], but must be at least [cluster.election.initial_timeout] which is ["
                + TimeValue.timeValueMillis(initialTimeoutMillis) + "]"));
        }
    }

    public void testRandomPositiveLongLessThan() {
        for (long input : new long[]{0, 1, -1, Long.MIN_VALUE, Long.MAX_VALUE, randomLong()}) {
            for (long upperBound : new long[]{1, 2, 3, 100, Long.MAX_VALUE}) {
                long l = toPositiveLongAtMost(input, upperBound);
                assertThat(l, greaterThan(0L));
                assertThat(l, lessThanOrEqualTo(upperBound));
            }
        }
    }
}
