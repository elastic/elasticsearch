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
import static org.elasticsearch.cluster.coordination.ElectionSchedulerFactory.ELECTION_INITIAL_TIMEOUT_SETTING;
import static org.elasticsearch.cluster.coordination.ElectionSchedulerFactory.ELECTION_MAX_TIMEOUT_SETTING;
import static org.elasticsearch.cluster.coordination.ElectionSchedulerFactory.toPositiveLongAtMost;
import static org.elasticsearch.node.Node.NODE_NAME_SETTING;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class ElectionSchedulerFactoryTests extends ESTestCase {

    private TimeValue randomGracePeriod() {
        return TimeValue.timeValueMillis(randomLongBetween(0, 10000));
    }

    private void assertElectionSchedule(final DeterministicTaskQueue deterministicTaskQueue,
                                        final ElectionSchedulerFactory electionSchedulerFactory,
                                        final long initialTimeout, final long backOffTime, final long maxTimeout) {

        final TimeValue initialGracePeriod = randomGracePeriod();
        final AtomicBoolean electionStarted = new AtomicBoolean();

        try (Releasable ignored = electionSchedulerFactory.startElectionScheduler(initialGracePeriod,
            () -> assertTrue(electionStarted.compareAndSet(false, true)))) {

            long lastElectionTime = deterministicTaskQueue.getCurrentTimeMillis();
            int electionCount = 0;

            while (true) {
                electionCount++;

                while (electionStarted.get() == false) {
                    if (deterministicTaskQueue.hasRunnableTasks() == false) {
                        deterministicTaskQueue.advanceTime();
                    }
                    deterministicTaskQueue.runAllRunnableTasks(random());
                }
                assertTrue(electionStarted.compareAndSet(true, false));

                final long thisElectionTime = deterministicTaskQueue.getCurrentTimeMillis();

                if (electionCount == 1) {
                    final long electionDelay = thisElectionTime - lastElectionTime;

                    // Check grace period
                    assertThat(electionDelay, greaterThanOrEqualTo(initialGracePeriod.millis()));

                    // Check upper bound
                    assertThat(electionDelay, lessThanOrEqualTo(initialTimeout + initialGracePeriod.millis()));
                    assertThat(electionDelay, lessThanOrEqualTo(maxTimeout + initialGracePeriod.millis()));

                } else {

                    final long electionDelay = thisElectionTime - lastElectionTime;

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

                lastElectionTime = thisElectionTime;
            }
        }
        deterministicTaskQueue.runAllTasks(random());
        assertFalse(electionStarted.get());
    }

    public void testRetriesOnCorrectSchedule() {
        final Builder settingsBuilder = Settings.builder();

        if (randomBoolean()) {
            settingsBuilder.put(ELECTION_INITIAL_TIMEOUT_SETTING.getKey(), randomLongBetween(1, 100000) + "ms");
        }

        if (randomBoolean()) {
            settingsBuilder.put(ELECTION_BACK_OFF_TIME_SETTING.getKey(), randomLongBetween(1, 100000) + "ms");
        }

        if (randomBoolean()) {
            settingsBuilder.put(ELECTION_MAX_TIMEOUT_SETTING.getKey(), randomLongBetween(200, 100000) + "ms");
        }

        final Settings settings = settingsBuilder.put(NODE_NAME_SETTING.getKey(), "node").build();
        final long initialTimeout = ELECTION_INITIAL_TIMEOUT_SETTING.get(settings).millis();
        final long backOffTime = ELECTION_BACK_OFF_TIME_SETTING.get(settings).millis();
        final long maxTimeout = ELECTION_MAX_TIMEOUT_SETTING.get(settings).millis();

        final DeterministicTaskQueue deterministicTaskQueue = new DeterministicTaskQueue(settings);
        final ElectionSchedulerFactory electionSchedulerFactory
            = new ElectionSchedulerFactory(settings, random(), deterministicTaskQueue.getThreadPool());

        assertElectionSchedule(deterministicTaskQueue, electionSchedulerFactory, initialTimeout, backOffTime, maxTimeout);

        // do it again to show that the max is reset when the scheduler is restarted
        assertElectionSchedule(deterministicTaskQueue, electionSchedulerFactory, initialTimeout, backOffTime, maxTimeout);
    }

    public void testSettingsMustBeReasonable() {
        {
            final Settings settings = Settings.builder().put(ELECTION_INITIAL_TIMEOUT_SETTING.getKey(), "0s").build();
            IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> ELECTION_INITIAL_TIMEOUT_SETTING.get(settings));
            assertThat(e.getMessage(), is("Failed to parse value [0s] for setting [cluster.election.initial_timeout] must be >= 1ms"));
        }

        {
            final Settings settings = Settings.builder().put(ELECTION_INITIAL_TIMEOUT_SETTING.getKey(), "10001ms").build();
            IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> ELECTION_INITIAL_TIMEOUT_SETTING.get(settings));
            assertThat(e.getMessage(), is("Failed to parse value [10001ms] for setting [cluster.election.initial_timeout] must be <= 10s"));
        }

        {
            final Settings settings = Settings.builder().put(ELECTION_BACK_OFF_TIME_SETTING.getKey(), "0s").build();
            IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> ELECTION_BACK_OFF_TIME_SETTING.get(settings));
            assertThat(e.getMessage(), is("Failed to parse value [0s] for setting [cluster.election.back_off_time] must be >= 1ms"));
        }

        {
            final Settings settings = Settings.builder().put(ELECTION_BACK_OFF_TIME_SETTING.getKey(), "60001ms").build();
            IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> ELECTION_BACK_OFF_TIME_SETTING.get(settings));
            assertThat(e.getMessage(), is("Failed to parse value [60001ms] for setting [cluster.election.back_off_time] must be <= 1m"));
        }

        {
            final Settings settings = Settings.builder().put(ELECTION_MAX_TIMEOUT_SETTING.getKey(), "199ms").build();
            IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> ELECTION_MAX_TIMEOUT_SETTING.get(settings));
            assertThat(e.getMessage(), is("Failed to parse value [199ms] for setting [cluster.election.max_timeout] must be >= 200ms"));
        }

        {
            final Settings settings = Settings.builder().put(ELECTION_MAX_TIMEOUT_SETTING.getKey(), "301s").build();
            IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> ELECTION_MAX_TIMEOUT_SETTING.get(settings));
            assertThat(e.getMessage(), is("Failed to parse value [301s] for setting [cluster.election.max_timeout] must be <= 5m"));
        }

        {
            final Settings settings = Settings.builder()
                .put(ELECTION_INITIAL_TIMEOUT_SETTING.getKey(), "1ms")
                .put(ELECTION_BACK_OFF_TIME_SETTING.getKey(), "150ms")
                .put(ELECTION_MAX_TIMEOUT_SETTING.getKey(), "60s")
                .build();

            assertThat(ELECTION_INITIAL_TIMEOUT_SETTING.get(settings), is(TimeValue.timeValueMillis(1)));
            assertThat(ELECTION_BACK_OFF_TIME_SETTING.get(settings), is(TimeValue.timeValueMillis(150)));
            assertThat(ELECTION_MAX_TIMEOUT_SETTING.get(settings), is(TimeValue.timeValueSeconds(60)));
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
