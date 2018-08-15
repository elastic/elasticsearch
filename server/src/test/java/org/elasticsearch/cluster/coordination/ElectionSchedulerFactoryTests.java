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
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.test.ESTestCase;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.LongSupplier;

import static org.elasticsearch.cluster.coordination.ElectionSchedulerFactory.ELECTION_BACK_OFF_TIME_SETTING;
import static org.elasticsearch.cluster.coordination.ElectionSchedulerFactory.ELECTION_INITIAL_TIMEOUT_SETTING;
import static org.elasticsearch.cluster.coordination.ElectionSchedulerFactory.ELECTION_MAX_TIMEOUT_SETTING;
import static org.elasticsearch.cluster.coordination.ElectionSchedulerFactory.randomPositiveLongLessThan;
import static org.elasticsearch.node.Node.NODE_NAME_SETTING;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class ElectionSchedulerFactoryTests extends ESTestCase {

    private TimeValue randomGracePeriod() {
        return TimeValue.timeValueMillis(randomLongBetween(0, 10000));
    }

    private void assertElectionSchedule(final DeterministicTaskQueue deterministicTaskQueue,
                                        final ElectionSchedulerFactory electionSchedulerFactory) {

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
                    assertThat(electionDelay, lessThanOrEqualTo(ELECTION_INITIAL_TIMEOUT_SETTING.get(Settings.EMPTY).millis()
                        + ELECTION_BACK_OFF_TIME_SETTING.get(Settings.EMPTY).millis() + initialGracePeriod.millis()));
                    assertThat(electionDelay, lessThanOrEqualTo(
                        ELECTION_MAX_TIMEOUT_SETTING.get(Settings.EMPTY).millis() + initialGracePeriod.millis()));

                } else {

                    final long electionDelay = thisElectionTime - lastElectionTime;
                    final long backedOffMaximum = ELECTION_INITIAL_TIMEOUT_SETTING.get(Settings.EMPTY).millis()
                        + ELECTION_BACK_OFF_TIME_SETTING.get(Settings.EMPTY).millis() * electionCount;

                    // Check upper bound
                    assertThat(electionDelay, lessThanOrEqualTo(backedOffMaximum));
                    assertThat(electionDelay, lessThanOrEqualTo(ELECTION_MAX_TIMEOUT_SETTING.get(Settings.EMPTY).millis()));

                    // Run until we get a delay close to the maximum to show that backing off does work
                    if (electionDelay >= ELECTION_MAX_TIMEOUT_SETTING.get(Settings.EMPTY).millis() - 100 && electionCount >= 1000) {
                        break;
                    }
                }

                lastElectionTime = thisElectionTime;
            }
        }
        deterministicTaskQueue.runAllTasks(random());
        assertFalse(electionStarted.get());
    }

    public void testRetriesOnCorrectSchedule() {
        final Settings settings = Settings.builder().put(NODE_NAME_SETTING.getKey(), "node").build();
        final DeterministicTaskQueue deterministicTaskQueue = new DeterministicTaskQueue(settings);
        final ElectionSchedulerFactory electionSchedulerFactory
            = new ElectionSchedulerFactory(settings, random(), deterministicTaskQueue.getThreadPool());

        assertElectionSchedule(deterministicTaskQueue, electionSchedulerFactory);

        // do it again to show that the max is reset when the scheduler is restarted
        assertElectionSchedule(deterministicTaskQueue, electionSchedulerFactory);
    }

    public void testSettingsMustBeReasonable() {
        final Settings s0 = Settings.builder().put(ELECTION_INITIAL_TIMEOUT_SETTING.getKey(), "0s").build();
        IllegalArgumentException ex = expectThrows(IllegalArgumentException.class, () -> ELECTION_INITIAL_TIMEOUT_SETTING.get(s0));
        assertThat(ex.getMessage(), is("Failed to parse value [0s] for setting [cluster.election.initial_timeout] must be >= 1ms"));

        final Settings s2 = Settings.builder().put(ELECTION_MAX_TIMEOUT_SETTING.getKey(), "199ms").build();
        ex = expectThrows(IllegalArgumentException.class, () -> ELECTION_MAX_TIMEOUT_SETTING.get(s2));
        assertThat(ex.getMessage(), is("Failed to parse value [199ms] for setting [cluster.election.max_timeout] must be >= 200ms"));

        final Settings s4 = Settings.builder()
            .put(ELECTION_INITIAL_TIMEOUT_SETTING.getKey(), "1ms")
            .put(ELECTION_MAX_TIMEOUT_SETTING.getKey(), "60s")
            .build();

        assertThat(ELECTION_INITIAL_TIMEOUT_SETTING.get(s4), is(TimeValue.timeValueMillis(1)));
        assertThat(ELECTION_MAX_TIMEOUT_SETTING.get(s4), is(TimeValue.timeValueSeconds(60)));
    }

    public void testRandomPositiveLongLessThan() {
        LongSupplier[] longSuppliers = new LongSupplier[]{
            () -> 0,
            () -> 1,
            () -> -1,
            () -> Long.MIN_VALUE,
            () -> Long.MAX_VALUE,
            ESTestCase::randomLong
        };

        for (LongSupplier longSupplier : longSuppliers) {
            for (long upperBound : new long[]{2, 3, 100, Long.MAX_VALUE}) {
                long l = randomPositiveLongLessThan(longSupplier, upperBound);
                assertThat(l, greaterThan(0L));
                assertThat(l, lessThan(upperBound));
            }
        }
    }
}
