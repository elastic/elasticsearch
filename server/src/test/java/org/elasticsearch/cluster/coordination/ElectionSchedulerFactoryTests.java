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
import org.junit.Before;

import java.util.function.LongSupplier;

import static org.elasticsearch.cluster.coordination.ElectionSchedulerFactory.ELECTION_BACK_OFF_TIME_SETTING;
import static org.elasticsearch.cluster.coordination.ElectionSchedulerFactory.ELECTION_MAX_TIMEOUT_SETTING;
import static org.elasticsearch.cluster.coordination.ElectionSchedulerFactory.ELECTION_MIN_TIMEOUT_SETTING;
import static org.elasticsearch.cluster.coordination.ElectionSchedulerFactory.randomPositiveLongLessThan;
import static org.elasticsearch.cluster.coordination.ElectionSchedulerFactory.validationExceptionMessage;
import static org.elasticsearch.node.Node.NODE_NAME_SETTING;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class ElectionSchedulerFactoryTests extends ESTestCase {

    private DeterministicTaskQueue deterministicTaskQueue;
    private ElectionSchedulerFactory electionSchedulerFactory;
    private boolean electionStarted = false;

    @Before
    public void createObjects() {
        final Settings settings = Settings.builder().put(NODE_NAME_SETTING.getKey(), "node").build();
        deterministicTaskQueue = new DeterministicTaskQueue(settings);
        electionSchedulerFactory = new ElectionSchedulerFactory(settings, random(), deterministicTaskQueue.getThreadPool());
    }

    private TimeValue randomGracePeriod() {
        return TimeValue.timeValueMillis(randomLongBetween(0, 10000));
    }

    private void startElection() {
        assertFalse(electionStarted);
        electionStarted = true;
    }

    private void assertElectionSchedule() {
        final TimeValue initialGracePeriod = randomGracePeriod();

        try (Releasable ignored = electionSchedulerFactory.startElectionScheduler(initialGracePeriod, this::startElection)) {
            long lastElectionTime = deterministicTaskQueue.getCurrentTimeMillis();
            int electionCount = 0;
            while (true) {
                electionCount++;

                runElection();

                final long thisElectionTime = deterministicTaskQueue.getCurrentTimeMillis();

                if (electionCount == 1) {
                    final long electionDelay = thisElectionTime - lastElectionTime;

                    // Check grace period
                    assertThat(electionDelay, greaterThanOrEqualTo(initialGracePeriod.millis()));

                    // Check upper bound
                    assertThat(electionDelay, lessThanOrEqualTo(ELECTION_MIN_TIMEOUT_SETTING.get(Settings.EMPTY).millis()
                        + ELECTION_BACK_OFF_TIME_SETTING.get(Settings.EMPTY).millis() + initialGracePeriod.millis()));
                    assertThat(electionDelay, lessThanOrEqualTo(
                        ELECTION_MAX_TIMEOUT_SETTING.get(Settings.EMPTY).millis() + initialGracePeriod.millis()));

                } else {

                    final long electionDelay = thisElectionTime - lastElectionTime;
                    final long backedOffMaximum = ELECTION_MIN_TIMEOUT_SETTING.get(Settings.EMPTY).millis()
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
        assertFalse(electionStarted);
    }

    public void testRetriesOnCorrectSchedule() {
        assertElectionSchedule();

        // do it again to show that the max is reset when the scheduler is restarted
        assertElectionSchedule();
    }

    private void runElection() {
        while (electionStarted == false) {
            if (deterministicTaskQueue.hasRunnableTasks() == false) {
                deterministicTaskQueue.advanceTime();
            }
            deterministicTaskQueue.runAllRunnableTasks(random());
        }
        electionStarted = false;
    }

    public void testSettingsMustBeReasonable() {
        final Settings s0 = Settings.builder().put(ELECTION_MIN_TIMEOUT_SETTING.getKey(), "0s").build();
        IllegalArgumentException ex = expectThrows(IllegalArgumentException.class, () -> ELECTION_MIN_TIMEOUT_SETTING.get(s0));
        assertThat(ex.getMessage(), is("Failed to parse value [0s] for setting [cluster.election.min_timeout] must be >= 1ms"));

        final Settings s2 = Settings.builder().put(ELECTION_MAX_TIMEOUT_SETTING.getKey(), "199ms").build();
        ex = expectThrows(IllegalArgumentException.class, () -> ELECTION_MAX_TIMEOUT_SETTING.get(s2));
        assertThat(ex.getMessage(), is("Failed to parse value [199ms] for setting [cluster.election.max_timeout] must be >= 200ms"));

        final Settings s4 = Settings.builder()
            .put(ELECTION_MIN_TIMEOUT_SETTING.getKey(), "1ms")
            .put(ELECTION_MAX_TIMEOUT_SETTING.getKey(), "60s")
            .build();

        assertThat(ELECTION_MIN_TIMEOUT_SETTING.get(s4), is(TimeValue.timeValueMillis(1)));
        assertThat(ELECTION_MAX_TIMEOUT_SETTING.get(s4), is(TimeValue.timeValueSeconds(60)));
    }

    private void validateSettings(Settings settings) {
        new ElectionSchedulerFactory(settings, random(), null);
    }

    private IllegalArgumentException expectInvalid(Settings settings) {
        return expectThrows(IllegalArgumentException.class, () -> validateSettings(settings));
    }

    public void testValidationChecksMinIsReasonblyLessThanMax() {
        assertThat(validationExceptionMessage("foo", "bar"), is("Invalid election retry timeouts: " +
            "[cluster.election.min_timeout] is [foo] and [cluster.election.max_timeout] is [bar], " +
            "but [cluster.election.max_timeout] should be at least 100ms longer than [cluster.election.min_timeout]"));

        assertThat(expectInvalid(Settings.builder().put(ELECTION_MIN_TIMEOUT_SETTING.getKey(), "9901ms").build()).getMessage(),
            is(validationExceptionMessage("9.9s", "10s")));

        validateSettings(Settings.builder().put(ELECTION_MIN_TIMEOUT_SETTING.getKey(), "9900ms").build());

        validateSettings(Settings.builder().put(ELECTION_MIN_TIMEOUT_SETTING.getKey(), "200ms").build());

        assertThat(expectInvalid(Settings.builder()
                .put(ELECTION_MIN_TIMEOUT_SETTING.getKey(), "150ms")
                .put(ELECTION_MAX_TIMEOUT_SETTING.getKey(), "249ms")
                .build()).getMessage(),
            is(validationExceptionMessage("150ms", "249ms")));

        validateSettings(Settings.builder()
            .put(ELECTION_MIN_TIMEOUT_SETTING.getKey(), "150ms")
            .put(ELECTION_MAX_TIMEOUT_SETTING.getKey(), "250ms")
            .build());

        validateSettings(Settings.builder()
            .put(ELECTION_MIN_TIMEOUT_SETTING.getKey(), "149ms")
            .put(ELECTION_MAX_TIMEOUT_SETTING.getKey(), "249ms")
            .build());
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
