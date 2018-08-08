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

import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import static org.elasticsearch.cluster.coordination.ElectionScheduler.ELECTION_MAX_RETRY_INTERVAL_SETTING;
import static org.elasticsearch.cluster.coordination.ElectionScheduler.ELECTION_MIN_RETRY_INTERVAL_SETTING;
import static org.elasticsearch.cluster.coordination.ElectionScheduler.validationExceptionMessage;
import static org.elasticsearch.node.Node.NODE_NAME_SETTING;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class ElectionSchedulerTests extends ESTestCase {

    private ClusterSettings clusterSettings;
    private DeterministicTaskQueue deterministicTaskQueue;
    private ElectionScheduler electionScheduler;
    private boolean electionOccurred = false;

    @Before
    public void createObjects() {
        final Settings settings = Settings.builder().put(NODE_NAME_SETTING.getKey(), "node").build();
        clusterSettings = new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        deterministicTaskQueue = new DeterministicTaskQueue(settings);
        electionScheduler = new ElectionScheduler(settings, clusterSettings, random(), deterministicTaskQueue.getThreadPool()) {
            @Override
            protected void startElection() {
                electionOccurred = true;
            }
        };
    }

    private void runElectionsAndValidate(int electionCount, long minRetryInterval, long maxRetryInterval, long backoffStartPoint) {
        for (int i = 0; i < electionCount; i++) {
            final String description = "election " + i;

            final long lastElectionTime = deterministicTaskQueue.getCurrentTimeMillis();
            runElection(description);
            final long thisElectionTime = deterministicTaskQueue.getCurrentTimeMillis();
            final long electionDelay = thisElectionTime - lastElectionTime;

            assertThat(description, electionDelay, greaterThanOrEqualTo(minRetryInterval));
            assertThat(description, electionDelay, lessThanOrEqualTo(maxRetryInterval));
            assertThat(description, electionDelay, lessThanOrEqualTo(backoffStartPoint + minRetryInterval * (i + 1)));
        }
    }

    private void runElection(String description) {
        logger.debug("--> runElection: {}", description);
        electionOccurred = false;
        while (electionOccurred == false) {
            assertFalse(description, deterministicTaskQueue.hasRunnableTasks());
            assertTrue(description, deterministicTaskQueue.hasDeferredTasks());
            deterministicTaskQueue.advanceTime();
            deterministicTaskQueue.runAllRunnableTasks(random());
        }
        assertFalse(description, deterministicTaskQueue.hasRunnableTasks());
        assertTrue(description, deterministicTaskQueue.hasDeferredTasks());
    }

    public void testElectionScheduler() {
        assertFalse(deterministicTaskQueue.hasRunnableTasks());
        assertFalse(deterministicTaskQueue.hasDeferredTasks());

        electionScheduler.start();

        final long defaultMinRetryInterval = ELECTION_MIN_RETRY_INTERVAL_SETTING.get(Settings.EMPTY).millis();
        final long defaultMaxRetryInterval = ELECTION_MAX_RETRY_INTERVAL_SETTING.get(Settings.EMPTY).millis();
        runElectionsAndValidate(randomInt(100), defaultMinRetryInterval, defaultMaxRetryInterval, defaultMinRetryInterval);

        clusterSettings.applySettings(Settings.builder()
            .put(ELECTION_MIN_RETRY_INTERVAL_SETTING.getKey(), "100ms")
            .put(ELECTION_MAX_RETRY_INTERVAL_SETTING.getKey(), "200ms")
            .build());
        runElection("pick up reduction in retry intervals"); // change in retry interval is not picked up until the next election
        runElectionsAndValidate(randomInt(100), 100, 200, 200);

        clusterSettings.applySettings(Settings.EMPTY);
        runElection("pick up reset of retry intervals"); // change in retry interval is not picked up until the next election
        runElectionsAndValidate(randomInt(100), defaultMinRetryInterval, defaultMaxRetryInterval, defaultMinRetryInterval);

        electionScheduler.stop();
        electionScheduler.start(); // should reset the backoff interval
        runElectionsAndValidate(randomInt(100), defaultMinRetryInterval, defaultMaxRetryInterval, defaultMinRetryInterval);
    }

    public void testSettingsMustBeReasonable() {
        IllegalArgumentException ex = expectThrows(IllegalArgumentException.class,
            () -> clusterSettings.applySettings(Settings.builder().put(ELECTION_MIN_RETRY_INTERVAL_SETTING.getKey(), "0s").build()));
        assertThat(ex.getCause().getMessage(), is(
            "Failed to parse value [0s] for setting [discovery.election.min_retry_interval] must be >= [1ms]"));

        ex = expectThrows(IllegalArgumentException.class,
            () -> clusterSettings.applySettings(Settings.builder().put(ELECTION_MAX_RETRY_INTERVAL_SETTING.getKey(), "0s").build()));
        assertThat(ex.getCause().getMessage(), is(
            "Failed to parse value [0s] for setting [discovery.election.max_retry_interval] must be >= [1ms]"));

        ex = expectThrows(IllegalArgumentException.class,
            () -> clusterSettings.applySettings(Settings.builder().put(ELECTION_MIN_RETRY_INTERVAL_SETTING.getKey(), "60001ms").build()));
        assertThat(ex.getCause().getMessage(), is(
            "Failed to parse value [60001ms] for setting [discovery.election.min_retry_interval] must be <= [60s]"));

        ex = expectThrows(IllegalArgumentException.class,
            () -> clusterSettings.applySettings(Settings.builder().put(ELECTION_MAX_RETRY_INTERVAL_SETTING.getKey(), "60001ms").build()));
        assertThat(ex.getCause().getMessage(), is(
            "Failed to parse value [60001ms] for setting [discovery.election.max_retry_interval] must be <= [60s]"));

        clusterSettings.applySettings(Settings.builder()
            .put(ELECTION_MIN_RETRY_INTERVAL_SETTING.getKey(), "1ms")
            .put(ELECTION_MAX_RETRY_INTERVAL_SETTING.getKey(), "60s")
            .build());
    }

    public void testValidationChecksMinIsReasonblyLessThanMax() {
        assertThat(validationExceptionMessage("foo", "bar"), is("Invalid election retry intervals: " +
            "[discovery.election.min_retry_interval] is [foo] and [discovery.election.max_retry_interval] is [bar], " +
            "but [discovery.election.max_retry_interval] should be at least 100ms longer than [discovery.election.min_retry_interval]"));

        IllegalArgumentException ex = expectThrows(IllegalArgumentException.class,
            () -> clusterSettings.applySettings(Settings.builder().put(ELECTION_MIN_RETRY_INTERVAL_SETTING.getKey(), "9901ms").build()));
        assertThat(ex.getCause().getMessage(), is(validationExceptionMessage("9.9s", "10s")));

        clusterSettings.applySettings(Settings.builder().put(ELECTION_MIN_RETRY_INTERVAL_SETTING.getKey(), "9900ms").build());

        ex = expectThrows(IllegalArgumentException.class,
            () -> clusterSettings.applySettings(Settings.builder().put(ELECTION_MAX_RETRY_INTERVAL_SETTING.getKey(), "399ms").build()));
        assertThat(ex.getCause().getMessage(), is(validationExceptionMessage("300ms", "399ms")));

        clusterSettings.applySettings(Settings.builder().put(ELECTION_MAX_RETRY_INTERVAL_SETTING.getKey(), "400ms").build());

        ex = expectThrows(IllegalArgumentException.class,
            () -> clusterSettings.applySettings(Settings.builder()
                .put(ELECTION_MIN_RETRY_INTERVAL_SETTING.getKey(), "100ms")
                .put(ELECTION_MAX_RETRY_INTERVAL_SETTING.getKey(), "199ms")
                .build()));
        assertThat(ex.getCause().getMessage(), is(validationExceptionMessage("100ms", "199ms")));

        clusterSettings.applySettings(Settings.builder()
            .put(ELECTION_MIN_RETRY_INTERVAL_SETTING.getKey(), "100ms")
            .put(ELECTION_MAX_RETRY_INTERVAL_SETTING.getKey(), "200ms")
            .build());

        clusterSettings.applySettings(Settings.builder()
            .put(ELECTION_MIN_RETRY_INTERVAL_SETTING.getKey(), "20s")
            .put(ELECTION_MAX_RETRY_INTERVAL_SETTING.getKey(), "30s")
            .build());
    }
}
