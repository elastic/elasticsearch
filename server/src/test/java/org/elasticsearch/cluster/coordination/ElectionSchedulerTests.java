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

import org.elasticsearch.Version;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.transport.CapturingTransport;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportService;
import org.junit.Before;

import static java.util.Collections.emptySet;
import static org.elasticsearch.cluster.coordination.ElectionScheduler.ELECTION_BACK_OFF_TIME_SETTING;
import static org.elasticsearch.cluster.coordination.ElectionScheduler.ELECTION_MAX_TIMEOUT_SETTING;
import static org.elasticsearch.cluster.coordination.ElectionScheduler.ELECTION_MIN_TIMEOUT_SETTING;
import static org.elasticsearch.cluster.coordination.ElectionScheduler.validationExceptionMessage;
import static org.elasticsearch.node.Node.NODE_NAME_SETTING;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class ElectionSchedulerTests extends ESTestCase {

    private DeterministicTaskQueue deterministicTaskQueue;
    private ElectionScheduler electionScheduler;
    private boolean electionOccurred = false;

    @Before
    public void createObjects() {
        final Settings settings = Settings.builder().put(NODE_NAME_SETTING.getKey(), "node").build();
        deterministicTaskQueue = new DeterministicTaskQueue(settings);
        final Transport capturingTransport = new CapturingTransport();
        final DiscoveryNode localNode = new DiscoveryNode("local-node", buildNewFakeTransportAddress(), Version.CURRENT);
        final TransportService transportService = new TransportService(settings, capturingTransport,
            deterministicTaskQueue.getThreadPool(), TransportService.NOOP_TRANSPORT_INTERCEPTOR,
            boundTransportAddress -> localNode, null, emptySet());
        electionScheduler = new ElectionScheduler(settings, random(), transportService) {
            @Override
            protected void startElection(long maxTermSeen) {
                electionOccurred = true;
            }
        };
    }

    private void runElectionsAndValidate(int electionCount, long minRetryInterval, long maxRetryInterval,
                                         long backoffStartPoint, long backoffTime) {
        for (int i = 0; i < electionCount; i++) {
            final String description = "election " + i;

            final long lastElectionTime = deterministicTaskQueue.getCurrentTimeMillis();
            runElection(description);
            final long thisElectionTime = deterministicTaskQueue.getCurrentTimeMillis();
            final long electionDelay = thisElectionTime - lastElectionTime;

            assertThat(description, electionDelay, greaterThanOrEqualTo(minRetryInterval));
            assertThat(description, electionDelay, lessThanOrEqualTo(maxRetryInterval));
            assertThat(description, electionDelay, lessThanOrEqualTo(backoffStartPoint + backoffTime * (i + 1)));
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

        final long defaultMinTimeout = ELECTION_MIN_TIMEOUT_SETTING.get(Settings.EMPTY).millis();
        final long defaultBackoff = ELECTION_BACK_OFF_TIME_SETTING.get(Settings.EMPTY).millis();
        final long defaultMaxTimeout = ELECTION_MAX_TIMEOUT_SETTING.get(Settings.EMPTY).millis();
        runElectionsAndValidate(randomInt(100), defaultMinTimeout, defaultMaxTimeout, defaultMinTimeout, defaultBackoff);

        electionScheduler.stop();
        electionScheduler.start(); // should reset the backoff interval
        runElectionsAndValidate(randomInt(100), defaultMinTimeout, defaultMaxTimeout, defaultMinTimeout, defaultBackoff);
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
        new ElectionScheduler(settings, random(), null) {
            @Override
            protected void startElection(long maxTermSeen) {
                fail();
            }
        };
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
}
