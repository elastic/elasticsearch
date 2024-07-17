/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.coordination.stateless;

import org.apache.logging.log4j.Level;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.MockLog;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;

import java.util.Collections;
import java.util.OptionalLong;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

public class AtomicRegisterPreVoteCollectorTests extends ESTestCase {
    CapturingThreadPool threadPool;

    @Before
    public void setupThreadPool() {
        threadPool = new CapturingThreadPool(getTestName());
    }

    @After
    public void teardownThreadPool() {
        ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS);
    }

    public void testElectionRunsWhenThereAreNoLeader() throws Exception {
        final var currentTermProvider = new AtomicLong(1);
        final var heartbeatFrequency = TimeValue.timeValueSeconds(randomIntBetween(15, 30));
        final var maxTimeSinceLastHeartbeat = TimeValue.timeValueSeconds(2 * heartbeatFrequency.seconds());
        final var leaderNode = DiscoveryNodeUtils.create("master");

        final var fakeClock = new AtomicLong();
        final var heartbeatStore = new InMemoryHeartbeatStore();
        final var heartbeatService = new StoreHeartbeatService(
            heartbeatStore,
            threadPool,
            heartbeatFrequency,
            maxTimeSinceLastHeartbeat,
            listener -> listener.onResponse(OptionalLong.of(currentTermProvider.get()))
        ) {
            @Override
            protected long absoluteTimeInMillis() {
                return fakeClock.get();
            }
        };

        // Either there's no heartbeat or is stale
        if (randomBoolean()) {
            safeAwait((ActionListener<Void> l) -> heartbeatStore.writeHeartbeat(new Heartbeat(1, fakeClock.get()), l));
            fakeClock.set(maxTimeSinceLastHeartbeat.millis() + randomLongBetween(0, 1000));
        }

        var startElection = new AtomicBoolean();
        var preVoteCollector = new AtomicRegisterPreVoteCollector(heartbeatService, () -> startElection.set(true));

        preVoteCollector.start(ClusterState.EMPTY_STATE, Collections.emptyList());

        assertThat(startElection.get(), is(true));
    }

    public void testLogSkippedElectionIfRecentLeaderHeartbeat() throws Exception {
        final var currentTermProvider = new AtomicLong(1);
        final var heartbeatFrequency = TimeValue.timeValueSeconds(randomIntBetween(15, 30));
        final var maxTimeSinceLastHeartbeat = TimeValue.timeValueSeconds(2 * heartbeatFrequency.seconds());
        DiscoveryNodeUtils.create("master");
        try (var mockLog = MockLog.capture(AtomicRegisterPreVoteCollector.class)) {
            mockLog.addExpectation(
                new MockLog.SeenEventExpectation(
                    "log emitted when skipping election",
                    AtomicRegisterPreVoteCollector.class.getCanonicalName(),
                    Level.INFO,
                    "skipping election since there is a recent heartbeat*"
                )
            );
            final var fakeClock = new AtomicLong();
            final var heartbeatStore = new InMemoryHeartbeatStore();
            final var heartbeatService = new StoreHeartbeatService(
                heartbeatStore,
                threadPool,
                heartbeatFrequency,
                maxTimeSinceLastHeartbeat,
                listener -> listener.onResponse(OptionalLong.of(currentTermProvider.get()))
            ) {
                @Override
                protected long absoluteTimeInMillis() {
                    return fakeClock.get();
                }
            };

            safeAwait((ActionListener<Void> l) -> heartbeatStore.writeHeartbeat(new Heartbeat(1, fakeClock.get()), l));
            fakeClock.addAndGet(randomLongBetween(0L, maxTimeSinceLastHeartbeat.millis() - 1));

            var startElection = new AtomicBoolean();
            var preVoteCollector = new AtomicRegisterPreVoteCollector(heartbeatService, () -> startElection.set(true));

            preVoteCollector.start(ClusterState.EMPTY_STATE, Collections.emptyList());

            assertThat(startElection.get(), is(false));
            mockLog.assertAllExpectationsMatched();
        }
    }

    public void testElectionDoesNotRunWhenThereIsALeader() throws Exception {
        final var currentTermProvider = new AtomicLong(1);
        final var heartbeatFrequency = TimeValue.timeValueSeconds(randomIntBetween(15, 30));
        final var maxTimeSinceLastHeartbeat = TimeValue.timeValueSeconds(2 * heartbeatFrequency.seconds());
        final var leaderNode = DiscoveryNodeUtils.create("master");

        final var fakeClock = new AtomicLong();
        final var heartbeatStore = new InMemoryHeartbeatStore();
        final var heartbeatService = new StoreHeartbeatService(
            heartbeatStore,
            threadPool,
            heartbeatFrequency,
            maxTimeSinceLastHeartbeat,
            listener -> listener.onResponse(OptionalLong.of(currentTermProvider.get()))
        ) {
            @Override
            protected long absoluteTimeInMillis() {
                return fakeClock.get();
            }
        };

        safeAwait((ActionListener<Void> l) -> heartbeatStore.writeHeartbeat(new Heartbeat(1, fakeClock.get()), l));

        var startElection = new AtomicBoolean();
        var preVoteCollector = new AtomicRegisterPreVoteCollector(heartbeatService, () -> startElection.set(true));

        preVoteCollector.start(ClusterState.EMPTY_STATE, Collections.emptyList());

        assertThat(startElection.get(), is(false));
    }

    public void testCancelPreVotingRound() {
        final var currentTermProvider = new AtomicLong(1);
        final var heartbeatFrequency = TimeValue.timeValueSeconds(randomIntBetween(15, 30));
        final var maxTimeSinceLastHeartbeat = TimeValue.timeValueSeconds(2 * heartbeatFrequency.seconds());

        final var readHeartBeatListenerRef = new AtomicReference<ActionListener<Heartbeat>>();
        final var heartbeatStore = new InMemoryHeartbeatStore() {
            @Override
            public void readLatestHeartbeat(ActionListener<Heartbeat> listener) {
                readHeartBeatListenerRef.set(listener);
            }
        };
        final var heartbeatService = new StoreHeartbeatService(
            heartbeatStore,
            threadPool,
            heartbeatFrequency,
            maxTimeSinceLastHeartbeat,
            listener -> listener.onResponse(OptionalLong.of(currentTermProvider.get()))
        );

        var startElection = new AtomicBoolean();
        var preVoteCollector = new AtomicRegisterPreVoteCollector(heartbeatService, () -> startElection.set(true));

        var preVotingRound = preVoteCollector.start(ClusterState.EMPTY_STATE, Collections.emptyList());

        var readHeartBeatListener = readHeartBeatListenerRef.get();
        assertThat(readHeartBeatListener, is(notNullValue()));

        preVotingRound.close();

        readHeartBeatListener.onResponse(null);
        assertThat(startElection.get(), is(false));
    }
}
