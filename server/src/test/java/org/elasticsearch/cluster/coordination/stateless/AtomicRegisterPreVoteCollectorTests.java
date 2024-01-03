/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.coordination.stateless;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;
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
            PlainActionFuture.<Void, Exception>get(f -> heartbeatStore.writeHeartbeat(new Heartbeat(1, fakeClock.get()), f));
            fakeClock.set(maxTimeSinceLastHeartbeat.millis() + 1);
        }

        var startElection = new AtomicBoolean();
        var preVoteCollector = new AtomicRegisterPreVoteCollector(heartbeatService, () -> startElection.set(true));

        preVoteCollector.start(ClusterState.EMPTY_STATE, Collections.emptyList());

        assertThat(startElection.get(), is(true));
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

        PlainActionFuture.<Void, Exception>get(f -> heartbeatStore.writeHeartbeat(new Heartbeat(1, fakeClock.get()), f));

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
