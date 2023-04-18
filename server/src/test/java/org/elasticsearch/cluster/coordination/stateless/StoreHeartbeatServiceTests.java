/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.coordination.stateless;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class StoreHeartbeatServiceTests extends ESTestCase {
    CapturingThreadPool threadPool;

    @Before
    public void setupThreadPool() {
        threadPool = new CapturingThreadPool(getTestName());
    }

    @After
    public void teardownThreadPool() {
        ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS);
    }

    public void testHeartBeatStoreScheduling() {
        final var currentTermProvider = new AtomicLong(1);
        final var heartbeatFrequency = TimeValue.timeValueSeconds(randomIntBetween(15, 30));
        final var maxTimeSinceLastHeartbeat = TimeValue.timeValueSeconds(2 * heartbeatFrequency.seconds());

        final var heartbeatStore = new InMemoryHeartbeatStore();
        final var heartbeatService = new StoreHeartbeatService(
            heartbeatStore,
            threadPool,
            heartbeatFrequency,
            maxTimeSinceLastHeartbeat,
            currentTermProvider::get
        );

        PlainActionFuture<Long> completionListener = PlainActionFuture.newFuture();
        final var currentLeader = new DiscoveryNode("master", buildNewFakeTransportAddress(), Version.CURRENT);
        heartbeatService.start(currentLeader, currentTermProvider.get(), completionListener);

        Heartbeat firstHeartbeat = PlainActionFuture.get(heartbeatStore::readLatestHeartbeat);
        assertThat(firstHeartbeat, is(notNullValue()));
        assertThat(firstHeartbeat.term(), is(equalTo(1L)));
        assertThat(firstHeartbeat.absoluteTimeInMillis(), is(lessThanOrEqualTo(threadPool.absoluteTimeInMillis())));

        final var nextTask = threadPool.scheduledTasks.poll();
        assertThat(nextTask, is(notNullValue()));
        assertThat(nextTask.v1(), is(equalTo(heartbeatFrequency)));

        nextTask.v2().run();

        assertThat(completionListener.isDone(), is(false));

        Heartbeat secondHeartbeat = PlainActionFuture.get(heartbeatStore::readLatestHeartbeat);
        assertThat(secondHeartbeat, is(notNullValue()));
        assertThat(secondHeartbeat.term(), is(equalTo(1L)));
        assertThat(secondHeartbeat.absoluteTimeInMillis(), is(greaterThanOrEqualTo(firstHeartbeat.absoluteTimeInMillis())));

        final var secondScheduledTask = threadPool.scheduledTasks.poll();
        assertThat(secondScheduledTask, is(notNullValue()));
        assertThat(secondScheduledTask.v1(), is(equalTo(heartbeatFrequency)));

        heartbeatService.stop();

        secondScheduledTask.v2().run();

        // No new tasks are scheduled after stopping the heart beat service
        assertThat(threadPool.scheduledTasks.poll(), is(nullValue()));

        Heartbeat heartbeatAfterStoppingTheService = PlainActionFuture.get(heartbeatStore::readLatestHeartbeat);
        assertThat(heartbeatAfterStoppingTheService, is(equalTo(secondHeartbeat)));

        assertThat(completionListener.isDone(), is(false));
    }

    public void testServiceStopsAfterHeartbeatStoreFailure() {
        final var currentTermProvider = new AtomicLong(1);
        final var heartbeatFrequency = TimeValue.timeValueSeconds(randomIntBetween(15, 30));
        final var maxTimeSinceLastHeartbeat = TimeValue.timeValueSeconds(2 * heartbeatFrequency.seconds());

        final var injectWriteHeartBeatFailure = new AtomicBoolean(false);
        final var heartbeatStore = new InMemoryHeartbeatStore() {
            @Override
            public void writeHeartbeat(Heartbeat newHeartbeat, ActionListener<Void> listener) {
                if (injectWriteHeartBeatFailure.get()) {
                    listener.onFailure(new IOException("Unable to store heart beat"));
                } else {
                    super.writeHeartbeat(newHeartbeat, listener);
                }
            }
        };
        final var heartbeatService = new StoreHeartbeatService(
            heartbeatStore,
            threadPool,
            heartbeatFrequency,
            maxTimeSinceLastHeartbeat,
            currentTermProvider::get
        );

        PlainActionFuture<Long> completionListener = PlainActionFuture.newFuture();
        final var currentLeader = new DiscoveryNode("master", buildNewFakeTransportAddress(), Version.CURRENT);

        final boolean failFirstHeartBeat = randomBoolean();
        injectWriteHeartBeatFailure.set(failFirstHeartBeat);

        heartbeatService.start(currentLeader, currentTermProvider.get(), completionListener);

        if (failFirstHeartBeat == false) {
            Heartbeat firstHeartbeat = PlainActionFuture.get(heartbeatStore::readLatestHeartbeat);
            assertThat(firstHeartbeat, is(notNullValue()));

            var scheduledTask = threadPool.scheduledTasks.poll();
            assertThat(scheduledTask, is(notNullValue()));
            assertThat(scheduledTask.v1(), is(equalTo(heartbeatFrequency)));

            injectWriteHeartBeatFailure.set(true);

            scheduledTask.v2().run();
        }

        assertThat(threadPool.scheduledTasks.poll(), is(nullValue()));

        ExecutionException executionException = expectThrows(ExecutionException.class, completionListener::get);
        assertThat(executionException.getCause(), is(notNullValue()));
        assertThat(executionException.getCause().getMessage(), is(equalTo("Unable to store heart beat")));
    }

    public void testServiceStopsAfterTermBump() throws Exception {
        final var currentTermProvider = new AtomicLong(1);
        final var heartbeatFrequency = TimeValue.timeValueSeconds(randomIntBetween(15, 30));
        final var maxTimeSinceLastHeartbeat = TimeValue.timeValueSeconds(2 * heartbeatFrequency.seconds());

        final var heartbeatStore = new InMemoryHeartbeatStore();
        final var heartbeatService = new StoreHeartbeatService(
            heartbeatStore,
            threadPool,
            heartbeatFrequency,
            maxTimeSinceLastHeartbeat,
            currentTermProvider::get
        );

        PlainActionFuture<Long> completionListener = PlainActionFuture.newFuture();
        final var currentLeader = new DiscoveryNode("master", buildNewFakeTransportAddress(), Version.CURRENT);

        final long currentTerm = currentTermProvider.get();
        boolean termBumpBeforeStart = randomBoolean();
        if (termBumpBeforeStart) {
            currentTermProvider.incrementAndGet();
        }

        heartbeatService.start(currentLeader, currentTerm, completionListener);

        if (termBumpBeforeStart == false) {
            Heartbeat firstHeartbeat = PlainActionFuture.get(heartbeatStore::readLatestHeartbeat);
            assertThat(firstHeartbeat, is(notNullValue()));

            var scheduledTask = threadPool.scheduledTasks.poll();
            assertThat(scheduledTask, is(notNullValue()));
            assertThat(scheduledTask.v1(), is(equalTo(heartbeatFrequency)));

            currentTermProvider.incrementAndGet();

            scheduledTask.v2().run();
        }

        assertThat(threadPool.scheduledTasks.poll(), is(nullValue()));

        long newTerm = completionListener.get();
        assertThat(newTerm, is(equalTo(2L)));
    }

    public void testLeaderCheck() throws Exception {
        final var currentTermProvider = new AtomicLong(1);
        final var heartbeatFrequency = TimeValue.timeValueSeconds(randomIntBetween(15, 30));
        final var maxTimeSinceLastHeartbeat = TimeValue.timeValueSeconds(2 * heartbeatFrequency.seconds());

        final var fakeClock = new AtomicLong();
        final var failReadingHeartbeat = new AtomicBoolean();
        final var heartbeatStore = new InMemoryHeartbeatStore() {
            @Override
            public void readLatestHeartbeat(ActionListener<Heartbeat> listener) {
                if (failReadingHeartbeat.get()) {
                    listener.onFailure(new IOException("Unable to read heartbeat"));
                } else {
                    super.readLatestHeartbeat(listener);
                }
            }
        };
        final var heartbeatService = new StoreHeartbeatService(
            heartbeatStore,
            threadPool,
            heartbeatFrequency,
            maxTimeSinceLastHeartbeat,
            currentTermProvider::get
        ) {
            @Override
            protected long absoluteTimeInMillis() {
                return fakeClock.get();
            }
        };

        // Empty store
        {
            Heartbeat heartbeat = PlainActionFuture.get(heartbeatStore::readLatestHeartbeat);
            assertThat(heartbeat, is(nullValue()));

            AtomicBoolean noRecentLeaderFound = new AtomicBoolean();
            heartbeatService.runIfNoRecentLeader(() -> noRecentLeaderFound.set(true));
            assertThat(noRecentLeaderFound.get(), is(true));
        }

        // Recent heartbeat
        {
            PlainActionFuture.<Void, Exception>get(f -> heartbeatStore.writeHeartbeat(new Heartbeat(1, fakeClock.get()), f));

            AtomicBoolean noRecentLeaderFound = new AtomicBoolean();
            heartbeatService.runIfNoRecentLeader(() -> noRecentLeaderFound.set(true));
            assertThat(noRecentLeaderFound.get(), is(false));
        }

        // Stale heartbeat
        {
            PlainActionFuture.<Void, Exception>get(f -> heartbeatStore.writeHeartbeat(new Heartbeat(1, fakeClock.get()), f));
            fakeClock.set(maxTimeSinceLastHeartbeat.millis() + 1);

            AtomicBoolean noRecentLeaderFound = new AtomicBoolean();
            heartbeatService.runIfNoRecentLeader(() -> noRecentLeaderFound.set(true));
            assertThat(noRecentLeaderFound.get(), is(true));
        }

        // Failing store
        {
            PlainActionFuture.<Void, Exception>get(f -> heartbeatStore.writeHeartbeat(new Heartbeat(1, fakeClock.get()), f));
            fakeClock.set(maxTimeSinceLastHeartbeat.millis() + 1);

            failReadingHeartbeat.set(true);

            AtomicBoolean noRecentLeaderFound = new AtomicBoolean();
            heartbeatService.runIfNoRecentLeader(() -> noRecentLeaderFound.set(true));
            assertThat(noRecentLeaderFound.get(), is(false));
        }
    }
}
