/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.service;

import org.elasticsearch.action.support.RefCountingListener;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.ArrayList;
import java.util.Collections;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.sameInstance;

public class AsyncClusterStateApplierTests extends ESTestCase {

    public void testAwaitCurrentStateApplication() {
        final var threadPool = new TestThreadPool(getTestName());
        try {
            final var initialState = ClusterState.EMPTY_STATE;

            final var clusterServiceState = new AtomicReference<ClusterState>();
            clusterServiceState.set(initialState);

            final var isApplying = new AtomicBoolean();
            final var appliedState = new AtomicReference<>(initialState);
            final var applier = new AsyncClusterStateApplier(event -> {
                assertFalse(isApplying.getAndSet(true));
                assertThat(Thread.currentThread().getName(), containsString("[generic]"));
                assertThat(
                    event.source(),
                    equalTo(
                        "async update state from version ["
                            + event.previousState().version()
                            + "] to version ["
                            + event.state().version()
                            + "]"
                    )
                );
                Thread.yield();
                appliedState.set(event.state());
                assertTrue(isApplying.getAndSet(false));
            }, threadPool.generic());

            runInParallel(() -> {
                for (int i = 0; i < 1000; i++) {
                    final var oldState = clusterServiceState.get();
                    final var newState = ClusterState.builder(oldState).version(oldState.version() + 1L).build();
                    applier.applyClusterState(new ClusterChangedEvent("iteration " + i, newState, oldState));
                    clusterServiceState.set(newState);
                }
            }, () -> safeAwait(l -> {
                try (var listeners = new RefCountingListener(l.map(ignored -> null))) {
                    for (int i = 0; i < 100; i++) {
                        final var expectedVersion = clusterServiceState.get().version();
                        SubscribableListener.newForked(applier::awaitCurrentStateApplication).addListener(listeners.acquire(ignored -> {
                            assertThat(appliedState.get().version(), greaterThanOrEqualTo(expectedVersion));
                        }));
                    }
                }
            }));
        } finally {
            ThreadPool.terminate(threadPool, SAFE_AWAIT_TIMEOUT.seconds(), TimeUnit.SECONDS);
        }
    }

    public void testCompletesListenersEvenWhileActive() {
        final var threadPool = new TestThreadPool(getTestName());
        try {
            final var events = new ClusterChangedEvent[4];
            var oldState = ClusterState.EMPTY_STATE;
            for (int i = 0; i < events.length; i++) {
                final var newState = ClusterState.builder(oldState).version(oldState.version() + 1L).build();
                events[i] = new ClusterChangedEvent("event " + i, newState, oldState);
                oldState = newState;
            }

            final var barrier = new CyclicBarrier(2);
            final var appliedStates = Collections.synchronizedList(new ArrayList<>(events.length));

            final var applier = new AsyncClusterStateApplier(event -> {
                assertThat(Thread.currentThread().getName(), containsString("[generic]"));
                safeAwait(barrier);
                safeAwait(barrier);
                appliedStates.add(event.state()); // after barriers, but checked after waiting on a listener each time so no race
            }, threadPool.generic());

            applier.applyClusterState(events[0]);
            final var apply1Listener = SubscribableListener.newForked(applier::awaitCurrentStateApplication);

            safeAwait(barrier); // first application now running

            applier.applyClusterState(events[1]);
            final var apply2Listener = SubscribableListener.newForked(applier::awaitCurrentStateApplication);

            applier.applyClusterState(events[2]);
            final var apply3Listener = SubscribableListener.newForked(applier::awaitCurrentStateApplication);

            assertFalse(apply1Listener.isDone());
            safeAwait(barrier); // allow first application to complete
            safeAwait(apply1Listener);
            assertThat(appliedStates, contains(sameInstance(events[0].state())));
            assertFalse(apply2Listener.isDone());
            assertFalse(apply3Listener.isDone());

            safeAwait(barrier); // wait for second application to start

            applier.applyClusterState(events[3]);
            final var apply4Listener = SubscribableListener.newForked(applier::awaitCurrentStateApplication);

            assertFalse(apply2Listener.isDone());
            assertFalse(apply3Listener.isDone());
            safeAwait(barrier); // allow second application to complete
            safeAwait(apply2Listener);
            safeAwait(apply3Listener);
            assertThat(appliedStates, contains(sameInstance(events[0].state()), sameInstance(events[2].state())));
            assertFalse(apply4Listener.isDone());

            safeAwait(barrier); // wait for third application to start
            assertFalse(apply4Listener.isDone());
            safeAwait(barrier); // allow third application to complete
            safeAwait(apply4Listener);
            assertThat(
                appliedStates,
                contains(sameInstance(events[0].state()), sameInstance(events[2].state()), sameInstance(events[3].state()))
            );
        } finally {
            ThreadPool.terminate(threadPool, SAFE_AWAIT_TIMEOUT.seconds(), TimeUnit.SECONDS);
        }
    }
}
