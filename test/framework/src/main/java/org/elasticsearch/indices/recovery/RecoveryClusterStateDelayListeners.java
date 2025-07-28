/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.indices.recovery;

import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.core.AbstractRefCounted;
import org.elasticsearch.core.RefCounted;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import static org.hamcrest.Matchers.greaterThanOrEqualTo;

/**
 * Allows one node in an integ test to delay the application of cluster states on another node until a recovery starts, without unduly
 * delaying other cluster states. The controlling node gets a listener for each cluster state version it applies using {@link
 * #getClusterStateDelayListener} and either completes it straight away (when applying states unrelated to the recovery) or delays it with
 * {@link #delayUntilRecoveryStart} and releases the delay with {@link #onStartRecovery()}; meanwhile the other node gets the same listener
 * for each cluster state commit message and uses it to delay handling the commit.
 */
public class RecoveryClusterStateDelayListeners implements Releasable {
    private final Map<Long, SubscribableListener<Void>> clusterStateBarriers = ConcurrentCollections.newConcurrentMap();
    private final SubscribableListener<Void> startRecoveryListener = new SubscribableListener<>();

    private final CountDownLatch completeLatch = new CountDownLatch(1);
    private final RefCounted refCounted = AbstractRefCounted.of(completeLatch::countDown);
    private final List<Runnable> cleanup = new ArrayList<>(2);
    private final long initialClusterStateVersion;

    public RecoveryClusterStateDelayListeners(long initialClusterStateVersion) {
        this.initialClusterStateVersion = initialClusterStateVersion;
    }

    @Override
    public void close() {
        refCounted.decRef();
        ESTestCase.safeAwait(completeLatch);
        cleanup.forEach(Runnable::run);
        clusterStateBarriers.values().forEach(l -> l.onResponse(null));
    }

    public void addCleanup(Runnable runnable) {
        cleanup.add(runnable);
    }

    public SubscribableListener<Void> getClusterStateDelayListener(long clusterStateVersion) {
        ESTestCase.assertThat(clusterStateVersion, greaterThanOrEqualTo(initialClusterStateVersion));
        if (refCounted.tryIncRef()) {
            try {
                return clusterStateBarriers.computeIfAbsent(clusterStateVersion, ignored -> new SubscribableListener<>());
            } finally {
                refCounted.decRef();
            }
        } else {
            return SubscribableListener.nullSuccess();
        }
    }

    public void onStartRecovery() {
        Thread.yield();
        ESTestCase.assertFalse(startRecoveryListener.isDone());
        startRecoveryListener.onResponse(null);
    }

    public void delayUntilRecoveryStart(SubscribableListener<Void> listener) {
        startRecoveryListener.addListener(listener);
    }
}
