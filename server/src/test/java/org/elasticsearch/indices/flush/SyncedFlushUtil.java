/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.indices.flush;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.InternalTestCluster;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.test.ESTestCase.assertBusy;

/** Utils for SyncedFlush */
public class SyncedFlushUtil {

    private SyncedFlushUtil() {

    }

    /**
     * Blocking version of {@link SyncedFlushService#attemptSyncedFlush(ShardId, ActionListener)}
     */
    public static ShardsSyncedFlushResult attemptSyncedFlush(Logger logger, InternalTestCluster cluster, ShardId shardId) throws Exception {
        /*
         * When the last indexing operation is completed, we will fire a global checkpoint sync.
         * Since a global checkpoint sync request is a replication request, it will acquire an index
         * shard permit on the primary when executing. If this happens at the same time while we are
         * issuing the synced-flush, the synced-flush request will fail as it thinks there are
         * in-flight operations. We can avoid such situation by continuing issuing another synced-flush
         * if the synced-flush failed due to the ongoing operations on the primary.
         */
        SyncedFlushService service = cluster.getInstance(SyncedFlushService.class);
        AtomicReference<LatchedListener<ShardsSyncedFlushResult>> listenerHolder = new AtomicReference<>();
        assertBusy(() -> {
            LatchedListener<ShardsSyncedFlushResult> listener = new LatchedListener<>();
            listenerHolder.set(listener);
            service.attemptSyncedFlush(shardId, listener);
            listener.latch.await();
            if (listener.result != null
                && listener.result.failureReason() != null
                && listener.result.failureReason().contains("ongoing operations on primary")) {
                throw new AssertionError(listener.result.failureReason()); // cause the assert busy to retry
            }
        });
        if (listenerHolder.get().error != null) {
            throw ExceptionsHelper.convertToElastic(listenerHolder.get().error);
        }
        return listenerHolder.get().result;
    }

    public static final class LatchedListener<T> implements ActionListener<T> {
        public volatile T result;
        public volatile Exception error;
        public final CountDownLatch latch = new CountDownLatch(1);

        @Override
        public void onResponse(T result) {
            this.result = result;
            latch.countDown();
        }

        @Override
        public void onFailure(Exception e) {
            error = e;
            latch.countDown();
        }
    }

    /**
     * Blocking version of {@link SyncedFlushService#sendPreSyncRequests(List, ClusterState, ShardId, ActionListener)}
     */
    public static Map<String, SyncedFlushService.PreSyncedFlushResponse> sendPreSyncRequests(
        SyncedFlushService service,
        List<ShardRouting> activeShards,
        ClusterState state,
        ShardId shardId
    ) {
        LatchedListener<Map<String, SyncedFlushService.PreSyncedFlushResponse>> listener = new LatchedListener<>();
        service.sendPreSyncRequests(activeShards, state, shardId, listener);
        try {
            listener.latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        if (listener.error != null) {
            throw ExceptionsHelper.convertToElastic(listener.error);
        }
        return listener.result;
    }
}
