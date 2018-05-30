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
package org.elasticsearch.indices.flush;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.test.InternalTestCluster;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

import static org.elasticsearch.test.ESTestCase.assertBusy;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

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
         * in-flight operations. We can avoid such situation by not issue the synced-flush until the
         * global checkpoint on the primary is propagated to replicas.
         */
        assertBusy(() -> {
            long globalCheckpointOnPrimary = SequenceNumbers.NO_OPS_PERFORMED;
            Set<String> assignedNodes = cluster.nodesInclude(shardId.getIndexName());
            for (String node : assignedNodes) {
                IndicesService indicesService = cluster.getInstance(IndicesService.class, node);
                IndexShard shard = indicesService.indexServiceSafe(shardId.getIndex()).getShard(shardId.id());
                if (shard.routingEntry().primary()) {
                    globalCheckpointOnPrimary = shard.getGlobalCheckpoint();
                }
            }
            for (String node : assignedNodes) {
                IndicesService indicesService = cluster.getInstance(IndicesService.class, node);
                IndexShard shard = indicesService.indexServiceSafe(shardId.getIndex()).getShard(shardId.id());
                assertThat(shard.getLastSyncedGlobalCheckpoint(), equalTo(globalCheckpointOnPrimary));
            }
        });
        SyncedFlushService service = cluster.getInstance(SyncedFlushService.class);
        LatchedListener<ShardsSyncedFlushResult> listener = new LatchedListener<>();
        service.attemptSyncedFlush(shardId, listener);
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
    public static Map<String, SyncedFlushService.PreSyncedFlushResponse> sendPreSyncRequests(SyncedFlushService service, List<ShardRouting> activeShards, ClusterState state, ShardId shardId) {
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
