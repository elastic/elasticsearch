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

package org.elasticsearch.action.admin.indices.syncedflush;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.GroupShardsIterator;
import org.elasticsearch.cluster.routing.ShardIterator;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.SyncedFlushService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.*;

import java.io.IOException;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Synced flush action
 */
public class TransportSyncedFlushIndicesAction extends HandledTransportAction<SyncedFlushIndicesRequest, SyncedFlushIndicesResponse> {


    final private SyncedFlushService syncedFlushService;
    final private ClusterService clusterService;
    private final TransportService transportService;
    public static final String INDICES_SYNCED_FLUSH_ACTION_NAME = "internal:indices/flush/synced/indices";

    @Inject
    public TransportSyncedFlushIndicesAction(Settings settings, ThreadPool threadPool, TransportService transportService, ActionFilters actionFilters, SyncedFlushService syncedFlushService, ClusterService clusterService) {
        super(settings, SyncedFlushIndicesAction.NAME, threadPool, transportService, actionFilters, SyncedFlushIndicesRequest.class);
        this.syncedFlushService = syncedFlushService;
        this.clusterService = clusterService;
        this.transportService = transportService;
        transportService.registerRequestHandler(INDICES_SYNCED_FLUSH_ACTION_NAME, ShardSyncedFlushRequest.class, ThreadPool.Names.GENERIC, new ShardSyncedFlushHandler());
    }


    @Override
    protected void doExecute(final SyncedFlushIndicesRequest request, final ActionListener<SyncedFlushIndicesResponse> listener) {
        // we need to execute in a thread here because getSyncedFlushResults(request, state) blocks
        threadPool.generic().execute(new AbstractRunnable() {
            @Override
            public void onFailure(Throwable t) {
                listener.onFailure(t);
            }

            @Override
            protected void doRun() throws Exception {
                ClusterState state = clusterService.state();
                final Set<SyncedFlushService.SyncedFlushResult> results = getSyncedFlushResults(request, state);
                listener.onResponse(new SyncedFlushIndicesResponse(results));
            }
        });

    }

    private Set<SyncedFlushService.SyncedFlushResult> getSyncedFlushResults(SyncedFlushIndicesRequest request, ClusterState state) {
        String[] concreteIndices = state.metaData().concreteIndices(request.indicesOptions(), request.indices());
        GroupShardsIterator primaries = state.routingTable().activePrimaryShardsGrouped(concreteIndices, false);
        final CountDownLatch countDownLatch = new CountDownLatch(primaries.size());
        DiscoveryNode localNode = state.getNodes().localNode();
        final Set<SyncedFlushService.SyncedFlushResult> results = ConcurrentCollections.newConcurrentSet();
        for (ShardIterator shard : primaries) {
            final ShardId shardId = shard.shardId();
            transportService.sendRequest(localNode, INDICES_SYNCED_FLUSH_ACTION_NAME, new ShardSyncedFlushRequest(shardId),
                    new BaseTransportResponseHandler<SyncedFlushService.SyncedFlushResult>() {
                        @Override
                        public SyncedFlushService.SyncedFlushResult newInstance() {
                            return new SyncedFlushService.SyncedFlushResult();
                        }

                        @Override
                        public void handleResponse(SyncedFlushService.SyncedFlushResult response) {
                            results.add(response);
                            countDownLatch.countDown();
                        }

                        @Override
                        public void handleException(TransportException exp) {
                            logger.debug("{} unexpected error while executing synced flush", shardId);
                            results.add(new SyncedFlushService.SyncedFlushResult(shardId, exp.getMessage()));
                            countDownLatch.countDown();
                        }

                        @Override
                        public String executor() {
                            return ThreadPool.Names.GENERIC;
                        }
                    });
        }
        try {
            if (countDownLatch.await(request.getTimeout(), TimeUnit.MILLISECONDS) == false) {
                logger.warn("waiting for synced flush timed out");
            }
        } catch (InterruptedException e) {
            logger.warn("interrupted while waiting for synced flush to finish");
        }
        return results;
    }

    final static class ShardSyncedFlushRequest extends TransportRequest {
        private ShardId shardId;

        @Override
        public String toString() {
            return "ShardSyncedFlushRequest{" +
                    "shardId=" + shardId +
                    '}';
        }

        ShardSyncedFlushRequest() {
        }

        public ShardSyncedFlushRequest(ShardId shardId) {
            this.shardId = shardId;
        }


        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            shardId.writeTo(out);
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            this.shardId = ShardId.readShardId(in);
        }

        public ShardId shardId() {
            return shardId;
        }
    }


    private class ShardSyncedFlushHandler implements TransportRequestHandler<ShardSyncedFlushRequest> {

        @Override
        public void messageReceived(final ShardSyncedFlushRequest request, final TransportChannel channel) throws Exception {
            syncedFlushService.attemptSyncedFlush(request.shardId(), new ActionListener<SyncedFlushService.SyncedFlushResult>() {

                @Override
                public void onResponse(SyncedFlushService.SyncedFlushResult syncedFlushResult) {
                    try {
                        channel.sendResponse(syncedFlushResult);
                    } catch (IOException e) {
                        logger.warn("failed to send response for synced flush on shard {}, cause: {}", request.shardId(), e.getMessage());
                    }
                }

                @Override
                public void onFailure(Throwable e) {
                    try {
                        channel.sendResponse(new SyncedFlushService.SyncedFlushResult(request.shardId(), e.getMessage()));
                    } catch (IOException e1) {
                        logger.warn("failed to send failure for synced flush on shard {}, cause: {}", request.shardId(), e.getMessage());
                    }
                }
            });
        }
    }
}
