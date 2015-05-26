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

package org.elasticsearch.action.admin.indices.seal;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.routing.GroupShardsIterator;
import org.elasticsearch.cluster.routing.ShardIterator;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.common.util.concurrent.CountDown;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.SyncedFlushService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.*;

import java.util.Set;

/**
 */
public class TransportSealIndicesAction extends HandledTransportAction<SealIndicesRequest, SealIndicesResponse> {


    final private SyncedFlushService syncedFlushService;
    final private ClusterService clusterService;

    @Inject
    public TransportSealIndicesAction(Settings settings, ThreadPool threadPool, TransportService transportService, ActionFilters actionFilters, SyncedFlushService syncedFlushService, ClusterService clusterService) {
        super(settings, SealIndicesAction.NAME, threadPool, transportService, actionFilters, SealIndicesRequest.class);
        this.syncedFlushService = syncedFlushService;
        this.clusterService = clusterService;
    }

    @Override
    protected void doExecute(final SealIndicesRequest request, final ActionListener<SealIndicesResponse> listener) {
        ClusterState state = clusterService.state();
        String[] concreteIndices = state.metaData().concreteIndices(request.indicesOptions(), request.indices());
        GroupShardsIterator primaries = state.routingTable().activePrimaryShardsGrouped(concreteIndices, true);
        final Set<SyncedFlushService.SyncedFlushResult> results = ConcurrentCollections.newConcurrentSet();

        final CountDown countDown = new CountDown(primaries.size());

        for (final ShardIterator shard : primaries) {
            if (shard.size() == 0) {
                results.add(new SyncedFlushService.SyncedFlushResult(shard.shardId(), "no active primary available"));
                if (countDown.countDown()) {
                    listener.onResponse(new SealIndicesResponse(results));
                }
            } else {
                final ShardId shardId = shard.shardId();
                syncedFlushService.attemptSyncedFlush(shardId, new ActionListener<SyncedFlushService.SyncedFlushResult>() {
                    @Override
                    public void onResponse(SyncedFlushService.SyncedFlushResult syncedFlushResult) {
                        results.add(syncedFlushResult);
                        if (countDown.countDown()) {
                            listener.onResponse(new SealIndicesResponse(results));
                        }
                    }

                    @Override
                    public void onFailure(Throwable e) {
                        logger.debug("{} unexpected error while executing synced flush", shardId);
                        results.add(new SyncedFlushService.SyncedFlushResult(shardId, e.getMessage()));
                        if (countDown.countDown()) {
                            listener.onResponse(new SealIndicesResponse(results));
                        }
                    }
                });
            }
        }

    }
}
