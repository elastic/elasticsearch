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

package org.elasticsearch.action.support.replication;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionWriteResponse;
import org.elasticsearch.action.ActionWriteResponse.ShardInfo.Failure;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.routing.GroupShardsIterator;
import org.elasticsearch.cluster.routing.ShardIterator;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReferenceArray;

/**
 * Internal transport action that executes on multiple shards, doesn't register any transport handler as it is always executed locally.
 * It relies on a shard sub-action that gets sent over the transport and executed on each of the shard.
 * The index provided with the request is expected to be a concrete index, properly resolved by the callers (parent actions).
 */
public abstract class TransportIndexReplicationOperationAction<Request extends IndexReplicationOperationRequest, Response extends ActionResponse, ShardRequest extends ShardReplicationOperationRequest, ShardResponse extends ActionWriteResponse>
        extends TransportAction<Request, Response> {

    protected final ClusterService clusterService;

    protected final TransportShardReplicationOperationAction<ShardRequest, ShardRequest, ShardResponse> shardAction;

    protected TransportIndexReplicationOperationAction(Settings settings, String actionName, ClusterService clusterService,
                                                       ThreadPool threadPool, TransportShardReplicationOperationAction<ShardRequest, ShardRequest, ShardResponse> shardAction, ActionFilters actionFilters) {
        super(settings, actionName, threadPool, actionFilters);
        this.clusterService = clusterService;
        this.shardAction = shardAction;
    }

    @Override
    protected void doExecute(final Request request, final ActionListener<Response> listener) {
        ClusterState clusterState = clusterService.state();
        ClusterBlockException blockException = checkGlobalBlock(clusterState, request);
        if (blockException != null) {
            throw blockException;
        }
        blockException = checkRequestBlock(clusterState, request);
        if (blockException != null) {
            throw blockException;
        }

        final GroupShardsIterator groups;
        try {
            groups = shards(request);
        } catch (Throwable e) {
            listener.onFailure(e);
            return;
        }
        final AtomicInteger indexCounter = new AtomicInteger();
        final AtomicInteger failureCounter = new AtomicInteger();
        final AtomicInteger completionCounter = new AtomicInteger(groups.size());
        final AtomicReferenceArray<ShardActionResult> shardsResponses = new AtomicReferenceArray<>(groups.size());

        for (final ShardIterator shardIt : groups) {
            final ShardRequest shardRequest = newShardRequestInstance(request, shardIt.shardId().id());
            shardRequest.operationThreaded(true);
            // no need for threaded listener, we will fork when its done based on the index request
            shardRequest.listenerThreaded(false);
            shardAction.execute(shardRequest, new ActionListener<ShardResponse>() {
                @Override
                public void onResponse(ShardResponse result) {
                    shardsResponses.set(indexCounter.getAndIncrement(), new ShardActionResult(result));
                    returnIfNeeded();
                }

                @Override
                public void onFailure(Throwable e) {
                    failureCounter.getAndIncrement();
                    int index = indexCounter.getAndIncrement();
                    // this is a failure for an entire shard group, constructs shard info accordingly
                    final RestStatus status = ExceptionsHelper.status(e);
                    Failure failure = new Failure(request.index(), shardIt.shardId().id(), null, e, status, true);
                    shardsResponses.set(index, new ShardActionResult(new ActionWriteResponse.ShardInfo(shardIt.size(), 0, failure)));
                    returnIfNeeded();
                }

                private void returnIfNeeded() {
                    if (completionCounter.decrementAndGet() == 0) {
                        List<ShardResponse> responses = new ArrayList<>();
                        List<Failure> failureList = new ArrayList<>();

                        int total = 0;
                        int successful = 0;
                        for (int i = 0; i < shardsResponses.length(); i++) {
                            ShardActionResult shardActionResult = shardsResponses.get(i);
                            final ActionWriteResponse.ShardInfo sf;
                            if (shardActionResult.isFailure()) {
                                assert shardActionResult.shardInfoOnFailure != null;
                                sf = shardActionResult.shardInfoOnFailure;
                            } else {
                                responses.add(shardActionResult.shardResponse);
                                sf = shardActionResult.shardResponse.getShardInfo();
                            }
                            total += sf.getTotal();
                            successful += sf.getSuccessful();
                            failureList.addAll(Arrays.asList(sf.getFailures()));
                        }
                        assert failureList.size() == 0 || numShardGroupFailures(failureList) == failureCounter.get();

                        final Failure[] failures;
                        if (failureList.isEmpty()) {
                            failures = ActionWriteResponse.EMPTY;
                        } else {
                            failures = failureList.toArray(new Failure[failureList.size()]);
                        }
                        listener.onResponse(newResponseInstance(request, responses, new ActionWriteResponse.ShardInfo(total, successful, failures)));
                    }
                }

                private int numShardGroupFailures(List<Failure> failures) {
                    int numShardGroupFailures = 0;
                    for (Failure failure : failures) {
                        if (failure.primary()) {
                            numShardGroupFailures++;
                        }
                    }
                    return numShardGroupFailures;
                }
            });

        }
    }

    protected abstract Response newResponseInstance(Request request, List<ShardResponse> shardResponses, ActionWriteResponse.ShardInfo shardInfo);

    protected abstract GroupShardsIterator shards(Request request) throws ElasticsearchException;

    protected abstract ShardRequest newShardRequestInstance(Request request, int shardId);

    protected ClusterBlockException checkGlobalBlock(ClusterState state, Request request) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.WRITE);
    }

    protected ClusterBlockException checkRequestBlock(ClusterState state, Request request) {
        return state.blocks().indexBlockedException(ClusterBlockLevel.WRITE, request.index());
    }

    private class ShardActionResult {

        private final ShardResponse shardResponse;
        private final ActionWriteResponse.ShardInfo shardInfoOnFailure;

        private ShardActionResult(ShardResponse shardResponse) {
            assert shardResponse != null;
            this.shardResponse = shardResponse;
            this.shardInfoOnFailure = null;
        }

        private ShardActionResult(ActionWriteResponse.ShardInfo shardInfoOnFailure) {
            assert shardInfoOnFailure != null;
            this.shardInfoOnFailure = shardInfoOnFailure;
            this.shardResponse = null;
        }

        boolean isFailure() {
            return shardInfoOnFailure != null;
        }
    }
}
