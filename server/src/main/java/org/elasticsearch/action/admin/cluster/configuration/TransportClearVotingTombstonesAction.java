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
package org.elasticsearch.action.admin.cluster.configuration;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchTimeoutException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterState.Builder;
import org.elasticsearch.cluster.ClusterStateObserver;
import org.elasticsearch.cluster.ClusterStateObserver.Listener;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.threadpool.ThreadPool.Names;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.function.Predicate;

public class TransportClearVotingTombstonesAction
    extends TransportMasterNodeAction<ClearVotingTombstonesRequest, ClearVotingTombstonesResponse> {

    @Inject
    public TransportClearVotingTombstonesAction(TransportService transportService, ClusterService clusterService, ThreadPool threadPool,
                                                ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver) {
        super(ClearVotingTombstonesAction.NAME, transportService, clusterService, threadPool, actionFilters,
            ClearVotingTombstonesRequest::new, indexNameExpressionResolver);
    }

    @Override
    protected String executor() {
        return Names.SAME;
    }

    @Override
    protected ClearVotingTombstonesResponse newResponse() {
        throw new UnsupportedOperationException("usage of Streamable is to be replaced by Writeable");
    }

    @Override
    protected ClearVotingTombstonesResponse read(StreamInput in) throws IOException {
        return new ClearVotingTombstonesResponse(in);
    }

    @Override
    protected void masterOperation(ClearVotingTombstonesRequest request, ClusterState initialState,
                                   ActionListener<ClearVotingTombstonesResponse> listener) throws Exception {

        final long startTimeMillis = threadPool.relativeTimeInMillis();

        final Predicate<ClusterState> allTombstonedNodesRemoved = newState -> {
            for (DiscoveryNode tombstone : initialState.getVotingTombstones()) {
                // NB checking for the existence of any node with this persistent ID, because persistent IDs are how votes are counted.
                // Calling nodeExists(tombstone) is insufficient because this compares on the ephemeral ID.
                if (newState.nodes().nodeExists(tombstone.getId())) {
                    return false;
                }
            }
            return true;
        };

        if (request.getWaitForRemoval() && allTombstonedNodesRemoved.test(initialState) == false) {
            final ClusterStateObserver clusterStateObserver = new ClusterStateObserver(initialState, clusterService, request.getTimeout(),
                logger, threadPool.getThreadContext());

            clusterStateObserver.waitForNextChange(new Listener() {
                @Override
                public void onNewClusterState(ClusterState state) {
                    submitClearTombstonesTask(request, startTimeMillis, listener);
                }

                @Override
                public void onClusterServiceClose() {
                    listener.onFailure(new ElasticsearchException("cluster service closed while waiting for removal of nodes "
                        + initialState.getVotingTombstones()));
                }

                @Override
                public void onTimeout(TimeValue timeout) {
                    listener.onFailure(new ElasticsearchTimeoutException(
                        "timed out waiting for removal of nodes; if nodes should not be removed, set waitForRemoval to false. "
                        + initialState.getVotingTombstones()));
                }
            }, allTombstonedNodesRemoved);
        } else {
            submitClearTombstonesTask(request, startTimeMillis, listener);
        }
    }

    private void submitClearTombstonesTask(ClearVotingTombstonesRequest request, long startTimeMillis,
                                           ActionListener<ClearVotingTombstonesResponse> listener) {
        clusterService.submitStateUpdateTask("clear-voting-tombstones", new ClusterStateUpdateTask() {
            @Override
            public ClusterState execute(ClusterState currentState) {
                final Builder builder = ClusterState.builder(currentState);
                builder.clearVotingTombstones();
                return builder.build();
            }

            @Override
            public void onFailure(String source, Exception e) {
                listener.onFailure(e);
            }

            @Override
            public TimeValue timeout() {
                return TimeValue.timeValueMillis(request.getTimeout().millis() + startTimeMillis - threadPool.relativeTimeInMillis());
            }

            @Override
            public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                listener.onResponse(new ClearVotingTombstonesResponse());
            }
        });
    }

    @Override
    protected ClusterBlockException checkBlock(ClearVotingTombstonesRequest request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }
}
