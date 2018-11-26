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
import org.elasticsearch.cluster.ClusterStateObserver;
import org.elasticsearch.cluster.ClusterStateObserver.Listener;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.coordination.CoordinationMetaData;
import org.elasticsearch.cluster.coordination.CoordinationMetaData.VotingTombstone;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.threadpool.ThreadPool.Names;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class TransportAddVotingTombstonesAction extends TransportMasterNodeAction<AddVotingTombstonesRequest, AddVotingTombstonesResponse> {

    public static final Setting<Integer> MAXIMUM_VOTING_TOMBSTONES_SETTING
        = Setting.intSetting("cluster.max_voting_tombstones", 10, 1, Property.Dynamic, Property.NodeScope);

    @Inject
    public TransportAddVotingTombstonesAction(TransportService transportService, ClusterService clusterService, ThreadPool threadPool,
                                              ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver) {
        super(AddVotingTombstonesAction.NAME, transportService, clusterService, threadPool, actionFilters, AddVotingTombstonesRequest::new,
            indexNameExpressionResolver);
    }

    @Override
    protected String executor() {
        return Names.SAME;
    }

    @Override
    protected AddVotingTombstonesResponse newResponse() {
        throw new UnsupportedOperationException("usage of Streamable is to be replaced by Writeable");
    }

    @Override
    protected AddVotingTombstonesResponse read(StreamInput in) throws IOException {
        return new AddVotingTombstonesResponse(in);
    }

    @Override
    protected void masterOperation(AddVotingTombstonesRequest request, ClusterState state,
                                   ActionListener<AddVotingTombstonesResponse> listener) throws Exception {

        resolveVotingTombstonesAndCheckMaximum(request, state); // throws IllegalArgumentException if no nodes matched or maximum exceeded

        clusterService.submitStateUpdateTask("add-voting-tombstones", new ClusterStateUpdateTask(Priority.URGENT) {

            private Set<VotingTombstone> resolvedNodes;

            @Override
            public ClusterState execute(ClusterState currentState) {
                assert resolvedNodes == null : resolvedNodes;
                resolvedNodes = resolveVotingTombstonesAndCheckMaximum(request, currentState);

                final CoordinationMetaData.Builder builder = CoordinationMetaData.builder(currentState.coordinationMetaData());
                resolvedNodes.forEach(builder::addVotingTombstone);
                final MetaData newMetaData = MetaData.builder(currentState.metaData()).coordinationMetaData(builder.build()).build();
                final ClusterState newState = ClusterState.builder(currentState).metaData(newMetaData).build();
                assert newState.getVotingTombstones().size() <= MAXIMUM_VOTING_TOMBSTONES_SETTING.get(currentState.metaData().settings());
                return newState;
            }

            @Override
            public void onFailure(String source, Exception e) {
                listener.onFailure(e);
            }

            @Override
            public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {

                final ClusterStateObserver observer
                    = new ClusterStateObserver(clusterService, request.getTimeout(), logger, threadPool.getThreadContext());

                final Set<String> resolvedNodeIds = resolvedNodes.stream().map(VotingTombstone::getNodeId).collect(Collectors.toSet());

                final Predicate<ClusterState> allNodesRemoved = clusterState -> {
                    final Set<String> votingNodeIds = clusterState.getLastCommittedConfiguration().getNodeIds();
                    return resolvedNodeIds.stream().noneMatch(votingNodeIds::contains);
                };

                final Listener clusterStateListener = new Listener() {
                    @Override
                    public void onNewClusterState(ClusterState state) {
                        listener.onResponse(new AddVotingTombstonesResponse());
                    }

                    @Override
                    public void onClusterServiceClose() {
                        listener.onFailure(new ElasticsearchException("cluster service closed while waiting for withdrawal of votes from "
                            + resolvedNodes));
                    }

                    @Override
                    public void onTimeout(TimeValue timeout) {
                        listener.onFailure(new ElasticsearchTimeoutException("timed out waiting for withdrawal of votes from "
                            + resolvedNodes));
                    }
                };

                if (allNodesRemoved.test(newState)) {
                    clusterStateListener.onNewClusterState(newState);
                } else {
                    observer.waitForNextChange(clusterStateListener, allNodesRemoved);
                }
            }
        });
    }

    private static Set<VotingTombstone> resolveVotingTombstonesAndCheckMaximum(AddVotingTombstonesRequest request, ClusterState state) {
        return request.resolveVotingTombstonesAndCheckMaximum(state,
            MAXIMUM_VOTING_TOMBSTONES_SETTING.get(state.metaData().settings()), MAXIMUM_VOTING_TOMBSTONES_SETTING.getKey());
    }

    @Override
    protected ClusterBlockException checkBlock(AddVotingTombstonesRequest request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }
}
