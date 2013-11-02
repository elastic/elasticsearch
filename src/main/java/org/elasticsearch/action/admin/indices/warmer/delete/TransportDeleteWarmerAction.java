/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.action.admin.indices.warmer.delete;

import com.google.common.collect.Lists;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.master.TransportMasterNodeOperationAction;
import org.elasticsearch.cluster.AckedClusterStateUpdateTask;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.indices.IndexMissingException;
import org.elasticsearch.search.warmer.IndexWarmerMissingException;
import org.elasticsearch.search.warmer.IndexWarmersMetaData;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.List;

/**
 * Delete index warmer.
 */
public class TransportDeleteWarmerAction extends TransportMasterNodeOperationAction<DeleteWarmerRequest, DeleteWarmerResponse> {

    @Inject
    public TransportDeleteWarmerAction(Settings settings, TransportService transportService, ClusterService clusterService, ThreadPool threadPool) {
        super(settings, transportService, clusterService, threadPool);
    }

    @Override
    protected String executor() {
        // we go async right away
        return ThreadPool.Names.SAME;
    }

    @Override
    protected String transportAction() {
        return DeleteWarmerAction.NAME;
    }

    @Override
    protected DeleteWarmerRequest newRequest() {
        return new DeleteWarmerRequest();
    }

    @Override
    protected DeleteWarmerResponse newResponse() {
        return new DeleteWarmerResponse();
    }

    @Override
    protected void doExecute(DeleteWarmerRequest request, ActionListener<DeleteWarmerResponse> listener) {
        // update to concrete indices
        request.indices(clusterService.state().metaData().concreteIndices(request.indices()));
        super.doExecute(request, listener);
    }

    @Override
    protected ClusterBlockException checkBlock(DeleteWarmerRequest request, ClusterState state) {
        return state.blocks().indicesBlockedException(ClusterBlockLevel.METADATA, request.indices());
    }

    @Override
    protected void masterOperation(final DeleteWarmerRequest request, final ClusterState state, final ActionListener<DeleteWarmerResponse> listener) throws ElasticSearchException {
        clusterService.submitStateUpdateTask("delete_warmer [" + request.name() + "]", new AckedClusterStateUpdateTask() {

            @Override
            public boolean mustAck(DiscoveryNode discoveryNode) {
                return true;
            }

            @Override
            public void onAllNodesAcked(@Nullable Throwable t) {
                listener.onResponse(new DeleteWarmerResponse(true));
            }

            @Override
            public void onAckTimeout() {
                listener.onResponse(new DeleteWarmerResponse(false));
            }

            @Override
            public TimeValue ackTimeout() {
                return request.timeout();
            }

            @Override
            public TimeValue timeout() {
                return request.masterNodeTimeout();
            }

            @Override
            public void onFailure(String source, Throwable t) {
                logger.debug("failed to delete warmer [{}] on indices [{}]", t, request.name(), request.indices());
                listener.onFailure(t);
            }

            @Override
            public ClusterState execute(ClusterState currentState) {
                MetaData.Builder mdBuilder = MetaData.builder(currentState.metaData());

                boolean globalFoundAtLeastOne = false;
                for (String index : request.indices()) {
                    IndexMetaData indexMetaData = currentState.metaData().index(index);
                    if (indexMetaData == null) {
                        throw new IndexMissingException(new Index(index));
                    }
                    IndexWarmersMetaData warmers = indexMetaData.custom(IndexWarmersMetaData.TYPE);
                    if (warmers != null) {
                        List<IndexWarmersMetaData.Entry> entries = Lists.newArrayList();
                        for (IndexWarmersMetaData.Entry entry : warmers.entries()) {
                            if (request.name() == null || Regex.simpleMatch(request.name(), entry.name())) {
                                globalFoundAtLeastOne = true;
                                // don't add it...
                            } else {
                                entries.add(entry);
                            }
                        }
                        // a change, update it...
                        if (entries.size() != warmers.entries().size()) {
                            warmers = new IndexWarmersMetaData(entries.toArray(new IndexWarmersMetaData.Entry[entries.size()]));
                            IndexMetaData.Builder indexBuilder = IndexMetaData.builder(indexMetaData).putCustom(IndexWarmersMetaData.TYPE, warmers);
                            mdBuilder.put(indexBuilder);
                        }
                    }
                }

                if (!globalFoundAtLeastOne) {
                    if (request.name() == null) {
                        // full match, just return with no failure
                        return currentState;
                    }
                    throw new IndexWarmerMissingException(request.name());
                }

                if (logger.isInfoEnabled()) {
                    for (String index : request.indices()) {
                        IndexMetaData indexMetaData = currentState.metaData().index(index);
                        if (indexMetaData == null) {
                            throw new IndexMissingException(new Index(index));
                        }
                        IndexWarmersMetaData warmers = indexMetaData.custom(IndexWarmersMetaData.TYPE);
                        if (warmers != null) {
                            for (IndexWarmersMetaData.Entry entry : warmers.entries()) {
                                if (Regex.simpleMatch(request.name(), entry.name())) {
                                    logger.info("[{}] delete warmer [{}]", index, entry.name());
                                }
                            }
                        }
                    }
                }

                return ClusterState.builder(currentState).metaData(mdBuilder).build();
            }

            @Override
            public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {

            }
        });
    }
}
