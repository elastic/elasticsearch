/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.cluster.metadata;

import com.google.common.collect.Lists;
import org.elasticsearch.action.admin.indices.warmer.delete.DeleteWarmerClusterStateUpdateRequest;
import org.elasticsearch.action.admin.indices.warmer.put.PutWarmerClusterStateUpdateRequest;
import org.elasticsearch.cluster.AckedClusterStateUpdateTask;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ack.ClusterStateUpdateListener;
import org.elasticsearch.cluster.ack.ClusterStateUpdateResponse;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.indices.IndexMissingException;
import org.elasticsearch.search.warmer.IndexWarmerMissingException;
import org.elasticsearch.search.warmer.IndexWarmersMetaData;

import java.util.ArrayList;
import java.util.List;

/**
 * Service responsible for submitting pu and delete warmer requests
 */
public class MetaDataWarmersService extends AbstractComponent {

    final ClusterService clusterService;

    @Inject
    public MetaDataWarmersService(Settings settings, ClusterService clusterService) {
        super(settings);
        this.clusterService = clusterService;
    }

    public void putWarmer(final PutWarmerClusterStateUpdateRequest request, final ClusterStateUpdateListener<ClusterStateUpdateResponse> listener) {
        clusterService.submitStateUpdateTask("put_warmer [" + request.name() + "]", new AckedClusterStateUpdateTask() {

            @Override
            public boolean mustAck(DiscoveryNode discoveryNode) {
                return true;
            }

            @Override
            public void onAllNodesAcked(@Nullable Throwable t) {
                listener.onResponse(new ClusterStateUpdateResponse(true));
            }

            @Override
            public void onAckTimeout() {
                listener.onResponse(new ClusterStateUpdateResponse(false));
            }

            @Override
            public TimeValue ackTimeout() {
                return request.ackTimeout();
            }

            @Override
            public TimeValue timeout() {
                return request.masterNodeTimeout();
            }

            @Override
            public void onFailure(String source, Throwable t) {
                logger.debug("failed to put warmer [{}] on indices [{}]", t, request.name(), request.indices());
                listener.onFailure(t);
            }

            @Override
            public ClusterState execute(ClusterState currentState) {
                MetaData metaData = currentState.metaData();
                String[] concreteIndices = metaData.concreteIndices(request.indices());

                // now replace it on the metadata
                MetaData.Builder mdBuilder = MetaData.builder().metaData(currentState.metaData());

                for (String index : concreteIndices) {
                    IndexMetaData indexMetaData = metaData.index(index);
                    if (indexMetaData == null) {
                        throw new IndexMissingException(new Index(index));
                    }
                    IndexWarmersMetaData warmers = indexMetaData.custom(IndexWarmersMetaData.TYPE);
                    if (warmers == null) {
                        logger.info("[{}] putting warmer [{}]", index, request.name());
                        warmers = new IndexWarmersMetaData(new IndexWarmersMetaData.Entry(request.name(), request.types(), request.source()));
                    } else {
                        boolean found = false;
                        List<IndexWarmersMetaData.Entry> entries = new ArrayList<IndexWarmersMetaData.Entry>(warmers.entries().size() + 1);
                        for (IndexWarmersMetaData.Entry entry : warmers.entries()) {
                            if (entry.name().equals(request.name())) {
                                found = true;
                                entries.add(new IndexWarmersMetaData.Entry(request.name(), request.types(), request.source()));
                            } else {
                                entries.add(entry);
                            }
                        }
                        if (!found) {
                            logger.info("[{}] put warmer [{}]", index, request.name());
                            entries.add(new IndexWarmersMetaData.Entry(request.name(), request.types(), request.source()));
                        } else {
                            logger.info("[{}] update warmer [{}]", index, request.name());
                        }
                        warmers = new IndexWarmersMetaData(entries.toArray(new IndexWarmersMetaData.Entry[entries.size()]));
                    }
                    IndexMetaData.Builder indexBuilder = IndexMetaData.newIndexMetaDataBuilder(indexMetaData).putCustom(IndexWarmersMetaData.TYPE, warmers);
                    mdBuilder.put(indexBuilder);
                }

                return ClusterState.builder().state(currentState).metaData(mdBuilder).build();
            }

            @Override
            public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {

            }
        });
    }

    public void deleteWarmer(final DeleteWarmerClusterStateUpdateRequest request, final ClusterStateUpdateListener<ClusterStateUpdateResponse> listener) {

        clusterService.submitStateUpdateTask("delete_warmer [" + request.name() + "]", new AckedClusterStateUpdateTask() {

            @Override
            public boolean mustAck(DiscoveryNode discoveryNode) {
                return true;
            }

            @Override
            public void onAllNodesAcked(@Nullable Throwable t) {
                listener.onResponse(new ClusterStateUpdateResponse(true));
            }

            @Override
            public void onAckTimeout() {
                listener.onResponse(new ClusterStateUpdateResponse(false));
            }

            @Override
            public TimeValue ackTimeout() {
                return request.ackTimeout();
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
                MetaData.Builder mdBuilder = MetaData.builder().metaData(currentState.metaData());

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
                            IndexMetaData.Builder indexBuilder = IndexMetaData.newIndexMetaDataBuilder(indexMetaData).putCustom(IndexWarmersMetaData.TYPE, warmers);
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

                return ClusterState.builder().state(currentState).metaData(mdBuilder).build();
            }

            @Override
            public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {

            }
        });
    }
}
