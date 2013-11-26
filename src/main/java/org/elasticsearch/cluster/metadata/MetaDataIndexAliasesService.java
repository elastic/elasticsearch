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

import com.carrotsearch.hppc.cursors.ObjectCursor;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.elasticsearch.ElasticSearchIllegalArgumentException;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesClusterStateUpdateRequest;
import org.elasticsearch.cluster.AckedClusterStateUpdateTask;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ack.ClusterStateUpdateListener;
import org.elasticsearch.cluster.ack.ClusterStateUpdateResponse;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.query.IndexQueryParserService;
import org.elasticsearch.index.service.IndexService;
import org.elasticsearch.indices.IndexMissingException;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.InvalidAliasNameException;

import java.util.List;
import java.util.Map;

/**
 * Service responsible for submitting add and remove aliases requests
 */
public class MetaDataIndexAliasesService extends AbstractComponent {

    private final ClusterService clusterService;

    private final IndicesService indicesService;

    @Inject
    public MetaDataIndexAliasesService(Settings settings, ClusterService clusterService, IndicesService indicesService) {
        super(settings);
        this.clusterService = clusterService;
        this.indicesService = indicesService;
    }

    public void indicesAliases(final IndicesAliasesClusterStateUpdateRequest request, final ClusterStateUpdateListener listener) {
        clusterService.submitStateUpdateTask("index-aliases", Priority.URGENT, new AckedClusterStateUpdateTask() {

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
                listener.onFailure(t);
            }

            @Override
            public ClusterState execute(final ClusterState currentState) {
                List<String> indicesToClose = Lists.newArrayList();
                Map<String, IndexService> indices = Maps.newHashMap();
                try {
                    for (AliasAction aliasAction : request.actions()) {
                        if (!Strings.hasText(aliasAction.alias()) || !Strings.hasText(aliasAction.index())) {
                            throw new ElasticSearchIllegalArgumentException("Index name and alias name are required");
                        }
                        if (!currentState.metaData().hasIndex(aliasAction.index())) {
                            throw new IndexMissingException(new Index(aliasAction.index()));
                        }
                        if (currentState.metaData().hasIndex(aliasAction.alias())) {
                            throw new InvalidAliasNameException(new Index(aliasAction.index()), aliasAction.alias(), "an index exists with the same name as the alias");
                        }
                        if (aliasAction.indexRouting() != null && aliasAction.indexRouting().indexOf(',') != -1) {
                            throw new ElasticSearchIllegalArgumentException("alias [" + aliasAction.alias() + "] has several routing values associated with it");
                        }
                    }

                    boolean changed = false;
                    MetaData.Builder builder = MetaData.builder(currentState.metaData());
                    for (AliasAction aliasAction : request.actions()) {
                        IndexMetaData indexMetaData = builder.get(aliasAction.index());
                        if (indexMetaData == null) {
                            throw new IndexMissingException(new Index(aliasAction.index()));
                        }
                        // TODO: not copy (putAll)
                        IndexMetaData.Builder indexMetaDataBuilder = IndexMetaData.builder(indexMetaData);
                        if (aliasAction.actionType() == AliasAction.Type.ADD) {
                            String filter = aliasAction.filter();
                            if (Strings.hasLength(filter)) {
                                // parse the filter, in order to validate it
                                IndexService indexService = indices.get(indexMetaData.index());
                                if (indexService == null) {
                                    indexService = indicesService.indexService(indexMetaData.index());
                                    if (indexService == null) {
                                        // temporarily create the index and add mappings so we have can parse the filter
                                        try {
                                            indexService = indicesService.createIndex(indexMetaData.index(), indexMetaData.settings(), clusterService.localNode().id());
                                            if (indexMetaData.mappings().containsKey(MapperService.DEFAULT_MAPPING)) {
                                                indexService.mapperService().merge(MapperService.DEFAULT_MAPPING, indexMetaData.mappings().get(MapperService.DEFAULT_MAPPING).source(), false);
                                            }
                                            for (ObjectCursor<MappingMetaData> cursor : indexMetaData.mappings().values()) {
                                                MappingMetaData mappingMetaData = cursor.value;
                                                indexService.mapperService().merge(mappingMetaData.type(), mappingMetaData.source(), false);
                                            }
                                        } catch (Exception e) {
                                            logger.warn("[{}] failed to temporary create in order to apply alias action", e, indexMetaData.index());
                                            continue;
                                        }
                                        indicesToClose.add(indexMetaData.index());
                                    }
                                    indices.put(indexMetaData.index(), indexService);
                                }

                                // now, parse the filter
                                IndexQueryParserService indexQueryParser = indexService.queryParserService();
                                try {
                                    XContentParser parser = XContentFactory.xContent(filter).createParser(filter);
                                    try {
                                        indexQueryParser.parseInnerFilter(parser);
                                    } finally {
                                        parser.close();
                                    }
                                } catch (Throwable e) {
                                    throw new ElasticSearchIllegalArgumentException("failed to parse filter for [" + aliasAction.alias() + "]", e);
                                }
                            }
                            AliasMetaData newAliasMd = AliasMetaData.newAliasMetaDataBuilder(
                                    aliasAction.alias())
                                    .filter(filter)
                                    .indexRouting(aliasAction.indexRouting())
                                    .searchRouting(aliasAction.searchRouting())
                                    .build();
                            // Check if this alias already exists
                            AliasMetaData aliasMd = indexMetaData.aliases().get(aliasAction.alias());
                            if (aliasMd != null && aliasMd.equals(newAliasMd)) {
                                // It's the same alias - ignore it
                                continue;
                            }
                            indexMetaDataBuilder.putAlias(newAliasMd);
                        } else if (aliasAction.actionType() == AliasAction.Type.REMOVE) {
                            if (!indexMetaData.aliases().containsKey(aliasAction.alias())) {
                                // This alias doesn't exist - ignore
                                continue;
                            }
                            indexMetaDataBuilder.removerAlias(aliasAction.alias());
                        }
                        changed = true;
                        builder.put(indexMetaDataBuilder);
                    }

                    if (changed) {
                        ClusterState updatedState = ClusterState.builder(currentState).metaData(builder).build();
                        // even though changes happened, they resulted in 0 actual changes to metadata
                        // i.e. remove and add the same alias to the same index
                        if (!updatedState.metaData().aliases().equals(currentState.metaData().aliases())) {
                            return updatedState;
                        }
                    }
                    return currentState;
                } finally {
                    for (String index : indicesToClose) {
                        indicesService.removeIndex(index, "created for alias processing");
                    }
                }
            }

            @Override
            public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {

            }
        });
    }
}
