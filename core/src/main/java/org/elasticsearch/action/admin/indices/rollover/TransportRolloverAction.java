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

package org.elasticsearch.action.admin.indices.rollover;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesClusterStateUpdateRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexClusterStateUpdateRequest;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ack.ClusterStateUpdateResponse;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.AliasAction;
import org.elasticsearch.cluster.metadata.AliasOrIndex;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.metadata.MetaDataCreateIndexService;
import org.elasticsearch.cluster.metadata.MetaDataIndexAliasesService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.shard.DocsStats;
import org.elasticsearch.indices.IndexAlreadyExistsException;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.Set;

/**
 * Main class to swap the index pointed to by an alias, given some predicates
 */
public class TransportRolloverAction extends TransportMasterNodeAction<RolloverRequest, RolloverResponse> {

    private final MetaDataCreateIndexService createIndexService;
    private final MetaDataIndexAliasesService indexAliasesService;
    private final Client client;

    @Inject
    public TransportRolloverAction(Settings settings, TransportService transportService, ClusterService clusterService,
                                   ThreadPool threadPool, MetaDataCreateIndexService createIndexService,
                                   ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver,
                                   MetaDataIndexAliasesService indexAliasesService, Client client) {
        super(settings, RolloverAction.NAME, transportService, clusterService, threadPool, actionFilters, indexNameExpressionResolver,
            RolloverRequest::new);
        this.createIndexService = createIndexService;
        this.indexAliasesService = indexAliasesService;
        this.client = client;
    }

    @Override
    protected String executor() {
        // we go async right away
        return ThreadPool.Names.SAME;
    }

    @Override
    protected RolloverResponse newResponse() {
        return new RolloverResponse();
    }

    @Override
    protected ClusterBlockException checkBlock(RolloverRequest request, ClusterState state) {
        IndicesOptions indicesOptions = IndicesOptions.fromOptions(true, true,
            request.indicesOptions().expandWildcardsOpen(), request.indicesOptions().expandWildcardsClosed());
        return state.blocks().indicesBlockedException(ClusterBlockLevel.METADATA_WRITE,
            indexNameExpressionResolver.concreteIndexNames(state, indicesOptions, request.indices()));
    }

    @Override
    protected void masterOperation(final RolloverRequest rolloverRequest, final ClusterState state,
                                   final ActionListener<RolloverResponse> listener) {
        final MetaData metaData = state.metaData();
        validate(metaData, rolloverRequest);
        final AliasOrIndex aliasOrIndex = metaData.getAliasAndIndexLookup().get(rolloverRequest.getSourceAlias());
        final IndexMetaData indexMetaData = aliasOrIndex.getIndices().get(0);
        final String sourceIndexName = indexMetaData.getIndex().getName();
        client.admin().indices().prepareStats(sourceIndexName).clear().setDocs(true).execute(
            new ActionListener<IndicesStatsResponse>() {
                @Override
                public void onResponse(IndicesStatsResponse indicesStatsResponse) {
                    final IndexMetaData sourceIndex = metaData.index(sourceIndexName);
                    DocsStats docsStats = indicesStatsResponse.getTotal().getDocs();
                    long docCount = docsStats == null ? 0 : docsStats.getCount();
                    long indexAge = System.currentTimeMillis() - sourceIndex.getCreationDate();
                    if (satisfiesConditions(rolloverRequest.getConditions(), docCount, indexAge)) {
                        final String rolloverIndexName = generateRolloverIndexName(sourceIndexName);
                        boolean createRolloverIndex = metaData.index(rolloverIndexName) == null;
                        if (createRolloverIndex) {
                            CreateIndexClusterStateUpdateRequest updateRequest =
                                prepareCreateIndexRequest(rolloverIndexName, rolloverRequest);
                            createIndexService.createIndex(updateRequest, new ActionListener<ClusterStateUpdateResponse>() {
                                @Override
                                public void onResponse(ClusterStateUpdateResponse response) {
                                    indexAliasesService.indicesAliases(
                                        prepareIndicesAliasesRequest(sourceIndexName, rolloverIndexName, rolloverRequest),
                                        new IndicesAliasesListener(sourceIndexName, rolloverIndexName, listener));
                                }

                                @Override
                                public void onFailure(Throwable t) {
                                    if (t instanceof IndexAlreadyExistsException) {
                                        logger.trace("[{}] failed to create rollover index", t, updateRequest.index());
                                    } else {
                                        logger.debug("[{}] failed to create rollover index", t, updateRequest.index());
                                    }
                                    listener.onFailure(t);
                                }
                            });
                        } else {
                            indexAliasesService.indicesAliases(
                                prepareIndicesAliasesRequest(sourceIndexName, rolloverIndexName, rolloverRequest),
                                new IndicesAliasesListener(sourceIndexName, rolloverIndexName, listener));
                        }
                    } else {
                        // conditions not met
                        listener.onResponse(new RolloverResponse(sourceIndexName, sourceIndexName));
                    }
                }

                @Override
                public void onFailure(Throwable e) {
                    listener.onFailure(e);
                }
            }
        );

    }

    private static final class IndicesAliasesListener implements ActionListener<ClusterStateUpdateResponse> {

        private final ActionListener<RolloverResponse> listener;
        private final String oldIndex;
        private final String newIndex;

        public IndicesAliasesListener(String oldIndex, String newIndex, ActionListener<RolloverResponse> listener) {
            this.oldIndex = oldIndex;
            this.newIndex = newIndex;
            this.listener = listener;
        }

        @Override
        public void onResponse(ClusterStateUpdateResponse clusterStateUpdateResponse) {
            listener.onResponse(new RolloverResponse(oldIndex, newIndex));
        }

        @Override
        public void onFailure(Throwable e) {
            listener.onFailure(e);
        }
    }

    static IndicesAliasesClusterStateUpdateRequest prepareIndicesAliasesRequest(String concreteSourceIndex, String targetIndex,
                                                                                RolloverRequest request) {
        final IndicesAliasesClusterStateUpdateRequest updateRequest = new IndicesAliasesClusterStateUpdateRequest()
            .ackTimeout(request.ackTimeout())
            .masterNodeTimeout(request.masterNodeTimeout());
        AliasAction[] actions = new AliasAction[2];
        actions[0] = new AliasAction(AliasAction.Type.ADD, targetIndex, request.getSourceAlias());
        actions[1] = new AliasAction(AliasAction.Type.REMOVE, concreteSourceIndex, request.getSourceAlias());
        updateRequest.actions(actions);
        return updateRequest;
    }

    static String generateRolloverIndexName(String sourceIndexName) {
        int numberIndex = sourceIndexName.lastIndexOf("-");
        int counter = 1;
        String indexPrefix = sourceIndexName;
        if (numberIndex != -1) {
            try {
                counter = Integer.parseInt(sourceIndexName.substring(numberIndex + 1));
                counter++;
                indexPrefix = sourceIndexName.substring(0, numberIndex);
            } catch (NumberFormatException ignored) {
            }
        }
        return String.join("-", indexPrefix, String.valueOf(counter));
    }

    static boolean satisfiesConditions(Set<Condition> conditions, long docCount, long indexAge) {
        for (Condition condition: conditions) {
            if (condition instanceof Condition.MaxAge) {
                Condition.MaxAge maxAge = (Condition.MaxAge) condition;
                final TimeValue age = TimeValue.timeValueMillis(indexAge);
                if (maxAge.matches(age) == false) {
                    return false;
                }
            } else if (condition instanceof Condition.MaxDocs) {
                final Condition.MaxDocs maxDocs = (Condition.MaxDocs) condition;
                if (maxDocs.matches(docCount) == false) {
                    return false;
                }
            } else {
                throw new IllegalArgumentException("unknown condition [" + condition.getClass().getSimpleName() + "]");
            }
        }
        return true;
    }

    static void validate(MetaData metaData, RolloverRequest request) {
        final AliasOrIndex aliasOrIndex = metaData.getAliasAndIndexLookup().get(request.getSourceAlias());
        if (aliasOrIndex == null) {
            throw new IllegalArgumentException("source alias does not exist");
        }
        if (aliasOrIndex.isAlias() == false) {
            throw new IllegalArgumentException("source alias is a concrete index");
        }
        if (aliasOrIndex.getIndices().size() != 1) {
            throw new IllegalArgumentException("source alias maps to multiple indices");
        }
    }

    static CreateIndexClusterStateUpdateRequest prepareCreateIndexRequest(final String targetIndexName,
                                                                          final RolloverRequest rolloverRequest) {
        return new CreateIndexClusterStateUpdateRequest(rolloverRequest,
            "rollover_index", targetIndexName, true)
            .ackTimeout(rolloverRequest.timeout())
            .masterNodeTimeout(rolloverRequest.masterNodeTimeout());
    }

}
