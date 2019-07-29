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
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsAction;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsRequest;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.action.support.ActiveShardsObserver;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.AliasAction;
import org.elasticsearch.cluster.metadata.AliasOrIndex;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.IndexTemplateMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.metadata.MetaDataCreateIndexService;
import org.elasticsearch.cluster.metadata.MetaDataIndexAliasesService;
import org.elasticsearch.cluster.metadata.MetaDataIndexTemplateService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.index.shard.DocsStats;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Main class to swap the index pointed to by an alias, given some conditions
 */
public class TransportRolloverAction extends TransportMasterNodeAction<RolloverRequest, RolloverResponse> {

    private static final Pattern INDEX_NAME_PATTERN = Pattern.compile("^.*-\\d+$");
    private final MetaDataCreateIndexService createIndexService;
    private final MetaDataIndexAliasesService indexAliasesService;
    private final ActiveShardsObserver activeShardsObserver;
    private final Client client;

    @Inject
    public TransportRolloverAction(TransportService transportService, ClusterService clusterService,
                                   ThreadPool threadPool, MetaDataCreateIndexService createIndexService,
                                   ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver,
                                   MetaDataIndexAliasesService indexAliasesService, Client client) {
        super(RolloverAction.NAME, transportService, clusterService, threadPool, actionFilters, RolloverRequest::new,
            indexNameExpressionResolver);
        this.createIndexService = createIndexService;
        this.indexAliasesService = indexAliasesService;
        this.client = client;
        this.activeShardsObserver = new ActiveShardsObserver(clusterService, threadPool);
    }

    @Override
    protected String executor() {
        // we go async right away
        return ThreadPool.Names.SAME;
    }

    @Override
    protected RolloverResponse read(StreamInput in) throws IOException {
        return new RolloverResponse(in);
    }

    @Override
    protected ClusterBlockException checkBlock(RolloverRequest request, ClusterState state) {
        IndicesOptions indicesOptions = IndicesOptions.fromOptions(true, true,
            request.indicesOptions().expandWildcardsOpen(), request.indicesOptions().expandWildcardsClosed());
        return state.blocks().indicesBlockedException(ClusterBlockLevel.METADATA_WRITE,
            indexNameExpressionResolver.concreteIndexNames(state, indicesOptions, request.indices()));
    }

    @Override
    protected void masterOperation(Task task, final RolloverRequest rolloverRequest, final ClusterState state,
                                   final ActionListener<RolloverResponse> listener) {
        final MetaData metaData = state.metaData();
        validate(metaData, rolloverRequest);
        final AliasOrIndex.Alias alias = (AliasOrIndex.Alias) metaData.getAliasAndIndexLookup().get(rolloverRequest.getAlias());
        final IndexMetaData indexMetaData = alias.getWriteIndex();
        final boolean explicitWriteIndex = Boolean.TRUE.equals(indexMetaData.getAliases().get(alias.getAliasName()).writeIndex());
        final String sourceProvidedName = indexMetaData.getSettings().get(IndexMetaData.SETTING_INDEX_PROVIDED_NAME,
            indexMetaData.getIndex().getName());
        final String sourceIndexName = indexMetaData.getIndex().getName();
        final String unresolvedName = (rolloverRequest.getNewIndexName() != null)
            ? rolloverRequest.getNewIndexName()
            : generateRolloverIndexName(sourceProvidedName, indexNameExpressionResolver);
        final String rolloverIndexName = indexNameExpressionResolver.resolveDateMathExpression(unresolvedName);
        MetaDataCreateIndexService.validateIndexName(rolloverIndexName, state); // will fail if the index already exists
        checkNoDuplicatedAliasInIndexTemplate(metaData, rolloverIndexName, rolloverRequest.getAlias());
        IndicesStatsRequest statsRequest = new IndicesStatsRequest().indices(rolloverRequest.getAlias()).clear().docs(true);
        statsRequest.setParentTask(clusterService.localNode().getId(), task.getId());
        client.execute(IndicesStatsAction.INSTANCE, statsRequest,
            new ActionListener<IndicesStatsResponse>() {
                @Override
                public void onResponse(IndicesStatsResponse statsResponse) {
                    final Map<String, Boolean> conditionResults = evaluateConditions(rolloverRequest.getConditions().values(),
                        metaData.index(sourceIndexName), statsResponse);

                    if (rolloverRequest.isDryRun()) {
                        listener.onResponse(
                            new RolloverResponse(sourceIndexName, rolloverIndexName, conditionResults, true, false, false, false));
                        return;
                    }
                    List<Condition<?>> metConditions =  rolloverRequest.getConditions().values().stream()
                        .filter(condition -> conditionResults.get(condition.toString())).collect(Collectors.toList());
                    if (conditionResults.size() == 0 || metConditions.size() > 0) {
                        CreateIndexClusterStateUpdateRequest updateRequest = prepareCreateIndexRequest(unresolvedName, rolloverIndexName,
                            rolloverRequest);
                        createIndexService.createIndex(updateRequest, ActionListener.wrap(createIndexClusterStateUpdateResponse -> {
                            final IndicesAliasesClusterStateUpdateRequest aliasesUpdateRequest;
                            if (explicitWriteIndex) {
                                aliasesUpdateRequest = prepareRolloverAliasesWriteIndexUpdateRequest(sourceIndexName,
                                    rolloverIndexName, rolloverRequest);
                            } else {
                                aliasesUpdateRequest = prepareRolloverAliasesUpdateRequest(sourceIndexName,
                                    rolloverIndexName, rolloverRequest);
                            }
                            indexAliasesService.indicesAliases(aliasesUpdateRequest,
                                ActionListener.wrap(aliasClusterStateUpdateResponse -> {
                                    if (aliasClusterStateUpdateResponse.isAcknowledged()) {
                                        clusterService.submitStateUpdateTask("update_rollover_info", new ClusterStateUpdateTask() {
                                            @Override
                                            public ClusterState execute(ClusterState currentState) {
                                                RolloverInfo rolloverInfo = new RolloverInfo(rolloverRequest.getAlias(), metConditions,
                                                    threadPool.absoluteTimeInMillis());
                                                return ClusterState.builder(currentState)
                                                    .metaData(MetaData.builder(currentState.metaData())
                                                        .put(IndexMetaData.builder(currentState.metaData().index(sourceIndexName))
                                                            .putRolloverInfo(rolloverInfo))).build();
                                            }

                                            @Override
                                            public void onFailure(String source, Exception e) {
                                                listener.onFailure(e);
                                            }

                                            @Override
                                            public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                                                activeShardsObserver.waitForActiveShards(new String[]{rolloverIndexName},
                                                    rolloverRequest.getCreateIndexRequest().waitForActiveShards(),
                                                    rolloverRequest.masterNodeTimeout(),
                                                    isShardsAcknowledged -> listener.onResponse(new RolloverResponse(
                                                        sourceIndexName, rolloverIndexName, conditionResults, false, true, true,
                                                        isShardsAcknowledged)),
                                                    listener::onFailure);
                                            }
                                        });
                                    } else {
                                        listener.onResponse(new RolloverResponse(sourceIndexName, rolloverIndexName, conditionResults,
                                                                                    false, true, false, false));
                                    }
                                }, listener::onFailure));
                        }, listener::onFailure));
                    } else {
                        // conditions not met
                        listener.onResponse(
                            new RolloverResponse(sourceIndexName, rolloverIndexName, conditionResults, false, false, false, false)
                        );
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    listener.onFailure(e);
                }
            }
        );
    }

    static IndicesAliasesClusterStateUpdateRequest prepareRolloverAliasesUpdateRequest(String oldIndex, String newIndex,
                                                                                       RolloverRequest request) {
        final List<AliasAction> actions = List.of(
                new AliasAction.Add(newIndex, request.getAlias(), null, null, null, null),
                new AliasAction.Remove(oldIndex, request.getAlias()));
        return new IndicesAliasesClusterStateUpdateRequest(actions)
            .ackTimeout(request.ackTimeout())
            .masterNodeTimeout(request.masterNodeTimeout());
    }

    static IndicesAliasesClusterStateUpdateRequest prepareRolloverAliasesWriteIndexUpdateRequest(String oldIndex, String newIndex,
                                                                                                 RolloverRequest request) {
        final List<AliasAction> actions = List.of(
                new AliasAction.Add(newIndex, request.getAlias(), null, null, null, true),
                new AliasAction.Add(oldIndex, request.getAlias(), null, null, null, false));
        return new IndicesAliasesClusterStateUpdateRequest(actions)
            .ackTimeout(request.ackTimeout())
            .masterNodeTimeout(request.masterNodeTimeout());
    }


    static String generateRolloverIndexName(String sourceIndexName, IndexNameExpressionResolver indexNameExpressionResolver) {
        String resolvedName = indexNameExpressionResolver.resolveDateMathExpression(sourceIndexName);
        final boolean isDateMath = sourceIndexName.equals(resolvedName) == false;
        if (INDEX_NAME_PATTERN.matcher(resolvedName).matches()) {
            int numberIndex = sourceIndexName.lastIndexOf("-");
            assert numberIndex != -1 : "no separator '-' found";
            int counter = Integer.parseInt(sourceIndexName.substring(numberIndex + 1, isDateMath ? sourceIndexName.length()-1 :
                sourceIndexName.length()));
            String newName = sourceIndexName.substring(0, numberIndex) + "-" + String.format(Locale.ROOT, "%06d", ++counter)
                + (isDateMath ? ">" : "");
            return newName;
        } else {
            throw new IllegalArgumentException("index name [" + sourceIndexName + "] does not match pattern '^.*-\\d+$'");
        }
    }

    static Map<String, Boolean> evaluateConditions(final Collection<Condition<?>> conditions,
                                                   @Nullable final DocsStats docsStats,
                                                   @Nullable final IndexMetaData metaData) {
        if (metaData == null) {
            return conditions.stream().collect(Collectors.toMap(Condition::toString, cond -> false));
        }
        final long numDocs = docsStats == null ? 0 : docsStats.getCount();
        final long indexSize = docsStats == null ? 0 : docsStats.getTotalSizeInBytes();
        final Condition.Stats stats = new Condition.Stats(numDocs, metaData.getCreationDate(), new ByteSizeValue(indexSize));
        return conditions.stream()
            .map(condition -> condition.evaluate(stats))
            .collect(Collectors.toMap(result -> result.condition.toString(), result -> result.matched));
    }

    static Map<String, Boolean> evaluateConditions(final Collection<Condition<?>> conditions,
                                                   @Nullable final IndexMetaData metaData,
                                                   @Nullable final IndicesStatsResponse statsResponse) {
        if (metaData == null) {
            return conditions.stream().collect(Collectors.toMap(Condition::toString, cond -> false));
        } else {
            final DocsStats docsStats = Optional.ofNullable(statsResponse)
                .map(stats -> stats.getIndex(metaData.getIndex().getName()))
                .map(indexStats -> indexStats.getPrimaries().getDocs())
                .orElse(null);
            return evaluateConditions(conditions, docsStats, metaData);
        }
    }

    static void validate(MetaData metaData, RolloverRequest request) {
        final AliasOrIndex aliasOrIndex = metaData.getAliasAndIndexLookup().get(request.getAlias());
        if (aliasOrIndex == null) {
            throw new IllegalArgumentException("source alias does not exist");
        }
        if (aliasOrIndex.isAlias() == false) {
            throw new IllegalArgumentException("source alias is a concrete index");
        }
        final AliasOrIndex.Alias alias = (AliasOrIndex.Alias) aliasOrIndex;
        if (alias.getWriteIndex() == null) {
            throw new IllegalArgumentException("source alias [" + alias.getAliasName() + "] does not point to a write index");
        }
    }

    static CreateIndexClusterStateUpdateRequest prepareCreateIndexRequest(final String providedIndexName, final String targetIndexName,
                                                                          final RolloverRequest rolloverRequest) {

        final CreateIndexRequest createIndexRequest = rolloverRequest.getCreateIndexRequest();
        createIndexRequest.cause("rollover_index");
        createIndexRequest.index(targetIndexName);
        return new CreateIndexClusterStateUpdateRequest(
            "rollover_index", targetIndexName, providedIndexName)
            .ackTimeout(createIndexRequest.timeout())
            .masterNodeTimeout(createIndexRequest.masterNodeTimeout())
            .settings(createIndexRequest.settings())
            .aliases(createIndexRequest.aliases())
            .waitForActiveShards(ActiveShardCount.NONE) // not waiting for shards here, will wait on the alias switch operation
            .mappings(createIndexRequest.mappings());
    }

    /**
     * If the newly created index matches with an index template whose aliases contains the rollover alias,
     * the rollover alias will point to multiple indices. This causes indexing requests to be rejected.
     * To avoid this, we make sure that there is no duplicated alias in index templates before creating a new index.
     */
    static void checkNoDuplicatedAliasInIndexTemplate(MetaData metaData, String rolloverIndexName, String rolloverRequestAlias) {
        final List<IndexTemplateMetaData> matchedTemplates = MetaDataIndexTemplateService.findTemplates(metaData, rolloverIndexName);
        for (IndexTemplateMetaData template : matchedTemplates) {
            if (template.aliases().containsKey(rolloverRequestAlias)) {
                throw new IllegalArgumentException(String.format(Locale.ROOT,
                    "Rollover alias [%s] can point to multiple indices, found duplicated alias [%s] in index template [%s]",
                    rolloverRequestAlias, template.aliases().keys(), template.name()));
            }
        }
    }
}
