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
import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.action.support.ActiveShardsObserver;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
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
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.shard.DocsStats;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.*;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static java.util.Collections.unmodifiableList;

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
    public TransportRolloverAction(Settings settings, TransportService transportService, ClusterService clusterService,
                                   ThreadPool threadPool, MetaDataCreateIndexService createIndexService,
                                   ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver,
                                   MetaDataIndexAliasesService indexAliasesService, Client client) {
        super(settings, RolloverAction.NAME, transportService, clusterService, threadPool, actionFilters, indexNameExpressionResolver,
            RolloverRequest::new);
        this.createIndexService = createIndexService;
        this.indexAliasesService = indexAliasesService;
        this.client = client;
        this.activeShardsObserver = new ActiveShardsObserver(settings, clusterService, threadPool);
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
        final ActionListener<RolloverResponse> aggListener = new AggRolloverResponseActionListener(
            rolloverRequest.getAliases().length, listener);
        final MetaData metaData = state.metaData();
        validate(metaData, rolloverRequest);

        Arrays.stream(rolloverRequest.getAliases())
            .map(alias -> createTask(metaData, alias, rolloverRequest, state))
            .forEach(task -> {
                client.admin().indices().prepareStats(task.sourceIndexName).clear().setDocs(true).execute(
                    new IndicesStatsResponseActionListener(
                        rolloverRequest, aggListener, createIndexService, indexAliasesService, activeShardsObserver, task)
                );
            });
    }

    private static class RolloverTask {

        final MetaData metaData;
        final String sourceIndexName;
        final String unresolvedName;
        final String rolloverIndexName;

        RolloverTask(MetaData metadata, String sourceIndexName, String unresolvedName, String rolloverIndexName) {
            this.metaData = metadata;
            this.sourceIndexName = sourceIndexName;
            this.unresolvedName = unresolvedName;
            this.rolloverIndexName = rolloverIndexName;
        }
    }

    private RolloverTask createTask(MetaData metaData, String alias, RolloverRequest rolloverRequest, ClusterState state) {
        final IndexMetaData indexMetaData = getFirstIndexMetaData(metaData, alias);
        final String sourceIndexName = indexMetaData.getIndex().getName();
        final String unresolvedName = getUnresolvedName(indexMetaData, rolloverRequest.getNewIndexName());
        final String rolloverIndexName = indexNameExpressionResolver.resolveDateMathExpression(unresolvedName);
        MetaDataCreateIndexService.validateIndexName(rolloverIndexName, state);
        return new RolloverTask(metaData, sourceIndexName, unresolvedName, rolloverIndexName);
    }

    private IndexMetaData getFirstIndexMetaData(MetaData metaData, String alias) {
        final AliasOrIndex aliasOrIndex = metaData.getAliasAndIndexLookup().get(alias);
        return aliasOrIndex.getIndices().get(0);
    }

    private String getUnresolvedName(IndexMetaData indexMetaData, String newIndexName) {
        final String sourceProvidedName = indexMetaData.getSettings().get(IndexMetaData.SETTING_INDEX_PROVIDED_NAME,
            indexMetaData.getIndex().getName());

        return (newIndexName != null) ? newIndexName : generateRolloverIndexName(sourceProvidedName, indexNameExpressionResolver);
    }

    static IndicesAliasesClusterStateUpdateRequest prepareRolloverAliasesUpdateRequest(String oldIndex, String newIndex,
                                                                                       RolloverRequest request) {
        List<AliasAction> actions = unmodifiableList(Arrays.asList(
                new AliasAction.Add(newIndex, request.getAlias(), null, null, null),
                new AliasAction.Remove(oldIndex, request.getAlias())));
        final IndicesAliasesClusterStateUpdateRequest updateRequest = new IndicesAliasesClusterStateUpdateRequest(actions)
            .ackTimeout(request.ackTimeout())
            .masterNodeTimeout(request.masterNodeTimeout());
        return updateRequest;
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

    static Set<Condition.Result> evaluateConditions(final Set<Condition> conditions,
                                                    final DocsStats docsStats, final IndexMetaData metaData) {
        final long numDocs = docsStats == null ? 0 : docsStats.getCount();
        final Condition.Stats stats = new Condition.Stats(numDocs, metaData.getCreationDate());
        return conditions.stream()
            .map(condition -> condition.evaluate(stats))
            .collect(Collectors.toSet());
    }

    static Set<Condition.Result> evaluateConditions(final Set<Condition> conditions, final IndexMetaData metaData,
                                                    final IndicesStatsResponse statsResponse) {
        return evaluateConditions(conditions, statsResponse.getPrimaries().getDocs(), metaData);
    }

    static void validate(MetaData metaData, RolloverRequest request) {
        final AliasOrIndex aliasOrIndex = metaData.getAliasAndIndexLookup().get(request.getAlias());
        if (aliasOrIndex == null) {
            throw new IllegalArgumentException("source alias does not exist");
        }
        if (aliasOrIndex.isAlias() == false) {
            throw new IllegalArgumentException("source alias is a concrete index");
        }
        if (aliasOrIndex.getIndices().size() != 1) {
            //throw new IllegalArgumentException("source alias maps to multiple indices");
        }
    }

    static CreateIndexClusterStateUpdateRequest prepareCreateIndexRequest(final String providedIndexName, final String targetIndexName,
                                                                          final RolloverRequest rolloverRequest) {
        final CreateIndexRequest createIndexRequest = rolloverRequest.getCreateIndexRequest();
        createIndexRequest.cause("rollover_index");
        createIndexRequest.index(targetIndexName);
        return new CreateIndexClusterStateUpdateRequest(createIndexRequest,
            "rollover_index", targetIndexName, providedIndexName, true)
            .ackTimeout(createIndexRequest.timeout())
            .masterNodeTimeout(createIndexRequest.masterNodeTimeout())
            .settings(createIndexRequest.settings())
            .aliases(createIndexRequest.aliases())
            .waitForActiveShards(ActiveShardCount.NONE) // not waiting for shards here, will wait on the alias switch operation
            .mappings(createIndexRequest.mappings());
    }

    private static class IndicesStatsResponseActionListener implements ActionListener<IndicesStatsResponse> {
        private final RolloverRequest rolloverRequest;
        private final MetaData metaData;
        private final String sourceIndexName;
        private final ActionListener<RolloverResponse> listener;
        private final String rolloverIndexName;
        private final String unresolvedName;
        private final MetaDataCreateIndexService createIndexService;
        private final MetaDataIndexAliasesService indexAliasesService;
        private final ActiveShardsObserver activeShardsObserver;

        IndicesStatsResponseActionListener(RolloverRequest rolloverRequest, ActionListener<RolloverResponse> listener,
                                           MetaDataCreateIndexService createIndexService, MetaDataIndexAliasesService indexAliasesService,
                                           ActiveShardsObserver activeShardsObserver, RolloverTask task) {
            this.rolloverRequest = rolloverRequest;
            this.metaData = task.metaData;
            this.sourceIndexName = task.sourceIndexName;
            this.listener = listener;
            this.rolloverIndexName = task.rolloverIndexName;
            this.unresolvedName = task.unresolvedName;
            this.createIndexService = createIndexService;
            this.indexAliasesService = indexAliasesService;
            this.activeShardsObserver = activeShardsObserver;
        }

        @Override
        public void onResponse(IndicesStatsResponse statsResponse) {
            final Set<Condition.Result> conditionResults = evaluateConditions(rolloverRequest.getConditions(),
                metaData.index(sourceIndexName), statsResponse);

            if (rolloverRequest.isDryRun()) {
                listener.onResponse(
                    new RolloverResponse(sourceIndexName, rolloverIndexName, conditionResults, true, false, false, false));
                return;
            }
            if (conditionResults.size() == 0 || conditionResults.stream().anyMatch(result -> result.matched)) {
                CreateIndexClusterStateUpdateRequest updateRequest = prepareCreateIndexRequest(unresolvedName, rolloverIndexName,
                    rolloverRequest);
                createIndexService.createIndex(updateRequest, ActionListener.wrap(createIndexClusterStateUpdateResponse -> {
                    // switch the alias to point to the newly created index
                    indexAliasesService.indicesAliases(
                        prepareRolloverAliasesUpdateRequest(sourceIndexName, rolloverIndexName,
                            rolloverRequest),
                        ActionListener.wrap(aliasClusterStateUpdateResponse -> {
                            if (aliasClusterStateUpdateResponse.isAcknowledged()) {
                                activeShardsObserver.waitForActiveShards(rolloverIndexName,
                                    rolloverRequest.getCreateIndexRequest().waitForActiveShards(),
                                    rolloverRequest.masterNodeTimeout(),
                                    isShardsAcked -> listener.onResponse(new RolloverResponse(sourceIndexName, rolloverIndexName,
                                                                            conditionResults, false, true, true, isShardsAcked)),
                                    listener::onFailure);
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

    private static class AggRolloverResponseActionListener implements ActionListener<RolloverResponse> {

        private final int size;
        private final List<RolloverResponse> responses;
        private final List<Exception> failedResponses;
        private final ActionListener<RolloverResponse> listener;

        AggRolloverResponseActionListener(int cnt, ActionListener<RolloverResponse> listener) {
            this.listener = listener;
            size = cnt;
            responses = new ArrayList<>();
            failedResponses = new ArrayList<>();
        }

        @Override
        public void onResponse(RolloverResponse rolloverResponse) {
            responses.add(rolloverResponse);
            if (responses.size() + failedResponses.size() == size) {
                listener.onResponse(responses.get(0)); // todo add all response to aggresponse
            }
        }

        @Override
        public void onFailure(Exception e) {
            failedResponses.add(e);
            listener.onFailure(e); // todo add all response to aggresponse
        }
    }
}
