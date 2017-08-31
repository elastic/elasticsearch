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
import org.elasticsearch.index.shard.DocsStats;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.Arrays;
import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.Locale;
import java.util.ArrayList;
import java.util.Collections;
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
                                   final ActionListener<RolloverResponse> mainListener) {
        final Set<SingleAliasRolloverRequest> requests = createSingleAliasRequests(state, rolloverRequest);

        if (requests.size() == 0) {
            throw new IllegalArgumentException("No aliases for [" + String.join(",", rolloverRequest.getAliases()) + "] found");
        }

        if (requests.size() > 1 && rolloverRequest.getNewIndexName() != null) {
            throw new IllegalArgumentException("new index name not supported when rolling over multiple aliases");
        }

        final ActionListener<RolloverResponse.SingleAliasRolloverResponse> aggListener = new AggRolloverResponseActionListener(
            requests.size(), mainListener);
        final MetaData metaData = state.getMetaData();

        for (SingleAliasRolloverRequest req: requests) {
            final EnrichErrorInfoListener listener = new EnrichErrorInfoListener(aggListener, req.alias);

            client.admin().indices().prepareStats(req.sourceIndexName).clear().setDocs(true).execute(
                new ActionListener<IndicesStatsResponse>() {
                    @Override
                    public void onResponse(IndicesStatsResponse statsResponse) {
                        final Set<Condition.Result> conditionResults = evaluateConditions(rolloverRequest.getConditions(),
                            metaData.index(req.sourceIndexName), statsResponse);

                        if (rolloverRequest.isDryRun()) {
                            listener.onResponse(
                                new RolloverResponse.SingleAliasRolloverResponse(req.alias, req.sourceIndexName, req.rolloverIndexName,
                                    conditionResults, true, false, false, false));
                            return;
                        }
                        if (conditionResults.size() == 0 || conditionResults.stream().anyMatch(result -> result.matched)) {
                            final CreateIndexClusterStateUpdateRequest updateRequest = prepareCreateIndexRequest(
                                req.unresolvedName, req.rolloverIndexName, rolloverRequest);
                            createIndexService.createIndex(updateRequest, ActionListener.wrap(createIndexClusterStateUpdateResponse -> {
                                // switch the alias to point to the newly created index
                                indexAliasesService.indicesAliases(
                                    prepareRolloverAliasesUpdateRequest(req.sourceIndexName, req.rolloverIndexName, rolloverRequest,
                                        req.alias),
                                    ActionListener.wrap(aliasClusterStateUpdateResponse -> {
                                        if (aliasClusterStateUpdateResponse.isAcknowledged()) {
                                            activeShardsObserver.waitForActiveShards(req.rolloverIndexName,
                                                rolloverRequest.getCreateIndexRequest().waitForActiveShards(),
                                                rolloverRequest.masterNodeTimeout(),
                                                isShardsAcked -> listener.onResponse(new RolloverResponse.SingleAliasRolloverResponse(
                                                    req.alias, req.sourceIndexName, req.rolloverIndexName, conditionResults,
                                                    false, true, true, isShardsAcked)),
                                                listener::onFailure);
                                        } else {
                                            listener.onResponse(new RolloverResponse.SingleAliasRolloverResponse(
                                                req.alias, req.sourceIndexName, req.rolloverIndexName, conditionResults,
                                                false, true, false, false));
                                        }
                                    }, listener::onFailure));
                            }, listener::onFailure));
                        } else {
                            // conditions not met
                            listener.onResponse(
                                new RolloverResponse.SingleAliasRolloverResponse(req.alias, req.sourceIndexName, req.rolloverIndexName,
                                    conditionResults, false, false, false, false)
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
    }

    static IndicesAliasesClusterStateUpdateRequest prepareRolloverAliasesUpdateRequest(String oldIndex, String newIndex,
                                                                                       RolloverRequest request, String alias) {
        List<AliasAction> actions = unmodifiableList(Arrays.asList(
                new AliasAction.Add(newIndex, alias, null, null, null),
                new AliasAction.Remove(oldIndex, alias)));
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

    static void validate(MetaData metaData, String alias) {
        final AliasOrIndex aliasOrIndex = metaData.getAliasAndIndexLookup().get(alias);
        if (aliasOrIndex == null) {
            throw new IllegalArgumentException("source alias [" + alias + "] does not exist");
        }
        if (aliasOrIndex.isAlias() == false) {
            throw new IllegalArgumentException("source alias [" + alias + "] is a concrete index");
        }
        if (aliasOrIndex.getIndices().size() != 1) {
            throw new IllegalArgumentException("source alias [" + alias + "]  maps to multiple indices");
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

    private IndexMetaData getFirstIndexMetaData(MetaData metaData, String alias) {
        final AliasOrIndex aliasOrIndex = metaData.getAliasAndIndexLookup().get(alias);
        return aliasOrIndex.getIndices().get(0);
    }

    private String getUnresolvedName(IndexMetaData indexMetaData, String newIndexName) {
        final String sourceProvidedName = indexMetaData.getSettings().get(IndexMetaData.SETTING_INDEX_PROVIDED_NAME,
            indexMetaData.getIndex().getName());

        return (newIndexName != null) ? newIndexName : generateRolloverIndexName(sourceProvidedName, indexNameExpressionResolver);
    }

    private Set<SingleAliasRolloverRequest> createSingleAliasRequests(final ClusterState state, final RolloverRequest rolloverRequest) {
        final MetaData metaData = state.metaData();
        return indexNameExpressionResolver
            .resolveAliases(metaData, rolloverRequest.getAliases())
            .stream()
            .map(alias -> createTask(metaData, alias, rolloverRequest.getNewIndexName(), state))
            .collect(Collectors.toSet());
    }

    private static class SingleAliasRolloverRequest {

        final MetaData metaData;
        final String sourceIndexName;
        final String unresolvedName;
        final String rolloverIndexName;
        final String alias;

        SingleAliasRolloverRequest(String alias, MetaData metadata, String sourceIndexName, String unresolvedName,
                                   String rolloverIndexName) {
            this.alias = alias;
            this.metaData = metadata;
            this.sourceIndexName = sourceIndexName;
            this.unresolvedName = unresolvedName;
            this.rolloverIndexName = rolloverIndexName;
        }
    }

    private SingleAliasRolloverRequest createTask(MetaData metaData, String alias, String newIndexName, ClusterState state) {
        validate(metaData, alias);
        final IndexMetaData indexMetaData = getFirstIndexMetaData(metaData, alias);
        final String sourceIndexName = indexMetaData.getIndex().getName();
        final String unresolvedName = getUnresolvedName(indexMetaData, newIndexName);
        final String rolloverIndexName = indexNameExpressionResolver.resolveDateMathExpression(unresolvedName);
        MetaDataCreateIndexService.validateIndexName(rolloverIndexName, state);
        return new SingleAliasRolloverRequest(alias, metaData, sourceIndexName, unresolvedName, rolloverIndexName);
    }

    // package visible for testing
    static class RolloverFailureException extends Exception {

        RolloverFailureException(String alias, Exception cause) {
            super(alias, cause);
        }
    }

    private static class EnrichErrorInfoListener implements ActionListener<RolloverResponse.SingleAliasRolloverResponse> {

        private final ActionListener<RolloverResponse.SingleAliasRolloverResponse> listener;
        private final String alias;

        EnrichErrorInfoListener(ActionListener<RolloverResponse.SingleAliasRolloverResponse> listener, String alias) {
            this.listener = listener;
            this.alias = alias;
        }

        @Override
        public void onResponse(RolloverResponse.SingleAliasRolloverResponse rolloverResponse) {
            listener.onResponse(rolloverResponse);
        }

        @Override
        public void onFailure(Exception e) {
            listener.onFailure(new RolloverFailureException(alias, e));
        }
    }

    // package visible for testing
    static class AggRolloverResponseActionListener implements ActionListener<RolloverResponse.SingleAliasRolloverResponse> {

        private final int size;
        private final List<RolloverResponse.SingleAliasRolloverResponse> responses;
        private final ActionListener<RolloverResponse> listener;
        private final Map<String, Exception> failures;

        AggRolloverResponseActionListener(int cnt, ActionListener<RolloverResponse> listener) {
            this.listener = listener;
            size = cnt;
            responses = Collections.synchronizedList(new ArrayList<>());
            failures = Collections.synchronizedMap(new HashMap<>());
        }

        @Override
        public void onResponse(RolloverResponse.SingleAliasRolloverResponse rolloverResponse) {
            responses.add(rolloverResponse);
            notifyListenerIfAllAliasesProcessed();
        }

        @Override
        public void onFailure(Exception e) {
            assert e instanceof RolloverFailureException;
            if (size == 1) {
                // keep single alias Rollover API
                listener.onFailure((Exception)e.getCause());
            } else {
                failures.put(e.getMessage(), (Exception)e.getCause());
            }

            notifyListenerIfAllAliasesProcessed();
        }

        private void notifyListenerIfAllAliasesProcessed() {
            if (responses.size() + failures.size() == size) {
                listener.onResponse(new RolloverResponse(responses, failures));
            }
        }
    }
}
