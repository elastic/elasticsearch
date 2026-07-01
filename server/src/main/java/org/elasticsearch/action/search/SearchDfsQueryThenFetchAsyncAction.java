/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.search;

import org.apache.logging.log4j.Logger;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.join.ScoreMode;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.index.query.NestedQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.rest.action.search.SearchResponseMetrics;
import org.elasticsearch.search.SearchPhaseResult;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.builder.SubSearchSourceBuilder;
import org.elasticsearch.search.dfs.DfsKnnResults;
import org.elasticsearch.search.dfs.DfsSearchResult;
import org.elasticsearch.search.internal.AliasFilter;
import org.elasticsearch.search.internal.ShardSearchRequest;
import org.elasticsearch.search.vectors.KnnScoreDocQueryBuilder;
import org.elasticsearch.transport.Transport;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.function.BiFunction;

class SearchDfsQueryThenFetchAsyncAction extends AbstractSearchAsyncAction<DfsSearchResult> {

    private final SearchPhaseResults<SearchPhaseResult> queryPhaseResultConsumer;
    private final SearchProgressListener progressListener;
    private final Client client;
    // Set by DfsQueryPhase after the DFS round completes; used to apply per-shard KNN rewrites
    // in buildShardSearchRequest for all subsequent phases (query, fetch, rank feature).
    List<DfsKnnResults> knnResults;

    SearchDfsQueryThenFetchAsyncAction(
        Logger logger,
        NamedWriteableRegistry namedWriteableRegistry,
        SearchTransportService searchTransportService,
        BigArrays bigArrays,
        BiFunction<String, String, Transport.Connection> nodeIdToConnection,
        Map<String, AliasFilter> aliasFilter,
        Map<String, Float> concreteIndexBoosts,
        Executor executor,
        SearchPhaseResults<SearchPhaseResult> queryPhaseResultConsumer,
        SearchRequest request,
        ActionListener<SearchResponse> listener,
        List<SearchShardIterator> shardsIts,
        Map<String, Integer> skippedByClusterAlias,
        TransportSearchAction.SearchTimeProvider timeProvider,
        ClusterState clusterState,
        SearchTask task,
        SearchResponse.Clusters clusters,
        Client client,
        SearchResponseMetrics searchResponseMetrics,
        Map<String, Object> searchRequestAttributes,
        boolean pitRelocationEnabled
    ) {
        super(
            "dfs",
            logger,
            namedWriteableRegistry,
            searchTransportService,
            bigArrays,
            nodeIdToConnection,
            aliasFilter,
            concreteIndexBoosts,
            executor,
            request,
            listener,
            shardsIts,
            skippedByClusterAlias,
            timeProvider,
            clusterState,
            task,
            new ArraySearchPhaseResults<>(shardsIts.size()),
            request.getMaxConcurrentShardRequests(),
            clusters,
            searchResponseMetrics,
            searchRequestAttributes,
            pitRelocationEnabled
        );
        this.queryPhaseResultConsumer = queryPhaseResultConsumer;
        addReleasable(queryPhaseResultConsumer);
        this.progressListener = task.getProgressListener();
        // don't build the SearchShard list (can be expensive) if the SearchProgressListener won't use it
        if (progressListener != SearchProgressListener.NOOP) {
            notifyListShards(progressListener, clusters, request, skippedByClusterAlias);
        }
        this.client = client;
    }

    @Override
    protected void executePhaseOnShard(
        final SearchShardIterator shardIt,
        final Transport.Connection connection,
        final SearchActionListener<DfsSearchResult> listener
    ) {
        getSearchTransport().sendExecuteDfs(
            connection,
            buildShardSearchRequest(shardIt, listener.requestIndex),
            getTask(),
            listener,
            this::trackPhaseResultBytesRead,
            this::trackPhaseRequestBytesWritten
        );
    }

    @Override
    protected SearchPhase getNextPhase() {
        return new DfsQueryPhase(queryPhaseResultConsumer, client, this);
    }

    @Override
    protected void onShardGroupFailure(int shardIndex, SearchShardTarget shardTarget, Exception exc) {
        progressListener.notifyQueryFailure(shardIndex, shardTarget, exc);
    }

    @Override
    protected ShardSearchRequest buildShardSearchRequest(SearchShardIterator shardIt, int shardIndex) {
        ShardSearchRequest shardSearchRequest = super.buildShardSearchRequest(shardIt, shardIndex);
        if (knnResults != null) {
            rewriteShardSearchRequest(knnResults, shardSearchRequest);
        }
        return shardSearchRequest;
    }

    private static void rewriteShardSearchRequest(List<DfsKnnResults> knnResults, ShardSearchRequest request) {
        SearchSourceBuilder source = request.source();
        if (source == null || source.knnSearch().isEmpty()) {
            return;
        }

        List<SubSearchSourceBuilder> subSearchSourceBuilders = new ArrayList<>(source.subSearches());

        int i = 0;
        for (DfsKnnResults dfsKnnResults : knnResults) {
            List<ScoreDoc> scoreDocs = new ArrayList<>();
            for (ScoreDoc scoreDoc : dfsKnnResults.scoreDocs()) {
                if (scoreDoc.shardIndex == request.shardRequestIndex()) {
                    scoreDocs.add(scoreDoc);
                }
            }
            scoreDocs.sort(Comparator.comparingInt(scoreDoc -> scoreDoc.doc));
            String nestedPath = dfsKnnResults.getNestedPath();
            QueryBuilder query = new KnnScoreDocQueryBuilder(
                scoreDocs.toArray(Lucene.EMPTY_SCORE_DOCS),
                source.knnSearch().get(i).getField(),
                source.knnSearch().get(i).getQueryVector(),
                source.knnSearch().get(i).getSimilarity(),
                source.knnSearch().get(i).getFilterQueries()
            ).boost(source.knnSearch().get(i).boost()).queryName(source.knnSearch().get(i).queryName());
            if (nestedPath != null) {
                query = new NestedQueryBuilder(nestedPath, query, ScoreMode.Max).innerHit(source.knnSearch().get(i).innerHit());
            }
            subSearchSourceBuilders.add(new SubSearchSourceBuilder(query));
            i++;
        }

        source = source.shallowCopy().subSearches(subSearchSourceBuilders).knnSearch(List.of());
        request.source(source);
    }
}
