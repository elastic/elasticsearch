/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.action.search;

import org.apache.lucene.search.ScoreDoc;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.SearchPhaseResult;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.dfs.AggregatedDfs;
import org.elasticsearch.search.dfs.DfsKnnResults;
import org.elasticsearch.search.dfs.DfsSearchResult;
import org.elasticsearch.search.internal.ShardSearchRequest;
import org.elasticsearch.search.query.QuerySearchRequest;
import org.elasticsearch.search.query.QuerySearchResult;
import org.elasticsearch.search.vectors.KnnScoreDocQueryBuilder;
import org.elasticsearch.transport.Transport;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.function.Function;

/**
 * This search phase fans out to every shards to execute a distributed search with a pre-collected distributed frequencies for all
 * search terms used in the actual search query. This phase is very similar to a the default query-then-fetch search phase but it doesn't
 * retry on another shard if any of the shards are failing. Failures are treated as shard failures and are counted as a non-successful
 * operation.
 * @see CountedCollector#onFailure(int, SearchShardTarget, Exception)
 */
final class DfsQueryPhase extends SearchPhase {
    private final QueryPhaseResultConsumer queryResult;
    private final List<DfsSearchResult> searchResults;
    private final AggregatedDfs dfs;
    private final List<DfsKnnResults> knnResults;
    private final Function<ArraySearchPhaseResults<SearchPhaseResult>, SearchPhase> nextPhaseFactory;
    private final SearchPhaseContext context;
    private final SearchTransportService searchTransportService;
    private final SearchProgressListener progressListener;

    DfsQueryPhase(
        List<DfsSearchResult> searchResults,
        AggregatedDfs dfs,
        List<DfsKnnResults> knnResults,
        QueryPhaseResultConsumer queryResult,
        Function<ArraySearchPhaseResults<SearchPhaseResult>, SearchPhase> nextPhaseFactory,
        SearchPhaseContext context
    ) {
        super("dfs_query");
        this.progressListener = context.getTask().getProgressListener();
        this.queryResult = queryResult;
        this.searchResults = searchResults;
        this.dfs = dfs;
        this.knnResults = knnResults;
        this.nextPhaseFactory = nextPhaseFactory;
        this.context = context;
        this.searchTransportService = context.getSearchTransport();

        // register the release of the query consumer to free up the circuit breaker memory
        // at the end of the search
        context.addReleasable(queryResult);
    }

    @Override
    public void run() throws IOException {
        // TODO we can potentially also consume the actual per shard results from the initial phase here in the aggregateDfs
        // to free up memory early
        final CountedCollector<SearchPhaseResult> counter = new CountedCollector<>(
            queryResult,
            searchResults.size(),
            () -> context.executeNextPhase(this, nextPhaseFactory.apply(queryResult)),
            context
        );

        for (final DfsSearchResult dfsResult : searchResults) {
            final SearchShardTarget shardTarget = dfsResult.getSearchShardTarget();
            Transport.Connection connection = context.getConnection(shardTarget.getClusterAlias(), shardTarget.getNodeId());
            ShardSearchRequest shardRequest = rewriteShardSearchRequest(dfsResult.getShardSearchRequest());
            QuerySearchRequest querySearchRequest = new QuerySearchRequest(
                context.getOriginalIndices(dfsResult.getShardIndex()),
                dfsResult.getContextId(),
                shardRequest,
                dfs
            );
            final int shardIndex = dfsResult.getShardIndex();
            searchTransportService.sendExecuteQuery(
                connection,
                querySearchRequest,
                context.getTask(),
                new SearchActionListener<QuerySearchResult>(shardTarget, shardIndex) {

                    @Override
                    protected void innerOnResponse(QuerySearchResult response) {
                        try {
                            response.setSearchProfileDfsPhaseResult(dfsResult.searchProfileDfsPhaseResult());
                            counter.onResult(response);
                        } catch (Exception e) {
                            context.onPhaseFailure(DfsQueryPhase.this, "", e);
                        }
                    }

                    @Override
                    public void onFailure(Exception exception) {
                        try {
                            context.getLogger()
                                .debug(() -> "[" + querySearchRequest.contextId() + "] Failed to execute query phase", exception);
                            progressListener.notifyQueryFailure(shardIndex, shardTarget, exception);
                            counter.onFailure(shardIndex, shardTarget, exception);
                        } finally {
                            if (context.isPartOfPointInTime(querySearchRequest.contextId()) == false) {
                                // the query might not have been executed at all (for example because thread pool rejected
                                // execution) and the search context that was created in dfs phase might not be released.
                                // release it again to be in the safe side
                                context.sendReleaseSearchContext(
                                    querySearchRequest.contextId(),
                                    connection,
                                    context.getOriginalIndices(shardIndex)
                                );
                            }
                        }
                    }
                }
            );
        }
    }

    // package private for testing
    ShardSearchRequest rewriteShardSearchRequest(ShardSearchRequest request) {
        SearchSourceBuilder source = request.source();
        if (source == null || source.knnSearch().isEmpty()) {
            return request;
        }

        if (source.rankBuilder() == null) {
            // this path will use linear combination if there are
            // multiple knn queries to combine all knn queries into
            // a single query per shard

            List<ScoreDoc> scoreDocs = new ArrayList<>();
            for (DfsKnnResults dfsKnnResults : knnResults) {
                for (ScoreDoc scoreDoc : dfsKnnResults.scoreDocs()) {
                    if (scoreDoc.shardIndex == request.shardRequestIndex()) {
                        scoreDocs.add(scoreDoc);
                    }
                }
            }
            scoreDocs.sort(Comparator.comparingInt(scoreDoc -> scoreDoc.doc));
            // It is possible that the different results refer to the same doc.
            for (int i = 0; i < scoreDocs.size() - 1; i++) {
                ScoreDoc scoreDoc = scoreDocs.get(i);
                int j = i + 1;
                for (; j < scoreDocs.size(); j++) {
                    ScoreDoc otherScoreDoc = scoreDocs.get(j);
                    if (otherScoreDoc.doc != scoreDoc.doc) {
                        break;
                    }
                    scoreDoc.score += otherScoreDoc.score;
                }
                if (j > i + 1) {
                    scoreDocs.subList(i + 1, j).clear();
                }
            }

            KnnScoreDocQueryBuilder knnQuery = new KnnScoreDocQueryBuilder(scoreDocs.toArray(new ScoreDoc[0]));
            SearchSourceBuilder newSource = source.shallowCopy().knnSearch(List.of());
            if (source.query() == null) {
                newSource.query(knnQuery);
            } else {
                newSource.query(new BoolQueryBuilder().should(knnQuery).should(source.query()));
            }
            request.source(newSource);
        } else {
            // this path will keep knn queries separate for ranking per shard
            // if there are multiple knn queries

            List<QueryBuilder> rankQueryBuilders = new ArrayList<>();
            if (source.query() != null) {
                rankQueryBuilders.add(source.query());
            }

            for (DfsKnnResults dfsKnnResults : knnResults) {
                List<ScoreDoc> scoreDocs = new ArrayList<>();
                for (ScoreDoc scoreDoc : dfsKnnResults.scoreDocs()) {
                    if (scoreDoc.shardIndex == request.shardRequestIndex()) {
                        scoreDocs.add(scoreDoc);
                    }
                }
                scoreDocs.sort(Comparator.comparingInt(scoreDoc -> scoreDoc.doc));
                KnnScoreDocQueryBuilder knnQuery = new KnnScoreDocQueryBuilder(scoreDocs.toArray(new ScoreDoc[0]));
                rankQueryBuilders.add(knnQuery);
            }

            BoolQueryBuilder searchQuery = new BoolQueryBuilder();
            for (QueryBuilder queryBuilder : rankQueryBuilders) {
                searchQuery.should(queryBuilder);
            }

            SearchSourceBuilder newSource = source.shallowCopy().query(searchQuery).knnSearch(List.of());
            request.source(newSource);
            request.rankQueryBuilders(rankQueryBuilders);
        }

        return request;
    }
}
