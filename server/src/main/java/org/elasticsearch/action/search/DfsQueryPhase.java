/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.action.search;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.CollectionStatistics;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TermStatistics;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHits;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.search.SearchPhaseResult;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.dfs.AggregatedDfs;
import org.elasticsearch.search.dfs.DfsKnnResults;
import org.elasticsearch.search.dfs.DfsSearchResult;
import org.elasticsearch.search.internal.ShardSearchRequest;
import org.elasticsearch.search.query.QuerySearchRequest;
import org.elasticsearch.transport.Transport;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * This search phase fans out to every shards to execute a distributed search with a pre-collected distributed frequencies for all
 * search terms used in the actual search query. This phase is very similar to the default query-then-fetch search phase, but it doesn't
 * retry on another shard if any of the shards are failing. Failures are treated as shard failures and are counted as a non-successful
 * operation.
 * @see CountedCollector#onFailure(int, SearchShardTarget, Exception)
 */
class DfsQueryPhase extends SearchPhase {

    public static final String NAME = "dfs_query";

    private final SearchPhaseResults<SearchPhaseResult> queryResult;
    private final Client client;
    private final SearchDfsQueryThenFetchAsyncAction context;
    private final SearchProgressListener progressListener;
    private long phaseStartTimeInNanos;

    DfsQueryPhase(SearchPhaseResults<SearchPhaseResult> queryResult, Client client, SearchDfsQueryThenFetchAsyncAction context) {
        super(NAME);
        this.progressListener = context.getTask().getProgressListener();
        this.queryResult = queryResult;
        this.client = client;
        this.context = context;
    }

    // protected for testing
    protected SearchPhase nextPhase(AggregatedDfs dfs) {
        return SearchQueryThenFetchAsyncAction.nextPhase(client, context, queryResult, dfs);
    }

    @Override
    protected void run() {
        phaseStartTimeInNanos = System.nanoTime();
        List<DfsSearchResult> searchResults = context.results.getAtomicArray().asList();
        AggregatedDfs dfs = aggregateDfs(searchResults);
        // TODO we can potentially also consume the actual per shard results from the initial phase here in the aggregateDfs
        // to free up memory early
        final CountedCollector<SearchPhaseResult> counter = new CountedCollector<>(
            queryResult,
            searchResults.size(),
            () -> onFinish(dfs),
            context
        );

        context.knnResults = mergeKnnResults(context.getRequest(), searchResults);
        for (final DfsSearchResult dfsResult : searchResults) {
            final SearchShardTarget shardTarget = dfsResult.getSearchShardTarget();
            final int shardIndex = dfsResult.getShardIndex();
            ShardSearchRequest shardSearchRequest = context.buildShardSearchRequest(context.shardIterators[shardIndex], shardIndex);
            shardSearchRequest.canReturnNullResponseIfMatchNoDocs(context.hasShardResponse() && shardSearchRequest.scroll() == null);
            QuerySearchRequest querySearchRequest = new QuerySearchRequest(
                context.getOriginalIndices(shardIndex),
                dfsResult.getContextId(),
                shardSearchRequest,
                dfs
            );
            final Transport.Connection connection;
            try {
                connection = context.getConnection(shardTarget.getClusterAlias(), shardTarget.getNodeId());
            } catch (Exception e) {
                shardFailure(e, querySearchRequest, shardIndex, shardTarget, counter);
                continue;
            }
            context.getSearchTransport()
                .sendExecuteQuery(connection, querySearchRequest, context.getTask(), new SearchActionListener<>(shardTarget, shardIndex) {

                    @Override
                    protected void innerOnResponse(SearchPhaseResult response) {
                        try {
                            response.queryResult().setSearchProfileDfsPhaseResult(dfsResult.searchProfileDfsPhaseResult());
                            if (dfsResult.searchTimedOut()) {
                                response.queryResult().searchTimedOut(true);
                            }
                            counter.onResult(response);
                        } catch (Exception e) {
                            context.onPhaseFailure(NAME, "", e);
                        }
                    }

                    @Override
                    public void onFailure(Exception exception) {
                        try {
                            shardFailure(exception, querySearchRequest, shardIndex, shardTarget, counter);
                        } finally {
                            if (context.isPartOfPointInTime(querySearchRequest.contextId()) == false) {
                                // the query might not have been executed at all (for example because thread pool rejected
                                // execution) and the search context that was created in dfs phase might not be released.
                                // release it again to be in the safe side
                                context.sendReleaseSearchContext(querySearchRequest.contextId(), connection);
                            }
                        }
                    }
                }, context::trackPhaseResultBytesRead, context::trackPhaseRequestBytesWritten);
        }
    }

    private void onFinish(AggregatedDfs dfs) {
        context.getSearchResponseMetrics()
            .recordSearchPhaseDuration(getName(), System.nanoTime() - phaseStartTimeInNanos, context.getSearchRequestAttributes());
        context.executeNextPhase(NAME, () -> nextPhase(dfs));
    }

    private void shardFailure(
        Exception exception,
        QuerySearchRequest querySearchRequest,
        int shardIndex,
        SearchShardTarget shardTarget,
        CountedCollector<SearchPhaseResult> counter
    ) {
        context.getLogger().debug(() -> "[" + querySearchRequest.contextId() + "] Failed to execute query phase", exception);
        progressListener.notifyQueryFailure(shardIndex, shardTarget, exception);
        counter.onFailure(shardIndex, shardTarget, exception);
    }

    private static List<DfsKnnResults> mergeKnnResults(SearchRequest request, List<DfsSearchResult> dfsSearchResults) {
        if (request.hasKnnSearch() == false) {
            return Collections.emptyList();
        }
        SearchSourceBuilder source = request.source();
        List<List<TopDocs>> topDocsLists = new ArrayList<>(source.knnSearch().size());
        List<SetOnce<String>> nestedPath = new ArrayList<>(source.knnSearch().size());
        for (int i = 0; i < source.knnSearch().size(); i++) {
            topDocsLists.add(new ArrayList<>());
            nestedPath.add(new SetOnce<>());
        }

        for (DfsSearchResult dfsSearchResult : dfsSearchResults) {
            if (dfsSearchResult.knnResults() != null) {
                for (int i = 0; i < dfsSearchResult.knnResults().size(); i++) {
                    DfsKnnResults knnResults = dfsSearchResult.knnResults().get(i);
                    ScoreDoc[] scoreDocs = knnResults.scoreDocs();
                    TotalHits totalHits = new TotalHits(scoreDocs.length, TotalHits.Relation.EQUAL_TO);
                    TopDocs shardTopDocs = new TopDocs(totalHits, scoreDocs);
                    SearchPhaseController.setShardIndex(shardTopDocs, dfsSearchResult.getShardIndex());
                    topDocsLists.get(i).add(shardTopDocs);
                    nestedPath.get(i).trySet(knnResults.getNestedPath());
                }
            }
        }

        List<DfsKnnResults> mergedResults = new ArrayList<>(source.knnSearch().size());
        for (int i = 0; i < source.knnSearch().size(); i++) {
            List<TopDocs> topDocsList = topDocsLists.get(i);
            if (topDocsList.isEmpty()) {
                mergedResults.add(new DfsKnnResults(nestedPath.get(i).get(), new ScoreDoc[0]));
            } else {
                TopDocs mergedTopDocs = TopDocs.merge(source.knnSearch().get(i).k(), topDocsList.toArray(new TopDocs[0]));
                mergedResults.add(new DfsKnnResults(nestedPath.get(i).get(), mergedTopDocs.scoreDocs));
            }
        }
        return mergedResults;
    }

    private static AggregatedDfs aggregateDfs(Collection<DfsSearchResult> results) {
        Map<Term, TermStatistics> termStatistics = new HashMap<>();
        Map<String, CollectionStatistics> fieldStatistics = new HashMap<>();
        long aggMaxDoc = 0;
        for (DfsSearchResult lEntry : results) {
            final Term[] terms = lEntry.terms();
            final TermStatistics[] stats = lEntry.termStatistics();
            assert terms.length == stats.length;
            for (int i = 0; i < terms.length; i++) {
                assert terms[i] != null;
                if (stats[i] == null) {
                    continue;
                }
                TermStatistics existing = termStatistics.get(terms[i]);
                if (existing != null) {
                    assert terms[i].bytes().equals(existing.term());
                    termStatistics.put(
                        terms[i],
                        new TermStatistics(
                            existing.term(),
                            existing.docFreq() + stats[i].docFreq(),
                            existing.totalTermFreq() + stats[i].totalTermFreq()
                        )
                    );
                } else {
                    termStatistics.put(terms[i], stats[i]);
                }

            }

            assert lEntry.fieldStatistics().containsKey(null) == false;
            for (var entry : lEntry.fieldStatistics().entrySet()) {
                String key = entry.getKey();
                CollectionStatistics value = entry.getValue();
                if (value == null) {
                    continue;
                }
                assert key != null;
                CollectionStatistics existing = fieldStatistics.get(key);
                if (existing != null) {
                    CollectionStatistics merged = new CollectionStatistics(
                        key,
                        existing.maxDoc() + value.maxDoc(),
                        existing.docCount() + value.docCount(),
                        existing.sumTotalTermFreq() + value.sumTotalTermFreq(),
                        existing.sumDocFreq() + value.sumDocFreq()
                    );
                    fieldStatistics.put(key, merged);
                } else {
                    fieldStatistics.put(key, value);
                }
            }
            aggMaxDoc += lEntry.maxDoc();
        }
        return new AggregatedDfs(termStatistics, fieldStatistics, aggMaxDoc);
    }
}
