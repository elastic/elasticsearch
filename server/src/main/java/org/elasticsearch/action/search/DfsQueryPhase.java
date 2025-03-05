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
import org.apache.lucene.search.join.ScoreMode;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.index.query.NestedQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.SearchPhaseResult;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.builder.SubSearchSourceBuilder;
import org.elasticsearch.search.dfs.AggregatedDfs;
import org.elasticsearch.search.dfs.DfsKnnResults;
import org.elasticsearch.search.dfs.DfsSearchResult;
import org.elasticsearch.search.internal.ShardSearchRequest;
import org.elasticsearch.search.query.QuerySearchRequest;
import org.elasticsearch.search.query.QuerySearchResult;
import org.elasticsearch.search.vectors.KnnScoreDocQueryBuilder;
import org.elasticsearch.transport.Transport;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * This search phase fans out to every shards to execute a distributed search with a pre-collected distributed frequencies for all
 * search terms used in the actual search query. This phase is very similar to a the default query-then-fetch search phase but it doesn't
 * retry on another shard if any of the shards are failing. Failures are treated as shard failures and are counted as a non-successful
 * operation.
 * @see CountedCollector#onFailure(int, SearchShardTarget, Exception)
 */
class DfsQueryPhase extends SearchPhase {

    public static final String NAME = "dfs_query";

    private final SearchPhaseResults<SearchPhaseResult> queryResult;
    private final Client client;
    private final AbstractSearchAsyncAction<?> context;
    private final SearchProgressListener progressListener;

    DfsQueryPhase(SearchPhaseResults<SearchPhaseResult> queryResult, Client client, AbstractSearchAsyncAction<?> context) {
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

    @SuppressWarnings("unchecked")
    @Override
    protected void run() {
        List<DfsSearchResult> searchResults = (List<DfsSearchResult>) context.results.getAtomicArray().asList();
        AggregatedDfs dfs = aggregateDfs(searchResults);
        // TODO we can potentially also consume the actual per shard results from the initial phase here in the aggregateDfs
        // to free up memory early
        final CountedCollector<SearchPhaseResult> counter = new CountedCollector<>(
            queryResult,
            searchResults.size(),
            () -> context.executeNextPhase(NAME, () -> nextPhase(dfs)),
            context
        );

        List<DfsKnnResults> knnResults = mergeKnnResults(context.getRequest(), searchResults);
        for (final DfsSearchResult dfsResult : searchResults) {
            final SearchShardTarget shardTarget = dfsResult.getSearchShardTarget();
            final int shardIndex = dfsResult.getShardIndex();
            QuerySearchRequest querySearchRequest = new QuerySearchRequest(
                context.getOriginalIndices(shardIndex),
                dfsResult.getContextId(),
                rewriteShardSearchRequest(knnResults, dfsResult.getShardSearchRequest()),
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
                    protected void innerOnResponse(QuerySearchResult response) {
                        try {
                            response.setSearchProfileDfsPhaseResult(dfsResult.searchProfileDfsPhaseResult());
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
                });
        }
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

    // package private for testing
    ShardSearchRequest rewriteShardSearchRequest(List<DfsKnnResults> knnResults, ShardSearchRequest request) {
        SearchSourceBuilder source = request.source();
        if (source == null || source.knnSearch().isEmpty()) {
            return request;
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
                source.knnSearch().get(i).getSimilarity()
            ).boost(source.knnSearch().get(i).boost()).queryName(source.knnSearch().get(i).queryName());
            if (nestedPath != null) {
                query = new NestedQueryBuilder(nestedPath, query, ScoreMode.Max).innerHit(source.knnSearch().get(i).innerHit());
            }
            subSearchSourceBuilders.add(new SubSearchSourceBuilder(query));
            i++;
        }

        source = source.shallowCopy().subSearches(subSearchSourceBuilders).knnSearch(List.of());
        request.source(source);

        return request;
    }

    private static List<DfsKnnResults> mergeKnnResults(SearchRequest request, List<DfsSearchResult> dfsSearchResults) {
        if (request.hasKnnSearch() == false) {
            return null;
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
            TopDocs mergedTopDocs = TopDocs.merge(source.knnSearch().get(i).k(), topDocsLists.get(i).toArray(new TopDocs[0]));
            mergedResults.add(new DfsKnnResults(nestedPath.get(i).get(), mergedTopDocs.scoreDocs));
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
