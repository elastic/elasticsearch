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
import org.apache.lucene.index.Term;
import org.apache.lucene.search.CollectionStatistics;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TermStatistics;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHits;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.search.SearchPhaseResult;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.dfs.AggregatedDfs;
import org.elasticsearch.search.dfs.DfsKnnResults;
import org.elasticsearch.search.dfs.DfsSearchResult;
import org.elasticsearch.search.internal.AliasFilter;
import org.elasticsearch.transport.Transport;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.function.BiFunction;

final class SearchDfsQueryThenFetchAsyncAction extends AbstractSearchAsyncAction<DfsSearchResult> {

    private final SearchPhaseResults<SearchPhaseResult> queryPhaseResultConsumer;
    private final SearchProgressListener progressListener;
    private final Client client;

    SearchDfsQueryThenFetchAsyncAction(
        Logger logger,
        NamedWriteableRegistry namedWriteableRegistry,
        SearchTransportService searchTransportService,
        BiFunction<String, String, Transport.Connection> nodeIdToConnection,
        Map<String, AliasFilter> aliasFilter,
        Map<String, Float> concreteIndexBoosts,
        Executor executor,
        SearchPhaseResults<SearchPhaseResult> queryPhaseResultConsumer,
        SearchRequest request,
        ActionListener<SearchResponse> listener,
        List<SearchShardIterator> shardsIts,
        TransportSearchAction.SearchTimeProvider timeProvider,
        ClusterState clusterState,
        SearchTask task,
        SearchResponse.Clusters clusters,
        Client client
    ) {
        super(
            "dfs",
            logger,
            namedWriteableRegistry,
            searchTransportService,
            nodeIdToConnection,
            aliasFilter,
            concreteIndexBoosts,
            executor,
            request,
            listener,
            shardsIts,
            timeProvider,
            clusterState,
            task,
            new ArraySearchPhaseResults<>(shardsIts.size()),
            request.getMaxConcurrentShardRequests(),
            clusters
        );
        this.queryPhaseResultConsumer = queryPhaseResultConsumer;
        addReleasable(queryPhaseResultConsumer);
        this.progressListener = task.getProgressListener();
        // don't build the SearchShard list (can be expensive) if the SearchProgressListener won't use it
        if (progressListener != SearchProgressListener.NOOP) {
            notifyListShards(progressListener, clusters, request.source());
        }
        this.client = client;
    }

    @Override
    protected void executePhaseOnShard(
        final SearchShardIterator shardIt,
        final Transport.Connection connection,
        final SearchActionListener<DfsSearchResult> listener
    ) {
        getSearchTransport().sendExecuteDfs(connection, buildShardSearchRequest(shardIt, listener.requestIndex), getTask(), listener);
    }

    @Override
    protected SearchPhase getNextPhase() {
        final List<DfsSearchResult> dfsSearchResults = results.getAtomicArray().asList();
        final AggregatedDfs aggregatedDfs = aggregateDfs(dfsSearchResults);
        return new DfsQueryPhase(
            dfsSearchResults,
            aggregatedDfs,
            mergeKnnResults(getRequest(), dfsSearchResults),
            queryPhaseResultConsumer,
            (queryResults) -> SearchQueryThenFetchAsyncAction.nextPhase(client, this, queryResults, aggregatedDfs),
            this
        );
    }

    @Override
    protected void onShardGroupFailure(int shardIndex, SearchShardTarget shardTarget, Exception exc) {
        progressListener.notifyQueryFailure(shardIndex, shardTarget, exc);
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
