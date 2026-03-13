/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TermStatistics;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.OriginalIndices;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchShardTask;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.dfs.AggregatedDfs;
import org.elasticsearch.search.dfs.DfsSearchResult;
import org.elasticsearch.search.fetch.FetchSearchResult;
import org.elasticsearch.search.fetch.ShardFetchSearchRequest;
import org.elasticsearch.search.internal.AliasFilter;
import org.elasticsearch.search.internal.ShardSearchRequest;
import org.elasticsearch.search.query.QuerySearchRequest;
import org.elasticsearch.search.query.QuerySearchResult;
import org.elasticsearch.test.ESSingleNodeTestCase;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

import static java.util.Collections.emptyMap;
import static org.hamcrest.Matchers.greaterThan;

public class SearchDirectoryMetricsIT extends ESSingleNodeTestCase {

    public void testMultiPhaseSearchCapturesIncreasingBytesRead() throws Exception {
        assumeTrue("directory metrics must be enabled", Store.DIRECTORY_METRICS_FEATURE_FLAG.isEnabled());

        String indexName = randomIndexName();
        createIndex(indexName, indexSettings(1, 0).build());
        int numDocs = between(20, 80);
        IntStream.range(0, numDocs).forEach(i -> prepareIndex(indexName).setSource("field", "value" + i).get());
        indicesAdmin().prepareRefresh(indexName).get();

        SearchService searchService = getInstanceFromNode(SearchService.class);
        IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        IndexService indexService = indicesService.indexServiceSafe(resolveIndex(indexName));
        IndexShard indexShard = indexService.getShard(0);

        SearchRequest searchRequest = new SearchRequest().allowPartialSearchResults(true);
        searchRequest.source(new SearchSourceBuilder().query(new TermQueryBuilder("field", "value0")).size(10));

        ShardSearchRequest shardRequest = new ShardSearchRequest(
            OriginalIndices.NONE,
            searchRequest,
            indexShard.shardId(),
            0,
            1,
            AliasFilter.EMPTY,
            1.0f,
            -1,
            null
        );

        // --- Phase 1: DFS ---
        PlainActionFuture<SearchPhaseResult> dfsFuture = new PlainActionFuture<>();
        searchService.executeDfsPhase(shardRequest, new SearchShardTask(1L, "", "", "", null, emptyMap()), dfsFuture);
        DfsSearchResult dfsResult = (DfsSearchResult) dfsFuture.get();

        long dfsBytesRead = dfsResult.getBytesRead();
        assertThat(dfsBytesRead, greaterThan(0L));

        AggregatedDfs aggregatedDfs = buildAggregatedDfs(dfsResult);

        // --- Phase 2: Query ---
        PlainActionFuture<QuerySearchResult> queryFuture = new PlainActionFuture<>();
        searchService.executeQueryPhase(
            new QuerySearchRequest(OriginalIndices.NONE, dfsResult.getContextId(), shardRequest, aggregatedDfs),
            new SearchShardTask(2L, "", "", "", null, emptyMap()),
            queryFuture.delegateFailure((l, r) -> {
                r.incRef();
                l.onResponse(r);
            }),
            TransportVersion.current()
        );
        QuerySearchResult queryResult = queryFuture.get();

        long queryBytesRead = queryResult.getBytesRead();
        assertThat(queryBytesRead, greaterThan(0L));

        ScoreDoc[] scoreDocs = queryResult.topDocs().topDocs.scoreDocs;
        assertThat(scoreDocs.length, greaterThan(0));

        List<Integer> docIds = new ArrayList<>();
        for (ScoreDoc scoreDoc : scoreDocs) {
            docIds.add(scoreDoc.doc);
        }

        // --- Phase 3: Fetch ---
        PlainActionFuture<FetchSearchResult> fetchFuture = new PlainActionFuture<>();
        searchService.executeFetchPhase(
            new ShardFetchSearchRequest(
                OriginalIndices.NONE,
                queryResult.getContextId(),
                shardRequest,
                docIds,
                null,
                null,
                queryResult.getRescoreDocIds(),
                aggregatedDfs
            ),
            new SearchShardTask(3L, "", "", "", null, emptyMap()),
            fetchFuture.delegateFailure((l, r) -> {
                r.incRef();
                l.onResponse(r);
            })
        );
        FetchSearchResult fetchResult = fetchFuture.get();

        try {
            long fetchBytesRead = fetchResult.getBytesRead();
            assertThat(fetchBytesRead, greaterThan(0L));

            long totalBytesRead = dfsBytesRead + queryBytesRead + fetchBytesRead;
            assertThat(totalBytesRead, greaterThan(Math.max(dfsBytesRead, Math.max(queryBytesRead, fetchBytesRead))));
        } finally {
            fetchResult.decRef();
            queryResult.decRef();
        }
    }

    private static AggregatedDfs buildAggregatedDfs(DfsSearchResult dfsResult) {
        Map<Term, TermStatistics> termStats = new HashMap<>();
        Term[] terms = dfsResult.terms();
        TermStatistics[] stats = dfsResult.termStatistics();
        if (terms != null && stats != null) {
            for (int i = 0; i < terms.length; i++) {
                if (stats[i] != null) {
                    termStats.put(terms[i], stats[i]);
                }
            }
        }
        return new AggregatedDfs(termStats, dfsResult.fieldStatistics(), dfsResult.maxDoc());
    }
}
