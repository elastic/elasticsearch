/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.search;

import org.apache.lucene.search.ScoreDoc;
import org.elasticsearch.cluster.routing.SplitShardCountSummary;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.dfs.DfsKnnResults;
import org.elasticsearch.search.internal.ShardSearchRequest;
import org.elasticsearch.search.rank.TestRankBuilder;
import org.elasticsearch.search.vectors.KnnScoreDocQueryBuilder;
import org.elasticsearch.search.vectors.KnnSearchBuilder;
import org.elasticsearch.search.vectors.VectorData;
import org.elasticsearch.test.ESTestCase;

import java.util.Collections;
import java.util.List;

public class SearchDfsQueryThenFetchAsyncActionTests extends ESTestCase {

    public void testRewriteShardSearchRequestWithRank() {

        QueryBuilder bm25 = new TermQueryBuilder("field", "term");
        SearchSourceBuilder ssb = new SearchSourceBuilder().query(bm25)
            .knnSearch(
                List.of(
                    new KnnSearchBuilder("vector", new float[] { 0.0f }, 10, 100, 10f, null, null),
                    new KnnSearchBuilder("vector2", new float[] { 0.0f }, 10, 100, 10f, null, null)
                )
            )
            .rankBuilder(new TestRankBuilder(100));
        SearchRequest sr = new SearchRequest().allowPartialSearchResults(true).source(ssb);
        MockSearchDfsQueryThenFetchAsyncAction mockSearchPhaseContext = new MockSearchDfsQueryThenFetchAsyncAction(2, sr);

        mockSearchPhaseContext.knnResults = List.of(
            new DfsKnnResults(null, new ScoreDoc[] { new ScoreDoc(1, 3.0f, 1), new ScoreDoc(4, 1.5f, 1), new ScoreDoc(7, 0.1f, 2) }),
            new DfsKnnResults(
                null,
                new ScoreDoc[] { new ScoreDoc(2, 1.75f, 2), new ScoreDoc(1, 2.0f, 1), new ScoreDoc(3, 0.25f, 2), new ScoreDoc(6, 2.5f, 2) }
            )
        );

        SearchShardIterator searchShardIterator = new SearchShardIterator(
            null,
            new ShardId("index", "uuid", 0),
            Collections.emptyList(),
            null,
            SplitShardCountSummary.UNSET
        );

        ShardSearchRequest rewritten = mockSearchPhaseContext.buildShardSearchRequest(searchShardIterator, 1);

        KnnScoreDocQueryBuilder ksdqb0 = new KnnScoreDocQueryBuilder(
            new ScoreDoc[] { new ScoreDoc(1, 3.0f, 1), new ScoreDoc(4, 1.5f, 1) },
            "vector",
            VectorData.fromFloats(new float[] { 0.0f }),
            null,
            List.of()
        );
        KnnScoreDocQueryBuilder ksdqb1 = new KnnScoreDocQueryBuilder(
            new ScoreDoc[] { new ScoreDoc(1, 2.0f, 1) },
            "vector2",
            VectorData.fromFloats(new float[] { 0.0f }),
            null,
            List.of()
        );
        assertEquals(
            List.of(bm25, ksdqb0, ksdqb1),
            List.of(
                rewritten.source().subSearches().get(0).getQueryBuilder(),
                rewritten.source().subSearches().get(1).getQueryBuilder(),
                rewritten.source().subSearches().get(2).getQueryBuilder()
            )
        );

        mockSearchPhaseContext.results.close();
    }

}
