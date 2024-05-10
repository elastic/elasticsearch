/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.rank.rerank;

import org.apache.lucene.search.ScoreDoc;
import org.elasticsearch.action.search.SearchPhaseController;
import org.elasticsearch.search.query.QuerySearchResult;
import org.elasticsearch.search.rank.feature.RankFeatureDoc;
import org.elasticsearch.search.rank.feature.RankFeatureShardResult;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import java.util.List;

import static org.elasticsearch.search.internal.SearchContext.TRACK_TOTAL_HITS_ACCURATE;

public class RerankingQueryPhaseRankCoordinatorContextTests extends ESTestCase {

    private QuerySearchResult shard0SearchResult;
    private QuerySearchResult shard1SearchResult;
    private SearchPhaseController.TopDocsStats topDocsStats;

    @Before
    public void setup() {
        shard0SearchResult = new QuerySearchResult();
        shard0SearchResult.setRankShardResult(
            new RankFeatureShardResult(
                new RankFeatureDoc[] {
                    new RankFeatureDoc(0, 1.1f, 0), // Comment to keep line breaks
                    new RankFeatureDoc(2, 0.1f, 0),
                    new RankFeatureDoc(4, 8.8f, 0) }
            )
        );
        shard1SearchResult = new QuerySearchResult();
        shard1SearchResult.setRankShardResult(
            new RankFeatureShardResult(
                new RankFeatureDoc[] {
                    new RankFeatureDoc(1, 10.0f, 1), // Comment to keep line breaks
                    new RankFeatureDoc(3, 5.5f, 1),
                    new RankFeatureDoc(5, 6.6f, 1) }
            )
        );
        topDocsStats = new SearchPhaseController.TopDocsStats(TRACK_TOTAL_HITS_ACCURATE);
    }

    public void testRankQueryPhaseResults() {
        RerankingQueryPhaseRankCoordinatorContext subject = new RerankingQueryPhaseRankCoordinatorContext(100);
        ScoreDoc[] scoreDocs = subject.rankQueryPhaseResults(List.of(shard0SearchResult, shard1SearchResult), topDocsStats);

        assertEquals(6, topDocsStats.fetchHits);
        assertEquals(6, scoreDocs.length);
        // verify results are sorted by score descending, and the doc and shard IDs are set
        assertScoreDocEquals(new ScoreDoc(1, 10.0f, 1), scoreDocs[0]);
        assertScoreDocEquals(new ScoreDoc(4, 8.8f, 0), scoreDocs[1]);
        assertScoreDocEquals(new ScoreDoc(5, 6.6f, 1), scoreDocs[2]);
        assertScoreDocEquals(new ScoreDoc(3, 5.5f, 1), scoreDocs[3]);
        assertScoreDocEquals(new ScoreDoc(0, 1.1f, 0), scoreDocs[4]);
        assertScoreDocEquals(new ScoreDoc(2, 0.1f, 0), scoreDocs[5]);
    }

    public void testRankQueryPhaseResultsLimitsToWindowSize() {
        RerankingQueryPhaseRankCoordinatorContext subject = new RerankingQueryPhaseRankCoordinatorContext(3);
        ScoreDoc[] scoreDocs = subject.rankQueryPhaseResults(List.of(shard0SearchResult, shard1SearchResult), topDocsStats);

        // verify results are trimmed to windowSize
        assertEquals(3, topDocsStats.fetchHits);
        assertEquals(3, scoreDocs.length);
        assertScoreDocEquals(new ScoreDoc(1, 10.0f, 1), scoreDocs[0]);
        assertScoreDocEquals(new ScoreDoc(4, 8.8f, 0), scoreDocs[1]);
        assertScoreDocEquals(new ScoreDoc(5, 6.6f, 1), scoreDocs[2]);
    }

    private static void assertScoreDocEquals(ScoreDoc expected, ScoreDoc actual) {
        assertEquals(expected.doc, actual.doc);
        assertEquals(expected.score, actual.score, 0.0);
        assertEquals(expected.shardIndex, actual.shardIndex);
    }

}
