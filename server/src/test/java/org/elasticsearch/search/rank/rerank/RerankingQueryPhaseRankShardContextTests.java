/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.rank.rerank;

import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHits;
import org.elasticsearch.search.rank.feature.RankFeatureDoc;
import org.elasticsearch.search.rank.feature.RankFeatureShardResult;
import org.elasticsearch.test.ESTestCase;

import java.util.List;

public class RerankingQueryPhaseRankShardContextTests extends ESTestCase {

    public void testCombineQueryPhaseResults() {
        List<TopDocs> topDocsList = List.of(
            new TopDocs(
                new TotalHits(2, TotalHits.Relation.EQUAL_TO),
                new ScoreDoc[] { new ScoreDoc(0, 4.0f, 0), new ScoreDoc(1, 3.0f, 0) }
            ),
            new TopDocs(
                new TotalHits(2, TotalHits.Relation.EQUAL_TO),
                new ScoreDoc[] { new ScoreDoc(2, 2.0f, 0), new ScoreDoc(3, 1.0f, 0) }
            )
        );

        RerankingQueryPhaseRankShardContext subject = new RerankingQueryPhaseRankShardContext(List.of(), 50);
        RankFeatureShardResult rankShardResult = (RankFeatureShardResult) subject.combineQueryPhaseResults(topDocsList);

        // verify results are sorted by score descending, and the doc and shard IDs are set
        assertEquals(4, rankShardResult.rankFeatureDocs.length);
        assertRankFeatureDocEquals(new RankFeatureDoc(0, 4.0f, 0), rankShardResult.rankFeatureDocs[0]);
        assertRankFeatureDocEquals(new RankFeatureDoc(1, 3.0f, 0), rankShardResult.rankFeatureDocs[1]);
        assertRankFeatureDocEquals(new RankFeatureDoc(2, 2.0f, 0), rankShardResult.rankFeatureDocs[2]);
        assertRankFeatureDocEquals(new RankFeatureDoc(3, 1.0f, 0), rankShardResult.rankFeatureDocs[3]);
    }

    public void testCombineQueryPhaseResultsCombinesScoresOfSameDocsFromDifferentQueries() {
        List<TopDocs> topDocsList = List.of(
            new TopDocs(
                new TotalHits(2, TotalHits.Relation.EQUAL_TO),
                new ScoreDoc[] { new ScoreDoc(0, 4.0f, 0), new ScoreDoc(1, 3.0f, 0) }
            ),
            new TopDocs(
                new TotalHits(2, TotalHits.Relation.EQUAL_TO),
                new ScoreDoc[] { new ScoreDoc(0, 5.0f, 0), new ScoreDoc(3, 1.0f, 0) }
            )
        );

        RerankingQueryPhaseRankShardContext subject = new RerankingQueryPhaseRankShardContext(List.of(), 50);
        RankFeatureShardResult rankShardResult = (RankFeatureShardResult) subject.combineQueryPhaseResults(topDocsList);

        // verify that for doc 0 the highest score is selected
        assertEquals(3, rankShardResult.rankFeatureDocs.length);
        assertRankFeatureDocEquals(new RankFeatureDoc(0, 5.0f, 0), rankShardResult.rankFeatureDocs[0]);
        assertRankFeatureDocEquals(new RankFeatureDoc(1, 3.0f, 0), rankShardResult.rankFeatureDocs[1]);
        assertRankFeatureDocEquals(new RankFeatureDoc(3, 1.0f, 0), rankShardResult.rankFeatureDocs[2]);
    }

    private static void assertRankFeatureDocEquals(RankFeatureDoc expected, RankFeatureDoc actual) {
        assertEquals(expected.doc, actual.doc);
        assertEquals(expected.shardIndex, actual.shardIndex);
        assertEquals(expected.score, actual.score, 0.0);
    }

}
