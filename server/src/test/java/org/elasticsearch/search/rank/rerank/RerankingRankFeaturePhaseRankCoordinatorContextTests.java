/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.rank.rerank;

import org.apache.lucene.search.ScoreDoc;
import org.elasticsearch.search.rank.feature.RankFeatureDoc;
import org.elasticsearch.search.rank.feature.RankFeatureResult;
import org.elasticsearch.search.rank.feature.RankFeatureShardResult;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

public class RerankingRankFeaturePhaseRankCoordinatorContextTests extends ESTestCase {

    static class TestRerankingRankFeaturePhaseRankCoordinatorContext extends RerankingRankFeaturePhaseRankCoordinatorContext {

        TestRerankingRankFeaturePhaseRankCoordinatorContext() {
            this(10, 0);
        }

        TestRerankingRankFeaturePhaseRankCoordinatorContext(int size, int from) {
            super(size, from, 100);
        }

        @Override
        protected void computeScores(RankFeatureDoc[] featureDocs, BiConsumer<Integer, Float> scoreConsumer, Runnable onFinish) {
            // Set new scores to the docs, then call onFinish as per the contract of computeScores()
            scoreConsumer.accept(0, 10.0f);
            scoreConsumer.accept(1, 11.0f);
            scoreConsumer.accept(2, 12.0f);
            scoreConsumer.accept(3, 13.0f);
            onFinish.run();
        }
    }

    private List<RankFeatureResult> rankFeatureResults;
    private final List<ScoreDoc> rerankedDocs = new ArrayList<>();
    // simple callback that adds results to rerankedDocs for testing; will be called at the end of rankGlobalResults()
    private final Consumer<ScoreDoc[]> setRerankedDocs = (scoreDocs) -> rerankedDocs.addAll(Arrays.asList(scoreDocs));

    @Before
    public void setup() {
        RankFeatureResult rankFeatureResult1 = new RankFeatureResult();
        rankFeatureResult1.shardResult(new RankFeatureShardResult(new RankFeatureDoc[] {
            new RankFeatureDoc(0, 1.0f, 0),
            new RankFeatureDoc(1, 2.0f, 0)
        }));
        RankFeatureResult rankFeatureResult2 = new RankFeatureResult();
        rankFeatureResult2.shardResult(new RankFeatureShardResult(new RankFeatureDoc[] {
            new RankFeatureDoc(2, 3.0f, 1),
            new RankFeatureDoc(3, 4.0f, 1)
        }));
        rankFeatureResults = List.of(rankFeatureResult1, rankFeatureResult2);
        rerankedDocs.clear();
    }

    public void testRankGlobalResults() {
        RerankingRankFeaturePhaseRankCoordinatorContext subject = new TestRerankingRankFeaturePhaseRankCoordinatorContext();
        subject.rankGlobalResults(rankFeatureResults, setRerankedDocs);

        // verify the docs were reranked by their new scores in descending order
        assertEquals(4, rerankedDocs.size());
        assertRerankedDocProperties(1, 13.0f, rerankedDocs.get(0));
        assertRerankedDocProperties(2, 12.0f, rerankedDocs.get(1));
        assertRerankedDocProperties(3, 11.0f, rerankedDocs.get(2));
        assertRerankedDocProperties(4, 10.0f, rerankedDocs.get(3));
    }

    public void testRankGlobalResultsLimitsHitsToSize() {
        RerankingRankFeaturePhaseRankCoordinatorContext subject = new TestRerankingRankFeaturePhaseRankCoordinatorContext(2, 0);
        subject.rankGlobalResults(rankFeatureResults, setRerankedDocs);

        // verify the docs are trimmed to size
        assertEquals(2, rerankedDocs.size());
        assertRerankedDocProperties(1, 13.0f, rerankedDocs.get(0));
        assertRerankedDocProperties(2, 12.0f, rerankedDocs.get(1));
    }

    public void testRankGlobalResultsPaginatesHits() {
        RerankingRankFeaturePhaseRankCoordinatorContext subject = new TestRerankingRankFeaturePhaseRankCoordinatorContext(2, 2);
        subject.rankGlobalResults(rankFeatureResults, setRerankedDocs);

        // verify the docs are paginated and start at from
        assertEquals(2, rerankedDocs.size());
        assertRerankedDocProperties(3, 11.0f, rerankedDocs.get(0));
        assertRerankedDocProperties(4, 10.0f, rerankedDocs.get(1));
    }

    private static void assertRerankedDocProperties(int expectedRank, float expectedScore, ScoreDoc actualDoc) {
        assertTrue(actualDoc instanceof RankFeatureDoc);
        assertEquals(expectedScore, actualDoc.score, 0.0f);
        assertEquals(expectedRank, ((RankFeatureDoc) actualDoc).rank);
    }

}
