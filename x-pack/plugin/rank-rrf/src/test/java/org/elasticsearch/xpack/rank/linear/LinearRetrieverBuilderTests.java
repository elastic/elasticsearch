/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.xpack.rank.linear;

import org.apache.lucene.search.ScoreDoc;
import org.elasticsearch.search.rank.RankDoc;
import org.elasticsearch.search.retriever.CompoundRetrieverBuilder.RetrieverSource;
import org.elasticsearch.search.retriever.TestRetrieverBuilder;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.List;
import java.util.stream.Stream;

public class LinearRetrieverBuilderTests extends ESTestCase {

    public void testCombineInnerRetrieverResultsCalculatesWeightedSum() throws IOException {
        List<RetrieverSource> sources = List.of(
            new RetrieverSource(new TestRetrieverBuilder("r1"), null),
            new RetrieverSource(new TestRetrieverBuilder("r2"), null)
        );
        int rankWindowSize = 5;
        float[] weights = new float[] { 1.0f, 2.0f };
        ScoreNormalizer[] normalizers = new ScoreNormalizer[] { IdentityScoreNormalizer.INSTANCE, IdentityScoreNormalizer.INSTANCE };
        float minScore = 0f;

        LinearRetrieverBuilder builder = new LinearRetrieverBuilder(sources, rankWindowSize, weights, normalizers, minScore);

        ScoreDoc[] left = new ScoreDoc[] { new ScoreDoc(5, 1.0f, 0), new ScoreDoc(6, 2.0f, 0) };
        ScoreDoc[] right = new ScoreDoc[] { new ScoreDoc(5, 3.0f, 0), new ScoreDoc(6, 1.0f, 0), new ScoreDoc(7, 0.5f, 0) };

        RankDoc[] results = builder.combineInnerRetrieverResults(List.of(left, right), false);

        assertEquals("Should have 3 combined docs", 3, results.length);

        assertEquals("Doc 5 score", 7.0f, results[0].score, 0.001f);
        assertEquals("Doc 5 ID", 5, results[0].doc);
        assertEquals("Doc 5 rank", 1, results[0].rank);

        assertEquals("Doc 6 score", 4.0f, results[1].score, 0.001f);
        assertEquals("Doc 6 ID", 6, results[1].doc);
        assertEquals("Doc 6 rank", 2, results[1].rank);

        assertEquals("Doc 7 score", 1.0f, results[2].score, 0.001f);
        assertEquals("Doc 7 ID", 7, results[2].doc);
        assertEquals("Doc 7 rank", 3, results[2].rank);
    }

    public void testCombineAndFilterWithMinScore() throws IOException {
        List<RetrieverSource> sources = List.of(
            new RetrieverSource(new TestRetrieverBuilder("r1"), null),
            new RetrieverSource(new TestRetrieverBuilder("r2"), null)
        );
        int rankWindowSize = 5;
        float[] weights = new float[] { 1.0f, 1.0f };
        ScoreNormalizer[] normalizers = new ScoreNormalizer[] { IdentityScoreNormalizer.INSTANCE, IdentityScoreNormalizer.INSTANCE };
        final float minScoreThreshold = 1.5f;

        LinearRetrieverBuilder builder = new LinearRetrieverBuilder(sources, rankWindowSize, weights, normalizers, minScoreThreshold);
        assertEquals(minScoreThreshold, builder.getMinScore(), 0f);

        ScoreDoc[] left = new ScoreDoc[] { new ScoreDoc(0, 1.0f, 0), new ScoreDoc(1, 0.8f, 0) };
        ScoreDoc[] right = new ScoreDoc[] { new ScoreDoc(0, 2.0f, 0), new ScoreDoc(1, 0.6f, 0), new ScoreDoc(2, 1.8f, 0) };

        RankDoc[] combinedRankDocs = builder.combineInnerRetrieverResults(List.of(left, right), false);
        assertEquals("Combined docs count (already filtered)", 2, combinedRankDocs.length);

        List<RankDoc> filteredDocs = Stream.of(combinedRankDocs).sorted().toList();

        assertEquals("Filtered docs count", 2, filteredDocs.size());

        boolean foundDoc0 = false;
        boolean foundDoc2 = false;
        for (RankDoc scoreDoc : filteredDocs) {
            assertTrue("Score should be >= minScore", scoreDoc.score >= minScoreThreshold);
            if (scoreDoc.doc == 0) {
                assertEquals("Doc 0 score", 3.0f, scoreDoc.score, 0.001f);
                foundDoc0 = true;
            } else if (scoreDoc.doc == 2) {
                assertEquals("Doc 2 score", 1.8f, scoreDoc.score, 0.001f);
                foundDoc2 = true;
            } else {
                fail("Unexpected document ID returned: " + scoreDoc.doc + " (should have been filtered)");
            }
        }
        assertTrue("Document 0 should have been found", foundDoc0);
        assertTrue("Document 2 should have been found", foundDoc2);
    }
}
