/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.rank.textsimilarity;

import org.elasticsearch.search.rank.feature.RankFeatureDoc;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.inference.results.RankedDocsResults;

import java.util.List;

import static org.elasticsearch.xpack.inference.rank.textsimilarity.TextSimilarityRankFeaturePhaseRankCoordinatorContext.extractScoresFromRankedDocs;

public class TextSimilarityRankFeaturePhaseRankCoordinatorContextTests extends ESTestCase {

    public void testExtractScoresFromRankedDocs() {
        final List<RankedDocsResults.RankedDoc> rankedDocs = List.of(
            new RankedDocsResults.RankedDoc(1, 3.0f, "text 2"),
            new RankedDocsResults.RankedDoc(2, 2.0f, "text 3"),
            new RankedDocsResults.RankedDoc(0, 1.0f, "text 1")
        );
        final RankFeatureDoc[] featureDocs = new RankFeatureDoc[] {
            createRankFeatureDoc(0, 0.25f, 0, List.of("text 1")),
            createRankFeatureDoc(1, 0.50f, 1, List.of("text 2")),
            createRankFeatureDoc(2, 0.75f, 2, List.of("text 3")) };

        float[] scores = extractScoresFromRankedDocs(rankedDocs, featureDocs);
        assertArrayEquals(new float[] { 1.0f, 3.0f, 2.0f }, scores, 0.0f);

        // Truncate the list of ranked docs. This simulates if the reranker service returns fewer docs than are in featureDocs, which can
        // happen if the service has an unreported top_n setting in use.
        IllegalStateException e = assertThrows(
            IllegalStateException.class,
            () -> extractScoresFromRankedDocs(rankedDocs.subList(0, 1), featureDocs)
        );
        assertEquals(
            "Expected ranked doc size to be 3, got 1. Is the reranker service using an unreported top N task setting?",
            e.getMessage()
        );
    }

    public void testExtractScoresFromMultipleChunks() {
        final List<RankedDocsResults.RankedDoc> rankedDocs = List.of(
            new RankedDocsResults.RankedDoc(3, 3.5f, "this is text 2"),
            new RankedDocsResults.RankedDoc(4, 2.5f, "this is text 3"),
            new RankedDocsResults.RankedDoc(1, 2.0f, "some more text"),
            new RankedDocsResults.RankedDoc(2, 1.5f, "yet more text"),
            new RankedDocsResults.RankedDoc(5, 1.5f, "oh look, more text"),
            new RankedDocsResults.RankedDoc(0, 1.0f, "this is text 1")
        );
        final RankFeatureDoc[] featureDocs = new RankFeatureDoc[] {
            createRankFeatureDoc(0, 1.0f, 0, List.of("this is text 1", "some more text")),
            createRankFeatureDoc(1, 3.0f, 1, List.of("yet more text", "this is text 2")),
            createRankFeatureDoc(2, 2.0f, 0, List.of("this is text 3", "oh look, more text")) };

        // Returned scores are from the best-ranking chunk, not the whole text
        float[] scores = extractScoresFromRankedDocs(rankedDocs, featureDocs);
        assertArrayEquals(new float[] { 2.0f, 3.5f, 2.5f }, scores, 0.0f);

        // Truncate the list of ranked docs. This simulates if the reranker service returns fewer docs than are in featureDocs, which can
        // happen if the service has an unreported top_n setting in use.
        IllegalStateException e = assertThrows(
            IllegalStateException.class,
            () -> extractScoresFromRankedDocs(rankedDocs.subList(0, 3), featureDocs)
        );
        assertEquals(
            "Expected ranked doc size to be 6, got 3. Is the reranker service using an unreported top N task setting?",
            e.getMessage()
        );
    }

    public void testExtractScoresFromRankedDocWithEmptyFeatureData() {
        final List<RankedDocsResults.RankedDoc> rankedDocs = List.of(
            new RankedDocsResults.RankedDoc(0, 3.0f, "text 1"),
            new RankedDocsResults.RankedDoc(1, 2.0f, "text 3")
        );
        final RankFeatureDoc[] featureDocs = new RankFeatureDoc[] {
            createRankFeatureDoc(0, 0.25f, 0, List.of("text 1")),
            createRankFeatureDoc(1, 0.50f, 1, List.of()),
            createRankFeatureDoc(2, 0.75f, 2, List.of("text 3")) };

        IllegalStateException e = assertThrows(IllegalStateException.class, () -> extractScoresFromRankedDocs(rankedDocs, featureDocs));
        assertEquals("Feature doc at index 1 does not have any features", e.getMessage());
    }

    public void testExtractScoresFromEmptyRankedDocs() {
        // Tests the scenario when there are no docs to rerank. In this case, both rankedDocs and featureDocs are empty.
        float[] scores = extractScoresFromRankedDocs(List.of(), new RankFeatureDoc[0]);
        assertEquals(0, scores.length);
    }

    private RankFeatureDoc createRankFeatureDoc(int doc, float score, int shardIndex, List<String> featureData) {
        RankFeatureDoc featureDoc = new RankFeatureDoc(doc, score, shardIndex);
        featureDoc.featureData(featureData);
        return featureDoc;
    }
}
