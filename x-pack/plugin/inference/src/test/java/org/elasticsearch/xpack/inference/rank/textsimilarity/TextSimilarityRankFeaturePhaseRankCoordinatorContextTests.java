/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.rank.textsimilarity;

import org.elasticsearch.client.internal.Client;
import org.elasticsearch.search.rank.feature.RankFeatureDoc;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.inference.action.GetInferenceModelAction;
import org.elasticsearch.xpack.core.inference.action.GetRerankerWindowSizeAction;
import org.elasticsearch.xpack.core.inference.results.RankedDocsResults;

import java.util.List;

import static org.elasticsearch.action.support.ActionTestUtils.assertNoFailureListener;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

public class TextSimilarityRankFeaturePhaseRankCoordinatorContextTests extends ESTestCase {

    private final Client mockClient = mock(Client.class);

    TextSimilarityRankFeaturePhaseRankCoordinatorContext subject = new TextSimilarityRankFeaturePhaseRankCoordinatorContext(
        10,
        0,
        100,
        mockClient,
        "my-inference-id",
        "some query",
        0.0f,
        false,
        null
    );

    TextSimilarityRankFeaturePhaseRankCoordinatorContext withChunks = new TextSimilarityRankFeaturePhaseRankCoordinatorContext(
        10,
        0,
        100,
        mockClient,
        "my-inference-id",
        "some query",
        0.0f,
        false,
        new ChunkScorerConfig(2, "some query", null)
    );

    public void testExtractScoresFromRankedDocs() {
        List<RankedDocsResults.RankedDoc> rankedDocs = List.of(
            new RankedDocsResults.RankedDoc(0, 1.0f, "text 1"),
            new RankedDocsResults.RankedDoc(1, 3.0f, "text 2"),
            new RankedDocsResults.RankedDoc(2, 2.0f, "text 3")
        );
        float[] scores = subject.extractScoresFromRankedDocs(rankedDocs);
        assertArrayEquals(new float[] { 1.0f, 3.0f, 2.0f }, scores, 0.0f);
    }

    public void testExtractScoresFromSingleChunk() {

        List<RankedDocsResults.RankedDoc> rankedDocs = List.of(
            new RankedDocsResults.RankedDoc(0, 1.0f, "text 1"),
            new RankedDocsResults.RankedDoc(1, 2.5f, "text 2"),
            new RankedDocsResults.RankedDoc(2, 1.5f, "text 3")
        );
        RankFeatureDoc[] featureDocs = new RankFeatureDoc[] {
            createRankFeatureDoc(0, 1.0f, 0, List.of("text 1")),
            createRankFeatureDoc(1, 3.0f, 1, List.of("text 2")),
            createRankFeatureDoc(2, 2.0f, 0, List.of("text 3")) };

        float[] scores = withChunks.extractScoresFromRankedChunks(rankedDocs, featureDocs);
        // Returned cores are from the chunk, not the whole text
        assertArrayEquals(new float[] { 1.0f, 2.5f, 1.5f }, scores, 0.0f);
    }

    public void testExtractScoresFromMultipleChunks() {

        List<RankedDocsResults.RankedDoc> rankedDocs = List.of(
            new RankedDocsResults.RankedDoc(0, 1.0f, "this is text 1"),
            new RankedDocsResults.RankedDoc(1, 2.5f, "some more text"),
            new RankedDocsResults.RankedDoc(2, 1.5f, "yet more text"),
            new RankedDocsResults.RankedDoc(3, 3.0f, "this is text 2"),
            new RankedDocsResults.RankedDoc(4, 2.0f, "this is text 3"),
            new RankedDocsResults.RankedDoc(5, 1.5f, "oh look, more text")
        );
        RankFeatureDoc[] featureDocs = new RankFeatureDoc[] {
            createRankFeatureDoc(0, 1.0f, 0, List.of("this is text 1", "some more text")),
            createRankFeatureDoc(1, 3.0f, 1, List.of("yet more text", "this is text 2")),
            createRankFeatureDoc(2, 2.0f, 0, List.of("this is text 3", "oh look, more text")) };

        float[] scores = withChunks.extractScoresFromRankedChunks(rankedDocs, featureDocs);
        // Returned scores are from the best-ranking chunk, not the whole text
        assertArrayEquals(new float[] { 2.5f, 3.0f, 2.0f }, scores, 0.0f);
    }

    private RankFeatureDoc createRankFeatureDoc(int doc, float score, int shardIndex, List<String> featureData) {
        RankFeatureDoc featureDoc = new RankFeatureDoc(doc, score, shardIndex);
        featureDoc.featureData(featureData);
        return featureDoc;
    }

    public void testComputeScoresWithAutoResolveChunkingSettings() {
        ChunkScorerConfig configWithNullSettings = new ChunkScorerConfig(3, "test query", null);
        TextSimilarityRankFeaturePhaseRankCoordinatorContext context = new TextSimilarityRankFeaturePhaseRankCoordinatorContext(
            10,
            0,
            100,
            mockClient,
            "my-inference-id",
            "some query",
            0.0f,
            false,
            configWithNullSettings
        );

        context.computeScores(new RankFeatureDoc[0], assertNoFailureListener(scores -> {}));

        verify(mockClient).execute(
            eq(GetInferenceModelAction.INSTANCE),
            argThat(request -> "my-inference-id".equals(((GetInferenceModelAction.Request) request).getInferenceEntityId())),
            any()
        );
    }

    public void testComputeScoresWithUserProvidedChunkingSettings() {
        ChunkScorerConfig config = new ChunkScorerConfig(3, "test query", ChunkScorerConfig.defaultChunkingSettings(128));
        TextSimilarityRankFeaturePhaseRankCoordinatorContext context = new TextSimilarityRankFeaturePhaseRankCoordinatorContext(
            10,
            0,
            100,
            mockClient,
            "my-inference-id",
            "some query",
            0.0f,
            false,
            config
        );

        context.computeScores(new RankFeatureDoc[0], assertNoFailureListener(scores -> {}));

        verify(mockClient, never()).execute(eq(GetRerankerWindowSizeAction.INSTANCE), any(), any());
    }

    public void testResolveChunkingSettingsReturnsNullWhenNoConfig() {
        TextSimilarityRankFeaturePhaseRankCoordinatorContext context = new TextSimilarityRankFeaturePhaseRankCoordinatorContext(
            10,
            0,
            100,
            mockClient,
            "my-inference-id",
            "some query",
            0.0f,
            false,
            null
        );

        assertNull(context.resolveChunkingSettings(123));
    }

    public void testResolveChunkingSettingsReturnsOriginalWhenChunkingSettingsPresent() {
        ChunkScorerConfig config = new ChunkScorerConfig(3, "test query", ChunkScorerConfig.defaultChunkingSettings(128));
        TextSimilarityRankFeaturePhaseRankCoordinatorContext context = new TextSimilarityRankFeaturePhaseRankCoordinatorContext(
            10,
            0,
            100,
            mockClient,
            "my-inference-id",
            "some query",
            0.0f,
            false,
            config
        );

        ChunkScorerConfig resolved = context.resolveChunkingSettings(-1);
        assertSame(config, resolved);
    }

    public void testResolveChunkingSettingsUsesEndpointWindow() {
        ChunkScorerConfig config = new ChunkScorerConfig(3, "test query", null);
        TextSimilarityRankFeaturePhaseRankCoordinatorContext context = new TextSimilarityRankFeaturePhaseRankCoordinatorContext(
            10,
            0,
            100,
            mockClient,
            "my-inference-id",
            "some query",
            0.0f,
            false,
            config
        );

        ChunkScorerConfig resolved = context.resolveChunkingSettings(333);
        assertNotNull(resolved);
        assertEquals(Integer.valueOf(3), resolved.size());
        assertEquals("test query", resolved.inferenceText());
        assertNotNull(resolved.chunkingSettings());
        assertEquals(333, resolved.chunkingSettings().asMap().get("max_chunk_size"));
    }

    public void testResolveChunkingSettingsThrowsWhenWindowSizeInvalid() {
        ChunkScorerConfig config = new ChunkScorerConfig(3, "test query", null);
        TextSimilarityRankFeaturePhaseRankCoordinatorContext context = new TextSimilarityRankFeaturePhaseRankCoordinatorContext(
            10,
            0,
            100,
            mockClient,
            "my-inference-id",
            "some query",
            0.0f,
            false,
            config
        );

        expectThrows(IllegalStateException.class, () -> context.resolveChunkingSettings(0));
        expectThrows(IllegalStateException.class, () -> context.resolveChunkingSettings(-1));
    }
}
