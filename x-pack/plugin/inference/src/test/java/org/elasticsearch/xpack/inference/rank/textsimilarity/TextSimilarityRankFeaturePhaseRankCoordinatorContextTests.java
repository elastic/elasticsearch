/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.rank.textsimilarity;

import org.elasticsearch.client.internal.Client;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.search.rank.feature.RankFeatureDoc;
import org.elasticsearch.search.rank.feature.RerankSnippetConfig;
import org.elasticsearch.search.rank.feature.SnippetRankInput;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.inference.action.GetInferenceModelAction;
import org.elasticsearch.xpack.core.inference.results.RankedDocsResults;

import java.util.List;

import static org.elasticsearch.action.support.ActionTestUtils.assertNoFailureListener;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
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

    TextSimilarityRankFeaturePhaseRankCoordinatorContext withSnippets = new TextSimilarityRankFeaturePhaseRankCoordinatorContext(
        10,
        0,
        100,
        mockClient,
        "my-inference-id",
        "some query",
        0.0f,
        false,
        new SnippetRankInput(new RerankSnippetConfig(2), "some query", 10)
    );

    public void testComputeScores() {
        RankFeatureDoc featureDoc1 = new RankFeatureDoc(0, 1.0f, 0);
        featureDoc1.featureData(List.of("text 1"));
        RankFeatureDoc featureDoc2 = new RankFeatureDoc(1, 3.0f, 1);
        featureDoc2.featureData(List.of("text 2"));
        RankFeatureDoc featureDoc3 = new RankFeatureDoc(2, 2.0f, 0);
        featureDoc3.featureData(List.of("text 3"));
        RankFeatureDoc[] featureDocs = new RankFeatureDoc[] { featureDoc1, featureDoc2, featureDoc3 };

        subject.computeScores(featureDocs, assertNoFailureListener(f -> assertArrayEquals(new float[] { 1.0f, 3.0f, 2.0f }, f, 0.0f)));
        verify(mockClient).execute(
            eq(GetInferenceModelAction.INSTANCE),
            argThat(actionRequest -> ((GetInferenceModelAction.Request) actionRequest).getTaskType().equals(TaskType.RERANK)),
            any()
        );
    }

    public void testComputeScoresForEmpty() {
        subject.computeScores(new RankFeatureDoc[0], assertNoFailureListener(f -> assertArrayEquals(new float[0], f, 0.0f)));
        verify(mockClient).execute(
            eq(GetInferenceModelAction.INSTANCE),
            argThat(actionRequest -> ((GetInferenceModelAction.Request) actionRequest).getTaskType().equals(TaskType.RERANK)),
            any()
        );
    }

    public void testExtractScoresFromRankedDocs() {
        List<RankedDocsResults.RankedDoc> rankedDocs = List.of(
            new RankedDocsResults.RankedDoc(0, 1.0f, "text 1"),
            new RankedDocsResults.RankedDoc(1, 3.0f, "text 2"),
            new RankedDocsResults.RankedDoc(2, 2.0f, "text 3")
        );
        float[] scores = subject.extractScoresFromRankedDocs(rankedDocs);
        assertArrayEquals(new float[] { 1.0f, 3.0f, 2.0f }, scores, 0.0f);
    }

    public void testExtractScoresFromSingleSnippets() {

        List<RankedDocsResults.RankedDoc> rankedDocs = List.of(
            new RankedDocsResults.RankedDoc(0, 1.0f, "text 1"),
            new RankedDocsResults.RankedDoc(1, 2.5f, "text 2"),
            new RankedDocsResults.RankedDoc(2, 1.5f, "text 3")
        );
        RankFeatureDoc[] featureDocs = new RankFeatureDoc[] {
            createRankFeatureDoc(0, 1.0f, 0, List.of("text 1")),
            createRankFeatureDoc(1, 3.0f, 1, List.of("text 2")),
            createRankFeatureDoc(2, 2.0f, 0, List.of("text 3")) };

        float[] scores = withSnippets.extractScoresFromRankedSnippets(rankedDocs, featureDocs);
        // Returned cores are from the snippet, not the whole text
        assertArrayEquals(new float[] { 1.0f, 2.5f, 1.5f }, scores, 0.0f);
    }

    public void testExtractScoresFromMultipleSnippets() {

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

        float[] scores = withSnippets.extractScoresFromRankedSnippets(rankedDocs, featureDocs);
        // Returned scores are from the best-ranking snippet, not the whole text
        assertArrayEquals(new float[] { 2.5f, 3.0f, 2.0f }, scores, 0.0f);
    }

    private RankFeatureDoc createRankFeatureDoc(int doc, float score, int shardIndex, List<String> featureData) {
        RankFeatureDoc featureDoc = new RankFeatureDoc(doc, score, shardIndex);
        featureDoc.featureData(featureData);
        return featureDoc;
    }

}
