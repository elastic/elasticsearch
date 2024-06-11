/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.rerank;

import org.elasticsearch.inference.InputType;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.inference.action.InferenceAction;
import org.elasticsearch.xpack.core.inference.results.RankedDocsResults;

import java.util.List;

public class TextSimilarityRankFeaturePhaseRankCoordinatorContextTest extends ESTestCase {

    TextSimilarityRankFeaturePhaseRankCoordinatorContext subject = new TextSimilarityRankFeaturePhaseRankCoordinatorContext(
        10,
        0,
        100,
        null,
        "my-inference-id",
        "some query",
        0.0f
    );

    public void testGenerateRequest() {
        var featureStrings = List.of("some text", "some other text");

        InferenceAction.Request request = subject.generateRequest(featureStrings);

        assertEquals(TaskType.RERANK, request.getTaskType());
        assertEquals(InputType.SEARCH, request.getInputType());
    }

    public void testActionType() {
        assertEquals(InferenceAction.INSTANCE, subject.actionType());
    }

    public void testExtractScoresFromResponse() {
        var response = new InferenceAction.Response(
            new RankedDocsResults(List.of(new RankedDocsResults.RankedDoc(0, 2.0f, ""), new RankedDocsResults.RankedDoc(1, 1.0f, "")))
        );

        float[] scores = subject.extractScoresFromResponse(response);

        assertArrayEquals(new float[] { 2.0f, 1.0f }, scores, 0.0f);
    }

}
