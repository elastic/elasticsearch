/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.rank.textsimilarity;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.search.rank.feature.RankFeatureDoc;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.inference.action.GetInferenceModelAction;

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
        0.0f
    );

    public void testComputeScores() {
        RankFeatureDoc featureDoc1 = new RankFeatureDoc(0, 1.0f, 0);
        featureDoc1.featureData("text 1");
        RankFeatureDoc featureDoc2 = new RankFeatureDoc(1, 3.0f, 1);
        featureDoc2.featureData("text 2");
        RankFeatureDoc featureDoc3 = new RankFeatureDoc(2, 2.0f, 0);
        featureDoc3.featureData("text 3");
        RankFeatureDoc[] featureDocs = new RankFeatureDoc[] { featureDoc1, featureDoc2, featureDoc3 };

        subject.computeScores(featureDocs, new ActionListener<>() {
            @Override
            public void onResponse(float[] floats) {
                assertArrayEquals(new float[] { 1.0f, 3.0f, 2.0f }, floats, 0.0f);
            }

            @Override
            public void onFailure(Exception e) {
                fail();
            }
        });
        verify(mockClient).execute(
            eq(GetInferenceModelAction.INSTANCE),
            argThat(actionRequest -> ((GetInferenceModelAction.Request) actionRequest).getTaskType().equals(TaskType.RERANK)),
            any()
        );
    }

}
