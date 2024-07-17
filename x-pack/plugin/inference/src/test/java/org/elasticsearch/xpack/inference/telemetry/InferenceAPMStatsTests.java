/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.telemetry;

import org.elasticsearch.inference.TaskType;
import org.elasticsearch.telemetry.metric.LongCounter;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.inference.services.cohere.embeddings.CohereEmbeddingsModelTests;

import java.util.Map;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class InferenceAPMStatsTests extends ESTestCase {
    public void testIncrement_DoesNotThrowWhenModelIdIsNull() {
        var counterMock = mock(LongCounter.class);
        var stats = new InferenceAPMStats(CohereEmbeddingsModelTests.createModel("url", "api-key", null, null, null), counterMock);

        stats.increment();
        verify(counterMock, times(1)).incrementBy(eq(1L), eq(Map.of("service", "cohere", "task_type", TaskType.TEXT_EMBEDDING.toString())));
    }

    public void testIncrement_DoesNotThrowWhenModelIdIsNotNull() {
        var counterMock = mock(LongCounter.class);
        var stats = new InferenceAPMStats(CohereEmbeddingsModelTests.createModel("url", "api-key", null, "model_a", null), counterMock);

        stats.increment();
        verify(counterMock, times(1)).incrementBy(
            eq(1L),
            eq(Map.of("service", "cohere", "task_type", TaskType.TEXT_EMBEDDING.toString(), "model_id", "model_a"))
        );
    }
}
