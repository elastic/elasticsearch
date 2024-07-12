/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.telemetry;

import org.elasticsearch.inference.TaskType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.cohere.embeddings.CohereEmbeddingsModel;
import org.elasticsearch.xpack.inference.services.cohere.embeddings.CohereEmbeddingsServiceSettingsTests;
import org.elasticsearch.xpack.inference.services.cohere.embeddings.CohereEmbeddingsTaskSettingsTests;
import org.elasticsearch.xpack.inference.services.openai.embeddings.OpenAiEmbeddingsModel;
import org.elasticsearch.xpack.inference.services.openai.embeddings.OpenAiEmbeddingsServiceSettingsTests;
import org.elasticsearch.xpack.inference.services.openai.embeddings.OpenAiEmbeddingsTaskSettingsTests;

import java.util.Map;

import static org.hamcrest.Matchers.is;

public class StatsMapTests extends ESTestCase {
    public void testAddingEntry_InitializesTheCountToOne() {
        var stats = new StatsMap<>(InferenceStats::key, InferenceStats::new);

        stats.increment(
            new OpenAiEmbeddingsModel(
                "inference_id",
                TaskType.TEXT_EMBEDDING,
                "openai",
                OpenAiEmbeddingsServiceSettingsTests.getServiceSettingsMap("modelId", null, null),
                OpenAiEmbeddingsTaskSettingsTests.getTaskSettingsMap(null),
                null,
                ConfigurationParseContext.REQUEST
            )
        );

        var converted = stats.toSerializableMap();

        assertThat(
            converted,
            is(
                Map.of(
                    "openai:text_embedding:modelId",
                    new org.elasticsearch.xpack.core.inference.InferenceRequestStats("openai", TaskType.TEXT_EMBEDDING, "modelId", 1)
                )
            )
        );
    }

    public void testIncrementingWithSeparateModels_IncrementsTheCounterToTwo() {
        var stats = new StatsMap<>(InferenceStats::key, InferenceStats::new);

        var model1 = new OpenAiEmbeddingsModel(
            "inference_id",
            TaskType.TEXT_EMBEDDING,
            "openai",
            OpenAiEmbeddingsServiceSettingsTests.getServiceSettingsMap("modelId", null, null),
            OpenAiEmbeddingsTaskSettingsTests.getTaskSettingsMap(null),
            null,
            ConfigurationParseContext.REQUEST
        );

        var model2 = new OpenAiEmbeddingsModel(
            "inference_id",
            TaskType.TEXT_EMBEDDING,
            "openai",
            OpenAiEmbeddingsServiceSettingsTests.getServiceSettingsMap("modelId", null, null),
            OpenAiEmbeddingsTaskSettingsTests.getTaskSettingsMap(null),
            null,
            ConfigurationParseContext.REQUEST
        );

        stats.increment(model1);
        stats.increment(model2);

        var converted = stats.toSerializableMap();

        assertThat(
            converted,
            is(
                Map.of(
                    "openai:text_embedding:modelId",
                    new org.elasticsearch.xpack.core.inference.InferenceRequestStats("openai", TaskType.TEXT_EMBEDDING, "modelId", 2)
                )
            )
        );
    }

    public void testNullModelId_ResultsInKeyWithout() {
        var stats = new StatsMap<>(InferenceStats::key, InferenceStats::new);

        stats.increment(
            new CohereEmbeddingsModel(
                "inference_id",
                TaskType.TEXT_EMBEDDING,
                "cohere",
                CohereEmbeddingsServiceSettingsTests.getServiceSettingsMap(null, null, null),
                CohereEmbeddingsTaskSettingsTests.getTaskSettingsMap(null, null),
                null,
                ConfigurationParseContext.REQUEST
            )
        );

        var converted = stats.toSerializableMap();

        assertThat(
            converted,
            is(
                Map.of(
                    "cohere:text_embedding",
                    new org.elasticsearch.xpack.core.inference.InferenceRequestStats("cohere", TaskType.TEXT_EMBEDDING, null, 1)
                )
            )
        );
    }
}
