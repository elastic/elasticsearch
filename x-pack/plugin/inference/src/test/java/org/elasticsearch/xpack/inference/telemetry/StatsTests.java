/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.telemetry;

import org.elasticsearch.inference.TaskType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.inference.InferenceRequestStats;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.openai.embeddings.OpenAiEmbeddingsModel;
import org.elasticsearch.xpack.inference.services.openai.embeddings.OpenAiEmbeddingsServiceSettingsTests;
import org.elasticsearch.xpack.inference.services.openai.embeddings.OpenAiEmbeddingsTaskSettingsTests;

import java.util.Map;

import static org.hamcrest.Matchers.is;

public class StatsTests extends ESTestCase {
    public void testStats() {
        var stats = new Stats<>(InferenceRequestCounter::key, InferenceRequestCounter::new);

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
            is(Map.of("openai:text_embedding:modelId", new InferenceRequestStats("openai", TaskType.TEXT_EMBEDDING, "modelId", 1)))
        );
    }
}
