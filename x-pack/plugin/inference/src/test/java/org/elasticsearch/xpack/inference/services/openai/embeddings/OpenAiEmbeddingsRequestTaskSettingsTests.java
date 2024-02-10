/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.openai.embeddings;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.test.ESTestCase;

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.is;

public class OpenAiEmbeddingsRequestTaskSettingsTests extends ESTestCase {
    public void testFromMap_ReturnsEmptySettings_WhenTheMapIsEmpty() {
        var settings = OpenAiEmbeddingsRequestTaskSettings.fromMap(new HashMap<>(Map.of()));

        assertNull(settings.modelId());
        assertNull(settings.user());
    }

    public void testFromMap_ReturnsEmptySettings_WhenTheMapDoesNotContainTheFields() {
        var settings = OpenAiEmbeddingsRequestTaskSettings.fromMap(new HashMap<>(Map.of("key", "model")));

        assertNull(settings.modelId());
        assertNull(settings.user());
    }

    public void testFromMap_ReturnsEmptyModel_WhenTheMapDoesNotContainThatField() {
        var settings = OpenAiEmbeddingsRequestTaskSettings.fromMap(new HashMap<>(Map.of(OpenAiEmbeddingsTaskSettings.USER, "user")));

        assertNull(settings.modelId());
        assertThat(settings.user(), is("user"));
    }

    public void testFromMap_ReturnsEmptyUser_WhenTheDoesMapNotContainThatField() {
        var settings = OpenAiEmbeddingsRequestTaskSettings.fromMap(
            new HashMap<>(Map.of(OpenAiEmbeddingsTaskSettings.OLD_MODEL_ID_FIELD, "model"))
        );

        assertNull(settings.user());
        assertThat(settings.modelId(), is("model"));
    }

    public void testFromMap_PrefersModelId_OverModel() {
        var settings = OpenAiEmbeddingsRequestTaskSettings.fromMap(
            new HashMap<>(
                Map.of(OpenAiEmbeddingsTaskSettings.OLD_MODEL_ID_FIELD, "model", OpenAiEmbeddingsTaskSettings.MODEL_ID, "model_id")
            )
        );

        assertNull(settings.user());
        assertThat(settings.modelId(), is("model_id"));
    }

    public static Map<String, Object> getRequestTaskSettingsMap(@Nullable String model, @Nullable String user) {
        var map = new HashMap<String, Object>();

        if (model != null) {
            map.put(OpenAiEmbeddingsTaskSettings.OLD_MODEL_ID_FIELD, model);
        }

        if (user != null) {
            map.put(OpenAiEmbeddingsTaskSettings.USER, user);
        }

        return map;
    }

    public static Map<String, Object> getRequestTaskSettingsMap(@Nullable String model, @Nullable String modelId, @Nullable String user) {
        var map = new HashMap<String, Object>();

        if (model != null) {
            map.put(OpenAiEmbeddingsTaskSettings.OLD_MODEL_ID_FIELD, model);
        }

        if (modelId != null) {
            map.put(OpenAiEmbeddingsTaskSettings.MODEL_ID, model);
        }

        if (user != null) {
            map.put(OpenAiEmbeddingsTaskSettings.USER, user);
        }

        return map;
    }
}
