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

        assertNull(settings.model());
        assertNull(settings.user());
    }

    public void testFromMap_ReturnsEmptySettings_WhenTheMapDoesNotContainTheFields() {
        var settings = OpenAiEmbeddingsRequestTaskSettings.fromMap(new HashMap<>(Map.of("key", "model")));

        assertNull(settings.model());
        assertNull(settings.user());
    }

    public void testFromMap_ReturnsEmptyModel_WhenTheMapDoesNotContainThatField() {
        var settings = OpenAiEmbeddingsRequestTaskSettings.fromMap(new HashMap<>(Map.of(OpenAiEmbeddingsTaskSettings.USER, "user")));

        assertNull(settings.model());
        assertThat(settings.user(), is("user"));
    }

    public void testFromMap_ReturnsEmptyUser_WhenTheDoesMapNotContainThatField() {
        var settings = OpenAiEmbeddingsRequestTaskSettings.fromMap(new HashMap<>(Map.of(OpenAiEmbeddingsTaskSettings.MODEL, "model")));

        assertNull(settings.user());
        assertThat(settings.model(), is("model"));
    }

    public static Map<String, Object> getRequestTaskSettingsMap(@Nullable String model, @Nullable String user) {
        var map = new HashMap<String, Object>();

        if (model != null) {
            map.put(OpenAiEmbeddingsTaskSettings.MODEL, model);
        }

        if (user != null) {
            map.put(OpenAiEmbeddingsTaskSettings.USER, user);
        }

        return map;
    }
}
