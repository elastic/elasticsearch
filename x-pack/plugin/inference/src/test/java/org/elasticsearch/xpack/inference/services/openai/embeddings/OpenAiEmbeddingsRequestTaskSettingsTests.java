/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.openai.embeddings;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.inference.services.openai.OpenAiServiceFields;

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.is;

public class OpenAiEmbeddingsRequestTaskSettingsTests extends ESTestCase {

    public OpenAiEmbeddingsRequestTaskSettings createRandom() {
        return new OpenAiEmbeddingsRequestTaskSettings(randomBoolean() ? null : "username");
    }

    public void testFromMap_ReturnsEmptySettings_WhenTheMapIsEmpty() {
        var settings = OpenAiEmbeddingsRequestTaskSettings.fromMap(new HashMap<>(Map.of()));
        assertNull(settings.user());
    }

    public void testFromMap_ReturnsEmptySettings_WhenTheMapDoesNotContainTheFields() {
        var settings = OpenAiEmbeddingsRequestTaskSettings.fromMap(new HashMap<>(Map.of("key", "model")));
        assertNull(settings.user());
    }

    public void testFromMap_ReturnsUser() {
        var settings = OpenAiEmbeddingsRequestTaskSettings.fromMap(new HashMap<>(Map.of(OpenAiServiceFields.USER, "user")));
        assertThat(settings.user(), is("user"));
    }

    public static Map<String, Object> createRequestTaskSettingsMap(@Nullable String user) {
        var map = new HashMap<String, Object>();

        if (user != null) {
            map.put(OpenAiServiceFields.USER, user);
        }

        return map;
    }
}
