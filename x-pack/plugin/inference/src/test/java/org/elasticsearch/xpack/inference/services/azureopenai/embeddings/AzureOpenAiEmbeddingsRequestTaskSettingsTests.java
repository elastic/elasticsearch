/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.azureopenai.embeddings;

import org.elasticsearch.common.ValidationException;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.inference.services.openai.OpenAiServiceFields;

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

public class AzureOpenAiEmbeddingsRequestTaskSettingsTests extends ESTestCase {
    public void testFromMap_ReturnsEmptySettings_WhenTheMapIsEmpty() {
        var settings = AzureOpenAiEmbeddingsRequestTaskSettings.fromMap(new HashMap<>(Map.of()));
        assertThat(settings, is(AzureOpenAiEmbeddingsRequestTaskSettings.EMPTY_SETTINGS));
    }

    public void testFromMap_ReturnsEmptySettings_WhenTheMapDoesNotContainTheFields() {
        var settings = AzureOpenAiEmbeddingsRequestTaskSettings.fromMap(new HashMap<>(Map.of("key", "model")));
        assertNull(settings.user());
    }

    public void testFromMap_ReturnsUser() {
        var settings = AzureOpenAiEmbeddingsRequestTaskSettings.fromMap(new HashMap<>(Map.of(OpenAiServiceFields.USER, "user")));
        assertThat(settings.user(), is("user"));
    }

    public void testFromMap_WhenUserIsEmpty_ThrowsValidationException() {
        var exception = expectThrows(
            ValidationException.class,
            () -> AzureOpenAiEmbeddingsRequestTaskSettings.fromMap(new HashMap<>(Map.of(OpenAiServiceFields.USER, "")))
        );

        assertThat(exception.getMessage(), containsString("[user] must be a non-empty string"));
    }

    public static Map<String, Object> createRequestTaskSettingsMap(@Nullable String user) {
        var map = new HashMap<String, Object>();

        if (user != null) {
            map.put(OpenAiServiceFields.USER, user);
        }

        return map;
    }
}
