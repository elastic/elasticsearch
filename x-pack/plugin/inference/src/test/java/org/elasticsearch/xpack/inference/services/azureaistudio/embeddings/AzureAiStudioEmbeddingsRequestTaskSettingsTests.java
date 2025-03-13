/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.azureaistudio.embeddings;

import org.elasticsearch.common.ValidationException;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.inference.services.azureaistudio.AzureAiStudioConstants;

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

public class AzureAiStudioEmbeddingsRequestTaskSettingsTests extends ESTestCase {

    public void testFromMap_ReturnsEmptySettings_WhenTheMapIsEmpty() {
        var settings = AzureAiStudioEmbeddingsRequestTaskSettings.fromMap(new HashMap<>(Map.of()));
        assertThat(settings, is(AzureAiStudioEmbeddingsRequestTaskSettings.EMPTY_SETTINGS));
    }

    public void testFromMap_ReturnsEmptySettings_WhenTheMapDoesNotContainTheFields() {
        var settings = AzureAiStudioEmbeddingsRequestTaskSettings.fromMap(new HashMap<>(Map.of("key", "model")));
        assertNull(settings.user());
    }

    public void testFromMap_ReturnsUser() {
        var settings = AzureAiStudioEmbeddingsRequestTaskSettings.fromMap(new HashMap<>(Map.of(AzureAiStudioConstants.USER_FIELD, "user")));
        assertThat(settings.user(), is("user"));
    }

    public void testFromMap_WhenUserIsEmpty_ThrowsValidationException() {
        var exception = expectThrows(
            ValidationException.class,
            () -> AzureAiStudioEmbeddingsRequestTaskSettings.fromMap(new HashMap<>(Map.of(AzureAiStudioConstants.USER_FIELD, "")))
        );

        assertThat(exception.getMessage(), containsString("[user] must be a non-empty string"));
    }
}
