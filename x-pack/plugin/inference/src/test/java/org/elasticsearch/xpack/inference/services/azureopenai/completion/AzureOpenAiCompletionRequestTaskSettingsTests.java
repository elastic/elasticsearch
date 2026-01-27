/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.azureopenai.completion;

import org.elasticsearch.common.ValidationException;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.inference.services.azureopenai.AzureOpenAiServiceFields;

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

public class AzureOpenAiCompletionRequestTaskSettingsTests extends ESTestCase {

    public void testFromMap_ReturnsEmptySettings_WhenMapIsEmpty() {
        var settings = AzureOpenAiCompletionRequestTaskSettings.fromMap(new HashMap<>(Map.of()));
        assertThat(settings, is(AzureOpenAiCompletionRequestTaskSettings.EMPTY_SETTINGS));
    }

    public void testFromMap_ReturnsEmptySettings_WhenMapDoesNotContainKnownFields() {
        var settings = AzureOpenAiCompletionRequestTaskSettings.fromMap(new HashMap<>(Map.of("key", "model")));
        assertThat(settings, is(AzureOpenAiCompletionRequestTaskSettings.EMPTY_SETTINGS));
    }

    public void testFromMap_ReturnsUser() {
        var settings = AzureOpenAiCompletionRequestTaskSettings.fromMap(new HashMap<>(Map.of(AzureOpenAiServiceFields.USER, "user")));
        assertThat(settings.user(), is("user"));
    }

    public void testFromMap_WhenUserIsEmpty_ThrowsValidationException() {
        var exception = expectThrows(
            ValidationException.class,
            () -> AzureOpenAiCompletionRequestTaskSettings.fromMap(new HashMap<>(Map.of(AzureOpenAiServiceFields.USER, "")))
        );

        assertThat(exception.getMessage(), containsString("[user] must be a non-empty string"));
    }
}
