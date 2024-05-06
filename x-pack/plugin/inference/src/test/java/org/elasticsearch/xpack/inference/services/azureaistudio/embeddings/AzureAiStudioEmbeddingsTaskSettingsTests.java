/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.azureaistudio.embeddings;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.inference.services.azureaistudio.AzureAiStudioConstants;
import org.hamcrest.MatcherAssert;

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.is;

public class AzureAiStudioEmbeddingsTaskSettingsTests extends ESTestCase {

    public void testFromMap_WithUser() {
        assertEquals(
            new AzureAiStudioEmbeddingsTaskSettings("user"),
            AzureAiStudioEmbeddingsTaskSettings.fromMap(new HashMap<>(Map.of(AzureAiStudioConstants.USER_FIELD, "user")))
        );
    }

    public void testFromMap_UserIsEmptyString() {
        var thrownException = expectThrows(
            ValidationException.class,
            () -> AzureAiStudioEmbeddingsTaskSettings.fromMap(new HashMap<>(Map.of(AzureAiStudioConstants.USER_FIELD, "")))
        );

        MatcherAssert.assertThat(
            thrownException.getMessage(),
            is(Strings.format("Validation Failed: 1: [task_settings] Invalid value empty string. [user] must be a non-empty string;"))
        );
    }

    public void testFromMap_MissingUser_DoesNotThrowException() {
        var taskSettings = AzureAiStudioEmbeddingsTaskSettings.fromMap(new HashMap<>(Map.of()));
        assertNull(taskSettings.user());
    }

    public void testOverrideWith_KeepsOriginalValuesWithOverridesAreNull() {
        var taskSettings = AzureAiStudioEmbeddingsTaskSettings.fromMap(new HashMap<>(Map.of(AzureAiStudioConstants.USER_FIELD, "user")));

        var overriddenTaskSettings = AzureAiStudioEmbeddingsTaskSettings.of(
            taskSettings,
            AzureAiStudioEmbeddingsRequestTaskSettings.EMPTY_SETTINGS
        );
        MatcherAssert.assertThat(overriddenTaskSettings, is(taskSettings));
    }

    public void testOverrideWith_UsesOverriddenSettings() {
        var taskSettings = AzureAiStudioEmbeddingsTaskSettings.fromMap(new HashMap<>(Map.of(AzureAiStudioConstants.USER_FIELD, "user")));

        var requestTaskSettings = AzureAiStudioEmbeddingsRequestTaskSettings.fromMap(
            new HashMap<>(Map.of(AzureAiStudioConstants.USER_FIELD, "user2"))
        );

        var overriddenTaskSettings = AzureAiStudioEmbeddingsTaskSettings.of(taskSettings, requestTaskSettings);
        MatcherAssert.assertThat(overriddenTaskSettings, is(new AzureAiStudioEmbeddingsTaskSettings("user2")));
    }

    public static Map<String, Object> getTaskSettingsMap(@Nullable String user) {
        Map<String, Object> map = new HashMap<>();
        if (user != null) {
            map.put(AzureAiStudioConstants.USER_FIELD, user);
        }
        return map;
    }
}
