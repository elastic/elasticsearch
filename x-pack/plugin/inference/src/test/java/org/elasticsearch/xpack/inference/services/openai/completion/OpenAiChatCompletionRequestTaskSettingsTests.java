/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.openai.completion;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.inference.services.openai.OpenAiServiceFields;

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.is;

public class OpenAiChatCompletionRequestTaskSettingsTests extends ESTestCase {

    public void testFromMap_ReturnsEmptySettings_WhenTheMapIsEmpty() {
        var settings = OpenAiChatCompletionRequestTaskSettings.fromMap(new HashMap<>(Map.of()));
        assertNull(settings.user());
    }

    public void testFromMap_ReturnsEmptySettings_WhenTheMapDoesNotContainTheFields() {
        var settings = OpenAiChatCompletionRequestTaskSettings.fromMap(new HashMap<>(Map.of("key", "value")));
        assertNull(settings.user());
    }

    public void testFromMap_ReturnsUser() {
        var settings = OpenAiChatCompletionRequestTaskSettings.fromMap(new HashMap<>(Map.of(OpenAiServiceFields.USER, "user")));
        assertThat(settings.user(), is("user"));
    }

    public static Map<String, Object> getChatCompletionRequestTaskSettingsMap(@Nullable String user) {
        var map = new HashMap<String, Object>();

        if (user != null) {
            map.put(OpenAiServiceFields.USER, user);
        }

        return map;
    }

}
