/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.anthropic.completion;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.inference.services.anthropic.AnthropicServiceFields;

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.is;

public class AnthropicChatCompletionRequestTaskSettingsTests extends ESTestCase {

    public void testFromMap_ReturnsEmptySettings_WhenTheMapIsEmpty() {
        var settings = AnthropicChatCompletionRequestTaskSettings.fromMap(new HashMap<>(Map.of()));
        assertNull(settings.maxTokens());
    }

    public void testFromMap_ReturnsEmptySettings_WhenTheMapDoesNotContainTheFields() {
        var settings = AnthropicChatCompletionRequestTaskSettings.fromMap(new HashMap<>(Map.of("key", "value")));
        assertNull(settings.maxTokens());
    }

    public void testFromMap_ReturnsMaxTokens() {
        var settings = AnthropicChatCompletionRequestTaskSettings.fromMap(new HashMap<>(Map.of(AnthropicServiceFields.MAX_TOKENS, 1)));
        assertThat(settings.maxTokens(), is(1));
    }

    public static Map<String, Object> getChatCompletionRequestTaskSettingsMap(@Nullable Integer maxTokens) {
        var map = new HashMap<String, Object>();

        if (maxTokens != null) {
            map.put(AnthropicServiceFields.MAX_TOKENS, maxTokens);
        }

        return map;
    }

}
