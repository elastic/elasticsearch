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

public class AnthropicChatCompletionTaskSettingsTests extends ESTestCase {

    public static Map<String, Object> getChatCompletionRequestTaskSettingsMap(@Nullable Integer maxTokens) {
        var map = new HashMap<String, Object>();

        if (maxTokens != null) {
            map.put(AnthropicServiceFields.MAX_TOKENS, maxTokens);
        }

        return map;
    }
}
