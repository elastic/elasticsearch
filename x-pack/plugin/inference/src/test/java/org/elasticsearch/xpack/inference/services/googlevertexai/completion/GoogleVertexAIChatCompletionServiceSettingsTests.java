/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.googlevertexai.completion;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.InferenceSettingsTestCase;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;

import java.util.Map;

public class GoogleVertexAIChatCompletionServiceSettingsTests extends InferenceSettingsTestCase<
    GoogleVertexAiChatCompletionServiceSettings> {
    @Override
    protected Writeable.Reader<GoogleVertexAiChatCompletionServiceSettings> instanceReader() {
        return GoogleVertexAiChatCompletionServiceSettings::new;
    }

    @Override
    protected GoogleVertexAiChatCompletionServiceSettings fromMutableMap(Map<String, Object> mutableMap) {
        return GoogleVertexAiChatCompletionServiceSettings.fromMap(mutableMap, ConfigurationParseContext.PERSISTENT);

    }

    @Override
    protected GoogleVertexAiChatCompletionServiceSettings createTestInstance() {
        return new GoogleVertexAiChatCompletionServiceSettings(
            randomString(),
            randomString(),
            randomString(),
            new RateLimitSettings(randomIntBetween(1, 1000))
        );
    }
}
