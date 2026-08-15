/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.openai.completion;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.inference.common.parser.Headers;
import org.elasticsearch.xpack.inference.common.parser.StatefulValue;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.openai.OpenAiTaskSettings;
import org.elasticsearch.xpack.inference.services.openai.OpenAiTaskSettingsTests;

import java.util.Map;

import static org.elasticsearch.xpack.inference.common.parser.Headers.UNDEFINED_INSTANCE;

public class OpenAiChatCompletionTaskSettingsTests extends OpenAiTaskSettingsTests<OpenAiChatCompletionTaskSettings> {

    private static final TransportVersion INFERENCE_API_OPENAI_HEADERS = TransportVersion.fromName("inference_api_openai_headers");

    public static OpenAiChatCompletionTaskSettings createWithUserString(@Nullable String user) {
        return new OpenAiChatCompletionTaskSettings(
            user != null ? StatefulValue.of(user) : StatefulValue.undefined(),
            Headers.UNDEFINED_INSTANCE
        );
    }

    @Override
    protected Writeable.Reader<OpenAiChatCompletionTaskSettings> instanceReader() {
        return OpenAiChatCompletionTaskSettings::read;
    }

    @Override
    protected OpenAiChatCompletionTaskSettings createTestInstance() {
        return createRandom();
    }

    @Override
    protected OpenAiChatCompletionTaskSettings create(StatefulValue<String> user, Headers headers) {
        return new OpenAiChatCompletionTaskSettings(user, headers);
    }

    @Override
    protected OpenAiChatCompletionTaskSettings createFromMap(Map<String, Object> map, ConfigurationParseContext context) {
        return OpenAiChatCompletionTaskSettings.fromMap(map, context);
    }

    @Override
    protected OpenAiChatCompletionTaskSettings mutateInstanceForVersion(
        OpenAiChatCompletionTaskSettings instance,
        TransportVersion version
    ) {
        if (version.supports(OpenAiTaskSettings.INFERENCE_API_OPENAI_TASK_SETTINGS_TRI_STATE)) {
            return instance;
        }

        // Collapse null → undefined for user (legacy format only preserves present/absent)
        var user = instance.user().isPresent() ? instance.user() : StatefulValue.<String>undefined();

        if (version.supports(INFERENCE_API_OPENAI_HEADERS)) {
            // Collapse null → UNDEFINED_INSTANCE for headers
            var headers = instance.headers().isPresent() ? instance.headers() : UNDEFINED_INSTANCE;
            return new OpenAiChatCompletionTaskSettings(user, headers);
        }

        return new OpenAiChatCompletionTaskSettings(user, UNDEFINED_INSTANCE);
    }
}
