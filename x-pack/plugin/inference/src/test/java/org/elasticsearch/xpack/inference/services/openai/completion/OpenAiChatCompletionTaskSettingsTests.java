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
import org.elasticsearch.xpack.inference.services.openai.OpenAiTaskSettingsTests;

import java.util.Map;

public class OpenAiChatCompletionTaskSettingsTests extends OpenAiTaskSettingsTests<OpenAiChatCompletionTaskSettings> {

    private static final TransportVersion INFERENCE_API_OPENAI_HEADERS = TransportVersion.fromName("inference_api_openai_headers");

    @Override
    protected Writeable.Reader<OpenAiChatCompletionTaskSettings> instanceReader() {
        return OpenAiChatCompletionTaskSettings::new;
    }

    @Override
    protected OpenAiChatCompletionTaskSettings createTestInstance() {
        return createRandom();
    }

    @Override
    protected OpenAiChatCompletionTaskSettings mutateInstanceForVersion(
        OpenAiChatCompletionTaskSettings instance,
        TransportVersion version
    ) {
        if (version.supports(INFERENCE_API_OPENAI_HEADERS)) {
            return instance;
        }

        return create(instance.user(), null);
    }

    @Override
    protected OpenAiChatCompletionTaskSettings create(@Nullable String user, @Nullable Map<String, String> headers) {
        return new OpenAiChatCompletionTaskSettings(user, headers);
    }

    @Override
    protected OpenAiChatCompletionTaskSettings createFromMap(@Nullable Map<String, Object> map) {
        return new OpenAiChatCompletionTaskSettings(map);
    }
}
