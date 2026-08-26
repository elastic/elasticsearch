/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.fireworksai.completion;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.inference.services.openai.OpenAiTaskSettingsTests;

import java.util.Map;

public class FireworksAiChatCompletionTaskSettingsTests extends OpenAiTaskSettingsTests<FireworksAiChatCompletionTaskSettings> {

    @Override
    protected Writeable.Reader<FireworksAiChatCompletionTaskSettings> instanceReader() {
        return FireworksAiChatCompletionTaskSettings::new;
    }

    @Override
    protected FireworksAiChatCompletionTaskSettings createTestInstance() {
        return createRandom();
    }

    @Override
    protected FireworksAiChatCompletionTaskSettings mutateInstanceForVersion(
        FireworksAiChatCompletionTaskSettings instance,
        TransportVersion version
    ) {
        return instance;
    }

    @Override
    protected FireworksAiChatCompletionTaskSettings create(@Nullable String user, @Nullable Map<String, String> headers) {
        return new FireworksAiChatCompletionTaskSettings(user, headers);
    }

    @Override
    protected FireworksAiChatCompletionTaskSettings createFromMap(Map<String, Object> map) {
        return new FireworksAiChatCompletionTaskSettings(map);
    }
}
