/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.fireworksai.completion;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.inference.services.fireworksai.FireworksAiService;
import org.elasticsearch.xpack.inference.services.openai.OpenAiTaskSettings;

import java.io.IOException;
import java.util.Map;

public class FireworksAiChatCompletionTaskSettings extends OpenAiTaskSettings<FireworksAiChatCompletionTaskSettings> {

    public static final String NAME = "fireworksai_completion_task_settings";

    public FireworksAiChatCompletionTaskSettings(Map<String, Object> map) {
        super(map);
    }

    public FireworksAiChatCompletionTaskSettings(@Nullable String user, @Nullable Map<String, String> headers) {
        super(user, headers);
    }

    public FireworksAiChatCompletionTaskSettings(StreamInput in) throws IOException {
        super(readTaskSettingsFromStream(in));
    }

    private static Settings readTaskSettingsFromStream(StreamInput in) throws IOException {
        var user = in.readOptionalString();
        Map<String, String> headers = in.readOptionalImmutableMap(StreamInput::readString, StreamInput::readString);
        return createSettings(user, headers);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return FireworksAiService.INFERENCE_API_FIREWORKS_AI_CHAT_COMPLETION_ADDED;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalString(user());
        out.writeOptionalMap(headers(), StreamOutput::writeString, StreamOutput::writeString);
    }

    @Override
    protected FireworksAiChatCompletionTaskSettings create(@Nullable String user, @Nullable Map<String, String> headers) {
        return new FireworksAiChatCompletionTaskSettings(user, headers);
    }
}
