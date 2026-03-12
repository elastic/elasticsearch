/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.groq.completion;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.inference.services.groq.GroqService;
import org.elasticsearch.xpack.inference.services.openai.OpenAiTaskSettings;

import java.io.IOException;
import java.util.Map;

public class GroqChatCompletionTaskSettings extends OpenAiTaskSettings<GroqChatCompletionTaskSettings> {

    public static final String NAME = "groq_completion_task_settings";

    public GroqChatCompletionTaskSettings(Map<String, Object> map) {
        super(map);
    }

    public GroqChatCompletionTaskSettings(@Nullable String user, @Nullable Map<String, String> headers) {
        super(user, headers);
    }

    public GroqChatCompletionTaskSettings(StreamInput in) throws IOException {
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
        return GroqService.GROQ_INFERENCE_SERVICE;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalString(user());
        out.writeOptionalMap(headers(), StreamOutput::writeString, StreamOutput::writeString);
    }

    @Override
    protected GroqChatCompletionTaskSettings create(@Nullable String user, @Nullable Map<String, String> headers) {
        return new GroqChatCompletionTaskSettings(user, headers);
    }
}
