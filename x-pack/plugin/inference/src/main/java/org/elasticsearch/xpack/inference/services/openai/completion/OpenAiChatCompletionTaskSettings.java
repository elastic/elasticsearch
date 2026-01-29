/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.openai.completion;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.inference.services.openai.OpenAiTaskSettings;

import java.io.IOException;
import java.util.Map;

public class OpenAiChatCompletionTaskSettings extends OpenAiTaskSettings<OpenAiChatCompletionTaskSettings> {

    public static final String NAME = "openai_completion_task_settings";

    private static final TransportVersion INFERENCE_API_OPENAI_HEADERS = TransportVersion.fromName("inference_api_openai_headers");

    public OpenAiChatCompletionTaskSettings(Map<String, Object> map) {
        super(map);
    }

    public OpenAiChatCompletionTaskSettings(@Nullable String user, @Nullable Map<String, String> headers) {
        super(user, headers);
    }

    public OpenAiChatCompletionTaskSettings(StreamInput in) throws IOException {
        super(readTaskSettingsFromStream(in));
    }

    private static Settings readTaskSettingsFromStream(StreamInput in) throws IOException {
        var user = in.readOptionalString();

        Map<String, String> headers;

        if (in.getTransportVersion().supports(INFERENCE_API_OPENAI_HEADERS)) {
            headers = in.readOptionalImmutableMap(StreamInput::readString, StreamInput::readString);
        } else {
            headers = null;
        }

        return createSettings(user, headers);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersion.minimumCompatible();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalString(user());
        if (out.getTransportVersion().supports(INFERENCE_API_OPENAI_HEADERS)) {
            out.writeOptionalMap(headers(), StreamOutput::writeString, StreamOutput::writeString);
        }
    }

    @Override
    protected OpenAiChatCompletionTaskSettings create(@Nullable String user, @Nullable Map<String, String> headers) {
        return new OpenAiChatCompletionTaskSettings(user, headers);
    }
}
