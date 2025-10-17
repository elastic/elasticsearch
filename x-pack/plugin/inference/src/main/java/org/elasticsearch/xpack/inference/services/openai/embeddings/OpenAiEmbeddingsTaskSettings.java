/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.openai.embeddings;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.inference.services.openai.OpenAiTaskSettings;

import java.io.IOException;
import java.util.Map;

/**
 * Defines the task settings for the openai service.
 *
 * User is an optional unique identifier representing the end-user, which can help OpenAI to monitor and detect abuse
 *  <a href="https://platform.openai.com/docs/api-reference/embeddings/create">see the openai docs for more details</a>
 */
public class OpenAiEmbeddingsTaskSettings extends OpenAiTaskSettings<OpenAiEmbeddingsTaskSettings> {

    public static final String NAME = "openai_embeddings_task_settings";

    // default for testing
    static final TransportVersion INFERENCE_API_OPENAI_EMBEDDINGS_HEADERS = TransportVersion.fromName(
        "inference_api_openai_embeddings_headers"
    );

    public OpenAiEmbeddingsTaskSettings(Map<String, Object> map) {
        super(map);
    }

    public OpenAiEmbeddingsTaskSettings(@Nullable String user, @Nullable Map<String, String> headers) {
        super(user, headers);
    }

    public OpenAiEmbeddingsTaskSettings(StreamInput in) throws IOException {
        super(readTaskSettingsFromStream(in));
    }

    private static Settings readTaskSettingsFromStream(StreamInput in) throws IOException {
        String user = in.readOptionalString();

        Map<String, String> headers;

        if (in.getTransportVersion().supports(INFERENCE_API_OPENAI_EMBEDDINGS_HEADERS)) {
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
        return TransportVersions.V_8_12_0;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalString(user());

        if (out.getTransportVersion().supports(INFERENCE_API_OPENAI_EMBEDDINGS_HEADERS)) {
            out.writeOptionalMap(headers(), StreamOutput::writeString, StreamOutput::writeString);
        }
    }

    @Override
    protected OpenAiEmbeddingsTaskSettings create(@Nullable String user, @Nullable Map<String, String> headers) {
        return new OpenAiEmbeddingsTaskSettings(user, headers);
    }
}
