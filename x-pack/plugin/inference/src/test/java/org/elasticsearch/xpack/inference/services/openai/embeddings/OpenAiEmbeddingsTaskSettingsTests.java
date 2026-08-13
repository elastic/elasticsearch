/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.openai.embeddings;

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
import static org.elasticsearch.xpack.inference.services.openai.embeddings.OpenAiEmbeddingsTaskSettings.INFERENCE_API_OPENAI_EMBEDDINGS_HEADERS;

public class OpenAiEmbeddingsTaskSettingsTests extends OpenAiTaskSettingsTests<OpenAiEmbeddingsTaskSettings> {

    public static OpenAiEmbeddingsTaskSettings createWithUserString(@Nullable String user) {
        return new OpenAiEmbeddingsTaskSettings(
            user != null ? StatefulValue.of(user) : StatefulValue.undefined(),
            Headers.UNDEFINED_INSTANCE
        );
    }

    @Override
    protected Writeable.Reader<OpenAiEmbeddingsTaskSettings> instanceReader() {
        return OpenAiEmbeddingsTaskSettings::read;
    }

    @Override
    protected OpenAiEmbeddingsTaskSettings createTestInstance() {
        return createRandom();
    }

    @Override
    protected OpenAiEmbeddingsTaskSettings create(StatefulValue<String> user, Headers headers) {
        return new OpenAiEmbeddingsTaskSettings(user, headers);
    }

    @Override
    protected OpenAiEmbeddingsTaskSettings createFromMap(Map<String, Object> map, ConfigurationParseContext context) {
        return OpenAiEmbeddingsTaskSettings.fromMap(map, context);
    }

    @Override
    protected OpenAiEmbeddingsTaskSettings mutateInstanceForVersion(OpenAiEmbeddingsTaskSettings instance, TransportVersion version) {
        if (version.supports(OpenAiTaskSettings.INFERENCE_API_OPENAI_TASK_SETTINGS_TRI_STATE)) {
            return instance;
        }

        // Collapse null → undefined for user (legacy format only preserves present/absent)
        var user = instance.user().isPresent() ? instance.user() : StatefulValue.<String>undefined();

        if (version.supports(INFERENCE_API_OPENAI_EMBEDDINGS_HEADERS)) {
            // Collapse null → UNDEFINED_INSTANCE for headers
            var headers = instance.headers().isPresent() ? instance.headers() : UNDEFINED_INSTANCE;
            return new OpenAiEmbeddingsTaskSettings(user, headers);
        }

        return new OpenAiEmbeddingsTaskSettings(user, UNDEFINED_INSTANCE);
    }
}
