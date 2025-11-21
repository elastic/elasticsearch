/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.openai.embeddings;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xpack.inference.services.openai.OpenAiTaskSettingsTests;

import java.util.Map;

import static org.elasticsearch.xpack.inference.services.openai.embeddings.OpenAiEmbeddingsTaskSettings.INFERENCE_API_OPENAI_EMBEDDINGS_HEADERS;

public class OpenAiEmbeddingsTaskSettingsTests extends OpenAiTaskSettingsTests<OpenAiEmbeddingsTaskSettings> {

    @Override
    protected Writeable.Reader<OpenAiEmbeddingsTaskSettings> instanceReader() {
        return OpenAiEmbeddingsTaskSettings::new;
    }

    @Override
    protected OpenAiEmbeddingsTaskSettings createTestInstance() {
        return createRandom();
    }

    @Override
    protected OpenAiEmbeddingsTaskSettings create(String user, Map<String, String> headers) {
        return new OpenAiEmbeddingsTaskSettings(user, headers);
    }

    @Override
    protected OpenAiEmbeddingsTaskSettings createFromMap(Map<String, Object> map) {
        return new OpenAiEmbeddingsTaskSettings(map);
    }

    @Override
    protected OpenAiEmbeddingsTaskSettings mutateInstanceForVersion(OpenAiEmbeddingsTaskSettings instance, TransportVersion version) {
        if (version.supports(INFERENCE_API_OPENAI_EMBEDDINGS_HEADERS)) {
            return instance;
        }

        return create(instance.user(), null);
    }
}
