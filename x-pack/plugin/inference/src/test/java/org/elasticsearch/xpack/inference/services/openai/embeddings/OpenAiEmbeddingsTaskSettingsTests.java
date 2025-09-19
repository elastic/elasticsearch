/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.openai.embeddings;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.inference.services.openai.OpenAiServiceFields;
import org.elasticsearch.xpack.inference.services.openai.OpenAiTaskSettingsTests;

import java.util.HashMap;
import java.util.Map;

public class OpenAiEmbeddingsTaskSettingsTests extends OpenAiTaskSettingsTests<OpenAiEmbeddingsTaskSettings> {

    @Override
    protected Writeable.Reader<OpenAiEmbeddingsTaskSettings> instanceReader() {
        return OpenAiEmbeddingsTaskSettings::new;
    }

    @Override
    protected OpenAiEmbeddingsTaskSettings createTestInstance() {
        return createRandom();
    }

    public static Map<String, Object> getTaskSettingsMap(@Nullable String user) {
        var map = new HashMap<String, Object>();

        if (user != null) {
            map.put(OpenAiServiceFields.USER, user);
        }

        return map;
    }

    @Override
    protected OpenAiEmbeddingsTaskSettings create(String user, Map<String, String> headers) {
        return new OpenAiEmbeddingsTaskSettings(user, headers);
    }

    @Override
    protected OpenAiEmbeddingsTaskSettings create(Map<String, Object> map) {
        return new OpenAiEmbeddingsTaskSettings(map);
    }

    @Override
    protected OpenAiEmbeddingsTaskSettings mutateInstanceForVersion(OpenAiEmbeddingsTaskSettings instance, TransportVersion version) {
        if (version.onOrAfter(TransportVersions.INFERENCE_API_OPENAI_EMBEDDINGS_HEADERS)) {
            return instance;
        }

        return create(instance.user(), null);
    }
}
