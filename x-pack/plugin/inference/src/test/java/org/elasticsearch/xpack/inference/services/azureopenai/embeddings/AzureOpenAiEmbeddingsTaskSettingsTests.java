/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.azureopenai.embeddings;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.inference.common.parser.Headers;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.azureopenai.AzureOpenAiTaskSettingsTests;

import java.util.Map;

public class AzureOpenAiEmbeddingsTaskSettingsTests extends AzureOpenAiTaskSettingsTests<AzureOpenAiEmbeddingsTaskSettings> {

    @Override
    protected Writeable.Reader<AzureOpenAiEmbeddingsTaskSettings> instanceReader() {
        return AzureOpenAiEmbeddingsTaskSettings::new;
    }

    @Override
    protected AzureOpenAiEmbeddingsTaskSettings createTestInstance() {
        return createRandomWithUser();
    }

    @Override
    protected AzureOpenAiEmbeddingsTaskSettings create(@Nullable String user, @Nullable Headers headers) {
        return new AzureOpenAiEmbeddingsTaskSettings(user, headers);
    }

    @Override
    protected AzureOpenAiEmbeddingsTaskSettings createFromMap(Map<String, Object> map, ConfigurationParseContext context) {
        return AzureOpenAiEmbeddingsTaskSettings.fromMap(map, context);
    }

    @Override
    protected AzureOpenAiEmbeddingsTaskSettings emptySettings() {
        return AzureOpenAiEmbeddingsTaskSettings.EMPTY;
    }
}
