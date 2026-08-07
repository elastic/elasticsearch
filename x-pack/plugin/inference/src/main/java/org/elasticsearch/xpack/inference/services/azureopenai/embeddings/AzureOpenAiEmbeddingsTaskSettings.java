/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.azureopenai.embeddings;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.xpack.inference.common.parser.Headers;
import org.elasticsearch.xpack.inference.common.parser.StatefulValue;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.azureopenai.AzureOpenAiTaskSettings;

import java.io.IOException;
import java.util.Map;

/**
 * Defines the task settings for the Azure OpenAI embeddings service.
 * <p>
 * User is an optional unique identifier representing the end-user, which can help OpenAI to monitor and detect abuse.
 * Headers are optional custom HTTP headers to send with the request.
 */
public class AzureOpenAiEmbeddingsTaskSettings extends AzureOpenAiTaskSettings<AzureOpenAiEmbeddingsTaskSettings> {

    public static final String NAME = "azure_openai_embeddings_task_settings";

    private static final AzureOpenAiTaskSettings.Factory<AzureOpenAiEmbeddingsTaskSettings> FACTORY = new Factory<>() {
        @Override
        public AzureOpenAiEmbeddingsTaskSettings create(CommonSettings commonSettings) {
            return new AzureOpenAiEmbeddingsTaskSettings(commonSettings);
        }

        @Override
        protected AzureOpenAiEmbeddingsTaskSettings createEmptyInstance() {
            return new AzureOpenAiEmbeddingsTaskSettings();
        }
    };

    public static final AzureOpenAiEmbeddingsTaskSettings EMPTY = FACTORY.emptySettings();

    public static AzureOpenAiEmbeddingsTaskSettings fromMap(Map<String, Object> map, ConfigurationParseContext context) {
        return AzureOpenAiTaskSettings.parseSettingsFromMap(map, context, FACTORY);
    }

    private AzureOpenAiEmbeddingsTaskSettings() {
        super(null, null, FACTORY);
    }

    private AzureOpenAiEmbeddingsTaskSettings(CommonSettings commonSettings) {
        super(commonSettings, FACTORY);
    }

    // Default for testing
    AzureOpenAiEmbeddingsTaskSettings(StatefulValue<String> user, Headers headers) {
        this(new CommonSettings(user, headers));
    }

    public AzureOpenAiEmbeddingsTaskSettings(StreamInput in) throws IOException {
        super(in, FACTORY);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }
}
