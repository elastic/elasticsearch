/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.azureopenai.embeddings;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.TaskSettings;
import org.elasticsearch.xpack.inference.common.parser.Headers;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.azureopenai.AzureOpenAiTaskSettings;

import java.io.IOException;
import java.util.Map;

import static org.elasticsearch.xpack.inference.services.azureopenai.AzureOpenAiTaskSettings.createSettings;

/**
 * Defines the task settings for the Azure OpenAI embeddings service.
 * <p>
 * User is an optional unique identifier representing the end-user, which can help OpenAI to monitor and detect abuse.
 * Headers are optional custom HTTP headers to send with the request.
 */
public class AzureOpenAiEmbeddingsTaskSettings extends AzureOpenAiTaskSettings<AzureOpenAiEmbeddingsTaskSettings> {

    public static final String NAME = "azure_openai_embeddings_task_settings";

    /** Canonical empty instance for request overrides (e.g. when no overrides are supplied). */
    public static final AzureOpenAiEmbeddingsTaskSettings EMPTY = new AzureOpenAiEmbeddingsTaskSettings((String) null, null);

    /**
     * Creates a new {@link AzureOpenAiEmbeddingsTaskSettings} by overriding the values in originalSettings with the ones
     * passed in via requestSettings when the request fields are not null.
     */
    public static AzureOpenAiEmbeddingsTaskSettings of(
        AzureOpenAiEmbeddingsTaskSettings originalSettings,
        AzureOpenAiEmbeddingsTaskSettings requestSettings
    ) {
        var userToUse = requestSettings.user() == null ? originalSettings.user() : requestSettings.user();
        var headersToUse = requestSettings.headers() == null ? originalSettings.headers() : requestSettings.headers();
        return new AzureOpenAiEmbeddingsTaskSettings(userToUse, headersToUse);
    }

    public AzureOpenAiEmbeddingsTaskSettings(@Nullable String user, @Nullable Headers headers) {
        super(user, headers);
    }

    public AzureOpenAiEmbeddingsTaskSettings(StreamInput in) throws IOException {
        super(in);
    }

    public AzureOpenAiEmbeddingsTaskSettings(Map<String, Object> map, ConfigurationParseContext context) {
        super(map, context);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    protected AzureOpenAiEmbeddingsTaskSettings create(@Nullable String user, @Nullable Headers headers) {
        return new AzureOpenAiEmbeddingsTaskSettings(user, headers);
    }
}
