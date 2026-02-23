/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.azureopenai.completion;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.inference.common.parser.Headers;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.azureopenai.AzureOpenAiTaskSettings;

import java.io.IOException;
import java.util.Map;

public class AzureOpenAiCompletionTaskSettings extends AzureOpenAiTaskSettings<AzureOpenAiCompletionTaskSettings> {

    private static final String NAME = "azure_openai_completion_task_settings";

    public static final AzureOpenAiCompletionTaskSettings EMPTY = new AzureOpenAiCompletionTaskSettings(null, null);

    private static final AzureOpenAiTaskSettings.Factory<AzureOpenAiCompletionTaskSettings> FACTORY = new Factory<>(EMPTY) {
        @Override
        public AzureOpenAiCompletionTaskSettings create(@Nullable String user, @Nullable Headers headers) {
            if (user == null && headers == null) {
                return EMPTY;
            }

            return new AzureOpenAiCompletionTaskSettings(user, headers);
        }
    };

    public static AzureOpenAiCompletionTaskSettings fromMap(Map<String, Object> map, ConfigurationParseContext context) {
        return AzureOpenAiTaskSettings.parseSettingsFromMap(map, context, FACTORY);
    }

    public AzureOpenAiCompletionTaskSettings(@Nullable String user, @Nullable Headers headers) {
        super(user, headers, FACTORY);
    }

    public AzureOpenAiCompletionTaskSettings(StreamInput in) throws IOException {
        super(in, FACTORY);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }
}
