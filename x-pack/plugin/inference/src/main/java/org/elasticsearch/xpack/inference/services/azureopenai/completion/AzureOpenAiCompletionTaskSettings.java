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

    public static final String NAME = "azure_openai_completion_task_settings";

    private static final AzureOpenAiTaskSettings.Factory<AzureOpenAiCompletionTaskSettings> FACTORY = new Factory<>() {
        @Override
        public AzureOpenAiCompletionTaskSettings create(Settings settings) {
            if (settings.isEmpty()) {
                return emptySettings();
            }

            return new AzureOpenAiCompletionTaskSettings(settings);
        }

        @Override
        protected AzureOpenAiCompletionTaskSettings createEmptyInstance() {
            return new AzureOpenAiCompletionTaskSettings(null, null);
        }
    };

    public static final AzureOpenAiCompletionTaskSettings EMPTY = FACTORY.emptySettings();

    public static AzureOpenAiCompletionTaskSettings fromMap(Map<String, Object> map, ConfigurationParseContext context) {
        return AzureOpenAiTaskSettings.parseSettingsFromMap(map, context, FACTORY);
    }

    private AzureOpenAiCompletionTaskSettings(Settings settings) {
        super(settings, FACTORY);
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
