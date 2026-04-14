/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.azureopenai.completion;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.xpack.inference.common.parser.Headers;
import org.elasticsearch.xpack.inference.common.parser.StatefulValue;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.azureopenai.AzureOpenAiTaskSettings;

import java.io.IOException;
import java.util.Map;

public class AzureOpenAiCompletionTaskSettings extends AzureOpenAiTaskSettings<AzureOpenAiCompletionTaskSettings> {

    public static final String NAME = "azure_openai_completion_task_settings";

    private static final AzureOpenAiTaskSettings.Factory<AzureOpenAiCompletionTaskSettings> FACTORY = new Factory<>() {
        @Override
        public AzureOpenAiCompletionTaskSettings create(CommonSettings commonSettings) {
            return new AzureOpenAiCompletionTaskSettings(commonSettings);
        }

        @Override
        protected AzureOpenAiCompletionTaskSettings createEmptyInstance() {
            return new AzureOpenAiCompletionTaskSettings();
        }
    };

    public static final AzureOpenAiCompletionTaskSettings EMPTY = FACTORY.emptySettings();

    public static AzureOpenAiCompletionTaskSettings fromMap(Map<String, Object> map, ConfigurationParseContext context) {
        return AzureOpenAiTaskSettings.parseSettingsFromMap(map, context, FACTORY);
    }

    private AzureOpenAiCompletionTaskSettings() {
        super(null, null, FACTORY);
    }

    private AzureOpenAiCompletionTaskSettings(CommonSettings commonSettings) {
        super(commonSettings, FACTORY);
    }

    // Default for testing
    AzureOpenAiCompletionTaskSettings(StatefulValue<String> user, Headers headers) {
        this(new CommonSettings(user, headers));
    }

    public AzureOpenAiCompletionTaskSettings(StreamInput in) throws IOException {
        super(in, FACTORY);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }
}
