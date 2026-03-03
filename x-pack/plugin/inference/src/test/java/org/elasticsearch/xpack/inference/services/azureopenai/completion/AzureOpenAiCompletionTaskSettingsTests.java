/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.azureopenai.completion;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xpack.inference.common.parser.Headers;
import org.elasticsearch.xpack.inference.common.parser.StatefulValue;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.azureopenai.AzureOpenAiTaskSettingsTests;

import java.util.Map;

public class AzureOpenAiCompletionTaskSettingsTests extends AzureOpenAiTaskSettingsTests<AzureOpenAiCompletionTaskSettings> {

    @Override
    protected Writeable.Reader<AzureOpenAiCompletionTaskSettings> instanceReader() {
        return AzureOpenAiCompletionTaskSettings::new;
    }

    @Override
    protected AzureOpenAiCompletionTaskSettings createTestInstance() {
        return createRandom();
    }

    @Override
    protected AzureOpenAiCompletionTaskSettings create(StatefulValue<String> user, Headers headers) {
        return new AzureOpenAiCompletionTaskSettings(user, headers);
    }

    @Override
    protected AzureOpenAiCompletionTaskSettings createFromMap(Map<String, Object> map, ConfigurationParseContext context) {
        return AzureOpenAiCompletionTaskSettings.fromMap(map, context);
    }

    @Override
    protected AzureOpenAiCompletionTaskSettings emptySettings() {
        return AzureOpenAiCompletionTaskSettings.EMPTY;
    }
}
