/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.groq;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ModelSecrets;
import org.elasticsearch.inference.ServiceSettings;
import org.elasticsearch.inference.TaskSettings;
import org.elasticsearch.xpack.inference.external.action.ExecutableAction;
import org.elasticsearch.xpack.inference.services.groq.action.GroqActionVisitor;
import org.elasticsearch.xpack.inference.services.openai.OpenAiModel;
import org.elasticsearch.xpack.inference.services.openai.action.OpenAiActionVisitor;
import org.elasticsearch.xpack.inference.services.settings.ApiKeySecrets;

import java.net.URI;
import java.util.Map;

public abstract class GroqModel extends OpenAiModel {

    protected GroqModel(
        ModelConfigurations configurations,
        ModelSecrets secrets,
        GroqRateLimitServiceSettings rateLimitServiceSettings,
        @Nullable ApiKeySecrets apiKeySecrets,
        URI uri
    ) {
        super(configurations, secrets, rateLimitServiceSettings, apiKeySecrets, uri);
    }

    protected GroqModel(GroqModel model, TaskSettings taskSettings) {
        super(model, taskSettings);
    }

    protected GroqModel(GroqModel model, ServiceSettings serviceSettings) {
        super(model, serviceSettings);
    }

    @Override
    public ExecutableAction accept(OpenAiActionVisitor creator, Map<String, Object> taskSettings) {
        if (creator instanceof GroqActionVisitor groqVisitor) {
            return accept(groqVisitor, taskSettings);
        }

        throw new IllegalArgumentException("Groq models only support Groq action visitors");
    }

    public abstract ExecutableAction accept(GroqActionVisitor creator, Map<String, Object> taskSettings);
}
