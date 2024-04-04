/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.azureopenai;

import org.elasticsearch.inference.*;
import org.elasticsearch.xpack.inference.external.action.ExecutableAction;
import org.elasticsearch.xpack.inference.external.action.azureopenai.AzureOpenAiActionVisitor;

import java.util.Map;

public abstract class AzureOpenAiModel extends Model {

    public AzureOpenAiModel(ModelConfigurations configurations, ModelSecrets secrets) {
        super(configurations, secrets);
    }

    protected AzureOpenAiModel(AzureOpenAiModel model, TaskSettings taskSettings) {
        super(model, taskSettings);
    }

    protected AzureOpenAiModel(AzureOpenAiModel model, ServiceSettings serviceSettings) {
        super(model, serviceSettings);
    }

    public abstract ExecutableAction accept(AzureOpenAiActionVisitor creator, Map<String, Object> taskSettings);
}
