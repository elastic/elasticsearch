/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.azureopenai;

import org.elasticsearch.inference.Model;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ModelSecrets;
import org.elasticsearch.inference.ServiceSettings;
import org.elasticsearch.inference.TaskSettings;
import org.elasticsearch.xpack.inference.external.action.ExecutableAction;
import org.elasticsearch.xpack.inference.external.action.azureopenai.AzureOpenAiActionVisitor;

import java.net.URI;
import java.util.Map;

public abstract class AzureOpenAiModel extends Model {

    protected URI uri;

    public AzureOpenAiModel(ModelConfigurations configurations, ModelSecrets secrets) {
        super(configurations, secrets);
    }

    protected AzureOpenAiModel(AzureOpenAiModel model, TaskSettings taskSettings) {
        super(model, taskSettings);
        this.uri = model.getUri();
    }

    protected AzureOpenAiModel(AzureOpenAiModel model, ServiceSettings serviceSettings) {
        super(model, serviceSettings);
        this.uri = model.getUri();
    }

    public abstract ExecutableAction accept(AzureOpenAiActionVisitor creator, Map<String, Object> taskSettings);

    public URI getUri() {
        return uri;
    }

    // Needed for testing
    public void setUri(URI newUri) {
        this.uri = newUri;
    }
}
