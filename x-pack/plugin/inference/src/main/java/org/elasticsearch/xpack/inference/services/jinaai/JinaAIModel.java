/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.jinaai;

import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ModelSecrets;
import org.elasticsearch.inference.TaskSettings;
import org.elasticsearch.xpack.inference.external.action.ExecutableAction;
import org.elasticsearch.xpack.inference.services.RateLimitGroupingModel;
import org.elasticsearch.xpack.inference.services.ServiceUtils;
import org.elasticsearch.xpack.inference.services.jinaai.action.JinaAIActionVisitor;
import org.elasticsearch.xpack.inference.services.settings.DefaultSecretSettings;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;

import java.net.URI;
import java.util.Map;

public abstract class JinaAIModel extends RateLimitGroupingModel {
    private final URI uri;

    public JinaAIModel(ModelConfigurations configurations, ModelSecrets secrets, URI uri) {
        super(configurations, secrets);
        this.uri = uri;
    }

    protected JinaAIModel(JinaAIModel model, TaskSettings taskSettings) {
        super(model, taskSettings);
        uri = model.uri();
    }

    protected JinaAIModel(JinaAIModel model, JinaAIServiceSettings serviceSettings) {
        super(model, serviceSettings);
        uri = model.uri();
    }

    @Override
    public JinaAIServiceSettings getServiceSettings() {
        return (JinaAIServiceSettings) super.getServiceSettings();
    }

    @Override
    public DefaultSecretSettings getSecretSettings() {
        return (DefaultSecretSettings) super.getSecretSettings();
    }

    public SecureString apiKey() {
        return ServiceUtils.apiKey(getSecretSettings());
    }

    public URI uri() {
        return uri;
    }

    @Override
    public int rateLimitGroupingHash() {
        return apiKey().hashCode();
    }

    @Override
    public RateLimitSettings rateLimitSettings() {
        return getServiceSettings().rateLimitSettings();
    }

    public abstract ExecutableAction accept(JinaAIActionVisitor creator, Map<String, Object> taskSettings);
}
