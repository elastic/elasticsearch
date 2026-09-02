/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.alibabacloudsearch;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.Model;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ModelSecrets;
import org.elasticsearch.inference.ServiceSettings;
import org.elasticsearch.inference.TaskSettings;
import org.elasticsearch.xpack.inference.external.action.ExecutableAction;
import org.elasticsearch.xpack.inference.services.alibabacloudsearch.action.AlibabaCloudSearchActionVisitor;

import java.net.URI;
import java.util.Map;
import java.util.Objects;

public abstract class AlibabaCloudSearchModel extends Model {
    private final AlibabaCloudSearchRateLimitServiceSettings rateLimitServiceSettings;
    // Only used for testing, should always be null in production environments
    private final URI uri;

    public AlibabaCloudSearchModel(
        ModelConfigurations configurations,
        ModelSecrets secrets,
        AlibabaCloudSearchRateLimitServiceSettings rateLimitServiceSettings,
        @Nullable URI uri
    ) {
        super(configurations, secrets);
        this.rateLimitServiceSettings = Objects.requireNonNull(rateLimitServiceSettings);
        this.uri = uri;
    }

    protected AlibabaCloudSearchModel(AlibabaCloudSearchModel model, TaskSettings taskSettings) {
        super(model, taskSettings);
        rateLimitServiceSettings = model.rateLimitServiceSettings();
        this.uri = model.uri();
    }

    protected AlibabaCloudSearchModel(AlibabaCloudSearchModel model, ServiceSettings serviceSettings) {
        super(model, serviceSettings);
        rateLimitServiceSettings = model.rateLimitServiceSettings();
        this.uri = model.uri();
    }

    public abstract ExecutableAction accept(AlibabaCloudSearchActionVisitor creator, Map<String, Object> taskSettings);

    public AlibabaCloudSearchRateLimitServiceSettings rateLimitServiceSettings() {
        return rateLimitServiceSettings;
    }

    public URI uri() {
        return uri;
    }
}
