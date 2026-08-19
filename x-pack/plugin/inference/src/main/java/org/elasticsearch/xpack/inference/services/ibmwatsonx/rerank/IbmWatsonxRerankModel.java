/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.ibmwatsonx.rerank;

import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.utils.URIBuilder;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ModelSecrets;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xpack.inference.external.action.ExecutableAction;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.ibmwatsonx.IbmWatsonxModel;
import org.elasticsearch.xpack.inference.services.ibmwatsonx.action.IbmWatsonxActionVisitor;
import org.elasticsearch.xpack.inference.services.settings.DefaultSecretSettings;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;
import java.util.function.BiConsumer;

import static org.elasticsearch.xpack.inference.services.ibmwatsonx.request.IbmWatsonxUtils.ML;
import static org.elasticsearch.xpack.inference.services.ibmwatsonx.request.IbmWatsonxUtils.RERANKS;
import static org.elasticsearch.xpack.inference.services.ibmwatsonx.request.IbmWatsonxUtils.TEXT;
import static org.elasticsearch.xpack.inference.services.ibmwatsonx.request.IbmWatsonxUtils.V1;

public class IbmWatsonxRerankModel extends IbmWatsonxModel {

    private final URI uri;

    public static IbmWatsonxRerankModel of(IbmWatsonxRerankModel model, Map<String, Object> taskSettings) {
        var requestTaskSettings = IbmWatsonxRerankTaskSettings.fromMap(taskSettings);
        return new IbmWatsonxRerankModel(model, IbmWatsonxRerankTaskSettings.of(model.getTaskSettings(), requestTaskSettings));
    }

    public IbmWatsonxRerankModel(
        String modelId,
        TaskType taskType,
        String service,
        Map<String, Object> serviceSettings,
        Map<String, Object> taskSettings,
        @Nullable Map<String, Object> secrets,
        ConfigurationParseContext context
    ) {
        this(
            modelId,
            taskType,
            service,
            IbmWatsonxRerankServiceSettings.fromMap(serviceSettings, context),
            IbmWatsonxRerankTaskSettings.fromMap(taskSettings),
            DefaultSecretSettings.fromMap(secrets, context)
        );
    }

    public IbmWatsonxRerankModel(
        String modelId,
        TaskType taskType,
        String service,
        IbmWatsonxRerankServiceSettings serviceSettings,
        IbmWatsonxRerankTaskSettings taskSettings,
        @Nullable DefaultSecretSettings secretSettings
    ) {
        this(new ModelConfigurations(modelId, taskType, service, serviceSettings, taskSettings), new ModelSecrets(secretSettings));
    }

    public IbmWatsonxRerankModel(ModelConfigurations modelConfigurations, ModelSecrets modelSecrets) {
        super(modelConfigurations, modelSecrets);
        try {
            var serviceSettings = (IbmWatsonxRerankServiceSettings) modelConfigurations.getServiceSettings();
            this.uri = buildUri(serviceSettings.uri().toString(), serviceSettings.apiVersion());
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    // Should only be used directly for testing.
    // This constructor allows tests to override the behaviour when setting the auth header, which by default requires making a call to
    // IBM's token provider API. It also allows a custom URL to be set for unit testing.
    public IbmWatsonxRerankModel(
        String inferenceEntityId,
        TaskType taskType,
        String service,
        String url,
        IbmWatsonxRerankServiceSettings serviceSettings,
        IbmWatsonxRerankTaskSettings taskSettings,
        @Nullable DefaultSecretSettings secretSettings,
        BiConsumer<HttpPost, IbmWatsonxModel> authHeaderDecorator
    ) {
        super(
            new ModelConfigurations(inferenceEntityId, taskType, service, serviceSettings, taskSettings),
            new ModelSecrets(secretSettings),
            authHeaderDecorator
        );
        try {
            this.uri = url == null ? buildUri(serviceSettings.uri().toString(), serviceSettings.apiVersion()) : new URI(url);
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    private IbmWatsonxRerankModel(IbmWatsonxRerankModel model, IbmWatsonxRerankTaskSettings taskSettings) {
        super(model, taskSettings);
        this.uri = model.uri();
    }

    @Override
    public IbmWatsonxRerankServiceSettings getServiceSettings() {
        return (IbmWatsonxRerankServiceSettings) super.getServiceSettings();
    }

    @Override
    public IbmWatsonxRerankTaskSettings getTaskSettings() {
        return (IbmWatsonxRerankTaskSettings) super.getTaskSettings();
    }

    @Override
    public DefaultSecretSettings getSecretSettings() {
        return (DefaultSecretSettings) super.getSecretSettings();
    }

    public URI uri() {
        return uri;
    }

    /**
     * Accepts a visitor to create an executable action. The returned action will not return documents in the response.
     * @param visitor          Interface for creating {@link ExecutableAction} instances for IBM watsonx models.
     * @param taskSettings     Settings in the request to override the model's defaults
     * @return the rerank action
     */
    @Override
    public ExecutableAction accept(IbmWatsonxActionVisitor visitor, Map<String, Object> taskSettings) {
        return visitor.create(this, taskSettings);
    }

    public static URI buildUri(String uri, String apiVersion) throws URISyntaxException {
        return new URIBuilder().setScheme("https")
            .setHost(uri)
            .setPathSegments(ML, V1, TEXT, RERANKS)
            .setParameter("version", apiVersion)
            .build();
    }
}
