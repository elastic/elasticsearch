/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.ibmwatsonx.rerank;

import org.apache.http.client.utils.URIBuilder;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.InputType;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ModelSecrets;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xpack.inference.external.action.ExecutableAction;
import org.elasticsearch.xpack.inference.external.action.ibmwatsonx.IbmWatsonxActionVisitor;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.ibmwatsonx.IbmWatsonxModel;
import org.elasticsearch.xpack.inference.services.settings.DefaultSecretSettings;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;

import static org.elasticsearch.xpack.inference.external.request.ibmwatsonx.IbmWatsonxUtils.ML;
import static org.elasticsearch.xpack.inference.external.request.ibmwatsonx.IbmWatsonxUtils.RERANKS;
import static org.elasticsearch.xpack.inference.external.request.ibmwatsonx.IbmWatsonxUtils.TEXT;
import static org.elasticsearch.xpack.inference.external.request.ibmwatsonx.IbmWatsonxUtils.V1;


public class IbmWatsonxRerankModel extends IbmWatsonxModel {
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
            DefaultSecretSettings.fromMap(secrets)
        );
    }

    // should only be used for testing
    IbmWatsonxRerankModel(
        String modelId,
        TaskType taskType,
        String service,
        IbmWatsonxRerankServiceSettings serviceSettings,
        IbmWatsonxRerankTaskSettings taskSettings,
        @Nullable DefaultSecretSettings secretSettings
    ) {
        super(
            new ModelConfigurations(modelId, taskType, service, serviceSettings, taskSettings),
            new ModelSecrets(secretSettings),
            serviceSettings
        );
    }

    private IbmWatsonxRerankModel(IbmWatsonxRerankModel model, IbmWatsonxRerankTaskSettings taskSettings) {
        super(model, taskSettings);
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
        URI uri;
        try {
            uri = buildUri(this.getServiceSettings().uri().toString(),this.getServiceSettings().apiVersion());
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }

        return uri;
    }
    /**
     * Accepts a visitor to create an executable action. The returned action will not return documents in the response.
     * @param visitor _
     * @param taskSettings _
     * @param inputType ignored for rerank task
     * @return the rerank action
     */
    @Override
    public ExecutableAction accept(IbmWatsonxActionVisitor visitor, Map<String, Object> taskSettings, InputType inputType) {
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
