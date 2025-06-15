/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.voyageai.rerank;

import org.apache.http.client.utils.URIBuilder;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ModelSecrets;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xpack.inference.external.action.ExecutableAction;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.ServiceUtils;
import org.elasticsearch.xpack.inference.services.settings.DefaultSecretSettings;
import org.elasticsearch.xpack.inference.services.voyageai.VoyageAIModel;
import org.elasticsearch.xpack.inference.services.voyageai.VoyageAIService;
import org.elasticsearch.xpack.inference.services.voyageai.action.VoyageAIActionVisitor;
import org.elasticsearch.xpack.inference.services.voyageai.request.VoyageAIUtils;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;

import static org.elasticsearch.xpack.inference.external.request.RequestUtils.buildUri;
import static org.elasticsearch.xpack.inference.services.voyageai.request.VoyageAIUtils.HOST;

public class VoyageAIRerankModel extends VoyageAIModel {
    public static VoyageAIRerankModel of(VoyageAIRerankModel model, Map<String, Object> taskSettings) {
        var requestTaskSettings = VoyageAIRerankTaskSettings.fromMap(taskSettings);
        return new VoyageAIRerankModel(model, VoyageAIRerankTaskSettings.of(model.getTaskSettings(), requestTaskSettings));
    }

    public VoyageAIRerankModel(
        String inferenceId,
        String service,
        Map<String, Object> serviceSettings,
        Map<String, Object> taskSettings,
        @Nullable Map<String, Object> secrets,
        ConfigurationParseContext context
    ) {
        this(
            inferenceId,
            service,
            VoyageAIRerankServiceSettings.fromMap(serviceSettings, context),
            VoyageAIRerankTaskSettings.fromMap(taskSettings),
            DefaultSecretSettings.fromMap(secrets),
            buildUri(VoyageAIService.NAME, VoyageAIRerankModel::buildRequestUri)
        );
    }

    public static URI buildRequestUri() throws URISyntaxException {
        return new URIBuilder().setScheme("https")
            .setHost(HOST)
            .setPathSegments(VoyageAIUtils.VERSION_1, VoyageAIUtils.RERANK_PATH)
            .build();
    }

    // should only be used for testing
    VoyageAIRerankModel(
        String inferenceId,
        String service,
        String url,
        VoyageAIRerankServiceSettings serviceSettings,
        VoyageAIRerankTaskSettings taskSettings,
        @Nullable DefaultSecretSettings secretSettings
    ) {
        this(inferenceId, service, serviceSettings, taskSettings, secretSettings, ServiceUtils.createUri(url));
    }

    private VoyageAIRerankModel(
        String inferenceId,
        String service,
        VoyageAIRerankServiceSettings serviceSettings,
        VoyageAIRerankTaskSettings taskSettings,
        @Nullable DefaultSecretSettings secretSettings,
        URI uri
    ) {
        super(
            new ModelConfigurations(inferenceId, TaskType.RERANK, service, serviceSettings, taskSettings),
            new ModelSecrets(secretSettings),
            secretSettings,
            serviceSettings.getCommonSettings(),
            uri
        );
    }

    private VoyageAIRerankModel(VoyageAIRerankModel model, VoyageAIRerankTaskSettings taskSettings) {
        super(model, taskSettings);
    }

    @Override
    public VoyageAIRerankServiceSettings getServiceSettings() {
        return (VoyageAIRerankServiceSettings) super.getServiceSettings();
    }

    @Override
    public VoyageAIRerankTaskSettings getTaskSettings() {
        return (VoyageAIRerankTaskSettings) super.getTaskSettings();
    }

    @Override
    public DefaultSecretSettings getSecretSettings() {
        return (DefaultSecretSettings) super.getSecretSettings();
    }

    /**
     * Accepts a visitor to create an executable action. The returned action will not return documents in the response.
     * @param visitor          Interface for creating {@link ExecutableAction} instances for IBM Voyage AI models.
     * @param taskSettings     Settings in the request to override the model's defaults
     * @return the rerank action
     */
    @Override
    public ExecutableAction accept(VoyageAIActionVisitor visitor, Map<String, Object> taskSettings) {
        return visitor.create(this, taskSettings);
    }

}
