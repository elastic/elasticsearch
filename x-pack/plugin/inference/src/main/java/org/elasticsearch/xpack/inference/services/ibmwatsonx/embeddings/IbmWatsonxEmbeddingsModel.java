/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.ibmwatsonx.embeddings;

import org.apache.http.client.utils.URIBuilder;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.ChunkingSettings;
import org.elasticsearch.inference.EmptyTaskSettings;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ModelSecrets;
import org.elasticsearch.inference.TaskSettings;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xpack.inference.external.action.ExecutableAction;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.ibmwatsonx.IbmWatsonxModel;
import org.elasticsearch.xpack.inference.services.ibmwatsonx.action.IbmWatsonxActionVisitor;
import org.elasticsearch.xpack.inference.services.settings.DefaultSecretSettings;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;

import static org.elasticsearch.xpack.inference.services.ibmwatsonx.request.IbmWatsonxUtils.EMBEDDINGS;
import static org.elasticsearch.xpack.inference.services.ibmwatsonx.request.IbmWatsonxUtils.ML;
import static org.elasticsearch.xpack.inference.services.ibmwatsonx.request.IbmWatsonxUtils.TEXT;
import static org.elasticsearch.xpack.inference.services.ibmwatsonx.request.IbmWatsonxUtils.V1;

public class IbmWatsonxEmbeddingsModel extends IbmWatsonxModel {

    private URI uri;

    public IbmWatsonxEmbeddingsModel(
        String inferenceEntityId,
        TaskType taskType,
        String service,
        Map<String, Object> serviceSettings,
        Map<String, Object> taskSettings,
        ChunkingSettings chunkingSettings,
        Map<String, Object> secrets,
        ConfigurationParseContext context
    ) {
        this(
            inferenceEntityId,
            taskType,
            service,
            IbmWatsonxEmbeddingsServiceSettings.fromMap(serviceSettings, context),
            EmptyTaskSettings.INSTANCE,
            chunkingSettings,
            DefaultSecretSettings.fromMap(secrets)
        );
    }

    public IbmWatsonxEmbeddingsModel(IbmWatsonxEmbeddingsModel model, IbmWatsonxEmbeddingsServiceSettings serviceSettings) {
        super(model, serviceSettings);
    }

    // Should only be used directly for testing
    IbmWatsonxEmbeddingsModel(
        String inferenceEntityId,
        TaskType taskType,
        String service,
        IbmWatsonxEmbeddingsServiceSettings serviceSettings,
        TaskSettings taskSettings,
        ChunkingSettings chunkingsettings,
        @Nullable DefaultSecretSettings secrets
    ) {
        super(
            new ModelConfigurations(inferenceEntityId, taskType, service, serviceSettings, taskSettings, chunkingsettings),
            new ModelSecrets(secrets),
            serviceSettings
        );
        try {
            this.uri = buildUri(serviceSettings.url().toString(), serviceSettings.apiVersion());
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    // Should only be used directly for testing
    IbmWatsonxEmbeddingsModel(
        String inferenceEntityId,
        TaskType taskType,
        String service,
        String uri,
        IbmWatsonxEmbeddingsServiceSettings serviceSettings,
        TaskSettings taskSettings,
        @Nullable DefaultSecretSettings secrets
    ) {
        super(
            new ModelConfigurations(inferenceEntityId, taskType, service, serviceSettings, taskSettings),
            new ModelSecrets(secrets),
            serviceSettings
        );
        try {
            this.uri = new URI(uri);
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public IbmWatsonxEmbeddingsServiceSettings getServiceSettings() {
        return (IbmWatsonxEmbeddingsServiceSettings) super.getServiceSettings();
    }

    @Override
    public DefaultSecretSettings getSecretSettings() {
        return (DefaultSecretSettings) super.getSecretSettings();
    }

    public URI uri() {
        return uri;
    }

    @Override
    public ExecutableAction accept(IbmWatsonxActionVisitor visitor, Map<String, Object> taskSettings) {
        return visitor.create(this, taskSettings);
    }

    public static URI buildUri(String uri, String apiVersion) throws URISyntaxException {
        return new URIBuilder().setScheme("https")
            .setHost(uri)
            .setPathSegments(ML, V1, TEXT, EMBEDDINGS)
            .setParameter("version", apiVersion)
            .build();
    }
}
