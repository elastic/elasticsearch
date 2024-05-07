/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.azureopenai;

import org.apache.http.client.utils.URIBuilder;
import org.elasticsearch.inference.Model;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ModelSecrets;
import org.elasticsearch.inference.ServiceSettings;
import org.elasticsearch.inference.TaskSettings;
import org.elasticsearch.xpack.inference.external.action.ExecutableAction;
import org.elasticsearch.xpack.inference.external.action.azureopenai.AzureOpenAiActionVisitor;
import org.elasticsearch.xpack.inference.external.request.azureopenai.AzureOpenAiUtils;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.core.Strings.format;

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

    public URI buildUriString() throws URISyntaxException {
        return AzureOpenAiModel.buildUri(resourceName(), deploymentId(), apiVersion(), operationPathSegments());
    }

    // use only for testing directly
    public static URI buildUri(String resourceName, String deploymentId, String apiVersion, String... pathSegments)
        throws URISyntaxException {
        String hostname = format("%s.%s", resourceName, AzureOpenAiUtils.HOST_SUFFIX);

        return new URIBuilder().setScheme("https")
            .setHost(hostname)
            .setPathSegments(createPathSegmentsList(deploymentId, pathSegments))
            .addParameter(AzureOpenAiUtils.API_VERSION_PARAMETER, apiVersion)
            .build();
    }

    private static List<String> createPathSegmentsList(String deploymentId, String[] pathSegments) {
        List<String> pathSegmentsList = new ArrayList<>(
            List.of(AzureOpenAiUtils.OPENAI_PATH, AzureOpenAiUtils.DEPLOYMENTS_PATH, deploymentId)
        );
        pathSegmentsList.addAll(Arrays.asList(pathSegments));
        return pathSegmentsList;
    }

    public URI getUri() {
        return uri;
    }

    // Needed for testing
    public void setUri(URI newUri) {
        this.uri = newUri;
    }

    // TODO: can be inferred directly from modelConfigurations.getServiceSettings(); will be addressed with separate refactoring
    public abstract String resourceName();

    public abstract String deploymentId();

    public abstract String apiVersion();

    public abstract String[] operationPathSegments();
}
