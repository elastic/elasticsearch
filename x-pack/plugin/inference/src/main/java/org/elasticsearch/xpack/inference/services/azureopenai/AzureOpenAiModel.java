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
import org.elasticsearch.xpack.inference.services.azureopenai.action.AzureOpenAiActionVisitor;
import org.elasticsearch.xpack.inference.services.azureopenai.request.AzureOpenAiUtils;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.core.Strings.format;

public abstract class AzureOpenAiModel extends Model {

    protected URI uri;
    private final AzureOpenAiRateLimitServiceSettings rateLimitServiceSettings;

    public AzureOpenAiModel(
        ModelConfigurations configurations,
        ModelSecrets secrets,
        AzureOpenAiRateLimitServiceSettings rateLimitServiceSettings
    ) {
        super(configurations, secrets);

        this.rateLimitServiceSettings = Objects.requireNonNull(rateLimitServiceSettings);
    }

    protected AzureOpenAiModel(AzureOpenAiModel model, TaskSettings taskSettings) {
        super(model, taskSettings);

        this.uri = model.getUri();
        rateLimitServiceSettings = model.rateLimitServiceSettings();
    }

    protected AzureOpenAiModel(AzureOpenAiModel model, ServiceSettings serviceSettings) {
        super(model, serviceSettings);

        this.uri = model.getUri();
        rateLimitServiceSettings = model.rateLimitServiceSettings();
    }

    public abstract ExecutableAction accept(AzureOpenAiActionVisitor creator, Map<String, Object> taskSettings);

    public final URI buildUriString() throws URISyntaxException {
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

    public AzureOpenAiRateLimitServiceSettings rateLimitServiceSettings() {
        return rateLimitServiceSettings;
    }

    // TODO: can be inferred directly from modelConfigurations.getServiceSettings(); will be addressed with separate refactoring
    public abstract String resourceName();

    public abstract String deploymentId();

    public abstract String apiVersion();

    public abstract String[] operationPathSegments();
}
