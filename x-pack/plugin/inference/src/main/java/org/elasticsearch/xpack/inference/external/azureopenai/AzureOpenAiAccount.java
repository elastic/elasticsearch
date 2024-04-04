/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.azureopenai;

import org.apache.http.client.utils.URIBuilder;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.inference.external.request.azureopenai.AzureOpenAiUtils;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Objects;

public record AzureOpenAiAccount(
    String resourceName,
    String deploymentId,
    String apiVersion,
    @Nullable SecureString apiKey,
    @Nullable SecureString entraId
) {
    public AzureOpenAiAccount {
        Objects.requireNonNull(resourceName);
        Objects.requireNonNull(deploymentId);
        Objects.requireNonNull(apiVersion);
        Objects.requireNonNullElse(apiKey, entraId);
    }

    public URI getEmbeddingsUri() throws URISyntaxException {
        String hostname = String.format("%s.%s", resourceName, AzureOpenAiUtils.HOST_SUFFIX);
        return new URIBuilder().setScheme("https")
            .setHost(hostname)
            .setPathSegments(
                AzureOpenAiUtils.OPENAI_PATH,
                AzureOpenAiUtils.DEPLOYMENTS_PATH,
                deploymentId,
                AzureOpenAiUtils.EMBEDDINGS_PATH
            )
            .addParameter(AzureOpenAiUtils.API_VERSION_PARAMETER, apiVersion)
            .build();

    }
}
