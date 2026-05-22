/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.workload.credentials.azure;

import reactor.core.publisher.Mono;

import com.azure.core.credential.AccessToken;
import com.azure.core.credential.TokenCredential;
import com.azure.core.credential.TokenRequestContext;

import org.elasticsearch.workload.identity.WorkloadIssuerCachingClient;

import java.util.Map;
import java.util.Objects;

/**
 * Azure adapter for workload federation
 */
public final class AzureWorkloadCredentialsProvider implements TokenCredential {

    private final WorkloadIssuerCachingClient issuerClient;
    private final String tenantId;
    private final String clientId;
    private final String endpointOverride;
    private final Map<String, String> scopeLimitingContext;

    public AzureWorkloadCredentialsProvider(
        WorkloadIssuerCachingClient issuerClient,
        String tenantId,
        String clientId,
        String endpointOverride,
        Map<String, String> scopeLimitingContext
    ) {
        this.issuerClient = Objects.requireNonNull(issuerClient, "issuerClient");
        this.tenantId = Objects.requireNonNull(tenantId, "tenantId");
        this.clientId = Objects.requireNonNull(clientId, "clientId");
        this.endpointOverride = endpointOverride;
        this.scopeLimitingContext = Map.copyOf(Objects.requireNonNullElse(scopeLimitingContext, Map.of()));
    }

    @Override
    public Mono<AccessToken> getToken(TokenRequestContext request) {
        throw new UnsupportedOperationException("Not implemented yet");
    }
}
