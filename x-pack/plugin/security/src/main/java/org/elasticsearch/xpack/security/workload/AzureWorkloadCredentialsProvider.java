/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.workload;

import reactor.core.publisher.Mono;

import com.azure.core.credential.AccessToken;
import com.azure.core.credential.TokenCredential;
import com.azure.core.credential.TokenRequestContext;

import java.util.Map;

class AzureWorkloadCredentialsProvider implements TokenCredential {

    private final WorkloadIssuerCachingClient issuerClient;
    private final String tenantId;
    private final String clientId;
    private final String endpointOverride;
    private final Map<String, String> scopeLimitingContext;

    AzureWorkloadCredentialsProvider(
        WorkloadIssuerCachingClient issuerClient,
        String tenantId,
        String clientId,
        String endpointOverride,
        Map<String, String> scopeLimitingContext
    ) {
        this.issuerClient = issuerClient;
        this.tenantId = tenantId;
        this.clientId = clientId;
        this.endpointOverride = endpointOverride;
        this.scopeLimitingContext = scopeLimitingContext;
    }

    @Override
    public Mono<AccessToken> getToken(TokenRequestContext request) {
        throw new UnsupportedOperationException("Not implemented yet");
    }
}
