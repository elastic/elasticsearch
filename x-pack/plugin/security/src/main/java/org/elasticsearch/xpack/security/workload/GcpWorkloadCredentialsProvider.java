/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.workload;

import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.GoogleCredentials;

import java.io.IOException;
import java.util.Map;

class GcpWorkloadCredentialsProvider extends GoogleCredentials {

    private static final long serialVersionUID = 1L;

    private final transient WorkloadIssuerCachingClient issuerClient;
    private final String projectNumber;
    private final String poolId;
    private final String providerId;
    private final String serviceAccountEmail;
    private final String endpointOverride;
    private final Map<String, String> scopeLimitingContext;

    GcpWorkloadCredentialsProvider(
        WorkloadIssuerCachingClient issuerClient,
        String projectNumber,
        String poolId,
        String providerId,
        String serviceAccountEmail,
        String endpointOverride,
        Map<String, String> scopeLimitingContext
    ) {
        this.issuerClient = issuerClient;
        this.projectNumber = projectNumber;
        this.poolId = poolId;
        this.providerId = providerId;
        this.serviceAccountEmail = serviceAccountEmail;
        this.endpointOverride = endpointOverride;
        this.scopeLimitingContext = scopeLimitingContext;
    }

    @Override
    public AccessToken refreshAccessToken() throws IOException {
        throw new UnsupportedOperationException("Not implemented yet");
    }
}
