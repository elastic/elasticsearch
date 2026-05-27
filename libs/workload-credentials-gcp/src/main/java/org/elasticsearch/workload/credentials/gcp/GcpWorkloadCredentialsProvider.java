/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.workload.credentials.gcp;

import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.GoogleCredentials;

import org.elasticsearch.workload.identity.WorkloadIssuerCachingClient;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

/**
 * GCP adapter for workload federation
 */
public final class GcpWorkloadCredentialsProvider extends GoogleCredentials {

    private final transient WorkloadIssuerCachingClient issuerClient;
    private final String projectNumber;
    private final String poolId;
    private final String providerId;
    private final String serviceAccountEmail;
    private final String endpointOverride;
    private final Map<String, String> scopeLimitingContext;

    public GcpWorkloadCredentialsProvider(
        WorkloadIssuerCachingClient issuerClient,
        String projectNumber,
        String poolId,
        String providerId,
        String serviceAccountEmail,
        String endpointOverride,
        Map<String, String> scopeLimitingContext
    ) {
        this.issuerClient = Objects.requireNonNull(issuerClient, "issuerClient");
        this.projectNumber = Objects.requireNonNull(projectNumber, "projectNumber");
        this.poolId = Objects.requireNonNull(poolId, "poolId");
        this.providerId = Objects.requireNonNull(providerId, "providerId");
        this.serviceAccountEmail = Objects.requireNonNull(serviceAccountEmail, "serviceAccountEmail");
        this.endpointOverride = endpointOverride;
        this.scopeLimitingContext = Map.copyOf(Objects.requireNonNullElse(scopeLimitingContext, Map.of()));
    }

    @Override
    public AccessToken refreshAccessToken() throws IOException {
        throw new UnsupportedOperationException("Not implemented yet");
    }
}
