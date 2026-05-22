/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.workload.credentials.aws;

import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;

import org.elasticsearch.workload.identity.WorkloadIssuerCachingClient;

import java.util.Map;
import java.util.Objects;

/**
 * AWS adapter for workload federation
 */
public final class AwsWorkloadCredentialsProvider implements AwsCredentialsProvider {

    private final WorkloadIssuerCachingClient issuerClient;
    private final String roleArnToAssume;
    private final String endpointOverride;
    private final Map<String, String> scopeLimitingContext;

    public AwsWorkloadCredentialsProvider(
        WorkloadIssuerCachingClient issuerClient,
        String roleArnToAssume,
        String endpointOverride,
        Map<String, String> scopeLimitingContext
    ) {
        this.issuerClient = Objects.requireNonNull(issuerClient, "issuerClient");
        this.roleArnToAssume = Objects.requireNonNull(roleArnToAssume, "roleArnToAssume");
        this.endpointOverride = endpointOverride;
        this.scopeLimitingContext = Map.copyOf(Objects.requireNonNullElse(scopeLimitingContext, Map.of()));
    }

    @Override
    public AwsCredentials resolveCredentials() {
        throw new UnsupportedOperationException("Not implemented yet");
    }
}
