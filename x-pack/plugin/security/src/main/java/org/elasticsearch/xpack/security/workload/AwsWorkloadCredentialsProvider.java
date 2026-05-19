/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.workload;

import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;

import java.util.Map;

class AwsWorkloadCredentialsProvider implements AwsCredentialsProvider {

    private final WorkloadIssuerCachingClient issuerClient;
    private final String roleArnToAssume;
    private final String endpointOverride;
    private final Map<String, String> scopeLimitingContext;

    AwsWorkloadCredentialsProvider(
        WorkloadIssuerCachingClient issuerClient,
        String roleArnToAssume,
        String endpointOverride,
        Map<String, String> scopeLimitingContext
    ) {
        this.issuerClient = issuerClient;
        this.roleArnToAssume = roleArnToAssume;
        this.endpointOverride = endpointOverride;
        this.scopeLimitingContext = scopeLimitingContext;
    }

    @Override
    public AwsCredentials resolveCredentials() {
        throw new UnsupportedOperationException("Not implemented yet");
    }
}
