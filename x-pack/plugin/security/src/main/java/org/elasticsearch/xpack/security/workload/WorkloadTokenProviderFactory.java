/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.workload;

import org.elasticsearch.common.settings.Settings;

import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;

import com.azure.core.credential.TokenCredential;
import com.google.auth.oauth2.GoogleCredentials;

import java.util.Map;

public class WorkloadTokenProviderFactory {

    private final WorkloadIssuerCachingClient issuerClient;

    public WorkloadTokenProviderFactory(Settings settings) {
        this.issuerClient = null; // todo initialise
    }

    public AwsCredentialsProvider createAwsCredentialsProvider(
        String roleArnToAssume,
        String endpointOverride,
        Map<String, String> scopeLimitingContext
    ) {
        return new AwsWorkloadCredentialsProvider(issuerClient, roleArnToAssume, endpointOverride, scopeLimitingContext);
    }

    public TokenCredential createAzureCredentialsProvider(
        String tenantId,
        String clientId,
        String endpointOverride,
        Map<String, String> scopeLimitingContext
    ) {
        return new AzureWorkloadCredentialsProvider(issuerClient, tenantId, clientId, endpointOverride, scopeLimitingContext);
    }

    public GoogleCredentials createGcpCredentialsProvider(
        String projectNumber,
        String poolId,
        String providerId,
        String serviceAccountEmail,
        String endpointOverride,
        Map<String, String> scopeLimitingContext
    ) {
        return new GcpWorkloadCredentialsProvider(
            issuerClient,
            projectNumber,
            poolId,
            providerId,
            serviceAccountEmail,
            endpointOverride,
            scopeLimitingContext
        );
    }
}
