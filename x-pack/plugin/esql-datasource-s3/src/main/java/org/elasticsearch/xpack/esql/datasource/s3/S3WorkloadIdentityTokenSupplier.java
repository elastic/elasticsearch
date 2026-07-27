/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.s3;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.workloadidentity.spi.WorkloadIdentityIssuerClient;
import org.elasticsearch.workloadidentity.spi.WorkloadIdentityIssuerClient.IssueTokenRequest;
import org.elasticsearch.workloadidentity.spi.WorkloadIdentityIssuerClient.IssueTokenResponse;

import java.util.Objects;
import java.util.function.Consumer;

/**
 * Bridges {@link WorkloadIdentityIssuerClient#issueToken} to the {@link Consumer}-of-{@link ActionListener}
 * token source that {@link org.elasticsearch.workload.identity.aws.AsyncWebIdentityCredentialsProvider} invokes
 * before each STS {@code AssumeRoleWithWebIdentity} exchange. Both sides are listener-based, so this is a thin
 * async adapter that unwraps the JWT from the issuer response.
 */
final class S3WorkloadIdentityTokenSupplier implements Consumer<ActionListener<String>> {

    private final WorkloadIdentityIssuerClient issuerClient;
    private final String jwtAudience;

    S3WorkloadIdentityTokenSupplier(WorkloadIdentityIssuerClient issuerClient, String jwtAudience) {
        this.issuerClient = Objects.requireNonNull(issuerClient, "issuerClient must not be null");
        this.jwtAudience = Objects.requireNonNull(jwtAudience, "jwtAudience must not be null");
    }

    @Override
    public void accept(ActionListener<String> listener) {
        issuerClient.issueToken(new IssueTokenRequest(jwtAudience), listener.map(IssueTokenResponse::token));
    }
}
