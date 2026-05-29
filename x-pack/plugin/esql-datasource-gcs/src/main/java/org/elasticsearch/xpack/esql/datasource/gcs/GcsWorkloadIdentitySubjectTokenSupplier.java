/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.gcs;

import com.google.auth.oauth2.ExternalAccountSupplierContext;
import com.google.auth.oauth2.IdentityPoolSubjectTokenSupplier;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.workloadidentity.spi.WorkloadIdentityIssuerClient;
import org.elasticsearch.workloadidentity.spi.WorkloadIdentityIssuerClient.IssueTokenRequest;
import org.elasticsearch.workloadidentity.spi.WorkloadIdentityIssuerClient.IssueTokenResponse;

import java.io.IOException;
import java.util.Objects;

/**
 * Bridges {@link WorkloadIdentityIssuerClient#issueToken} to
 * {@link IdentityPoolSubjectTokenSupplier} for {@code IdentityPoolCredentials}.
 */
final class GcsWorkloadIdentitySubjectTokenSupplier implements IdentityPoolSubjectTokenSupplier {

    private final WorkloadIdentityIssuerClient issuerClient;
    private final String jwtAudience;

    GcsWorkloadIdentitySubjectTokenSupplier(WorkloadIdentityIssuerClient issuerClient, String jwtAudience) {
        this.issuerClient = Objects.requireNonNull(issuerClient, "issuerClient must not be null");
        this.jwtAudience = Objects.requireNonNull(jwtAudience, "jwtAudience must not be null");
    }

    @Override
    public String getSubjectToken(ExternalAccountSupplierContext context) throws IOException {
        PlainActionFuture<IssueTokenResponse> future = new PlainActionFuture<>();
        issuerClient.issueToken(new IssueTokenRequest(jwtAudience), future);
        try {
            return future.actionGet().token();
        } catch (RuntimeException e) {
            Throwable ioException = ExceptionsHelper.unwrap(e, IOException.class);
            if (ioException != null) {
                throw (IOException) ioException;
            }
            throw e;
        }
    }
}
