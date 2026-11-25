/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.gcs;

import com.google.auth.oauth2.ExternalAccountSupplierContext;
import com.google.auth.oauth2.IdentityPoolSubjectTokenSupplier;

import org.elasticsearch.ElasticsearchTimeoutException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.core.TimeValue;
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

    /**
     * Upper bound on the blocking wait for a token. The {@link WorkloadIdentityIssuerClient}
     * is expected to enforce its own (tighter) connect/request/retry budgets, so this only guards against a
     * misbehaving client that never completes the listener. Kept comfortably larger than the issuer's default
     * retry budget so it doesn't preempt that client's normal timing.
     */
    static final TimeValue ISSUE_TOKEN_TIMEOUT = TimeValue.timeValueSeconds(60);

    private final WorkloadIdentityIssuerClient issuerClient;
    private final String jwtAudience;
    private final TimeValue timeout;

    GcsWorkloadIdentitySubjectTokenSupplier(WorkloadIdentityIssuerClient issuerClient, String jwtAudience) {
        this(issuerClient, jwtAudience, ISSUE_TOKEN_TIMEOUT);
    }

    GcsWorkloadIdentitySubjectTokenSupplier(WorkloadIdentityIssuerClient issuerClient, String jwtAudience, TimeValue timeout) {
        this.issuerClient = Objects.requireNonNull(issuerClient, "issuerClient must not be null");
        this.jwtAudience = Objects.requireNonNull(jwtAudience, "jwtAudience must not be null");
        this.timeout = Objects.requireNonNull(timeout, "timeout must not be null");
    }

    @Override
    public String getSubjectToken(ExternalAccountSupplierContext context) throws IOException {
        PlainActionFuture<IssueTokenResponse> future = new PlainActionFuture<>();
        issuerClient.issueToken(new IssueTokenRequest(jwtAudience), future);
        try {
            return future.actionGet(timeout).token();
        } catch (ElasticsearchTimeoutException e) {
            // Surface as an IOException so the GCS credential machinery treats it as a transient fetch failure.
            throw new IOException("timed out after [" + timeout + "] waiting for the workload identity token", e);
        } catch (RuntimeException e) {
            Throwable ioException = ExceptionsHelper.unwrap(e, IOException.class);
            if (ioException != null) {
                throw (IOException) ioException;
            }
            throw e;
        }
    }
}
