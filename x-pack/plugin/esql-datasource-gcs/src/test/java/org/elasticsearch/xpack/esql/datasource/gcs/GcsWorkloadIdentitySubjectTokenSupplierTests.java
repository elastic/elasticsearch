/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.gcs;

import org.elasticsearch.ElasticsearchTimeoutException;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.workloadidentity.spi.WorkloadIdentityIssuerClient;
import org.elasticsearch.workloadidentity.spi.WorkloadIdentityIssuerClient.IssueTokenResponse;
import org.elasticsearch.workloadidentity.spi.WorkloadIdentityIssuerClient.WorkloadIdentityNotEnabledException;

import java.io.IOException;
import java.net.SocketTimeoutException;
import java.time.Instant;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.sameInstance;

public class GcsWorkloadIdentitySubjectTokenSupplierTests extends ESTestCase {

    public void testGetSubjectTokenReturnsIssuerJwt() throws IOException {
        String audience = "https://example-audience";
        String token = "signed.jwt.token";
        WorkloadIdentityIssuerClient client = (request, listener) -> {
            assertEquals(audience, request.audience());
            listener.onResponse(new IssueTokenResponse(token, Instant.now().plusSeconds(300)));
        };

        GcsWorkloadIdentitySubjectTokenSupplier supplier = new GcsWorkloadIdentitySubjectTokenSupplier(client, audience);
        assertEquals(token, supplier.getSubjectToken(null));
    }

    public void testGetSubjectTokenPropagatesIOException() {
        IOException failure = new IOException("issuer unavailable");
        WorkloadIdentityIssuerClient client = (request, listener) -> listener.onFailure(failure);

        GcsWorkloadIdentitySubjectTokenSupplier supplier = new GcsWorkloadIdentitySubjectTokenSupplier(client, "https://example-audience");
        IOException thrown = expectThrows(IOException.class, () -> supplier.getSubjectToken(null));
        assertThat(thrown, sameInstance(failure));
    }

    public void testGetSubjectTokenPropagatesIOExceptionSubclass() {
        SocketTimeoutException failure = new SocketTimeoutException("issuer timed out");
        WorkloadIdentityIssuerClient client = (request, listener) -> listener.onFailure(failure);

        GcsWorkloadIdentitySubjectTokenSupplier supplier = new GcsWorkloadIdentitySubjectTokenSupplier(client, "https://example-audience");
        SocketTimeoutException thrown = expectThrows(SocketTimeoutException.class, () -> supplier.getSubjectToken(null));
        assertThat(thrown, sameInstance(failure));
    }

    public void testGetSubjectTokenTimesOutWhenIssuerNeverResponds() {
        // A misbehaving client that never completes the listener must not block the caller indefinitely.
        WorkloadIdentityIssuerClient client = (request, listener) -> {};

        GcsWorkloadIdentitySubjectTokenSupplier supplier = new GcsWorkloadIdentitySubjectTokenSupplier(
            client,
            "https://example-audience",
            TimeValue.timeValueMillis(10)
        );
        IOException thrown = expectThrows(IOException.class, () -> supplier.getSubjectToken(null));
        assertThat(thrown.getMessage(), containsString("timed out"));
        assertThat(thrown.getCause(), instanceOf(ElasticsearchTimeoutException.class));
    }

    public void testGetSubjectTokenPropagatesRuntimeFailure() {
        WorkloadIdentityNotEnabledException failure = new WorkloadIdentityNotEnabledException("workload-identity disabled");
        WorkloadIdentityIssuerClient client = (request, listener) -> listener.onFailure(failure);

        GcsWorkloadIdentitySubjectTokenSupplier supplier = new GcsWorkloadIdentitySubjectTokenSupplier(client, "https://example-audience");
        WorkloadIdentityNotEnabledException thrown = expectThrows(
            WorkloadIdentityNotEnabledException.class,
            () -> supplier.getSubjectToken(null)
        );
        assertThat(thrown, sameInstance(failure));
    }
}
