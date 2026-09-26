/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.s3;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.workloadidentity.spi.WorkloadIdentityIssuerClient;
import org.elasticsearch.workloadidentity.spi.WorkloadIdentityIssuerClient.IssueTokenResponse;
import org.elasticsearch.workloadidentity.spi.WorkloadIdentityIssuerClient.WorkloadIdentityIssuerException;

import java.io.IOException;
import java.time.Instant;
import java.util.concurrent.ExecutionException;

import static org.hamcrest.Matchers.sameInstance;

public class S3WorkloadIdentityTokenSupplierTests extends ESTestCase {

    public void testForwardsAudienceAndUnwrapsToken() {
        String audience = "arn:aws:iam::123456789012:role/example";
        String token = "signed.jwt.token";
        WorkloadIdentityIssuerClient client = (request, listener) -> {
            assertEquals(audience, request.audience());
            listener.onResponse(new IssueTokenResponse(token, Instant.now().plusSeconds(300)));
        };

        S3WorkloadIdentityTokenSupplier supplier = new S3WorkloadIdentityTokenSupplier(client, audience);
        PlainActionFuture<String> future = new PlainActionFuture<>();
        supplier.accept(future);
        assertEquals(token, future.actionGet());
    }

    public void testPropagatesIssuerFailure() {
        IOException failure = new IOException("issuer unavailable");
        WorkloadIdentityIssuerClient client = (request, listener) -> listener.onFailure(failure);

        S3WorkloadIdentityTokenSupplier supplier = new S3WorkloadIdentityTokenSupplier(client, "arn:aws:iam::123456789012:role/example");
        PlainActionFuture<String> future = new PlainActionFuture<>();
        supplier.accept(future);
        ExecutionException thrown = expectThrows(ExecutionException.class, future::get);
        assertThat(thrown.getCause(), sameInstance(failure));
    }

    public void testPropagatesNotEnabledFailure() {
        WorkloadIdentityIssuerException failure = new WorkloadIdentityIssuerException("boom", 503);
        WorkloadIdentityIssuerClient client = (request, listener) -> listener.onFailure(failure);

        S3WorkloadIdentityTokenSupplier supplier = new S3WorkloadIdentityTokenSupplier(client, "arn:aws:iam::123456789012:role/example");
        PlainActionFuture<String> future = new PlainActionFuture<>();
        supplier.accept(future);
        ExecutionException thrown = expectThrows(ExecutionException.class, future::get);
        assertThat(thrown.getCause(), sameInstance(failure));
    }

    public void testRejectsNullArguments() {
        WorkloadIdentityIssuerClient client = (request, listener) -> ActionListener.noop();
        expectThrows(NullPointerException.class, () -> new S3WorkloadIdentityTokenSupplier(null, "aud"));
        expectThrows(NullPointerException.class, () -> new S3WorkloadIdentityTokenSupplier(client, null));
    }
}
