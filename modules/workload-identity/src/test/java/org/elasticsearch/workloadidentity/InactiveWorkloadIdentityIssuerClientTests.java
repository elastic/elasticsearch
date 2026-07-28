/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.workloadidentity;

import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.workloadidentity.spi.WorkloadIdentityIssuerClient.IssueTokenRequest;
import org.elasticsearch.workloadidentity.spi.WorkloadIdentityIssuerClient.IssueTokenResponse;
import org.elasticsearch.workloadidentity.spi.WorkloadIdentityIssuerClient.WorkloadIdentityNotEnabledException;

import java.util.concurrent.ExecutionException;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;

/**
 * Activation-gating tests for {@link InactiveWorkloadIdentityIssuerClient}. The stub is what gets
 * registered when {@link WorkloadIdentityIssuerSettings#isEnabled} is {@code false}, so consumers
 * can rely on a non-null binding while still being able to detect the disabled state.
 */
public class InactiveWorkloadIdentityIssuerClientTests extends ESTestCase {

    public void testIsEnabledReturnsFalse() {
        assertFalse(
            "the inactive stub must always report disabled so callers can gate workload-identity-backed paths",
            InactiveWorkloadIdentityIssuerClient.INSTANCE.isEnabled()
        );
    }

    public void testIssueTokenFailsListenerWithTypedException() {
        final var future = new PlainActionFuture<IssueTokenResponse>();
        InactiveWorkloadIdentityIssuerClient.INSTANCE.issueToken(new IssueTokenRequest(randomAlphaOfLengthBetween(4, 16)), future);

        final ExecutionException thrown = expectThrows(ExecutionException.class, future::get);
        assertThat(thrown.getCause(), instanceOf(WorkloadIdentityNotEnabledException.class));
        assertThat(thrown.getCause().getMessage(), containsString(WorkloadIdentityIssuerSettings.ISSUER_URL_SETTING.getKey()));
    }
}
