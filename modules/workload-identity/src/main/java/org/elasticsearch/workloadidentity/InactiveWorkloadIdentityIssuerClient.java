/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.workloadidentity;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.workloadidentity.spi.WorkloadIdentityIssuerClient;

/**
 * Null-object {@link WorkloadIdentityIssuerClient} registered when the workload-identity feature
 * is not enabled on this node. The transport (SSL config, HTTP client manager, IO reactor) is not
 * built in this case, so consumers always have a non-null binding to inject or look up; calls to
 * {@link #issueToken} fail the listener with {@link WorkloadIdentityNotEnabledException} so the
 * activation gap is observable rather than silent.
 */
public final class InactiveWorkloadIdentityIssuerClient implements WorkloadIdentityIssuerClient {

    public static final InactiveWorkloadIdentityIssuerClient INSTANCE = new InactiveWorkloadIdentityIssuerClient();

    private InactiveWorkloadIdentityIssuerClient() {}

    @Override
    public boolean isEnabled() {
        return false;
    }

    @Override
    public void issueToken(IssueTokenRequest request, ActionListener<IssueTokenResponse> listener) {
        listener.onFailure(
            new WorkloadIdentityNotEnabledException(
                "workload-identity is not enabled on this node; set [" + WorkloadIdentityIssuerSettings.ISSUER_URL_SETTING.getKey() + "]"
            )
        );
    }
}
