/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.workloadidentity;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;

/**
 * Locks in the activation contract: workload-identity is enabled on a node iff
 * {@link WorkloadIdentityIssuerSettings#ISSUER_URL_SETTING} is set to a non-blank value.
 * Other transport settings (SSL paths, HTTP timeouts, cache skew) are configuration of an
 * already-active client and must not gate activation on their own.
 */
public class WorkloadIdentityIssuerSettingsTests extends ESTestCase {

    public void testIsEnabledRequiresIssuerUrl() {
        assertFalse(WorkloadIdentityIssuerSettings.isEnabled(Settings.EMPTY));
        assertTrue(
            WorkloadIdentityIssuerSettings.isEnabled(
                Settings.builder().put(WorkloadIdentityIssuerSettings.ISSUER_URL_SETTING.getKey(), "https://issuer.example/").build()
            )
        );
    }

    public void testBlankIssuerUrlIsTreatedAsUnset() {
        // An explicitly-set but blank URL is unusable and should take the "feature off" branch
        // rather than falling through to a downstream URI-parse failure at node startup.
        for (String blank : new String[] { "", " ", "\t", "   " }) {
            final Settings settings = Settings.builder().put(WorkloadIdentityIssuerSettings.ISSUER_URL_SETTING.getKey(), blank).build();
            assertFalse("blank issuer URL must not enable workload-identity", WorkloadIdentityIssuerSettings.isEnabled(settings));
        }
    }

    public void testTransportSettingsAloneDoNotEnableTheFeature() {
        final Settings transportOnly = Settings.builder()
            .put(WorkloadIdentityIssuerSettings.TOKEN_CACHE_REFRESH_BEFORE_EXPIRY.getKey(), "30s")
            .build();
        assertFalse(
            "transport-tuning settings without an issuer URL must not enable workload-identity",
            WorkloadIdentityIssuerSettings.isEnabled(transportOnly)
        );
    }
}
