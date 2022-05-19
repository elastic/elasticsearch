/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ssl;

import org.elasticsearch.bootstrap.BootstrapCheck;
import org.elasticsearch.bootstrap.BootstrapContext;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.license.License;
import org.elasticsearch.license.License.OperationMode;
import org.elasticsearch.license.TestUtils;
import org.elasticsearch.test.AbstractBootstrapCheckTestCase;

public class TransportTLSBootstrapCheckTests extends AbstractBootstrapCheckTestCase {
    public void testBootstrapCheckOnEmptyMetadata() {
        assertTrue(new TransportTLSBootstrapCheck().check(emptyContext).isFailure());
        assertTrue(
            new TransportTLSBootstrapCheck().check(
                createTestContext(Settings.builder().put("xpack.security.transport.ssl.enabled", false).build(), Metadata.EMPTY_METADATA)
            ).isFailure()
        );
        assertTrue(
            new TransportTLSBootstrapCheck().check(
                createTestContext(Settings.builder().put("xpack.security.transport.ssl.enabled", true).build(), Metadata.EMPTY_METADATA)
            ).isSuccess()
        );
    }

    public void testBootstrapCheckFailureOnAnyLicense() throws Exception {
        final OperationMode mode = randomFrom(
            OperationMode.ENTERPRISE,
            OperationMode.PLATINUM,
            OperationMode.GOLD,
            OperationMode.STANDARD,
            OperationMode.BASIC,
            OperationMode.TRIAL
        );
        final Settings.Builder settings = Settings.builder();
        if (randomBoolean()) {
            // randomise between default-false & explicit-false
            settings.put("xpack.security.transport.ssl.enabled", false);
        }
        if (randomBoolean()) {
            // randomise between default-true & explicit-true
            settings.put("xpack.security.enabled", true);
        }

        final BootstrapCheck.BootstrapCheckResult result = runBootstrapCheck(mode, settings);
        assertTrue("Expected bootstrap failure", result.isFailure());
        assertEquals(
            "Transport SSL must be enabled if security is enabled. Please set "
                + "[xpack.security.transport.ssl.enabled] to [true] or disable security by setting "
                + "[xpack.security.enabled] to [false]",
            result.getMessage()
        );
    }

    public void testBootstrapCheckSucceedsWithTlsEnabledOnAnyLicense() throws Exception {
        final OperationMode mode = randomFrom(
            OperationMode.ENTERPRISE,
            OperationMode.PLATINUM,
            OperationMode.GOLD,
            OperationMode.STANDARD,
            OperationMode.BASIC,
            OperationMode.TRIAL
        );
        final Settings.Builder settings = Settings.builder().put("xpack.security.transport.ssl.enabled", true);
        final BootstrapCheck.BootstrapCheckResult result = runBootstrapCheck(mode, settings);
        assertSuccess(result);
    }

    public BootstrapCheck.BootstrapCheckResult runBootstrapCheck(OperationMode mode, Settings.Builder settings) throws Exception {
        final License license = TestUtils.generateSignedLicense(mode.description(), TimeValue.timeValueHours(24));
        Metadata.Builder builder = Metadata.builder();
        TestUtils.putLicense(builder, license);
        Metadata metadata = builder.build();
        final BootstrapContext context = createTestContext(settings.build(), metadata);
        return new TransportTLSBootstrapCheck().check(context);
    }

    public void assertSuccess(BootstrapCheck.BootstrapCheckResult result) {
        if (result.isFailure()) {
            fail("Bootstrap check failed unexpectedly: " + result.getMessage());
        }
    }

}
