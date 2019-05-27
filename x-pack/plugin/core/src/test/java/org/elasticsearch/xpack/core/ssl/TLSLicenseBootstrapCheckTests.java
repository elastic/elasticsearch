/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ssl;

import org.elasticsearch.bootstrap.BootstrapCheck;
import org.elasticsearch.bootstrap.BootstrapContext;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.license.License;
import org.elasticsearch.license.License.OperationMode;
import org.elasticsearch.license.TestUtils;
import org.elasticsearch.test.AbstractBootstrapCheckTestCase;

public class TLSLicenseBootstrapCheckTests extends AbstractBootstrapCheckTestCase {
    public void testBootstrapCheckOnEmptyMetadata() {
        assertTrue(new TLSLicenseBootstrapCheck().check(emptyContext).isSuccess());
        assertTrue(new TLSLicenseBootstrapCheck().check(createTestContext(Settings.builder().put("xpack.security.transport.ssl.enabled"
            , randomBoolean()).build(), MetaData.EMPTY_META_DATA)).isSuccess());
    }

    public void testBootstrapCheckFailureOnPremiumLicense() throws Exception {
        final OperationMode mode = randomFrom(OperationMode.PLATINUM, OperationMode.GOLD, OperationMode.STANDARD);
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
        assertEquals("Transport SSL must be enabled if security is enabled on a [" + mode.description() + "] license. Please set " +
                "[xpack.security.transport.ssl.enabled] to [true] or disable security by setting " +
                "[xpack.security.enabled] to [false]",
            result.getMessage());
    }

    public void testBootstrapCheckSucceedsWithTlsEnabledOnPremiumLicense() throws Exception {
        final OperationMode mode = randomFrom(OperationMode.PLATINUM, OperationMode.GOLD, OperationMode.STANDARD);
        final Settings.Builder settings = Settings.builder().put("xpack.security.transport.ssl.enabled", true);
        final BootstrapCheck.BootstrapCheckResult result = runBootstrapCheck(mode, settings);
        assertSuccess(result);
    }

    public void testBootstrapCheckFailureOnBasicLicense() throws Exception {
        final Settings.Builder settings = Settings.builder().put("xpack.security.enabled", true);
        if (randomBoolean()) {
            // randomise between default-false & explicit-false
            settings.put("xpack.security.transport.ssl.enabled", false);
        }
        final BootstrapCheck.BootstrapCheckResult result = runBootstrapCheck(OperationMode.BASIC, settings);
        assertTrue("Expected bootstrap failure", result.isFailure());
        assertEquals("Transport SSL must be enabled if security is enabled on a [basic] license. Please set " +
                "[xpack.security.transport.ssl.enabled] to [true] or disable security by setting " +
                "[xpack.security.enabled] to [false]",
            result.getMessage());
    }

    public void testBootstrapSucceedsIfSecurityIsNotEnabledOnBasicLicense() throws Exception {
        final Settings.Builder settings = Settings.builder();
        if (randomBoolean()) {
            // randomise between default-false & explicit-false
            settings.put("xpack.security.enabled", false);
        }
        if (randomBoolean()) {
            // it does not matter whether or not this is set, as security is not enabled.
            settings.put("xpack.security.transport.ssl.enabled", randomBoolean());
        }
        final BootstrapCheck.BootstrapCheckResult result = runBootstrapCheck(OperationMode.BASIC, settings);
        assertSuccess(result);
    }

    public void testBootstrapSucceedsIfTlsIsEnabledOnBasicLicense() throws Exception {
        final Settings.Builder settings = Settings.builder().put("xpack.security.transport.ssl.enabled", true);
        if (randomBoolean()) {
            // it does not matter whether or not this is set, as TLS is enabled.
            settings.put("xpack.security.enabled", randomBoolean());
        }
        final BootstrapCheck.BootstrapCheckResult result = runBootstrapCheck(OperationMode.BASIC, settings);
        assertSuccess(result);
    }

    public void testBootstrapCheckAlwaysSucceedsOnTrialLicense() throws Exception {
        final Settings.Builder settings = Settings.builder();
        if (randomBoolean()) {
            // it does not matter whether this is set, or to which value.
            settings.put("xpack.security.enabled", randomBoolean());
        }
        if (randomBoolean()) {
            // it does not matter whether this is set, or to which value.
            settings.put("xpack.security.transport.ssl.enabled", randomBoolean());
        }
        final BootstrapCheck.BootstrapCheckResult result = runBootstrapCheck(OperationMode.TRIAL, settings);
        assertSuccess(result);
    }

    public BootstrapCheck.BootstrapCheckResult runBootstrapCheck(OperationMode mode, Settings.Builder settings) throws Exception {
        final License license = TestUtils.generateSignedLicense(mode.description(), TimeValue.timeValueHours(24));
        MetaData.Builder builder = MetaData.builder();
        TestUtils.putLicense(builder, license);
        MetaData metaData = builder.build();
        final BootstrapContext context = createTestContext(settings.build(), metaData);
        return new TLSLicenseBootstrapCheck().check(context);
    }

    public void assertSuccess(BootstrapCheck.BootstrapCheckResult result) {
        if (result.isFailure()) {
            fail("Bootstrap check failed unexpectedly: " + result.getMessage());
        }
    }

}
