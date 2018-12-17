/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.security;

import org.elasticsearch.bootstrap.FIPSContext;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.license.License;
import org.elasticsearch.license.TestUtils;
import org.elasticsearch.test.ESTestCase;

public class LicenseFIPSCheckTests extends ESTestCase {
    public void testBootstrapCheck() throws Exception {
        assertTrue(new FIPSChecks().licenseCheck(new FIPSContext(Settings.EMPTY)).isSuccess());
        assertTrue(new FIPSChecks().licenseCheck(new FIPSContext(Settings.builder().put("xpack.security.fips_mode.enabled",
            randomBoolean()).build())).isSuccess());

        License license = TestUtils.generateSignedLicense(TimeValue.timeValueHours(24));

        if (FIPSChecks.ALLOWED_LICENSE_OPERATION_MODES.contains(license.operationMode())) {
            assertTrue(new FIPSChecks(license.operationMode()).licenseCheck(new FIPSContext(
                Settings.builder().put("xpack.security.fips_mode.enabled", true).build())).isSuccess());
            assertTrue(new FIPSChecks(license.operationMode()).licenseCheck(new FIPSContext(
                Settings.builder().put("xpack.security.fips_mode.enabled", false).build())).isSuccess());
        } else {
            assertTrue(new FIPSChecks(license.operationMode()).licenseCheck(new FIPSContext(
                Settings.builder().put("xpack.security.fips_mode.enabled", false).build())).isSuccess());
            assertTrue(new FIPSChecks(license.operationMode()).licenseCheck(new FIPSContext(
                Settings.builder().put("xpack.security.fips_mode.enabled", true).build())).isFailure());
            assertEquals("FIPS mode is only allowed with a Platinum or Trial license",
                new FIPSChecks(license.operationMode()).licenseCheck(new FIPSContext(
                    Settings.builder().put("xpack.security.fips_mode.enabled", true).build())).getMessage());
        }
    }
}
