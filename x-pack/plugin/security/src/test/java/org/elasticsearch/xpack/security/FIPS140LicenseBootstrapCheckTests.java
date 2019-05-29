/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.security;

import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.license.License;
import org.elasticsearch.license.TestUtils;
import org.elasticsearch.test.AbstractBootstrapCheckTestCase;

public class FIPS140LicenseBootstrapCheckTests extends AbstractBootstrapCheckTestCase {

    public void testBootstrapCheck() throws Exception {
        assertTrue(new FIPS140LicenseBootstrapCheck()
            .check(emptyContext).isSuccess());
        assertTrue(new FIPS140LicenseBootstrapCheck()
            .check(createTestContext(Settings.builder().put("xpack.security.fips_mode.enabled", randomBoolean()).build(), MetaData
                .EMPTY_META_DATA)).isSuccess());

        MetaData.Builder builder = MetaData.builder();
        License license = TestUtils.generateSignedLicense(TimeValue.timeValueHours(24));
        TestUtils.putLicense(builder, license);
        MetaData metaData = builder.build();

        if (FIPS140LicenseBootstrapCheck.ALLOWED_LICENSE_OPERATION_MODES.contains(license.operationMode())) {
            assertTrue(new FIPS140LicenseBootstrapCheck().check(createTestContext(
                Settings.builder().put("xpack.security.fips_mode.enabled", true).build(), metaData)).isSuccess());
            assertTrue(new FIPS140LicenseBootstrapCheck().check(createTestContext(
                Settings.builder().put("xpack.security.fips_mode.enabled", false).build(), metaData)).isSuccess());
        } else {
            assertTrue(new FIPS140LicenseBootstrapCheck().check(createTestContext(
                Settings.builder().put("xpack.security.fips_mode.enabled", false).build(), metaData)).isSuccess());
            assertTrue(new FIPS140LicenseBootstrapCheck().check(createTestContext(
                Settings.builder().put("xpack.security.fips_mode.enabled", true).build(), metaData)).isFailure());
            assertEquals("FIPS mode is only allowed with a Platinum or Trial license",
                new FIPS140LicenseBootstrapCheck().check(createTestContext(
                    Settings.builder().put("xpack.security.fips_mode.enabled", true).build(), metaData)).getMessage());
        }
    }
}
