/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ssl;

import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.license.License;
import org.elasticsearch.license.TestUtils;
import org.elasticsearch.test.AbstractBootstrapCheckTestCase;

import java.util.EnumSet;

public class TLSLicenseBootstrapCheckTests extends AbstractBootstrapCheckTestCase {
    public void testBootstrapCheck() throws Exception {
        assertTrue(new TLSLicenseBootstrapCheck().check(emptyContext).isSuccess());
        assertTrue(new TLSLicenseBootstrapCheck().check(createTestContext(Settings.builder().put("xpack.security.transport.ssl.enabled"
                    , randomBoolean()).build(), MetaData.EMPTY_META_DATA)).isSuccess());
        int numIters = randomIntBetween(1,10);
        for (int i = 0; i < numIters; i++) {
            License license = TestUtils.generateSignedLicense(TimeValue.timeValueHours(24));
            EnumSet<License.OperationMode> productionModes = EnumSet.of(License.OperationMode.GOLD, License.OperationMode.PLATINUM,
                    License.OperationMode.STANDARD);
            MetaData.Builder builder = MetaData.builder();
            TestUtils.putLicense(builder, license);
            MetaData build = builder.build();
            if (productionModes.contains(license.operationMode()) == false) {
                assertTrue(new TLSLicenseBootstrapCheck().check(createTestContext(
                        Settings.builder().put("xpack.security.transport.ssl.enabled", true).build(), build)).isSuccess());
            } else {
                assertTrue(new TLSLicenseBootstrapCheck().check(createTestContext(
                        Settings.builder().put("xpack.security.transport.ssl.enabled", false).build(), build)).isFailure());
                assertEquals("Transport SSL must be enabled for setups with production licenses. Please set " +
                                "[xpack.security.transport.ssl.enabled] to [true] or disable security by setting " +
                                "[xpack.security.enabled] to [false]",
                        new TLSLicenseBootstrapCheck().check(createTestContext(
                                Settings.builder().put("xpack.security.transport.ssl.enabled", false).build(), build)).getMessage());
            }
        }
    }
}
