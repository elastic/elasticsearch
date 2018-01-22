/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.license;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.SecurityIntegTestCase;

import static org.hamcrest.CoreMatchers.equalTo;

@ESIntegTestCase.ClusterScope(
        scope = ESIntegTestCase.Scope.TEST,
        numDataNodes = 1,
        numClientNodes = 0,
        supportsDedicatedMasters = false,
        autoMinMasterNodes = false)
public class LicenseServiceSingleNodeSecurityTests extends SecurityIntegTestCase {

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings
                .builder()
                .put(super.nodeSettings(nodeOrdinal))
                .put("discovery.type", "single-node")
                .put("transport.tcp.port", "0")
                .build();
    }

    public void testLicenseUpgradeSucceeds() throws Exception {
        LicensingClient licensingClient = new LicensingClient(client());
        License prodLicense = TestUtils.generateSignedLicense("platinum", TimeValue.timeValueHours(24));
        PutLicenseResponse putLicenseResponse = licensingClient.preparePutLicense(prodLicense).get();
        assertEquals(putLicenseResponse.status(), LicensesStatus.VALID);
        assertThat(licensingClient.prepareGetLicense().get().license(), equalTo(prodLicense));
    }
}
