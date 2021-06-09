/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.license;

import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.protocol.xpack.XPackInfoResponse;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xpack.core.LocalStateCompositeXPackPlugin;

import java.util.Arrays;
import java.util.Collection;
import java.util.Set;

import static org.elasticsearch.test.ESIntegTestCase.Scope.SUITE;
import static org.elasticsearch.test.NodeRoles.addRoles;

@ESIntegTestCase.ClusterScope(scope = SUITE)
public class StartBasicLicenseTests extends AbstractLicensesIntegrationTestCase {

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(addRoles(super.nodeSettings(nodeOrdinal, otherSettings), Set.of(DiscoveryNodeRole.DATA_ROLE)))
            .put(LicenseService.SELF_GENERATED_LICENSE_TYPE.getKey(), "basic").build();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(LocalStateCompositeXPackPlugin.class);
    }

    public void testStartBasicLicense() throws Exception {
        LicensingClient licensingClient = new LicensingClient(client());
        License license = TestUtils.generateSignedLicense("trial",  License.VERSION_CURRENT, -1, TimeValue.timeValueHours(24));
        licensingClient.preparePutLicense(license).get();

        assertBusy(() -> {
            GetLicenseResponse getLicenseResponse = licensingClient.prepareGetLicense().get();
            assertEquals("trial", getLicenseResponse.license().type());
        });

        GetBasicStatusResponse response = licensingClient.prepareGetStartBasic().get();
        assertTrue(response.isEligibleToStartBasic());

        PostStartBasicResponse startResponse = licensingClient.preparePostStartBasic().setAcknowledge(true).get();
        assertTrue(startResponse.isAcknowledged());
        assertTrue(startResponse.getStatus().isBasicStarted());

        assertBusy(() -> {
            GetLicenseResponse currentLicense = licensingClient.prepareGetLicense().get();
            assertEquals("basic", currentLicense.license().type());
        });

        long expirationMillis = licensingClient.prepareGetLicense().get().license().expiryDate();
        assertEquals(LicenseService.BASIC_SELF_GENERATED_LICENSE_EXPIRATION_MILLIS, expirationMillis);

        GetLicenseResponse licenseResponse = licensingClient.prepareGetLicense().get();
        assertEquals("basic", licenseResponse.license().type());
        assertEquals(XPackInfoResponse.BASIC_SELF_GENERATED_LICENSE_EXPIRATION_MILLIS, licenseResponse.license().expiryDate());

        GetBasicStatusResponse response4 = licensingClient.prepareGetStartBasic().get();
        assertFalse(response4.isEligibleToStartBasic());


        PostStartBasicResponse response5 = licensingClient.preparePostStartBasic().setAcknowledge(true).get();
        assertEquals(403, response5.status().getStatus());
        assertFalse(response5.getStatus().isBasicStarted());
        assertTrue(response5.isAcknowledged());
        assertEquals("Operation failed: Current license is basic.", response5.getStatus().getErrorMessage());
    }

    public void testUnacknowledgedStartBasicLicense() throws Exception {
        LicensingClient licensingClient = new LicensingClient(client());
        License license = TestUtils.generateSignedLicense("trial",  License.VERSION_CURRENT, -1, TimeValue.timeValueHours(24));
        licensingClient.preparePutLicense(license).get();

        assertBusy(() -> {
            GetLicenseResponse getLicenseResponse = licensingClient.prepareGetLicense().get();
            assertEquals("trial", getLicenseResponse.license().type());
        });

        PostStartBasicResponse response = licensingClient.preparePostStartBasic().get();
        assertEquals(200, response.status().getStatus());
        assertFalse(response.isAcknowledged());
        assertFalse(response.getStatus().isBasicStarted());
        assertEquals("Operation failed: Needs acknowledgement.", response.getStatus().getErrorMessage());
        assertEquals("This license update requires acknowledgement. To acknowledge the license, " +
                "please read the following messages and call /start_basic again, this time with the \"acknowledge=true\" parameter:",
            response.getAcknowledgeMessage());
    }
}
