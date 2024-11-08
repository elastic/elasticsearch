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
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;

@ESIntegTestCase.ClusterScope(scope = SUITE)
public class StartBasicLicenseTests extends AbstractLicensesIntegrationTestCase {

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(addRoles(super.nodeSettings(nodeOrdinal, otherSettings), Set.of(DiscoveryNodeRole.DATA_ROLE)))
            .put(LicenseSettings.SELF_GENERATED_LICENSE_TYPE.getKey(), "basic")
            .build();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(LocalStateCompositeXPackPlugin.class);
    }

    public void testStartBasicLicense() throws Exception {
        generateAndPutTestLicense();

        assertBusy(() -> assertEquals("trial", getLicense().license().type()));

        assertTrue(getBasicStatus().isEligibleToStartBasic());

        PostStartBasicResponse startResponse = startBasic(true);
        assertTrue(startResponse.isAcknowledged());
        assertTrue(startResponse.getStatus().isBasicStarted());

        assertBusy(() -> assertEquals("basic", getLicense().license().type()));

        long expirationMillis = getLicense().license().expiryDate();
        assertEquals(LicenseSettings.BASIC_SELF_GENERATED_LICENSE_EXPIRATION_MILLIS, expirationMillis);

        GetLicenseResponse licenseResponse = getLicense();
        assertEquals("basic", licenseResponse.license().type());
        assertEquals(XPackInfoResponse.BASIC_SELF_GENERATED_LICENSE_EXPIRATION_MILLIS, licenseResponse.license().expiryDate());

        assertFalse(getBasicStatus().isEligibleToStartBasic());

        PostStartBasicResponse response5 = startBasic(true);
        assertEquals(403, response5.status().getStatus());
        assertFalse(response5.getStatus().isBasicStarted());
        assertTrue(response5.isAcknowledged());
        assertEquals("Operation failed: Current license is basic.", response5.getStatus().getErrorMessage());
    }

    public void testUnacknowledgedStartBasicLicense() throws Exception {
        generateAndPutTestLicense();

        assertBusy(() -> assertEquals("trial", getLicense().license().type()));

        PostStartBasicResponse response = startBasic(false);
        assertEquals(200, response.status().getStatus());
        assertFalse(response.isAcknowledged());
        assertFalse(response.getStatus().isBasicStarted());
        assertEquals("Operation failed: Needs acknowledgement.", response.getStatus().getErrorMessage());
        assertEquals(
            "This license update requires acknowledgement. To acknowledge the license, "
                + "please read the following messages and call /start_basic again, this time with the \"acknowledge=true\" parameter:",
            response.getAcknowledgeMessage()
        );
    }

    private static GetBasicStatusResponse getBasicStatus() {
        return safeGet(clusterAdmin().execute(GetBasicStatusAction.INSTANCE, new GetBasicStatusRequest(TEST_REQUEST_TIMEOUT)));
    }

    private static PostStartBasicResponse startBasic(boolean acknowledged) {
        return safeGet(
            clusterAdmin().execute(
                PostStartBasicAction.INSTANCE,
                new PostStartBasicRequest(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT).acknowledge(acknowledged)
            )
        );
    }

    private static void generateAndPutTestLicense() throws Exception {
        final var license = TestUtils.generateSignedLicense("trial", License.VERSION_CURRENT, -1, TimeValue.timeValueHours(24));
        assertAcked(
            safeGet(
                client().execute(
                    PutLicenseAction.INSTANCE,
                    new PutLicenseRequest(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT).license(license).acknowledge(randomBoolean())
                )
            )
        );
    }
}
