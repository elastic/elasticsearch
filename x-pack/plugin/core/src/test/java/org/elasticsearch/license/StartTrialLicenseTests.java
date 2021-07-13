/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.license;

import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xpack.core.LocalStateCompositeXPackPlugin;

import java.util.Arrays;
import java.util.Collection;
import java.util.Set;

import static org.elasticsearch.test.ESIntegTestCase.Scope.SUITE;
import static org.elasticsearch.test.NodeRoles.addRoles;
import static org.hamcrest.Matchers.containsString;

@ESIntegTestCase.ClusterScope(scope = SUITE)
public class StartTrialLicenseTests extends AbstractLicensesIntegrationTestCase {

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(addRoles(super.nodeSettings(nodeOrdinal, otherSettings), Set.of(DiscoveryNodeRole.DATA_ROLE)))
            .put(LicenseService.SELF_GENERATED_LICENSE_TYPE.getKey(), "basic")
            .build();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(LocalStateCompositeXPackPlugin.class);
    }

    public void testStartTrial() throws Exception {
        LicensingClient licensingClient = new LicensingClient(client());
        ensureStartingWithBasic();

        GetTrialStatusResponse response = licensingClient.prepareGetStartTrial().get();
        assertTrue(response.isEligibleToStartTrial());

        // Test that starting will fail without acknowledgement
        PostStartTrialRequestBuilder builder = licensingClient.preparePostStartTrial();
        builder.request().setType(randomFrom(LicenseService.VALID_TRIAL_TYPES).getTypeName());
        PostStartTrialResponse response2 = builder.get();
        assertEquals(200, response2.getStatus().getRestStatus().getStatus());
        assertFalse(response2.getStatus().isTrialStarted());
        assertEquals("Operation failed: Needs acknowledgement.", response2.getStatus().getErrorMessage());

        assertBusy(() -> {
            GetLicenseResponse getLicenseResponse = licensingClient.prepareGetLicense().get();
            assertEquals("basic", getLicenseResponse.license().type());
        });

        License.LicenseType type = randomFrom(LicenseService.VALID_TRIAL_TYPES);

        PostStartTrialRequestBuilder builder2 = licensingClient.preparePostStartTrial();
        builder2.setAcknowledge(true);
        builder2.request().setType(type.getTypeName());
        PostStartTrialResponse response3 = builder2.get();
        assertEquals(200, response3.getStatus().getRestStatus().getStatus());
        assertTrue(response3.getStatus().isTrialStarted());

        assertBusy(() -> {
            GetLicenseResponse postTrialLicenseResponse = licensingClient.prepareGetLicense().get();
            assertEquals(type.getTypeName(), postTrialLicenseResponse.license().type());
        });

        GetTrialStatusResponse response4 = licensingClient.prepareGetStartTrial().get();
        assertFalse(response4.isEligibleToStartTrial());

        License.LicenseType secondAttemptType = randomFrom(LicenseService.VALID_TRIAL_TYPES);

        PostStartTrialRequestBuilder builder3 = licensingClient.preparePostStartTrial();
        builder3.setAcknowledge(true);
        builder3.request().setType(secondAttemptType.getTypeName());
        PostStartTrialResponse response5 = builder3.get();
        assertEquals(403, response5.getStatus().getRestStatus().getStatus());
        assertFalse(response5.getStatus().isTrialStarted());
        assertEquals("Operation failed: Trial was already activated.", response5.getStatus().getErrorMessage());
    }

    public void testInvalidType() throws Exception {
        LicensingClient licensingClient = new LicensingClient(client());
        ensureStartingWithBasic();

        PostStartTrialRequestBuilder builder = licensingClient.preparePostStartTrial();
        builder.request().setType("basic");
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, builder::get);
        assertThat(e.getMessage(), containsString("Cannot start trial of type [basic]. Valid trial types are ["));
    }

    private void ensureStartingWithBasic() throws Exception {
        LicensingClient licensingClient = new LicensingClient(client());
        GetLicenseResponse getLicenseResponse = licensingClient.prepareGetLicense().get();

        if ("basic".equals(getLicenseResponse.license().type()) == false) {
            licensingClient.preparePostStartBasic().setAcknowledge(true).get();
        }

        assertBusy(() -> {
            GetLicenseResponse postTrialLicenseResponse = licensingClient.prepareGetLicense().get();
            assertEquals("basic", postTrialLicenseResponse.license().type());
        });
    }
}
