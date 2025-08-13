/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.license;

import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xpack.core.LocalStateCompositeXPackPlugin;

import java.util.Arrays;
import java.util.Collection;
import java.util.Set;
import java.util.function.UnaryOperator;

import static org.elasticsearch.test.ESIntegTestCase.Scope.SUITE;
import static org.elasticsearch.test.NodeRoles.addRoles;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.containsString;

@ESIntegTestCase.ClusterScope(scope = SUITE)
public class StartTrialLicenseTests extends AbstractLicensesIntegrationTestCase {

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

    public void testStartTrial() throws Exception {
        ensureStartingWithBasic();

        assertTrue(getTrialStatus().isEligibleToStartTrial());

        License.LicenseType type = randomFrom(LicenseSettings.VALID_TRIAL_TYPES);

        // Test that starting will fail without acknowledgement
        final PostStartTrialResponse response2 = startTrial(pstr -> pstr.setType(type.getTypeName()));
        assertEquals(200, response2.getStatus().getRestStatus().getStatus());
        assertFalse(response2.getStatus().isTrialStarted());
        assertEquals("Operation failed: Needs acknowledgement.", response2.getStatus().getErrorMessage());

        assertBusy(() -> assertEquals("basic", getLicense().license().type()));

        final PostStartTrialResponse response3 = startTrial(pstr -> pstr.setType(type.getTypeName()).acknowledge(true));
        assertEquals(200, response3.getStatus().getRestStatus().getStatus());
        assertTrue(response3.getStatus().isTrialStarted());

        assertBusy(() -> assertEquals(type.getTypeName(), getLicense().license().type()));

        assertFalse(getTrialStatus().isEligibleToStartTrial());

        License.LicenseType secondAttemptType = randomFrom(LicenseSettings.VALID_TRIAL_TYPES);
        PostStartTrialResponse response5 = startTrial(pstr -> pstr.setType(secondAttemptType.getTypeName()).acknowledge(true));
        assertEquals(403, response5.getStatus().getRestStatus().getStatus());
        assertFalse(response5.getStatus().isTrialStarted());
        assertEquals("Operation failed: Trial was already activated.", response5.getStatus().getErrorMessage());
    }

    public void testInvalidType() throws Exception {
        ensureStartingWithBasic();
        final var future = startTrialFuture(pstr -> pstr.setType("basic").acknowledge(randomBoolean()));
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, future::actionGet);
        assertThat(e.getMessage(), containsString("Cannot start trial of type [basic]. Valid trial types are ["));
    }

    private void ensureStartingWithBasic() throws Exception {
        if ("basic".equals(getLicense().license().type()) == false) {
            assertAcked(startBasic());
        }

        assertBusy(() -> assertEquals("basic", getLicense().license().type()));
    }

    private ActionFuture<PostStartTrialResponse> startTrialFuture(UnaryOperator<PostStartTrialRequest> onRequest) {
        return client().execute(PostStartTrialAction.INSTANCE, onRequest.apply(new PostStartTrialRequest(TEST_REQUEST_TIMEOUT)));
    }

    private PostStartTrialResponse startTrial(UnaryOperator<PostStartTrialRequest> onRequest) {
        return safeGet(startTrialFuture(onRequest));
    }
}
