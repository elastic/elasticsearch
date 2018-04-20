/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.license;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.junit.After;
import org.junit.Before;
import org.mockito.ArgumentCaptor;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class LicenseClusterChangeTests extends AbstractLicenseServiceTestCase {

    private TestUtils.AssertingLicenseState licenseState;

    @Before
    public void setup() {
        licenseState = new TestUtils.AssertingLicenseState();
        setInitialState(null, licenseState, Settings.EMPTY);
        licenseService.start();
    }

    @After
    public void teardown() {
        licenseService.stop();
    }


    public void testNotificationOnNewLicense() throws Exception {
        ClusterState oldState = ClusterState.builder(new ClusterName("a")).build();
        final License license = TestUtils.generateSignedLicense(TimeValue.timeValueHours(24));
        MetaData metaData = MetaData.builder().putCustom(LicensesMetaData.TYPE, new LicensesMetaData(license, null)).build();
        ClusterState newState = ClusterState.builder(new ClusterName("a")).metaData(metaData).build();
        licenseService.clusterChanged(new ClusterChangedEvent("simulated", newState, oldState));
        assertThat(licenseState.activeUpdates.size(), equalTo(1));
        assertTrue(licenseState.activeUpdates.get(0));
    }

    public void testNoNotificationOnExistingLicense() throws Exception {
        final License license = TestUtils.generateSignedLicense(TimeValue.timeValueHours(24));
        MetaData metaData = MetaData.builder().putCustom(LicensesMetaData.TYPE, new LicensesMetaData(license, null)).build();
        ClusterState newState = ClusterState.builder(new ClusterName("a")).metaData(metaData).build();
        ClusterState oldState = ClusterState.builder(newState).build();
        licenseService.clusterChanged(new ClusterChangedEvent("simulated", newState, oldState));
        assertThat(licenseState.activeUpdates.size(), equalTo(0));
    }

    public void testSelfGeneratedLicenseGeneration() throws Exception {
        DiscoveryNode master = new DiscoveryNode("b", buildNewFakeTransportAddress(), emptyMap(), emptySet(), Version.CURRENT);
        ClusterState oldState = ClusterState.builder(new ClusterName("a"))
                .nodes(DiscoveryNodes.builder().masterNodeId(master.getId()).add(master)).build();
        when(discoveryNodes.isLocalNodeElectedMaster()).thenReturn(true);
        ClusterState newState = ClusterState.builder(oldState).nodes(discoveryNodes).build();

        licenseService.clusterChanged(new ClusterChangedEvent("simulated", newState, oldState));
        ArgumentCaptor<ClusterStateUpdateTask> stateUpdater = ArgumentCaptor.forClass(ClusterStateUpdateTask.class);
        verify(clusterService, times(1)).submitStateUpdateTask(any(), stateUpdater.capture());
        ClusterState stateWithLicense = stateUpdater.getValue().execute(newState);
        LicensesMetaData licenseMetaData = stateWithLicense.metaData().custom(LicensesMetaData.TYPE);
        assertNotNull(licenseMetaData);
        assertNotNull(licenseMetaData.getLicense());
        assertEquals(licenseType, licenseMetaData.getLicense().type());
        long expiration;
        if (licenseType.equals("basic")) {
            expiration = LicenseService.BASIC_SELF_GENERATED_LICENSE_EXPIRATION_MILLIS;
        } else {
            expiration = LicenseService.NON_BASIC_SELF_GENERATED_LICENSE_DURATION.millis() + clock.millis();
        }
        assertEquals(expiration, licenseMetaData.getLicense().expiryDate());
    }
}