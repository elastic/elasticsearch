/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.license.plugin.core;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.transport.LocalTransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.license.core.License;
import org.elasticsearch.license.plugin.TestUtils;
import org.elasticsearch.transport.EmptyTransportResponseHandler;
import org.elasticsearch.transport.TransportRequest;
import org.junit.After;
import org.junit.Before;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class LicenseClusterChangeTests extends AbstractLicenseServiceTestCase {

    private TestUtils.AssertingLicensee licensee;

    @Before
    public void setup() {
        setInitialState(null);
        licensesService.start();
        licensee = new TestUtils.AssertingLicensee("LicenseClusterChangeTests", logger);
        licensesService.register(licensee);
    }

    @After
    public void teardown() {
        licensesService.stop();
    }


    public void testNotificationOnNewLicense() throws Exception {
        ClusterState oldState = ClusterState.builder(new ClusterName("a")).build();
        final License license = TestUtils.generateSignedLicense(TimeValue.timeValueHours(24));
        MetaData metaData = MetaData.builder().putCustom(LicensesMetaData.TYPE, new LicensesMetaData(license)).build();
        ClusterState newState = ClusterState.builder(new ClusterName("a")).metaData(metaData).build();
        licensesService.clusterChanged(new ClusterChangedEvent("simulated", newState, oldState));
        assertThat(licensee.statuses.size(), equalTo(1));
        assertTrue(licensee.statuses.get(0).getLicenseState() == LicenseState.ENABLED);
    }

    public void testNoNotificationOnExistingLicense() throws Exception {
        final License license = TestUtils.generateSignedLicense(TimeValue.timeValueHours(24));
        MetaData metaData = MetaData.builder().putCustom(LicensesMetaData.TYPE, new LicensesMetaData(license)).build();
        ClusterState newState = ClusterState.builder(new ClusterName("a")).metaData(metaData).build();
        ClusterState oldState = ClusterState.builder(newState).build();
        licensesService.clusterChanged(new ClusterChangedEvent("simulated", newState, oldState));
        assertThat(licensee.statuses.size(), equalTo(0));
    }

    public void testTrialLicenseGeneration() throws Exception {
        DiscoveryNode master = new DiscoveryNode("b", LocalTransportAddress.buildUnique(), emptyMap(), emptySet(), Version.CURRENT);
        ClusterState oldState = ClusterState.builder(new ClusterName("a"))
                .nodes(DiscoveryNodes.builder().masterNodeId(master.getId()).put(master)).build();
        ClusterState newState = ClusterState.builder(oldState).build();
        licensesService.clusterChanged(new ClusterChangedEvent("simulated", newState, oldState));
        verify(transportService, times(2))
                .sendRequest(any(DiscoveryNode.class),
                        eq(LicensesService.REGISTER_TRIAL_LICENSE_ACTION_NAME),
                        any(TransportRequest.Empty.class), any(EmptyTransportResponseHandler.class));
    }
}