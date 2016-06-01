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
import org.elasticsearch.common.transport.DummyTransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.license.core.License;
import org.elasticsearch.license.plugin.TestUtils;
import org.elasticsearch.transport.EmptyTransportResponseHandler;
import org.elasticsearch.transport.TransportRequest;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class LicenseClusterChangeTests extends AbstractLicenseServiceTestCase {

    public void testNotificationOnNewLicense() throws Exception {
        setInitialState(null);
        licensesService.start();
        final TestUtils.AssertingLicensee licensee = new TestUtils.AssertingLicensee("testNotificationOnNewLicense", logger);
        licensesService.register(licensee);
        ClusterState oldState = ClusterState.builder(new ClusterName("a")).build();
        final License license = TestUtils.generateSignedLicense(TimeValue.timeValueHours(24));
        MetaData metaData = MetaData.builder().putCustom(LicensesMetaData.TYPE, new LicensesMetaData(license)).build();
        ClusterState newState = ClusterState.builder(new ClusterName("a")).metaData(metaData).build();
        licensesService.clusterChanged(new ClusterChangedEvent("simulated", newState, oldState));
        assertThat(licensee.statuses.size(), equalTo(1));
        assertTrue(licensee.statuses.get(0).getLicenseState() == LicenseState.ENABLED);
        licensesService.stop();
    }

    public void testNoNotificationOnExistingLicense() throws Exception {
        setInitialState(null);
        licensesService.start();
        final TestUtils.AssertingLicensee licensee = new TestUtils.AssertingLicensee("testNoNotificationOnExistingLicense", logger);
        licensesService.register(licensee);
        final License license = TestUtils.generateSignedLicense(TimeValue.timeValueHours(24));
        MetaData metaData = MetaData.builder().putCustom(LicensesMetaData.TYPE, new LicensesMetaData(license)).build();
        ClusterState newState = ClusterState.builder(new ClusterName("a")).metaData(metaData).build();
        ClusterState oldState = ClusterState.builder(newState).build();
        licensesService.clusterChanged(new ClusterChangedEvent("simulated", newState, oldState));
        assertThat(licensee.statuses.size(), equalTo(0));
        licensesService.stop();
    }

    public void testTrialLicenseGeneration() throws Exception {
        setInitialState(null);
        licensesService.start();
        final TestUtils.AssertingLicensee licensee = new TestUtils.AssertingLicensee("testNotificationOnNewLicense", logger);
        licensesService.register(licensee);
        DiscoveryNode master = new DiscoveryNode("b", DummyTransportAddress.INSTANCE, emptyMap(), emptySet(), Version.CURRENT);
        ClusterState oldState = ClusterState.builder(new ClusterName("a"))
                .nodes(DiscoveryNodes.builder().masterNodeId(master.getId()).put(master)).build();
        ClusterState newState = ClusterState.builder(oldState).build();
        licensesService.clusterChanged(new ClusterChangedEvent("simulated", newState, oldState));
        verify(transportService, times(2))
                .sendRequest(any(DiscoveryNode.class),
                        eq(LicensesService.REGISTER_TRIAL_LICENSE_ACTION_NAME),
                        any(TransportRequest.Empty.class), any(EmptyTransportResponseHandler.class));
        licensesService.stop();
    }
}