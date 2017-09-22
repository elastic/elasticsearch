/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.monitoring.collector;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.monitoring.MonitoringSettings;
import org.elasticsearch.xpack.security.InternalClient;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public abstract class BaseCollectorTestCase extends ESTestCase {

    protected ClusterName clusterName;
    protected ClusterService clusterService;
    protected ClusterState clusterState;
    protected DiscoveryNodes nodes;
    protected MetaData metaData;
    protected MonitoringSettings monitoringSettings;
    protected XPackLicenseState licenseState;
    protected InternalClient client;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        clusterName = mock(ClusterName.class);
        clusterService = mock(ClusterService.class);
        clusterState = mock(ClusterState.class);
        nodes = mock(DiscoveryNodes.class);
        metaData = mock(MetaData.class);
        monitoringSettings = mock(MonitoringSettings.class);
        licenseState = mock(XPackLicenseState.class);
        client = mock(InternalClient.class);
    }

    protected void whenLocalNodeElectedMaster(final boolean electedMaster) {
        when(clusterService.state()).thenReturn(clusterState);
        when(clusterState.nodes()).thenReturn(nodes);
        when(nodes.isLocalNodeElectedMaster()).thenReturn(electedMaster);
    }

    protected void whenClusterStateWithName(final String name) {
        when(clusterName.value()).thenReturn(name);
        when(clusterService.getClusterName()).thenReturn(clusterName);
        when(clusterState.getClusterName()).thenReturn(clusterName);
    }

    protected void whenClusterStateWithUUID(final String clusterUUID) {
        when(clusterService.state()).thenReturn(clusterState);
        when(clusterState.metaData()).thenReturn(metaData);
        when(metaData.clusterUUID()).thenReturn(clusterUUID);
    }

    protected static DiscoveryNode localNode(final String uuid) {
        return new DiscoveryNode(uuid, new TransportAddress(TransportAddress.META_ADDRESS, 9300), Version.CURRENT);
    }
}
