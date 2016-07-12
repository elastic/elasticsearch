/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.license.plugin.core;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.component.Lifecycle;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.LocalTransportAddress;
import org.elasticsearch.license.core.License;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.support.clock.ClockMock;
import org.junit.Before;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public abstract class AbstractLicenseServiceTestCase extends ESTestCase {

    protected LicensesService licensesService;
    protected ClusterService clusterService;
    protected ClockMock clock;
    protected DiscoveryNodes discoveryNodes;

    @Before
    public void init() throws Exception {
        clusterService = mock(ClusterService.class);
        clock = new ClockMock();
        licensesService = new LicensesService(Settings.EMPTY, clusterService, clock);
        discoveryNodes = mock(DiscoveryNodes.class);
    }

    protected void setInitialState(License license) {
        ClusterState state = mock(ClusterState.class);
        final ClusterBlocks noBlock = ClusterBlocks.builder().build();
        when(state.blocks()).thenReturn(noBlock);
        MetaData metaData = mock(MetaData.class);
        when(metaData.custom(LicensesMetaData.TYPE)).thenReturn(new LicensesMetaData(license));
        when(state.metaData()).thenReturn(metaData);
        final DiscoveryNode mockNode = new DiscoveryNode("b", LocalTransportAddress.buildUnique(), emptyMap(), emptySet(), Version.CURRENT);
        when(discoveryNodes.getMasterNode()).thenReturn(mockNode);
        when(discoveryNodes.isLocalNodeElectedMaster()).thenReturn(false);
        when(state.nodes()).thenReturn(discoveryNodes);
        when(state.getNodes()).thenReturn(discoveryNodes); // it is really ridiculous we have nodes() and getNodes()...
        when(clusterService.state()).thenReturn(state);
        when(clusterService.lifecycleState()).thenReturn(Lifecycle.State.STARTED);
        when(clusterService.getClusterName()).thenReturn(new ClusterName("a"));
    }
}