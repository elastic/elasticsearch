/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.license;

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
import org.elasticsearch.env.Environment;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.watcher.ResourceWatcherService;
import org.elasticsearch.xpack.support.clock.ClockMock;
import org.junit.After;
import org.junit.Before;

import java.nio.file.Path;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public abstract class AbstractLicenseServiceTestCase extends ESTestCase {

    protected LicenseService licenseService;
    protected ClusterService clusterService;
    protected ResourceWatcherService resourceWatcherService;
    protected ClockMock clock;
    protected DiscoveryNodes discoveryNodes;
    protected Environment environment;
    protected String licenseType;

    @Before
    public void init() throws Exception {
        clusterService = mock(ClusterService.class);
        clock = ClockMock.frozen();
        discoveryNodes = mock(DiscoveryNodes.class);
        resourceWatcherService = mock(ResourceWatcherService.class);
        environment = mock(Environment.class);
    }

    protected void setInitialState(License license, XPackLicenseState licenseState) {
        Path tempDir = createTempDir();
        when(environment.configFile()).thenReturn(tempDir);
        licenseType = randomBoolean() ? "trial" : "basic";
        Settings settings = Settings.builder().put(LicenseService.SELF_GENERATED_LICENSE_TYPE.getKey(), licenseType).build();
        licenseService = new LicenseService(settings, clusterService, clock, environment, resourceWatcherService, licenseState);
        ClusterState state = mock(ClusterState.class);
        final ClusterBlocks noBlock = ClusterBlocks.builder().build();
        when(state.blocks()).thenReturn(noBlock);
        MetaData metaData = mock(MetaData.class);
        when(metaData.custom(LicensesMetaData.TYPE)).thenReturn(new LicensesMetaData(license));
        when(state.metaData()).thenReturn(metaData);
        final DiscoveryNode mockNode = new DiscoveryNode("b", buildNewFakeTransportAddress(), emptyMap(), emptySet(), Version.CURRENT);
        when(discoveryNodes.getMasterNode()).thenReturn(mockNode);
        when(discoveryNodes.isLocalNodeElectedMaster()).thenReturn(false);
        when(state.nodes()).thenReturn(discoveryNodes);
        when(state.getNodes()).thenReturn(discoveryNodes); // it is really ridiculous we have nodes() and getNodes()...
        when(clusterService.state()).thenReturn(state);
        when(clusterService.lifecycleState()).thenReturn(Lifecycle.State.STARTED);
        when(clusterService.getClusterName()).thenReturn(new ClusterName("a"));
    }

    @After
    public void tearDown() throws Exception {
        super.tearDown();
        licenseService.stop();
    }
}
