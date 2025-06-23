/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.monitoring;

import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.license.MockLicenseState;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.monitoring.collector.Collector;

import java.util.function.Function;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public abstract class BaseCollectorTestCase extends ESTestCase {

    protected ClusterName clusterName;
    protected ClusterService clusterService;
    protected ClusterState clusterState;
    protected DiscoveryNodes nodes;
    protected Metadata metadata;
    protected ProjectMetadata projectMetadata;
    protected MockLicenseState licenseState;
    protected Client client;
    protected Settings settings;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        clusterName = mock(ClusterName.class);
        clusterService = mock(ClusterService.class);
        clusterState = mock(ClusterState.class);
        nodes = mock(DiscoveryNodes.class);
        metadata = mock(Metadata.class);
        projectMetadata = mock(ProjectMetadata.class);
        when(metadata.getProject()).thenReturn(projectMetadata);
        licenseState = mock(MockLicenseState.class);
        client = mock(Client.class);
        ThreadPool threadPool = mock(ThreadPool.class);
        when(client.threadPool()).thenReturn(threadPool);
        when(threadPool.getThreadContext()).thenReturn(new ThreadContext(Settings.EMPTY));
        settings = Settings.builder().put("path.home", createTempDir()).build();
    }

    protected void whenLocalNodeElectedMaster(final boolean electedMaster) {
        when(clusterService.state()).thenReturn(clusterState);
        when(clusterState.getNodes()).thenReturn(nodes);
        when(nodes.isLocalNodeElectedMaster()).thenReturn(electedMaster);
    }

    protected void whenClusterStateWithName(final String name) {
        when(clusterName.value()).thenReturn(name);
        when(clusterService.getClusterName()).thenReturn(clusterName);
        when(clusterState.getClusterName()).thenReturn(clusterName);
    }

    protected void whenClusterStateWithUUID(final String clusterUUID) {
        when(clusterService.state()).thenReturn(clusterState);
        when(clusterState.metadata()).thenReturn(metadata);
        when(metadata.clusterUUID()).thenReturn(clusterUUID);
    }

    protected void withCollectionTimeout(final Setting<TimeValue> collectionTimeout, final TimeValue timeout) throws Exception {
        withCollectionSetting(builder -> builder.put(collectionTimeout.getKey(), timeout.getStringRep()));
    }

    protected void withCollectionIndices(final String[] collectionIndices) throws Exception {
        final String key = Collector.INDICES.getKey();
        if (collectionIndices != null) {
            withCollectionSetting(builder -> builder.putList(key, collectionIndices));
        } else {
            withCollectionSetting(builder -> builder.putNull(key));
        }
    }

    protected void withCollectionSetting(final Function<Settings.Builder, Settings.Builder> builder) throws Exception {
        settings = Settings.builder().put(settings).put(builder.apply(Settings.builder()).build()).build();
        when(clusterService.getClusterSettings()).thenReturn(new ClusterSettings(settings, Sets.newHashSet(new Monitoring(settings) {
            @Override
            protected XPackLicenseState getLicenseState() {
                return licenseState;
            }
        }.getSettings())));
    }

    protected static DiscoveryNode localNode(final String uuid) {
        return DiscoveryNodeUtils.create(uuid, new TransportAddress(TransportAddress.META_ADDRESS, 9300));
    }
}
