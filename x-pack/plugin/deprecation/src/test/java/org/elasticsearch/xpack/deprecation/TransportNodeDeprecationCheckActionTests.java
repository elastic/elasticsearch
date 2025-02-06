/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.deprecation;

import org.elasticsearch.action.admin.cluster.node.info.PluginsAndModules;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.cluster.ClusterInfo;
import org.elasticsearch.cluster.ClusterInfoService;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.DiskUsage;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.deprecation.DeprecationIssue;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.mockito.Mockito;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.mockito.Mockito.when;

public class TransportNodeDeprecationCheckActionTests extends ESTestCase {

    private static ThreadPool threadPool;

    @Before
    public void setup() {
        threadPool = new TestThreadPool("TransportNodeDeprecationCheckActionTests");
    }

    @After
    public void cleanup() {
        ThreadPool.terminate(threadPool, 30, TimeUnit.SECONDS);
        threadPool = null;
    }

    public void testNodeOperation() {
        Settings.Builder settingsBuilder = Settings.builder();
        settingsBuilder.put("some.deprecated.property", "someValue1");
        settingsBuilder.put("some.other.bad.deprecated.property", "someValue2");
        settingsBuilder.put("some.undeprecated.property", "someValue3");
        settingsBuilder.putList("some.undeprecated.list.property", List.of("someValue4", "someValue5"));
        settingsBuilder.putList(
            TransportDeprecationInfoAction.SKIP_DEPRECATIONS_SETTING.getKey(),
            List.of("some.deprecated.property", "some.other.*.deprecated.property", "some.bad.dynamic.property")
        );
        Settings nodeSettings = settingsBuilder.build();
        settingsBuilder = Settings.builder();
        settingsBuilder.put("some.bad.dynamic.property", "someValue1");
        Settings dynamicSettings = settingsBuilder.build();
        final XPackLicenseState licenseState = null;
        Metadata metadata = Metadata.builder().transientSettings(dynamicSettings).build();
        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT).metadata(metadata).build();
        ClusterService clusterService = Mockito.mock(ClusterService.class);
        when(clusterService.state()).thenReturn(clusterState);
        ClusterSettings clusterSettings = new ClusterSettings(
            nodeSettings,
            Set.of(TransportDeprecationInfoAction.SKIP_DEPRECATIONS_SETTING)
        );
        when((clusterService.getClusterSettings())).thenReturn(clusterSettings);
        DiscoveryNode node = Mockito.mock(DiscoveryNode.class);
        when(node.getId()).thenReturn("mock-node");
        TransportService transportService = Mockito.mock(TransportService.class);
        when(transportService.getThreadPool()).thenReturn(threadPool);
        when(transportService.getLocalNode()).thenReturn(node);
        PluginsService pluginsService = Mockito.mock(PluginsService.class);
        ActionFilters actionFilters = Mockito.mock(ActionFilters.class);
        ClusterInfoService clusterInfoService = Mockito.mock(ClusterInfoService.class);
        ClusterInfo clusterInfo = ClusterInfo.EMPTY;
        when(clusterInfoService.getClusterInfo()).thenReturn(clusterInfo);
        TransportNodeDeprecationCheckAction transportNodeDeprecationCheckAction = new TransportNodeDeprecationCheckAction(
            nodeSettings,
            threadPool,
            licenseState,
            clusterService,
            transportService,
            pluginsService,
            actionFilters,
            clusterInfoService
        );
        NodesDeprecationCheckAction.NodeRequest nodeRequest = null;
        AtomicReference<Settings> visibleNodeSettings = new AtomicReference<>();
        AtomicReference<Settings> visibleClusterStateMetadataSettings = new AtomicReference<>();
        NodeDeprecationChecks.NodeDeprecationCheck<
            Settings,
            PluginsAndModules,
            ClusterState,
            XPackLicenseState,
            DeprecationIssue> nodeSettingCheck = (settings, p, clusterState1, l) -> {
                visibleNodeSettings.set(settings);
                visibleClusterStateMetadataSettings.set(clusterState1.getMetadata().settings());
                return null;
            };
        java.util.List<
            NodeDeprecationChecks.NodeDeprecationCheck<
                Settings,
                PluginsAndModules,
                ClusterState,
                XPackLicenseState,
                DeprecationIssue>> nodeSettingsChecks = List.of(nodeSettingCheck);
        transportNodeDeprecationCheckAction.nodeOperation(nodeRequest, nodeSettingsChecks);
        settingsBuilder = Settings.builder();
        settingsBuilder.put("some.undeprecated.property", "someValue3");
        settingsBuilder.putList("some.undeprecated.list.property", List.of("someValue4", "someValue5"));
        settingsBuilder.putList(
            TransportDeprecationInfoAction.SKIP_DEPRECATIONS_SETTING.getKey(),
            List.of("some.deprecated.property", "some.other.*.deprecated.property", "some.bad.dynamic.property")
        );
        Settings expectedSettings = settingsBuilder.build();
        Assert.assertNotNull(visibleNodeSettings.get());
        Assert.assertEquals(expectedSettings, visibleNodeSettings.get());
        Assert.assertNotNull(visibleClusterStateMetadataSettings.get());
        Assert.assertEquals(Settings.EMPTY, visibleClusterStateMetadataSettings.get());

        // Testing that the setting is dynamically updatable:
        Settings newSettings = Settings.builder()
            .putList(TransportDeprecationInfoAction.SKIP_DEPRECATIONS_SETTING.getKey(), List.of("some.undeprecated.property"))
            .build();
        clusterSettings.applySettings(newSettings);
        transportNodeDeprecationCheckAction.nodeOperation(nodeRequest, nodeSettingsChecks);
        settingsBuilder = Settings.builder();
        settingsBuilder.put("some.deprecated.property", "someValue1");
        settingsBuilder.put("some.other.bad.deprecated.property", "someValue2");
        settingsBuilder.putList("some.undeprecated.list.property", List.of("someValue4", "someValue5"));
        // This is the node setting (since this is the node deprecation check), not the cluster setting:
        settingsBuilder.putList(
            TransportDeprecationInfoAction.SKIP_DEPRECATIONS_SETTING.getKey(),
            List.of("some.deprecated.property", "some.other.*.deprecated.property", "some.bad.dynamic.property")
        );
        expectedSettings = settingsBuilder.build();
        Assert.assertNotNull(visibleNodeSettings.get());
        Assert.assertEquals(expectedSettings, visibleNodeSettings.get());
        Assert.assertNotNull(visibleClusterStateMetadataSettings.get());
        Assert.assertEquals(
            Settings.builder().put("some.bad.dynamic.property", "someValue1").build(),
            visibleClusterStateMetadataSettings.get()
        );
    }

    public void testCheckDiskLowWatermark() {
        Settings nodeSettings = Settings.EMPTY;
        Settings.Builder settingsBuilder = Settings.builder();
        settingsBuilder.put("cluster.routing.allocation.disk.watermark.low", "10%");
        Settings settingsWithLowWatermark = settingsBuilder.build();
        Settings dynamicSettings = settingsWithLowWatermark;
        ClusterSettings clusterSettings = new ClusterSettings(nodeSettings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        String nodeId = "123";
        long totalBytesOnMachine = 100;
        long totalBytesFree = 70;
        ClusterInfo clusterInfo = new ClusterInfo(
            Map.of(),
            Map.of(nodeId, new DiskUsage(nodeId, "", "", totalBytesOnMachine, totalBytesFree)),
            Map.of(),
            Map.of(),
            Map.of(),
            Map.of()
        );
        DeprecationIssue issue = TransportNodeDeprecationCheckAction.checkDiskLowWatermark(
            nodeSettings,
            dynamicSettings,
            clusterInfo,
            clusterSettings,
            nodeId
        );
        assertNotNull(issue);
        assertEquals("Disk usage exceeds low watermark", issue.getMessage());

        // Making sure there's no warning when we clear out the cluster settings:
        dynamicSettings = Settings.EMPTY;
        issue = TransportNodeDeprecationCheckAction.checkDiskLowWatermark(
            nodeSettings,
            dynamicSettings,
            clusterInfo,
            clusterSettings,
            nodeId
        );
        assertNull(issue);

        // And make sure there is a warning when the setting is in the node settings but not the cluster settings:
        nodeSettings = settingsWithLowWatermark;
        issue = TransportNodeDeprecationCheckAction.checkDiskLowWatermark(
            nodeSettings,
            dynamicSettings,
            clusterInfo,
            clusterSettings,
            nodeId
        );
        assertNotNull(issue);
        assertEquals("Disk usage exceeds low watermark", issue.getMessage());
    }
}
