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
import org.elasticsearch.cluster.routing.allocation.DiskThresholdSettings;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.deprecation.DeprecationIssue;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.common.settings.ClusterSettings.BUILT_IN_CLUSTER_SETTINGS;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
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
        transportNodeDeprecationCheckAction.nodeOperation(nodeSettingsChecks);
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
        transportNodeDeprecationCheckAction.nodeOperation(nodeSettingsChecks);
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
        ClusterSettings clusterSettings = new ClusterSettings(nodeSettings, BUILT_IN_CLUSTER_SETTINGS);
        String nodeId = "123";
        long totalBytesOnMachine = 100;
        long totalBytesFree = 70;
        ClusterInfo clusterInfo = ClusterInfo.builder()
            .mostAvailableSpaceUsage(Map.of(nodeId, new DiskUsage(nodeId, "", "", totalBytesOnMachine, totalBytesFree)))
            .build();
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

    public void testDiskLowWatermarkIsIncludedInDeprecationWarnings() {
        Settings.Builder settingsBuilder = Settings.builder();
        String deprecatedSettingKey = "some.deprecated.property";
        settingsBuilder.put(deprecatedSettingKey, "someValue1");
        settingsBuilder.put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_LOW_DISK_WATERMARK_SETTING.getKey(), "10%");
        Settings nodeSettings = settingsBuilder.build();
        final XPackLicenseState licenseState = null;
        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT).metadata(Metadata.builder().build()).build();
        ClusterService clusterService = Mockito.mock(ClusterService.class);
        when(clusterService.state()).thenReturn(clusterState);
        ClusterSettings clusterSettings = new ClusterSettings(
            nodeSettings,
            Set.copyOf(CollectionUtils.appendToCopy(BUILT_IN_CLUSTER_SETTINGS, TransportDeprecationInfoAction.SKIP_DEPRECATIONS_SETTING))
        );
        when((clusterService.getClusterSettings())).thenReturn(clusterSettings);
        DiscoveryNode node = Mockito.mock(DiscoveryNode.class);
        String nodeId = "123";
        when(node.getId()).thenReturn(nodeId);
        TransportService transportService = Mockito.mock(TransportService.class);
        when(transportService.getThreadPool()).thenReturn(threadPool);
        when(transportService.getLocalNode()).thenReturn(node);
        PluginsService pluginsService = Mockito.mock(PluginsService.class);
        ActionFilters actionFilters = Mockito.mock(ActionFilters.class);
        ClusterInfoService clusterInfoService = Mockito.mock(ClusterInfoService.class);
        long totalBytesOnMachine = 100;
        long totalBytesFree = 70;
        ClusterInfo clusterInfo = ClusterInfo.builder()
            .mostAvailableSpaceUsage(Map.of(nodeId, new DiskUsage(nodeId, "", "", totalBytesOnMachine, totalBytesFree)))
            .build();
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

        NodeDeprecationChecks.NodeDeprecationCheck<
            Settings,
            PluginsAndModules,
            ClusterState,
            XPackLicenseState,
            DeprecationIssue> deprecationCheck = (first, second, third, fourth) -> {
                if (first.keySet().contains(deprecatedSettingKey)) {
                    return new DeprecationIssue(DeprecationIssue.Level.WARNING, "Deprecated setting", null, null, false, null);
                }
                return null;
            };
        NodesDeprecationCheckAction.NodeResponse nodeResponse = transportNodeDeprecationCheckAction.nodeOperation(
            Collections.singletonList(deprecationCheck)
        );
        List<DeprecationIssue> deprecationIssues = nodeResponse.getDeprecationIssues();
        assertThat(deprecationIssues, hasSize(2));
        assertThat(deprecationIssues, hasItem(new DeprecationIssueMatcher(DeprecationIssue.Level.WARNING, equalTo("Deprecated setting"))));
        assertThat(
            deprecationIssues,
            hasItem(new DeprecationIssueMatcher(DeprecationIssue.Level.CRITICAL, equalTo("Disk usage exceeds low watermark")))
        );
    }

    private static class DeprecationIssueMatcher extends BaseMatcher<DeprecationIssue> {
        private final DeprecationIssue.Level level;
        private final Matcher<String> messageMatcher;

        private DeprecationIssueMatcher(DeprecationIssue.Level level, Matcher<String> messageMatcher) {
            this.level = level;
            this.messageMatcher = messageMatcher;
        }

        @Override
        public boolean matches(Object actual) {
            if (actual instanceof DeprecationIssue == false) {
                return false;
            }
            DeprecationIssue actualDeprecationIssue = (DeprecationIssue) actual;
            return level.equals(actualDeprecationIssue.getLevel()) && messageMatcher.matches(actualDeprecationIssue.getMessage());
        }

        @Override
        public void describeTo(Description description) {
            description.appendText("deprecation issue with level: ").appendValue(level).appendText(" and message: ");
            messageMatcher.describeTo(description);
        }
    }
}
