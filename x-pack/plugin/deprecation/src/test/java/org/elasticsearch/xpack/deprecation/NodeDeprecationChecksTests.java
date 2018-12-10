/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.deprecation;

import org.elasticsearch.Build;
import org.elasticsearch.Version;
import org.elasticsearch.action.admin.cluster.node.info.NodeInfo;
import org.elasticsearch.action.admin.cluster.node.info.PluginsAndModules;
import org.elasticsearch.action.admin.cluster.node.stats.NodeStats;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.monitor.fs.FsInfo;
import org.elasticsearch.monitor.os.OsInfo;
import org.elasticsearch.plugins.PluginInfo;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.VersionUtils;
import org.elasticsearch.xpack.core.deprecation.DeprecationIssue;
import org.junit.Before;

import java.util.Collections;
import java.util.List;

import static java.util.Collections.singletonList;
import static org.elasticsearch.xpack.deprecation.DeprecationChecks.NODE_SETTINGS_CHECKS;

public class NodeDeprecationChecksTests extends ESTestCase {
    private DiscoveryNode discoveryNode;
    private FsInfo.Path[] paths;
    private OsInfo osInfo;
    private PluginsAndModules pluginsAndModules;

    @Before
    public void setupDefaults() {
        discoveryNode = DiscoveryNode.createLocal(Settings.builder().put("node.name", "node_check").build(),
            new TransportAddress(TransportAddress.META_ADDRESS, 9200), "test");
        paths = new FsInfo.Path[] {};
        osInfo = new OsInfo(0L, 1, 1, randomAlphaOfLength(10),
            "foo-64", randomAlphaOfLength(10), randomAlphaOfLength(10));
        pluginsAndModules = new PluginsAndModules(Collections.emptyList(), Collections.emptyList());
    }

    private void assertSettingsAndIssue(String key, String value, DeprecationIssue expected) {
        Settings settings = Settings.builder()
            .put("cluster.name", "elasticsearch")
            .put("node.name", "node_check")
            .put(key, value)
            .build();
        List<NodeInfo> nodeInfos = Collections.singletonList(new NodeInfo(Version.CURRENT, Build.CURRENT,
            discoveryNode, settings, osInfo, null, null,
            null, null, null, pluginsAndModules, null, null));
        List<NodeStats> nodeStats = Collections.singletonList(new NodeStats(discoveryNode, 0L, null,
            null, null, null, null, new FsInfo(0L, null, paths), null, null, null,
            null, null, null, null));
        List<DeprecationIssue> issues = DeprecationChecks.filterChecks(NODE_SETTINGS_CHECKS, c -> c.apply(nodeInfos, nodeStats));
        assertEquals(singletonList(expected), issues);
    }

    public void testTribeNodeCheck() {
        String tribeSetting = "tribe." + randomAlphaOfLengthBetween(1, 20) + ".cluster.name";
        DeprecationIssue expected = new DeprecationIssue(DeprecationIssue.Level.CRITICAL,
            "Tribe Node removed in favor of Cross Cluster Search",
            "https://www.elastic.co/guide/en/elasticsearch/reference/master/breaking_70_cluster_changes.html" +
                "#_tribe_node_removed",
            "nodes with tribe node settings: [node_check]");
        assertSettingsAndIssue(tribeSetting, randomAlphaOfLength(5), expected);
    }

    public void testAzurePluginCheck() {
        Version esVersion = VersionUtils.randomVersionBetween(random(), Version.V_6_0_0, Version.CURRENT);
        PluginInfo deprecatedPlugin = new PluginInfo(
            "repository-azure", "dummy plugin description", "dummy_plugin_version", esVersion,
            "javaVersion", "DummyPluginName", Collections.emptyList(), false);
        pluginsAndModules = new PluginsAndModules(Collections.singletonList(deprecatedPlugin), Collections.emptyList());

        DeprecationIssue expected = new DeprecationIssue(DeprecationIssue.Level.WARNING,
            "Azure Repository settings changed",
            "https://www.elastic.co/guide/en/elasticsearch/reference/master/breaking_70_cluster_changes.html" +
                "#_azure_repository_plugin",
            "nodes with repository-azure installed: [node_check]");
        assertSettingsAndIssue("foo", "bar", expected);
    }

    public void testGCSPluginCheck() {
        Version esVersion = VersionUtils.randomVersionBetween(random(), Version.V_6_0_0, Version.CURRENT);
        PluginInfo deprecatedPlugin = new PluginInfo(
            "repository-gcs", "dummy plugin description", "dummy_plugin_version", esVersion,
            "javaVersion", "DummyPluginName", Collections.emptyList(), false);
        pluginsAndModules = new PluginsAndModules(Collections.singletonList(deprecatedPlugin), Collections.emptyList());

        DeprecationIssue expected = new DeprecationIssue(DeprecationIssue.Level.WARNING,
            "GCS Repository settings changed",
            "https://www.elastic.co/guide/en/elasticsearch/reference/master/breaking_70_cluster_changes.html" +
                "#_google_cloud_storage_repository_plugin",
            "nodes with repository-gcs installed: [node_check]");
        assertSettingsAndIssue("foo", "bar", expected);
    }

    public void testFileDiscoveryPluginCheck() {
        Version esVersion = VersionUtils.randomVersionBetween(random(), Version.V_6_0_0, Version.CURRENT);
        PluginInfo deprecatedPlugin = new PluginInfo(
            "discovery-file", "dummy plugin description", "dummy_plugin_version", esVersion,
            "javaVersion", "DummyPluginName", Collections.emptyList(), false);
        pluginsAndModules = new PluginsAndModules(Collections.singletonList(deprecatedPlugin), Collections.emptyList());

        DeprecationIssue expected = new DeprecationIssue(DeprecationIssue.Level.WARNING,
            "File-based discovery is no longer a plugin and uses a different path",
            "https://www.elastic.co/guide/en/elasticsearch/reference/master/breaking_70_cluster_changes.html" +
                "#_file_based_discovery_plugin",
            "nodes with discovery-file installed: [node_check]");
        assertSettingsAndIssue("foo", "bar", expected);
    }
}
