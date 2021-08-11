/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.transport;

import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.node.NodeRoleSettings;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.test.NodeRoles.nonRemoteClusterClientNode;
import static org.elasticsearch.test.NodeRoles.remoteClusterClientNode;
import static org.elasticsearch.transport.RemoteClusterService.ENABLE_REMOTE_CLUSTERS;
import static org.elasticsearch.transport.RemoteClusterService.REMOTE_CLUSTER_SKIP_UNAVAILABLE;
import static org.elasticsearch.transport.RemoteClusterService.REMOTE_INITIAL_CONNECTION_TIMEOUT_SETTING;
import static org.elasticsearch.transport.RemoteClusterService.REMOTE_NODE_ATTRIBUTE;
import static org.elasticsearch.transport.RemoteClusterService.SEARCH_ENABLE_REMOTE_CLUSTERS;
import static org.elasticsearch.transport.RemoteClusterService.SEARCH_REMOTE_CLUSTER_SKIP_UNAVAILABLE;
import static org.elasticsearch.transport.RemoteClusterService.SEARCH_REMOTE_INITIAL_CONNECTION_TIMEOUT_SETTING;
import static org.elasticsearch.transport.RemoteClusterService.SEARCH_REMOTE_NODE_ATTRIBUTE;
import static org.elasticsearch.transport.SniffConnectionStrategy.REMOTE_CLUSTERS_PROXY;
import static org.elasticsearch.transport.SniffConnectionStrategy.REMOTE_CLUSTER_SEEDS;
import static org.elasticsearch.transport.SniffConnectionStrategy.REMOTE_CONNECTIONS_PER_CLUSTER;
import static org.elasticsearch.transport.SniffConnectionStrategy.SEARCH_REMOTE_CLUSTERS_PROXY;
import static org.elasticsearch.transport.SniffConnectionStrategy.SEARCH_REMOTE_CLUSTERS_SEEDS;
import static org.elasticsearch.transport.SniffConnectionStrategy.SEARCH_REMOTE_CONNECTIONS_PER_CLUSTER;
import static org.hamcrest.Matchers.emptyCollectionOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.not;

@SuppressWarnings("rawtypes")
public class RemoteClusterSettingsTests extends ESTestCase {

    public void testConnectionsPerClusterFallback() {
        final int value = randomIntBetween(1, 8);
        final Settings settings = Settings.builder().put(SEARCH_REMOTE_CONNECTIONS_PER_CLUSTER.getKey(), value).build();
        assertThat(REMOTE_CONNECTIONS_PER_CLUSTER.get(settings), equalTo(value));
        assertSettingDeprecationsAndWarnings(new Setting[]{SEARCH_REMOTE_CONNECTIONS_PER_CLUSTER});
    }

    public void testConnectionsPerClusterDefault() {
        assertThat(REMOTE_CONNECTIONS_PER_CLUSTER.get(Settings.EMPTY), equalTo(3));
    }

    public void testInitialConnectTimeoutFallback() {
        final String value = randomTimeValue(30, 300, "s");
        final Settings settings = Settings.builder().put(SEARCH_REMOTE_INITIAL_CONNECTION_TIMEOUT_SETTING.getKey(), value).build();
        assertThat(
                REMOTE_INITIAL_CONNECTION_TIMEOUT_SETTING.get(settings),
                equalTo(TimeValue.parseTimeValue(value, SEARCH_REMOTE_INITIAL_CONNECTION_TIMEOUT_SETTING.getKey())));
        assertSettingDeprecationsAndWarnings(new Setting[]{SEARCH_REMOTE_INITIAL_CONNECTION_TIMEOUT_SETTING});
    }

    public void testInitialConnectTimeoutDefault() {
        assertThat(REMOTE_INITIAL_CONNECTION_TIMEOUT_SETTING.get(Settings.EMPTY), equalTo(new TimeValue(30, TimeUnit.SECONDS)));
    }

    public void testRemoteNodeAttributeFallback() {
        final String attribute = randomAlphaOfLength(8);
        final Settings settings = Settings.builder().put(SEARCH_REMOTE_NODE_ATTRIBUTE.getKey(), attribute).build();
        assertThat(REMOTE_NODE_ATTRIBUTE.get(settings), equalTo(attribute));
        assertSettingDeprecationsAndWarnings(new Setting[]{SEARCH_REMOTE_NODE_ATTRIBUTE});
    }

    public void testRemoteNodeAttributeDefault() {
        assertThat(REMOTE_NODE_ATTRIBUTE.get(Settings.EMPTY), equalTo(""));
    }

    public void testEnableRemoteClustersFallback() {
        final boolean enable = randomBoolean();
        final Settings settings = Settings.builder().put(SEARCH_ENABLE_REMOTE_CLUSTERS.getKey(), enable).build();
        assertThat(ENABLE_REMOTE_CLUSTERS.get(settings), equalTo(enable));
        assertSettingDeprecationsAndWarnings(new Setting[]{SEARCH_ENABLE_REMOTE_CLUSTERS});
    }

    public void testRemoteClusterClientDefault() {
        assertTrue(DiscoveryNode.isRemoteClusterClient(Settings.EMPTY));
        assertThat(NodeRoleSettings.NODE_ROLES_SETTING.get(Settings.EMPTY), hasItem(DiscoveryNodeRole.REMOTE_CLUSTER_CLIENT_ROLE));
    }

    public void testAddRemoteClusterClientRole() {
        final Settings settings = remoteClusterClientNode();
        assertTrue(DiscoveryNode.isRemoteClusterClient(settings));
        assertThat(NodeRoleSettings.NODE_ROLES_SETTING.get(settings), hasItem(DiscoveryNodeRole.REMOTE_CLUSTER_CLIENT_ROLE));
    }

    public void testRemoveRemoteClusterClientRole() {
        final Settings settings = nonRemoteClusterClientNode();
        assertFalse(DiscoveryNode.isRemoteClusterClient(settings));
        assertThat(NodeRoleSettings.NODE_ROLES_SETTING.get(settings), not(hasItem(DiscoveryNodeRole.REMOTE_CLUSTER_CLIENT_ROLE)));
    }

    public void testDisableEnableRemoteClusters() {
        assertFalse(DiscoveryNode.isRemoteClusterClient(Settings.builder().put(ENABLE_REMOTE_CLUSTERS.getKey(), false).build()));
        assertSettingDeprecationsAndWarnings(new Setting<?>[]{ENABLE_REMOTE_CLUSTERS});
    }

    public void testDisableSearchEnableRemoteClusters() {
        assertFalse(DiscoveryNode.isRemoteClusterClient(Settings.builder().put(SEARCH_ENABLE_REMOTE_CLUSTERS.getKey(), false).build()));
        assertSettingDeprecationsAndWarnings(new Setting<?>[]{SEARCH_ENABLE_REMOTE_CLUSTERS});
    }

    public void testSkipUnavailableFallback() {
        final String alias = randomAlphaOfLength(8);
        final boolean skip = randomBoolean();
        final Settings settings =
                Settings.builder().put(SEARCH_REMOTE_CLUSTER_SKIP_UNAVAILABLE.getConcreteSettingForNamespace(alias).getKey(), skip).build();
        assertThat(REMOTE_CLUSTER_SKIP_UNAVAILABLE.getConcreteSettingForNamespace(alias).get(settings), equalTo(skip));
        assertSettingDeprecationsAndWarnings(new Setting[]{SEARCH_REMOTE_CLUSTER_SKIP_UNAVAILABLE.getConcreteSettingForNamespace(alias)});
    }

    public void testSkipUnavailableDefault() {
        final String alias = randomAlphaOfLength(8);
        assertFalse(REMOTE_CLUSTER_SKIP_UNAVAILABLE.getConcreteSettingForNamespace(alias).get(Settings.EMPTY));
    }

    public void testSeedsFallback() {
        final String alias = randomAlphaOfLength(8);
        final int numberOfSeeds = randomIntBetween(1, 8);
        final List<String> seeds = new ArrayList<>(numberOfSeeds);
        for (int i = 0; i < numberOfSeeds; i++) {
            seeds.add("localhost:" + Integer.toString(9200 + i));
        }
        final Settings settings =
                Settings.builder()
                        .put(SEARCH_REMOTE_CLUSTERS_SEEDS.getConcreteSettingForNamespace(alias).getKey(), String.join(",", seeds)).build();
        assertThat(REMOTE_CLUSTER_SEEDS.getConcreteSettingForNamespace(alias).get(settings), equalTo(seeds));
        assertSettingDeprecationsAndWarnings(new Setting[]{SEARCH_REMOTE_CLUSTERS_SEEDS.getConcreteSettingForNamespace(alias)});
    }

    public void testSeedsDefault() {
        final String alias = randomAlphaOfLength(8);
        assertThat(REMOTE_CLUSTER_SEEDS.getConcreteSettingForNamespace(alias).get(Settings.EMPTY), emptyCollectionOf(String.class));
    }

    public void testProxyFallback() {
        final String alias = randomAlphaOfLength(8);
        final String proxy = randomAlphaOfLength(8);
        final int port = randomIntBetween(9200, 9300);
        final String value = proxy + ":" + port;
        final Settings settings =
                Settings.builder()
                        .put(SEARCH_REMOTE_CLUSTERS_PROXY.getConcreteSettingForNamespace(alias).getKey(), value).build();
        assertThat(REMOTE_CLUSTERS_PROXY.getConcreteSettingForNamespace(alias).get(settings), equalTo(value));
        assertSettingDeprecationsAndWarnings(new Setting[]{SEARCH_REMOTE_CLUSTERS_PROXY.getConcreteSettingForNamespace(alias)});
    }

    public void testProxyDefault() {
        final String alias = randomAlphaOfLength(8);
        assertThat(REMOTE_CLUSTERS_PROXY.getConcreteSettingForNamespace(alias).get(Settings.EMPTY), equalTo(""));
    }

}
