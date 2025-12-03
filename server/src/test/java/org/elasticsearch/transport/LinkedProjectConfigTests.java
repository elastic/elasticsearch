/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.transport;

import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;

import java.util.List;

import static org.elasticsearch.transport.LinkedProjectConfig.ProxyLinkedProjectConfig;
import static org.elasticsearch.transport.LinkedProjectConfig.ProxyLinkedProjectConfigBuilder;
import static org.elasticsearch.transport.LinkedProjectConfig.SniffLinkedProjectConfig;
import static org.elasticsearch.transport.LinkedProjectConfig.SniffLinkedProjectConfigBuilder;
import static org.elasticsearch.transport.RemoteClusterSettings.ProxyConnectionStrategySettings.PROXY_ADDRESS;
import static org.elasticsearch.transport.RemoteClusterSettings.ProxyConnectionStrategySettings.REMOTE_SOCKET_CONNECTIONS;
import static org.elasticsearch.transport.RemoteClusterSettings.ProxyConnectionStrategySettings.SERVER_NAME;
import static org.elasticsearch.transport.RemoteClusterSettings.REMOTE_CLUSTER_COMPRESS;
import static org.elasticsearch.transport.RemoteClusterSettings.REMOTE_CLUSTER_COMPRESSION_SCHEME;
import static org.elasticsearch.transport.RemoteClusterSettings.REMOTE_CLUSTER_PING_SCHEDULE;
import static org.elasticsearch.transport.RemoteClusterSettings.REMOTE_CLUSTER_SKIP_UNAVAILABLE;
import static org.elasticsearch.transport.RemoteClusterSettings.REMOTE_INITIAL_CONNECTION_TIMEOUT_SETTING;
import static org.elasticsearch.transport.RemoteClusterSettings.REMOTE_MAX_PENDING_CONNECTION_LISTENERS;
import static org.elasticsearch.transport.RemoteClusterSettings.SniffConnectionStrategySettings.REMOTE_CLUSTERS_PROXY;
import static org.elasticsearch.transport.RemoteClusterSettings.SniffConnectionStrategySettings.REMOTE_CLUSTER_SEEDS;
import static org.elasticsearch.transport.RemoteClusterSettings.SniffConnectionStrategySettings.REMOTE_NODE_CONNECTIONS;
import static org.hamcrest.Matchers.equalTo;

public class LinkedProjectConfigTests extends ESTestCase {

    private static final Settings EMPTY = Settings.EMPTY;

    public void testBuildProxyLinkedProjectConfig() {
        final var config = new ProxyLinkedProjectConfig(
            ProjectId.fromId(randomAlphaOfLength(10)),
            ProjectId.fromId(randomAlphaOfLength(10)),
            randomAlphanumericOfLength(20),
            randomPositiveTimeValue(),
            randomFrom(Compression.Enabled.values()),
            randomFrom(Compression.Scheme.values()),
            randomPositiveTimeValue(),
            randomPositiveTimeValue(),
            randomBoolean(),
            randomIntBetween(1, 1000),
            randomIntBetween(1, 100),
            randomFrom("foo:9400", "bar:9400"),
            randomAlphaOfLength(20)
        );

        final var builtConfig = new ProxyLinkedProjectConfigBuilder(
            config.originProjectId(),
            config.linkedProjectId(),
            config.linkedProjectAlias()
        ).transportConnectTimeout(config.transportConnectTimeout())
            .connectionCompression(config.connectionCompression())
            .connectionCompressionScheme(config.connectionCompressionScheme())
            .clusterPingSchedule(config.clusterPingSchedule())
            .initialConnectionTimeout(config.initialConnectionTimeout())
            .skipUnavailable(config.skipUnavailable())
            .maxPendingConnectionListeners(config.maxPendingConnectionListeners())
            .maxNumConnections(config.maxNumConnections())
            .proxyAddress(config.proxyAddress())
            .serverName(config.serverName())
            .build();

        assertThat("expect builder generated config to match the original config", builtConfig, equalTo(config));
    }

    public void testBuildSniffLinkedProjectConfig() {
        final var seedNodes = List.of("foo:9400", "bar:9400", "baz:9400");
        final var config = new SniffLinkedProjectConfig(
            ProjectId.fromId(randomAlphaOfLength(10)),
            ProjectId.fromId(randomAlphaOfLength(10)),
            randomAlphanumericOfLength(20),
            randomPositiveTimeValue(),
            randomFrom(Compression.Enabled.values()),
            randomFrom(Compression.Scheme.values()),
            randomPositiveTimeValue(),
            randomPositiveTimeValue(),
            randomBoolean(),
            randomIntBetween(1, 1000),
            randomIntBetween(1, 100),
            node -> true,
            randomNonEmptySubsetOf(seedNodes),
            randomFrom(seedNodes)
        );

        final var builtConfig = new SniffLinkedProjectConfigBuilder(
            config.originProjectId(),
            config.linkedProjectId(),
            config.linkedProjectAlias()
        ).transportConnectTimeout(config.transportConnectTimeout())
            .connectionCompression(config.connectionCompression())
            .connectionCompressionScheme(config.connectionCompressionScheme())
            .clusterPingSchedule(config.clusterPingSchedule())
            .initialConnectionTimeout(config.initialConnectionTimeout())
            .skipUnavailable(config.skipUnavailable())
            .maxPendingConnectionListeners(config.maxPendingConnectionListeners())
            .maxNumConnections(config.maxNumConnections())
            .nodePredicate(config.nodePredicate())
            .seedNodes(config.seedNodes())
            .proxyAddress(config.proxyAddress())
            .build();

        assertThat("expect builder generated config to match the original config", builtConfig, equalTo(config));
    }

    public void testValidations() {
        assertThrows(NullPointerException.class, () -> new ProxyLinkedProjectConfigBuilder(null));
        assertThrows(NullPointerException.class, () -> new SniffLinkedProjectConfigBuilder(null));
        assertThrows(IllegalArgumentException.class, () -> new ProxyLinkedProjectConfigBuilder(""));
        assertThrows(IllegalArgumentException.class, () -> new SniffLinkedProjectConfigBuilder(""));
        assertChecksForNullPointer(null, ProjectId.DEFAULT, "alias");
        assertChecksForNullPointer(ProjectId.DEFAULT, null, "alias");
        assertChecksForNullPointer(ProjectId.DEFAULT, ProjectId.DEFAULT, null);
        assertThrows(NullPointerException.class, () -> new ProxyLinkedProjectConfigBuilder("alias").transportConnectTimeout(null));
        assertThrows(NullPointerException.class, () -> new ProxyLinkedProjectConfigBuilder("alias").connectionCompression(null));
        assertThrows(NullPointerException.class, () -> new ProxyLinkedProjectConfigBuilder("alias").connectionCompressionScheme(null));
        assertThrows(NullPointerException.class, () -> new ProxyLinkedProjectConfigBuilder("alias").clusterPingSchedule(null));
        assertThrows(NullPointerException.class, () -> new ProxyLinkedProjectConfigBuilder("alias").initialConnectionTimeout(null));
        assertThrows(NullPointerException.class, () -> new SniffLinkedProjectConfigBuilder("alias").nodePredicate(null));
        assertThrows(NullPointerException.class, () -> new ProxyLinkedProjectConfigBuilder("alias").proxyAddress(null));
        assertThrows(IllegalArgumentException.class, () -> new ProxyLinkedProjectConfigBuilder("alias").proxyAddress(""));
        assertThrows(IllegalArgumentException.class, () -> new ProxyLinkedProjectConfigBuilder("alias").proxyAddress("invalid:port"));
        assertThrows(NullPointerException.class, () -> new SniffLinkedProjectConfigBuilder("alias").seedNodes(null));
        assertThrows(IllegalArgumentException.class, () -> new SniffLinkedProjectConfigBuilder("alias").seedNodes(List.of()));
        assertThrows(IllegalArgumentException.class, () -> new SniffLinkedProjectConfigBuilder("alias").seedNodes(List.of("invalid:port")));
        assertThrows(NullPointerException.class, () -> new ProxyLinkedProjectConfigBuilder("alias").serverName(null));
        assertThrows(IllegalArgumentException.class, () -> new ProxyLinkedProjectConfigBuilder("alias").serverName(""));
        assertThrows(
            IllegalArgumentException.class,
            () -> new ProxyLinkedProjectConfigBuilder("alias").maxNumConnections(-randomNonNegativeInt())
        );
        assertThrows(
            IllegalArgumentException.class,
            () -> new ProxyLinkedProjectConfigBuilder("alias").maxPendingConnectionListeners(-randomNonNegativeInt())
        );
    }

    public void testSniffDefaultsMatchSettingsDefaults() {
        final var alias = "foo";
        final var config = new SniffLinkedProjectConfigBuilder(alias).build();
        assertLinkedProjectConfigDefaultsMatchSettingsDefaults(config, alias);
        assertThat(config.nodePredicate(), equalTo(RemoteClusterSettings.SniffConnectionStrategySettings.getNodePredicate(EMPTY)));
        assertThat(config.seedNodes(), equalTo(getDefault(REMOTE_CLUSTER_SEEDS, alias)));
        assertThat(config.proxyAddress(), equalTo(getDefault(REMOTE_CLUSTERS_PROXY, alias)));
        assertThat(config.maxNumConnections(), equalTo(getDefault(REMOTE_NODE_CONNECTIONS, alias)));
    }

    public void testProxyDefaultsMatchSettingsDefaults() {
        final var alias = "foo";
        final var config = new ProxyLinkedProjectConfigBuilder(alias).build();
        assertLinkedProjectConfigDefaultsMatchSettingsDefaults(config, alias);
        assertThat(config.proxyAddress(), equalTo(getDefault(PROXY_ADDRESS, alias)));
        assertThat(config.maxNumConnections(), equalTo(getDefault(REMOTE_SOCKET_CONNECTIONS, alias)));
        assertThat(config.serverName(), equalTo(getDefault(SERVER_NAME, alias)));
    }

    private void assertLinkedProjectConfigDefaultsMatchSettingsDefaults(LinkedProjectConfig config, String alias) {
        assertThat(config.transportConnectTimeout(), equalTo(TransportSettings.CONNECT_TIMEOUT.getDefault(EMPTY)));
        assertThat(config.connectionCompression(), equalTo(getDefault(REMOTE_CLUSTER_COMPRESS, alias)));
        assertThat(config.connectionCompressionScheme(), equalTo(getDefault(REMOTE_CLUSTER_COMPRESSION_SCHEME, alias)));
        assertThat(config.clusterPingSchedule(), equalTo(getDefault(REMOTE_CLUSTER_PING_SCHEDULE, alias)));
        assertThat(config.initialConnectionTimeout(), equalTo(REMOTE_INITIAL_CONNECTION_TIMEOUT_SETTING.getDefault(EMPTY)));
        assertThat(config.maxPendingConnectionListeners(), equalTo(REMOTE_MAX_PENDING_CONNECTION_LISTENERS.getDefault(EMPTY)));
        assertThat(config.skipUnavailable(), equalTo(getDefault(REMOTE_CLUSTER_SKIP_UNAVAILABLE, alias)));
    }

    private static <T> T getDefault(Setting.AffixSetting<T> setting, String alias) {
        return setting.getConcreteSettingForNamespace(alias).getDefault(EMPTY);
    }

    private static <T> void assertChecksForNullPointer(ProjectId originProjectId, ProjectId linkedProjectId, String alias) {
        assertThrows(NullPointerException.class, () -> new ProxyLinkedProjectConfigBuilder(originProjectId, linkedProjectId, alias));
        assertThrows(NullPointerException.class, () -> new SniffLinkedProjectConfigBuilder(originProjectId, linkedProjectId, alias));
    }
}
