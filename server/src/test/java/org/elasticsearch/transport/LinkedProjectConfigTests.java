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

import org.elasticsearch.transport.RemoteConnectionStrategy.ConnectionStrategy;

import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Consumer;

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

    public void testBuildProxyConnectionStrategyConfig() {
        final var config = new LinkedProjectConfig.ProxyConnectionStrategyConfig(
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

        final var builtConfig = LinkedProjectConfig.buildForLinkedProject(
            config.originProjectId(),
            config.linkedProjectId(),
            config.linkedProjectAlias()
        )
            .transportConnectTimeout(config.transportConnectTimeout())
            .connectionCompression(config.connectionCompression())
            .connectionCompressionScheme(config.connectionCompressionScheme())
            .clusterPingSchedule(config.clusterPingSchedule())
            .initialConnectionTimeout(config.initialConnectionTimeout())
            .skipUnavailable(config.skipUnavailable())
            .maxPendingConnectionListeners(config.maxPendingConnectionListeners())
            .connectionStrategy(ConnectionStrategy.PROXY)
            .proxyNumSocketConnections(config.maxNumConnections())
            .proxyAddress(config.proxyAddress())
            .proxyServerName(config.serverName())
            .build();

        assertThat("expect builder generated config to match the original config", builtConfig, equalTo(config));
    }

    public void testBuildSniffConnectionStrategyConfig() {
        final var seedNodes = List.of("foo:9400", "bar:9400", "baz:9400");
        final var config = new LinkedProjectConfig.SniffConnectionStrategyConfig(
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
            randomSubsetOf(seedNodes),
            randomFrom(seedNodes)
        );

        final var builtConfig = LinkedProjectConfig.buildForLinkedProject(
            config.originProjectId(),
            config.linkedProjectId(),
            config.linkedProjectAlias()
        )
            .transportConnectTimeout(config.transportConnectTimeout())
            .connectionCompression(config.connectionCompression())
            .connectionCompressionScheme(config.connectionCompressionScheme())
            .clusterPingSchedule(config.clusterPingSchedule())
            .initialConnectionTimeout(config.initialConnectionTimeout())
            .skipUnavailable(config.skipUnavailable())
            .maxPendingConnectionListeners(config.maxPendingConnectionListeners())
            .connectionStrategy(ConnectionStrategy.SNIFF)
            .sniffMaxNumConnections(config.maxNumConnections())
            .sniffNodePredicate(config.nodePredicate())
            .sniffSeedNodes(config.seedNodes())
            .proxyAddress(config.proxyAddress())
            .build();

        assertThat("expect builder generated config to match the original config", builtConfig, equalTo(config));
    }

    public void testValidations() {
        assertThrows(NullPointerException.class, () -> LinkedProjectConfig.buildForAlias(null));
        assertThrows(IllegalArgumentException.class, () -> LinkedProjectConfig.buildForAlias(""));
        assertThrows(NullPointerException.class, () -> LinkedProjectConfig.buildForLinkedProject(null, ProjectId.DEFAULT, "alias"));
        assertThrows(NullPointerException.class, () -> LinkedProjectConfig.buildForLinkedProject(ProjectId.DEFAULT, null, "alias"));
        assertChecksForNullPointer(LinkedProjectConfig.Builder::originProjectId);
        assertChecksForNullPointer(LinkedProjectConfig.Builder::linkedProjectId);
        assertChecksForNullPointer(LinkedProjectConfig.Builder::linkedProjectAlias);
        assertChecksForNullPointer(LinkedProjectConfig.Builder::transportConnectTimeout);
        assertChecksForNullPointer(LinkedProjectConfig.Builder::connectionCompression);
        assertChecksForNullPointer(LinkedProjectConfig.Builder::connectionCompressionScheme);
        assertChecksForNullPointer(LinkedProjectConfig.Builder::clusterPingSchedule);
        assertChecksForNullPointer(LinkedProjectConfig.Builder::initialConnectionTimeout);
        assertChecksForNullPointer(LinkedProjectConfig.Builder::connectionStrategy);
        assertChecksForNullPointer(LinkedProjectConfig.Builder::sniffNodePredicate);
        assertChecksForNullPointer(LinkedProjectConfig.Builder::sniffSeedNodes);
        assertChecksGreaterThanZero(LinkedProjectConfig.Builder::proxyNumSocketConnections);
        assertChecksGreaterThanZero(LinkedProjectConfig.Builder::sniffMaxNumConnections);
        assertChecksGreaterThanZero(LinkedProjectConfig.Builder::maxPendingConnectionListeners);
        // Check port parsing validation.
        assertChecksIllegalArgument(builder -> builder.proxyAddress("invalid:port"));
        assertChecksIllegalArgument(builder -> builder.sniffSeedNodes(List.of("invalid:port")));
        // Checks that the ConnectionStrategy is correct if it was set before a specific strategy config build method is called.
        assertChecksIllegalArgument(builder -> builder.connectionStrategy(ConnectionStrategy.SNIFF).buildProxyConnectionStrategyConfig());
        assertChecksIllegalArgument(builder -> builder.connectionStrategy(ConnectionStrategy.PROXY).buildSniffConnectionStrategyConfig());
        // Check that the ConnectionStrategy is set before trying to build.
        assertChecksIllegalArgument(LinkedProjectConfig.Builder::build);
    }

    public void testSniffDefaultsMatchSettingsDefaults() {
        final var alias = "foo";
        final var config = LinkedProjectConfig.buildForAlias(alias).buildSniffConnectionStrategyConfig();
        assertLinkedProjectConfigDefaultsMatchSettingsDefaults(config, alias);
        assertThat(config.nodePredicate(), equalTo(RemoteClusterSettings.SniffConnectionStrategySettings.getNodePredicate(EMPTY)));
        assertThat(config.seedNodes(), equalTo(getDefault(REMOTE_CLUSTER_SEEDS, alias)));
        assertThat(config.proxyAddress(), equalTo(getDefault(REMOTE_CLUSTERS_PROXY, alias)));
        assertThat(config.maxNumConnections(), equalTo(getDefault(REMOTE_NODE_CONNECTIONS, alias)));
    }

    public void testProxyDefaultsMatchSettingsDefaults() {
        final var alias = "foo";
        final var config = LinkedProjectConfig.buildForAlias(alias).buildProxyConnectionStrategyConfig();
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

    private static <T> void assertChecksForNullPointer(BiFunction<LinkedProjectConfig.Builder, T, LinkedProjectConfig.Builder> setter) {
        assertThrows(NullPointerException.class, () -> setter.apply(LinkedProjectConfig.buildForAlias("alias"), null));
    }

    private static void assertChecksGreaterThanZero(BiFunction<LinkedProjectConfig.Builder, Integer, LinkedProjectConfig.Builder> setter) {
        assertThrows(IllegalArgumentException.class, () -> setter.apply(LinkedProjectConfig.buildForAlias("alias"), 0));
        assertThrows(IllegalArgumentException.class, () -> setter.apply(LinkedProjectConfig.buildForAlias("alias"), -1));
    }

    private static <T extends Throwable> void assertThrows(Class<T> exceptionClass, Consumer<LinkedProjectConfig.Builder> consumer) {
        assertThrows(exceptionClass, () -> consumer.accept(LinkedProjectConfig.buildForAlias("alias")));
    }

    private static <T> void assertChecksIllegalArgument(Consumer<LinkedProjectConfig.Builder> consumer) {
        assertThrows(IllegalArgumentException.class, consumer);
    }
}
