/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.transport;

import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.SecureSetting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.node.NodeRoleSettings;
import org.elasticsearch.test.ESTestCase;

import java.util.concurrent.TimeUnit;

import static org.elasticsearch.test.NodeRoles.nonRemoteClusterClientNode;
import static org.elasticsearch.test.NodeRoles.remoteClusterClientNode;
import static org.elasticsearch.transport.RemoteClusterSettings.ProxyConnectionStrategySettings.PROXY_ADDRESS;
import static org.elasticsearch.transport.RemoteClusterSettings.REMOTE_CLUSTER_CREDENTIALS;
import static org.elasticsearch.transport.RemoteClusterSettings.REMOTE_CLUSTER_SKIP_UNAVAILABLE;
import static org.elasticsearch.transport.RemoteClusterSettings.REMOTE_CONNECTION_MODE;
import static org.elasticsearch.transport.RemoteClusterSettings.REMOTE_INITIAL_CONNECTION_TIMEOUT_SETTING;
import static org.elasticsearch.transport.RemoteClusterSettings.REMOTE_NODE_ATTRIBUTE;
import static org.elasticsearch.transport.RemoteClusterSettings.SniffConnectionStrategySettings.REMOTE_CLUSTERS_PROXY;
import static org.elasticsearch.transport.RemoteClusterSettings.SniffConnectionStrategySettings.REMOTE_CLUSTER_SEEDS;
import static org.elasticsearch.transport.RemoteClusterSettings.SniffConnectionStrategySettings.REMOTE_CONNECTIONS_PER_CLUSTER;
import static org.hamcrest.Matchers.emptyCollectionOf;
import static org.hamcrest.Matchers.emptyString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.isA;
import static org.hamcrest.Matchers.not;

public class RemoteClusterSettingsTests extends ESTestCase {

    public void testConnectionsPerClusterDefault() {
        assertThat(REMOTE_CONNECTIONS_PER_CLUSTER.get(Settings.EMPTY), equalTo(3));
    }

    public void testInitialConnectTimeoutDefault() {
        assertThat(REMOTE_INITIAL_CONNECTION_TIMEOUT_SETTING.get(Settings.EMPTY), equalTo(new TimeValue(30, TimeUnit.SECONDS)));
    }

    public void testRemoteNodeAttributeDefault() {
        assertThat(REMOTE_NODE_ATTRIBUTE.get(Settings.EMPTY), equalTo(""));
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

    public void testSkipUnavailableDefault() {
        final String alias = randomAlphaOfLength(8);
        assertTrue(REMOTE_CLUSTER_SKIP_UNAVAILABLE.getConcreteSettingForNamespace(alias).get(Settings.EMPTY));
    }

    public void testSeedsDefault() {
        final String alias = randomAlphaOfLength(8);
        assertThat(REMOTE_CLUSTER_SEEDS.getConcreteSettingForNamespace(alias).get(Settings.EMPTY), emptyCollectionOf(String.class));
    }

    public void testCredentialsDefault() {
        final String alias = randomAlphaOfLength(8);
        final Settings.Builder builder = Settings.builder();
        if (randomBoolean()) {
            builder.setSecureSettings(new MockSecureSettings());
        }
        assertThat(REMOTE_CLUSTER_CREDENTIALS.getConcreteSettingForNamespace(alias).get(builder.build()).toString(), emptyString());
    }

    public void testCredentialsIsSecureSetting() {
        final String alias = randomAlphaOfLength(8);
        assertThat(REMOTE_CLUSTER_CREDENTIALS.getConcreteSettingForNamespace(alias), isA(SecureSetting.class));
    }

    public void testProxyDefault() {
        final String alias = randomAlphaOfLength(8);
        assertThat(REMOTE_CLUSTERS_PROXY.getConcreteSettingForNamespace(alias).get(Settings.EMPTY), equalTo(""));
    }

    public void testSkipUnavailableAlwaysTrueIfCPSEnabled() {
        final var alias = randomAlphaOfLength(8);
        final var skipUnavailableSetting = REMOTE_CLUSTER_SKIP_UNAVAILABLE.getConcreteSettingForNamespace(alias);
        final var modeSetting = REMOTE_CONNECTION_MODE.getConcreteSettingForNamespace(alias);
        final var proxyAddressSetting = PROXY_ADDRESS.getConcreteSettingForNamespace(alias);
        final var cpsEnabledSettings = Settings.builder().put("serverless.cross_project.enabled", true).build();
        final var proxyEnabledSettings = Settings.builder()
            .put(modeSetting.getKey(), RemoteConnectionStrategy.ConnectionStrategy.PROXY.toString())
            .put(proxyAddressSetting.getKey(), "localhost:9400")
            .build();

        // Ensure the validator still throws in non-CPS environment if a connection mode is not set.
        var exception = expectThrows(
            IllegalArgumentException.class,
            () -> skipUnavailableSetting.get(Settings.builder().put(skipUnavailableSetting.getKey(), true).build())
        );
        assertThat(
            exception.getMessage(),
            equalTo("Cannot configure setting [" + skipUnavailableSetting.getKey() + "] if remote cluster is not enabled.")
        );

        // Ensure we can still get the set value in non-CPS environment.
        final var randomSkipUnavailableSettingValue = randomBoolean();
        assertThat(
            skipUnavailableSetting.get(
                Settings.builder().put(proxyEnabledSettings).put(skipUnavailableSetting.getKey(), randomSkipUnavailableSettingValue).build()
            ),
            equalTo(randomSkipUnavailableSettingValue)
        );

        // Check the validator rejects the skip_unavailable setting if present when CPS is enabled.
        exception = expectThrows(
            IllegalArgumentException.class,
            () -> skipUnavailableSetting.get(
                Settings.builder()
                    .put(cpsEnabledSettings)
                    .put(proxyEnabledSettings)
                    .put(skipUnavailableSetting.getKey(), randomBoolean())
                    .build()
            )
        );
        assertThat(exception.getMessage(), equalTo("setting [" + skipUnavailableSetting.getKey() + "] is unavailable when CPS is enabled"));

        // Should not throw if the setting is not present, returning the expected default value of true.
        assertTrue(skipUnavailableSetting.get(Settings.builder().put(cpsEnabledSettings).put(proxyEnabledSettings).build()));
    }
}
