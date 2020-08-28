/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.transport;

import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.node.NodeRoleSettings;
import org.elasticsearch.test.ESTestCase;

import java.util.concurrent.TimeUnit;

import static org.elasticsearch.test.NodeRoles.nonRemoteClusterClientNode;
import static org.elasticsearch.test.NodeRoles.remoteClusterClientNode;
import static org.elasticsearch.transport.RemoteClusterService.REMOTE_CLUSTER_SKIP_UNAVAILABLE;
import static org.elasticsearch.transport.RemoteClusterService.REMOTE_INITIAL_CONNECTION_TIMEOUT_SETTING;
import static org.elasticsearch.transport.RemoteClusterService.REMOTE_NODE_ATTRIBUTE;
import static org.elasticsearch.transport.SniffConnectionStrategy.REMOTE_CLUSTERS_PROXY;
import static org.elasticsearch.transport.SniffConnectionStrategy.REMOTE_CLUSTER_SEEDS;
import static org.elasticsearch.transport.SniffConnectionStrategy.REMOTE_CONNECTIONS_PER_CLUSTER;
import static org.hamcrest.Matchers.emptyCollectionOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
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
        assertFalse(REMOTE_CLUSTER_SKIP_UNAVAILABLE.getConcreteSettingForNamespace(alias).get(Settings.EMPTY));
    }

    public void testSeedsDefault() {
        final String alias = randomAlphaOfLength(8);
        assertThat(REMOTE_CLUSTER_SEEDS.getConcreteSettingForNamespace(alias).get(Settings.EMPTY), emptyCollectionOf(String.class));
    }

    public void testProxyDefault() {
        final String alias = randomAlphaOfLength(8);
        assertThat(REMOTE_CLUSTERS_PROXY.getConcreteSettingForNamespace(alias).get(Settings.EMPTY), equalTo(""));
    }

}
