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

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.test.ESTestCase;

import java.util.concurrent.TimeUnit;

import static org.elasticsearch.node.Node.NODE_REMOTE_CLUSTER_CLIENT;
import static org.elasticsearch.transport.RemoteClusterService.REMOTE_CLUSTER_SKIP_UNAVAILABLE;
import static org.elasticsearch.transport.RemoteClusterService.REMOTE_INITIAL_CONNECTION_TIMEOUT_SETTING;
import static org.elasticsearch.transport.RemoteClusterService.REMOTE_NODE_ATTRIBUTE;
import static org.elasticsearch.transport.SniffConnectionStrategy.REMOTE_CLUSTERS_PROXY;
import static org.elasticsearch.transport.SniffConnectionStrategy.REMOTE_CLUSTER_SEEDS;
import static org.elasticsearch.transport.SniffConnectionStrategy.REMOTE_CONNECTIONS_PER_CLUSTER;
import static org.hamcrest.Matchers.emptyCollectionOf;
import static org.hamcrest.Matchers.equalTo;

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
        assertTrue(NODE_REMOTE_CLUSTER_CLIENT.get(Settings.EMPTY));
    }

    public void testDisableRemoteClusterClient() {
        assertFalse(NODE_REMOTE_CLUSTER_CLIENT.get(Settings.builder().put(NODE_REMOTE_CLUSTER_CLIENT.getKey(), false).build()));
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
