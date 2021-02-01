/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.test;

import org.elasticsearch.action.admin.cluster.remote.RemoteInfoAction;
import org.elasticsearch.action.admin.cluster.remote.RemoteInfoRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.network.NetworkModule;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.transport.RemoteClusterAware;
import org.elasticsearch.transport.RemoteConnectionInfo;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.transport.nio.MockNioTransportPlugin;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.elasticsearch.discovery.DiscoveryModule.DISCOVERY_SEED_PROVIDERS_SETTING;
import static org.elasticsearch.discovery.SettingsBasedSeedHostsProvider.DISCOVERY_SEED_HOSTS_SETTING;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.not;

public abstract class AbstractMultiClustersTestCase extends ESTestCase {
    public static final String LOCAL_CLUSTER = RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY;

    private static volatile ClusterGroup clusterGroup;

    protected Collection<String> remoteClusterAlias() {
        return randomSubsetOf(List.of("cluster-a", "cluster-b"));
    }

    protected Collection<Class<? extends Plugin>> nodePlugins(String clusterAlias) {
        return Collections.emptyList();
    }

    protected final Client client() {
        return client(LOCAL_CLUSTER);
    }

    protected final Client client(String clusterAlias) {
        return cluster(clusterAlias).client();
    }

    protected final InternalTestCluster cluster(String clusterAlias) {
        return clusterGroup.getCluster(clusterAlias);
    }

    protected final Map<String, InternalTestCluster> clusters() {
        return Collections.unmodifiableMap(clusterGroup.clusters);
    }

    protected boolean reuseClusters() {
        return true;
    }

    @Before
    public final void startClusters() throws Exception {
        if (clusterGroup != null && reuseClusters()) {
            return;
        }
        stopClusters();
        final Map<String, InternalTestCluster> clusters = new HashMap<>();
        final List<String> clusterAliases = new ArrayList<>(remoteClusterAlias());
        clusterAliases.add(LOCAL_CLUSTER);
        for (String clusterAlias : clusterAliases) {
            final String clusterName = clusterAlias.equals(LOCAL_CLUSTER) ? "main-cluster" : clusterAlias;
            final int numberOfNodes = randomIntBetween(1, 3);
            final List<Class<? extends Plugin>> mockPlugins =
                List.of(MockHttpTransport.TestPlugin.class, MockTransportService.TestPlugin.class, MockNioTransportPlugin.class);
            final Collection<Class<? extends Plugin>> nodePlugins = nodePlugins(clusterAlias);
            final Settings nodeSettings = Settings.EMPTY;
            final NodeConfigurationSource nodeConfigurationSource = nodeConfigurationSource(nodeSettings, nodePlugins);
            final InternalTestCluster cluster = new InternalTestCluster(randomLong(), createTempDir(), true, true, numberOfNodes,
                numberOfNodes, clusterName, nodeConfigurationSource, 0, clusterName + "-", mockPlugins, Function.identity());
            cluster.beforeTest(random());
            clusters.put(clusterAlias, cluster);
        }
        clusterGroup = new ClusterGroup(clusters);
        configureAndConnectsToRemoteClusters();
    }

    @After
    public void assertAfterTest() throws Exception {
        for (InternalTestCluster cluster : clusters().values()) {
            cluster.wipe(Set.of());
            cluster.assertAfterTest();
        }
    }

    @AfterClass
    public static void stopClusters() throws IOException {
        IOUtils.close(clusterGroup);
        clusterGroup = null;
    }

    protected void disconnectFromRemoteClusters() throws Exception {
        Settings.Builder settings = Settings.builder();
        final Set<String> clusterAliases = clusterGroup.clusterAliases();
        for (String clusterAlias : clusterAliases) {
            if (clusterAlias.equals(LOCAL_CLUSTER) == false) {
                settings.putNull("cluster.remote." + clusterAlias + ".seeds");
            }
        }
        client().admin().cluster().prepareUpdateSettings().setPersistentSettings(settings).get();
        assertBusy(() -> {
            for (TransportService transportService : cluster(LOCAL_CLUSTER).getInstances(TransportService.class)) {
                assertThat(transportService.getRemoteClusterService().getRegisteredRemoteClusterNames(), empty());
            }
        });
    }

    protected void configureAndConnectsToRemoteClusters() throws Exception {
        for (String clusterAlias : clusterGroup.clusterAliases()) {
            if (clusterAlias.equals(LOCAL_CLUSTER) == false) {
                final InternalTestCluster cluster = clusterGroup.getCluster(clusterAlias);
                final String[] allNodes = cluster.getNodeNames();
                final List<String> seedNodes = randomSubsetOf(randomIntBetween(1, Math.min(3, allNodes.length)), allNodes);
                configureRemoteCluster(clusterAlias, seedNodes);
            }
        }
    }

    protected void configureRemoteCluster(String clusterAlias, Collection<String> seedNodes) throws Exception {
        Settings.Builder settings = Settings.builder();
        final String seed = seedNodes.stream()
            .map(node -> {
                final TransportService transportService = cluster(clusterAlias).getInstance(TransportService.class, node);
                return transportService.boundAddress().publishAddress().toString();
            })
            .collect(Collectors.joining(","));
        settings.put("cluster.remote." + clusterAlias + ".seeds", seed);
        client().admin().cluster().prepareUpdateSettings().setPersistentSettings(settings).get();
        assertBusy(() -> {
            List<RemoteConnectionInfo> remoteConnectionInfos = client()
                .execute(RemoteInfoAction.INSTANCE, new RemoteInfoRequest()).actionGet().getInfos()
                .stream().filter(c -> c.isConnected() && c.getClusterAlias().equals(clusterAlias))
                .collect(Collectors.toList());
            assertThat(remoteConnectionInfos, not(empty()));
        });
    }

    static class ClusterGroup implements Closeable {
        private final Map<String, InternalTestCluster> clusters;

        ClusterGroup(Map<String, InternalTestCluster> clusters) {
            this.clusters = Collections.unmodifiableMap(clusters);
        }

        InternalTestCluster getCluster(String clusterAlias) {
            assertThat(clusters, hasKey(clusterAlias));
            return clusters.get(clusterAlias);
        }

        Set<String> clusterAliases() {
            return clusters.keySet();
        }

        @Override
        public void close() throws IOException {
            IOUtils.close(clusters.values());
        }
    }

    static NodeConfigurationSource nodeConfigurationSource(Settings nodeSettings, Collection<Class<? extends Plugin>> nodePlugins) {
        final Settings.Builder builder = Settings.builder();
        builder.putList(DISCOVERY_SEED_HOSTS_SETTING.getKey()); // empty list disables a port scan for other nodes
        builder.putList(DISCOVERY_SEED_PROVIDERS_SETTING.getKey(), "file");
        builder.put(NetworkModule.TRANSPORT_TYPE_KEY, getTestTransportType());
        builder.put(nodeSettings);

        return new NodeConfigurationSource() {
            @Override
            public Settings nodeSettings(int nodeOrdinal) {
                return builder.build();
            }

            @Override
            public Path nodeConfigPath(int nodeOrdinal) {
                return null;
            }

            @Override
            public Collection<Class<? extends Plugin>> nodePlugins() {
                return nodePlugins;
            }
        };
    }
}
