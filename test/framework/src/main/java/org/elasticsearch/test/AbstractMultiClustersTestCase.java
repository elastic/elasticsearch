/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.test;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.admin.cluster.remote.RemoteInfoAction;
import org.elasticsearch.action.admin.cluster.remote.RemoteInfoRequest;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.network.NetworkModule;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.transport.RemoteClusterAware;
import org.elasticsearch.transport.RemoteConnectionInfo;
import org.elasticsearch.transport.TransportService;
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

    private static final Logger LOGGER = LogManager.getLogger(AbstractMultiClustersTestCase.class);

    private static volatile ClusterGroup clusterGroup;

    protected Collection<String> remoteClusterAlias() {
        return randomSubsetOf(List.of("cluster-a", "cluster-b"));
    }

    protected Collection<Class<? extends Plugin>> nodePlugins(String clusterAlias) {
        return Collections.emptyList();
    }

    protected Settings nodeSettings() {
        return Settings.EMPTY;
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
            final List<Class<? extends Plugin>> mockPlugins = List.of(
                MockHttpTransport.TestPlugin.class,
                MockTransportService.TestPlugin.class,
                getTestTransportPlugin()
            );
            final Collection<Class<? extends Plugin>> nodePlugins = nodePlugins(clusterAlias);

            final NodeConfigurationSource nodeConfigurationSource = nodeConfigurationSource(nodeSettings(), nodePlugins);
            final InternalTestCluster cluster = new InternalTestCluster(
                randomLong(),
                createTempDir(),
                true,
                true,
                numberOfNodes,
                numberOfNodes,
                clusterName,
                nodeConfigurationSource,
                0,
                clusterName + "-",
                mockPlugins,
                Function.identity()
            );
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
        ESIntegTestCase.awaitGlobalNettyThreadsFinish();
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
                settings.putNull("cluster.remote." + clusterAlias + ".mode");
                settings.putNull("cluster.remote." + clusterAlias + ".proxy_address");
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
        final String remoteClusterSettingPrefix = "cluster.remote." + clusterAlias + ".";
        Settings.Builder settings = Settings.builder();
        final List<String> seedAdresses = seedNodes.stream().map(node -> {
            final TransportService transportService = cluster(clusterAlias).getInstance(TransportService.class, node);
            return transportService.boundAddress().publishAddress().toString();
        }).toList();
        if (randomBoolean()) {
            LOGGER.info("--> use sniff mode with seed [{}], remote nodes [{}]", Collectors.joining(","), seedNodes);
            settings.putNull(remoteClusterSettingPrefix + "proxy_address")
                .put(remoteClusterSettingPrefix + "mode", "sniff")
                .put(remoteClusterSettingPrefix + "seeds", String.join(",", seedAdresses))
                .build();
        } else {
            final String proxyNode = randomFrom(seedAdresses);
            LOGGER.info("--> use proxy node [{}], remote nodes [{}]", proxyNode, seedNodes);
            settings.putNull(remoteClusterSettingPrefix + "seeds")
                .put(remoteClusterSettingPrefix + "mode", "proxy")
                .put(remoteClusterSettingPrefix + "proxy_address", proxyNode)
                .build();
        }

        client().admin().cluster().prepareUpdateSettings().setPersistentSettings(settings).get();
        assertBusy(() -> {
            List<RemoteConnectionInfo> remoteConnectionInfos = client().execute(RemoteInfoAction.INSTANCE, new RemoteInfoRequest())
                .actionGet()
                .getInfos()
                .stream()
                .filter(c -> c.isConnected() && c.getClusterAlias().equals(clusterAlias))
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
            public Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
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
