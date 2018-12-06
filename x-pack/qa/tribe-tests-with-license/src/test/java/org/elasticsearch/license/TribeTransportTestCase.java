/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.license;

import org.elasticsearch.action.Action;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.node.info.NodeInfo;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.elasticsearch.analysis.common.CommonAnalysisPlugin;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.network.NetworkModule;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.discovery.zen.SettingsBasedHostsProvider;
import org.elasticsearch.env.Environment;
import org.elasticsearch.node.MockNode;
import org.elasticsearch.node.Node;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;
import org.elasticsearch.test.ESIntegTestCase.Scope;
import org.elasticsearch.test.InternalTestCluster;
import org.elasticsearch.test.NodeConfigurationSource;
import org.elasticsearch.test.TestCluster;
import org.elasticsearch.test.discovery.TestZenDiscovery;
import org.elasticsearch.tribe.TribePlugin;
import org.elasticsearch.xpack.CompositeTestingXPackPlugin;
import org.elasticsearch.xpack.core.XPackClientPlugin;
import org.elasticsearch.xpack.core.XPackPlugin;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.XPackField;
import org.elasticsearch.xpack.deprecation.Deprecation;
import org.elasticsearch.xpack.graph.Graph;
import org.elasticsearch.xpack.logstash.Logstash;
import org.elasticsearch.xpack.ml.MachineLearning;
import org.elasticsearch.xpack.core.ml.MachineLearningField;
import org.elasticsearch.xpack.monitoring.Monitoring;
import org.elasticsearch.xpack.security.Security;
import org.elasticsearch.xpack.upgrade.Upgrade;
import org.elasticsearch.xpack.watcher.Watcher;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

@ClusterScope(scope = Scope.TEST, transportClientRatio = 0, numClientNodes = 1, numDataNodes = 2)
public abstract class TribeTransportTestCase extends ESIntegTestCase {

    protected List<String> enabledFeatures() {
        return Collections.emptyList();
    }

    @Override
    protected final Settings nodeSettings(int nodeOrdinal) {
        final Settings.Builder builder = Settings.builder()
                .put(NetworkModule.HTTP_ENABLED.getKey(), false)
                .put("transport.type", getTestTransportType());
        List<String> enabledFeatures = enabledFeatures();
        builder.put(XPackSettings.SECURITY_ENABLED.getKey(), enabledFeatures.contains(XPackField.SECURITY));
        builder.put(XPackSettings.MONITORING_ENABLED.getKey(), enabledFeatures.contains(XPackField.MONITORING));
        builder.put(XPackSettings.WATCHER_ENABLED.getKey(), enabledFeatures.contains(XPackField.WATCHER));
        builder.put(XPackSettings.GRAPH_ENABLED.getKey(), enabledFeatures.contains(XPackField.GRAPH));
        builder.put(XPackSettings.MACHINE_LEARNING_ENABLED.getKey(), enabledFeatures.contains(XPackField.MACHINE_LEARNING));
        builder.put(MachineLearningField.AUTODETECT_PROCESS.getKey(), false);
        return builder.build();
    }

    @Override
    protected boolean ignoreExternalCluster() {
        return true;
    }

    @Override
    protected boolean addTestZenDiscovery() {
        return false;
    }

    public static class TribeAwareTestZenDiscoveryPlugin extends TestZenDiscovery.TestPlugin {

        public TribeAwareTestZenDiscoveryPlugin(Settings settings) {
            super(settings);
        }

        @Override
        public Settings additionalSettings() {
            if (settings.getGroups("tribe", true).isEmpty()) {
                return super.additionalSettings();
            } else {
                return Settings.EMPTY;
            }
        }
    }

    public static class MockTribePlugin extends TribePlugin {

        public MockTribePlugin(Settings settings) {
            super(settings);
        }

        protected Function<Settings, Node> nodeBuilder(Path configPath) {
            return settings -> new MockNode(new Environment(settings, configPath), internalCluster().getPlugins());
        }

    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        ArrayList<Class<? extends Plugin>> plugins = new ArrayList<>();
        plugins.add(MockTribePlugin.class);
        plugins.add(TribeAwareTestZenDiscoveryPlugin.class);
        plugins.add(CompositeTestingXPackPlugin.class);
        plugins.add(CommonAnalysisPlugin.class);
        return plugins;
    }

    @Override
    protected final Collection<Class<? extends Plugin>> transportClientPlugins() {
        ArrayList<Class<? extends Plugin>> plugins = new ArrayList<>();
        plugins.add(MockTribePlugin.class);
        plugins.add(TribeAwareTestZenDiscoveryPlugin.class);
        plugins.add(XPackClientPlugin.class);
        plugins.add(CommonAnalysisPlugin.class);
        return plugins;
    }
    public void testTribeSetup() throws Exception {
        NodeConfigurationSource nodeConfigurationSource = new NodeConfigurationSource() {
            @Override
            public Settings nodeSettings(int nodeOrdinal) {
                return TribeTransportTestCase.this.nodeSettings(nodeOrdinal);
            }

            @Override
            public Path nodeConfigPath(int nodeOrdinal) {
                return null;
            }

            @Override
            public Collection<Class<? extends Plugin>> nodePlugins() {
                return TribeTransportTestCase.this.nodePlugins();
            }

            @Override
            public Settings transportClientSettings() {
                return TribeTransportTestCase.this.transportClientSettings();
            }

            @Override
            public Collection<Class<? extends Plugin>> transportClientPlugins() {
                return TribeTransportTestCase.this.transportClientPlugins();
            }
        };
        final InternalTestCluster cluster2 = new InternalTestCluster(
                randomLong(), createTempDir(), true, true, 2, 2,
                UUIDs.randomBase64UUID(random()), nodeConfigurationSource, 1, false, "tribe_node2",
                getMockPlugins(), getClientWrapper());

        cluster2.beforeTest(random(), 0.0);

        logger.info("create 2 indices, test1 on t1, and test2 on t2");
        assertAcked(internalCluster().client().admin().indices().prepareCreate("test1").get());
        assertAcked(cluster2.client().admin().indices().prepareCreate("test2").get());
        ensureYellow(internalCluster());
        ensureYellow(cluster2);
//        Map<String,String> asMap = internalCluster().getDefaultSettings().getAsMap();
        Settings.Builder tribe1Defaults = Settings.builder();
        Settings.Builder tribe2Defaults = Settings.builder();
        internalCluster().getDefaultSettings().keySet().forEach(k -> {
            if (k.startsWith("path.") == false) {
                tribe1Defaults.copy(k, internalCluster().getDefaultSettings());
                tribe2Defaults.copy(k, internalCluster().getDefaultSettings());
            }
        });
        tribe1Defaults.normalizePrefix("tribe.t1.");
        tribe2Defaults.normalizePrefix("tribe.t2.");
        // give each tribe it's unicast hosts to connect to
        tribe1Defaults.putList("tribe.t1." + SettingsBasedHostsProvider.DISCOVERY_ZEN_PING_UNICAST_HOSTS_SETTING.getKey(),
                getUnicastHosts(internalCluster().client()));
        tribe1Defaults.putList("tribe.t2." + SettingsBasedHostsProvider.DISCOVERY_ZEN_PING_UNICAST_HOSTS_SETTING.getKey(),
                getUnicastHosts(cluster2.client()));

        Settings merged = Settings.builder()
                .put("tribe.t1.cluster.name", internalCluster().getClusterName())
                .put("tribe.t2.cluster.name", cluster2.getClusterName())
                .put("tribe.t1.transport.tcp.port", 0)
                .put("tribe.t2.transport.tcp.port", 0)
                .put("tribe.t1.transport.type", getTestTransportType())
                .put("tribe.t2.transport.type", getTestTransportType())
                .put("tribe.blocks.write", false)
                .put(tribe1Defaults.build())
                .put(tribe2Defaults.build())
                .put(NetworkModule.HTTP_ENABLED.getKey(), false)
                .put(internalCluster().getDefaultSettings())
                .put(XPackSettings.SECURITY_ENABLED.getKey(), false) // otherwise it conflicts with mock transport
                .put(XPackSettings.MACHINE_LEARNING_ENABLED.getKey(), false)
                .put(MachineLearningField.AUTODETECT_PROCESS.getKey(), false)
                .put("tribe.t1." + XPackSettings.SECURITY_ENABLED.getKey(), false)
                .put("tribe.t2." + XPackSettings.SECURITY_ENABLED.getKey(), false)
                .put("tribe.t1." + XPackSettings.WATCHER_ENABLED.getKey(), false)
                .put("tribe.t2." + XPackSettings.WATCHER_ENABLED.getKey(), false)
                .put("tribe.t1." + XPackSettings.MACHINE_LEARNING_ENABLED.getKey(), false)
                .put("tribe.t2." + XPackSettings.MACHINE_LEARNING_ENABLED.getKey(), false)
                .put("tribe.t1." + MachineLearningField.AUTODETECT_PROCESS.getKey(), false)
                .put("tribe.t2." + MachineLearningField.AUTODETECT_PROCESS.getKey(), false)
                .put("node.name", "tribe_node") // make sure we can identify threads from this node
                .put("transport.type", getTestTransportType())
                .build();

        final List<Class<? extends Plugin>> mockPlugins = Arrays.asList(MockTribePlugin.class, TribeAwareTestZenDiscoveryPlugin.class,
                getTestTransportPlugin(), Deprecation.class, Graph.class, Logstash.class, MachineLearning.class, Monitoring.class,
                Security.class, Upgrade.class, Watcher.class, XPackPlugin.class);
        final Node tribeNode = new MockNode(merged, mockPlugins).start();
        Client tribeClient = tribeNode.client();

        logger.info("wait till tribe has the same nodes as the 2 clusters");
        assertBusy(() -> {
            DiscoveryNodes tribeNodes = tribeNode.client().admin().cluster().prepareState().get().getState().getNodes();
            assertThat(countDataNodesForTribe("t1", tribeNodes),
                    equalTo(internalCluster().client().admin().cluster().prepareState().get().getState()
                            .getNodes().getDataNodes().size()));
            assertThat(countDataNodesForTribe("t2", tribeNodes),
                    equalTo(cluster2.client().admin().cluster().prepareState().get().getState()
                            .getNodes().getDataNodes().size()));
        });
        logger.info(" --> verify transport actions for tribe node");
        verifyActionOnTribeNode(tribeClient);
        logger.info(" --> verify transport actions for data node");
        verifyActionOnDataNode((randomBoolean() ? internalCluster() : cluster2).dataNodeClient());
        logger.info(" --> verify transport actions for master node");
        verifyActionOnMasterNode((randomBoolean() ? internalCluster() : cluster2).masterClient());
        logger.info(" --> verify transport actions for client node");
        verifyActionOnClientNode((randomBoolean() ? internalCluster() : cluster2).coordOnlyNodeClient());
        try {
            cluster2.wipe(Collections.<String>emptySet());
        } finally {
            cluster2.afterTest();
        }
        tribeNode.close();
        cluster2.close();
    }

    /**
     * Verify transport action behaviour on client node
     */
    protected abstract void verifyActionOnClientNode(Client client) throws Exception;

    /**
     * Verify transport action behaviour on master node
     */
    protected abstract void verifyActionOnMasterNode(Client masterClient) throws Exception;

    /**
     * Verify transport action behaviour on data node
     */
    protected abstract void verifyActionOnDataNode(Client dataNodeClient) throws Exception;

    /**
     * Verify transport action behaviour on tribe node
     */
    protected abstract void verifyActionOnTribeNode(Client tribeClient) throws Exception;

    protected void failAction(Client client, Action action) {
        try {
            client.execute(action, action.newRequestBuilder(client).request());
            fail("expected [" + action.name() + "] to fail");
        } catch (IllegalStateException e) {
            assertThat(e.getMessage(), containsString("failed to find action"));
        }
    }

    private void ensureYellow(TestCluster testCluster) {
        ClusterHealthResponse actionGet = testCluster.client().admin().cluster()
                        .health(Requests.clusterHealthRequest().waitForYellowStatus()
                                .waitForEvents(Priority.LANGUID).waitForNoRelocatingShards(true)).actionGet();
        if (actionGet.isTimedOut()) {
            logger.info("ensureGreen timed out, cluster state:\n{}\n{}", testCluster.client().admin().cluster()
                    .prepareState().get().getState(),
                    testCluster.client().admin().cluster().preparePendingClusterTasks().get());
            assertThat("timed out waiting for yellow state", actionGet.isTimedOut(), equalTo(false));
        }
        assertThat(actionGet.getStatus(), anyOf(equalTo(ClusterHealthStatus.YELLOW), equalTo(ClusterHealthStatus.GREEN)));
    }

    private int countDataNodesForTribe(String tribeName, DiscoveryNodes nodes) {
        int count = 0;
        for (DiscoveryNode node : nodes) {
            if (!node.isDataNode()) {
                continue;
            }
            if (tribeName.equals(node.getAttributes().get("tribe.name"))) {
                count++;
            }
        }
        return count;
    }

    private static String[] getUnicastHosts(Client client) {
        ArrayList<String> unicastHosts = new ArrayList<>();
        NodesInfoResponse nodeInfos = client.admin().cluster().prepareNodesInfo().clear().setTransport(true).get();
        for (NodeInfo info : nodeInfos.getNodes()) {
            TransportAddress address = info.getTransport().getAddress().publishAddress();
            unicastHosts.add(address.getAddress() + ":" + address.getPort());
        }
        return unicastHosts.toArray(new String[unicastHosts.size()]);
    }

}
