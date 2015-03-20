/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.tribe;

import com.carrotsearch.randomizedtesting.LifecycleScope;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthStatus;
import org.elasticsearch.client.support.Headers;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.collect.ImmutableMap;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.shield.authc.support.UsernamePasswordToken;
import org.elasticsearch.shield.signature.InternalSignatureService;
import org.elasticsearch.test.InternalTestCluster;
import org.elasticsearch.test.ShieldIntegrationTest;
import org.elasticsearch.test.ShieldSettingsSource;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.tribe.TribeService;
import org.junit.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.test.InternalTestCluster.clusterName;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.hamcrest.Matchers.*;

public class TribeTests extends ShieldIntegrationTest {

    //use known suite prefix since their threads are already ignored via ElasticsearchThreadFilter
    public static final String SECOND_CLUSTER_NODE_PREFIX =  SUITE_CLUSTER_NODE_PREFIX;
    public static final String TRIBE_CLUSTER_NODE_PREFIX = "tribe_cluster_node_";

    private static InternalTestCluster cluster2;
    private static ShieldSettingsSource tribeSettingsSource;
    private InternalTestCluster tribeNodeCluster;

    @Before
    public void setupSecondClusterAndTribeNode() throws Exception {
        final Settings globalClusterSettings = internalCluster().getInstance(Settings.class);

        //TODO tribe nodes and all of the tribes need to have either ssl disabled or enabled as a whole
        //we read the randomized setting from the global cluster and apply it to the other cluster that we are going to start
        //for simplicity the same certificates are used on all clusters
        final boolean sslTransportEnabled = globalClusterSettings.getAsBoolean("shield.transport.ssl", null);

        //we need to make sure that all clusters and the tribe node use the same system key, we just point to the same file on all clusters
        byte[] systemKey = Files.readAllBytes(Paths.get(globalClusterSettings.get(InternalSignatureService.FILE_SETTING)));

        //we run this part in @Before instead of beforeClass because we need to have the current cluster already assigned to global
        //so that we can retrieve its settings and apply some of them the the second cluster (and tribe node too)
        if (cluster2 == null) {
            // create another cluster
            String cluster2Name = clusterName(Scope.SUITE.name(), Integer.toString(CHILD_JVM_ID), randomLong());
            //no port conflicts as this test uses the global cluster and a suite cluster that gets manually created
            ShieldSettingsSource cluster2SettingsSource = new ShieldSettingsSource(2, sslTransportEnabled, systemKey, newTempDir(LifecycleScope.SUITE), Scope.SUITE);
            cluster2 = new InternalTestCluster(randomLong(), 2, 2, cluster2Name, cluster2SettingsSource, 0, false, CHILD_JVM_ID, SECOND_CLUSTER_NODE_PREFIX);

            assert tribeSettingsSource == null;
            //given the low (2 and 1) number of nodes that the 2 SUITE clusters will have, we are not going to have port conflicts
            tribeSettingsSource = new ShieldSettingsSource(1, sslTransportEnabled, systemKey, newTempDir(LifecycleScope.SUITE), Scope.SUITE) {
                @Override
                public Settings node(int nodeOrdinal) {
                    Settings shieldSettings = super.node(nodeOrdinal);
                    //all the settings are needed for the tribe node, some of them will also need to be copied to the tribe clients configuration
                    ImmutableSettings.Builder builder = ImmutableSettings.builder().put(shieldSettings);
                    //the tribe node itself won't join any cluster, no need for unicast discovery configuration
                    builder.remove("discovery.type");
                    builder.remove("discovery.zen.ping.multicast.enabled");
                    //remove doesn't remove all the elements of an array, but we know it has only one element
                    builder.remove("discovery.zen.ping.unicast.hosts.0");

                    //copy the needed settings to the tribe clients configuration
                    ImmutableMap<String, String> shieldSettingsAsMap = shieldSettings.getAsMap();
                    for (Map.Entry<String, String> entry : shieldSettingsAsMap.entrySet()) {
                        if (isSettingNeededForTribeClient(entry.getKey())) {
                            builder.put("tribe.t1." + entry.getKey(), entry.getValue());
                            builder.put("tribe.t2." + entry.getKey(), entry.getValue());
                        }
                    }

                    return builder.put("tribe.t1.cluster.name", internalCluster().getClusterName())
                            .putArray("tribe.t1.discovery.zen.ping.unicast.hosts", unicastHosts(internalCluster()))
                            .put("tribe.t1.shield.transport.ssl", sslTransportEnabled)
                            .put("tribe.t2.cluster.name", cluster2.getClusterName())
                            .putArray("tribe.t2.discovery.zen.ping.unicast.hosts", unicastHosts(cluster2))
                            .put("tribe.t2.shield.transport.ssl", sslTransportEnabled).build();
                }

                /**
                 * Returns true if the setting is needed to setup a tribe client and needs to get forwarded to it, false otherwise.
                 * Only some of the settings need to be forwarded e.g. realm configuration gets filtered out
                 */
                private boolean isSettingNeededForTribeClient(String settingKey) {
                    if (settingKey.equals("transport.host")) {
                        return true;
                    }
                    //discovery settings get forwarded to tribe clients to disable multicast discovery
                    if (settingKey.equals("discovery.type") || settingKey.equals("discovery.zen.ping.multicast.enabled")) {
                        return true;
                    }
                    //plugins need to be properly loaded on the tribe clients too
                    if (settingKey.startsWith("plugin")) {
                        return true;
                    }
                    //make sure node.mode is network on the tribe clients too
                    if (settingKey.equals("node.mode")) {
                        return true;
                    }
                    //forward the shield audit enabled to the tribe clients
                    if (settingKey.equals("shield.audit.enabled")) {
                        return true;
                    }
                    //forward the system key to the tribe clients, same file will be used
                    if (settingKey.equals(InternalSignatureService.FILE_SETTING)) {
                        return true;
                    }
                    //forward ssl settings to the tribe clients, same certificates will be used
                    if (settingKey.startsWith("shield.ssl") || settingKey.equals("shield.transport.ssl") || settingKey.equals("shield.http.ssl")) {
                        return true;
                    }
                    //forward the credentials to the tribe clients
                    if (settingKey.equals("shield.user") || settingKey.equals(Headers.PREFIX + "." + UsernamePasswordToken.BASIC_AUTH_HEADER)) {
                        return true;
                    }
                    return false;
                }
            };
        }

        cluster2.beforeTest(getRandom(), 0.5);

        //we need to recreate the tribe node after each test otherwise ensureClusterSizeConsistency barfs
        String tribeClusterName = clusterName(Scope.SUITE.name(), Integer.toString(CHILD_JVM_ID), randomLong());
        tribeNodeCluster = new InternalTestCluster(randomLong(), 1, 1, tribeClusterName, tribeSettingsSource, 0, false, CHILD_JVM_ID, TRIBE_CLUSTER_NODE_PREFIX);
        tribeNodeCluster.beforeTest(getRandom(), 0.5);
        awaitSameNodeCounts();
    }

    private static String[] unicastHosts(InternalTestCluster testCluster) {
        Iterable<Transport> transports = testCluster.getInstances(Transport.class);
        List<String> unicastHosts = new ArrayList<>();
        for (Transport transport : transports) {
            TransportAddress transportAddress = transport.boundAddress().boundAddress();
            assertThat(transportAddress, is(instanceOf(InetSocketTransportAddress.class)));
            InetSocketTransportAddress inetSocketTransportAddress = (InetSocketTransportAddress) transportAddress;
            unicastHosts.add("localhost:" + inetSocketTransportAddress.address().getPort());
        }
        return unicastHosts.toArray(new String[unicastHosts.size()]);
    }

    @After
    public void afterTest() throws IOException {
        //we need to close the tribe node after each test otherwise ensureClusterSizeConsistency barfs
        if (tribeNodeCluster != null) {
            try {
                tribeNodeCluster.close();
            } finally {
                tribeNodeCluster = null;
            }
        }
        //and clean up the second cluster that we manually started
        if (cluster2 != null) {
            try {
                cluster2.wipe();
            } finally {
                cluster2.afterTest();
            }
        }
    }

    @AfterClass
    public static void tearDownSecondCluster() {
        if (cluster2 != null) {
            try {
                cluster2.close();
            } finally {
                cluster2 = null;
                tribeSettingsSource = null;
            }
        }
    }

    @Test
    public void testIndexRefreshAndSearch() throws Exception {
        internalCluster().client().admin().indices().prepareCreate("test1").get();
        cluster2.client().admin().indices().prepareCreate("test2").get();
        assertThat(tribeNodeCluster.client().admin().cluster().prepareHealth().setWaitForGreenStatus().get().getStatus(), equalTo(ClusterHealthStatus.GREEN));

        tribeNodeCluster.client().prepareIndex("test1", "type1", "1").setSource("field1", "value1").get();
        tribeNodeCluster.client().prepareIndex("test2", "type1", "1").setSource("field1", "value1").get();
        assertNoFailures(tribeNodeCluster.client().admin().indices().prepareRefresh().get());

        assertHitCount(tribeNodeCluster.client().prepareSearch().get(), 2l);
    }

    private void awaitSameNodeCounts() throws Exception {
        assertBusy(new Runnable() {
            @Override
            public void run() {
                DiscoveryNodes tribeNodes = tribeNodeCluster.client().admin().cluster().prepareState().get().getState().getNodes();
                assertThat(countDataNodesForTribe("t1", tribeNodes), equalTo(internalCluster().client().admin().cluster().prepareState().get().getState().getNodes().dataNodes().size()));
                assertThat(countDataNodesForTribe("t2", tribeNodes), equalTo(cluster2.client().admin().cluster().prepareState().get().getState().getNodes().dataNodes().size()));
            }
        });
    }

    private int countDataNodesForTribe(String tribeName, DiscoveryNodes nodes) {
        int count = 0;
        for (DiscoveryNode node : nodes) {
            if (!node.dataNode()) {
                continue;
            }
            if (tribeName.equals(node.getAttributes().get(TribeService.TRIBE_NAME))) {
                count++;
            }
        }
        return count;
    }
}
