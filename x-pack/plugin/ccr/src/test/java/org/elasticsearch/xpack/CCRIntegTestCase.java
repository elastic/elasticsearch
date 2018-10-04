/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack;

import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest;
import org.elasticsearch.action.admin.indices.get.GetIndexResponse;
import org.elasticsearch.action.admin.indices.refresh.RefreshResponse;
import org.elasticsearch.analysis.common.CommonAnalysisPlugin;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.routing.allocation.DiskThresholdSettings;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.network.NetworkModule;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.index.Index;
import org.elasticsearch.indices.store.IndicesStore;
import org.elasticsearch.license.LicenseService;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.InternalTestCluster;
import org.elasticsearch.test.MockHttpTransport;
import org.elasticsearch.test.NodeConfigurationSource;
import org.elasticsearch.test.TestCluster;
import org.elasticsearch.test.discovery.TestZenDiscovery;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.ccr.LocalStateCcr;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.ccr.AutoFollowMetadata;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Locale;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static org.elasticsearch.discovery.DiscoveryModule.DISCOVERY_HOSTS_PROVIDER_SETTING;
import static org.elasticsearch.discovery.zen.SettingsBasedHostsProvider.DISCOVERY_ZEN_PING_UNICAST_HOSTS_SETTING;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public abstract class CCRIntegTestCase extends ESTestCase {

    private static ClusterGroup clusterGroup;

    @Before
    public final void startClusters() throws Exception {
        if (clusterGroup != null && reuseClusters()) {
            return;
        }

        stopClusters();
        Settings.Builder builder = Settings.builder();
        builder.put(NodeEnvironment.MAX_LOCAL_STORAGE_NODES_SETTING.getKey(), Integer.MAX_VALUE);
        // Default the watermarks to absurdly low to prevent the tests
        // from failing on nodes without enough disk space
        builder.put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_LOW_DISK_WATERMARK_SETTING.getKey(), "1b");
        builder.put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_WATERMARK_SETTING.getKey(), "1b");
        builder.put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_WATERMARK_SETTING.getKey(), "1b");
        builder.put(ScriptService.SCRIPT_MAX_COMPILATIONS_RATE.getKey(), "2048/1m");
            // wait short time for other active shards before actually deleting, default 30s not needed in tests
        builder.put(IndicesStore.INDICES_STORE_DELETE_SHARD_TIMEOUT.getKey(), new TimeValue(1, TimeUnit.SECONDS));
        builder.putList(DISCOVERY_ZEN_PING_UNICAST_HOSTS_SETTING.getKey()); // empty list disables a port scan for other nodes
        builder.putList(DISCOVERY_HOSTS_PROVIDER_SETTING.getKey(), "file");
        builder.put(NetworkModule.TRANSPORT_TYPE_KEY, getTestTransportType());
        builder.put(XPackSettings.SECURITY_ENABLED.getKey(), false);
        builder.put(XPackSettings.MONITORING_ENABLED.getKey(), false);
        builder.put(XPackSettings.WATCHER_ENABLED.getKey(), false);
        builder.put(XPackSettings.MACHINE_LEARNING_ENABLED.getKey(), false);
        builder.put(XPackSettings.LOGSTASH_ENABLED.getKey(), false);
        builder.put(LicenseService.SELF_GENERATED_LICENSE_TYPE.getKey(), "trial");

        NodeConfigurationSource nodeConfigurationSource = new NodeConfigurationSource() {
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
                return Arrays.asList(LocalStateCcr.class, CommonAnalysisPlugin.class);
            }

            @Override
            public Settings transportClientSettings() {
                return super.transportClientSettings();
            }

            @Override
            public Collection<Class<? extends Plugin>> transportClientPlugins() {
                return Arrays.asList(LocalStateCcr.class, getTestTransportPlugin());
            }
        };
        Collection<Class<? extends Plugin>> mockPlugins = Arrays.asList(ESIntegTestCase.TestSeedPlugin.class,
            TestZenDiscovery.TestPlugin.class, MockHttpTransport.TestPlugin.class, getTestTransportPlugin());

        InternalTestCluster leaderCluster = new InternalTestCluster(randomLong(), createTempDir(), true, true, numberOfNodesPerCluster(),
            numberOfNodesPerCluster(), UUIDs.randomBase64UUID(random()), nodeConfigurationSource, 0, "leader", mockPlugins,
            Function.identity());
        InternalTestCluster followerCluster = new InternalTestCluster(randomLong(), createTempDir(), true, true, numberOfNodesPerCluster(),
            numberOfNodesPerCluster(), UUIDs.randomBase64UUID(random()), nodeConfigurationSource, 0, "follower", mockPlugins,
            Function.identity());
        clusterGroup = new ClusterGroup(leaderCluster, followerCluster);

        leaderCluster.beforeTest(random(), 0.0D);
        leaderCluster.ensureAtLeastNumDataNodes(numberOfNodesPerCluster());
        followerCluster.beforeTest(random(), 0.0D);
        followerCluster.ensureAtLeastNumDataNodes(numberOfNodesPerCluster());

        ClusterUpdateSettingsRequest updateSettingsRequest = new ClusterUpdateSettingsRequest();
        String address = leaderCluster.getDataNodeInstance(TransportService.class).boundAddress().publishAddress().toString();
        updateSettingsRequest.persistentSettings(Settings.builder().put("cluster.remote.leader_cluster.seeds", address));
        assertAcked(followerClient().admin().cluster().updateSettings(updateSettingsRequest).actionGet());
    }

    @After
    public void afterTest() throws Exception {
        String masterNode = clusterGroup.followerCluster.getMasterName();
        ClusterService clusterService = clusterGroup.followerCluster.getInstance(ClusterService.class, masterNode);

        CountDownLatch latch = new CountDownLatch(1);
        clusterService.submitStateUpdateTask("", new ClusterStateUpdateTask() {
            @Override
            public ClusterState execute(ClusterState currentState) throws Exception {
                ClusterState.Builder newState = ClusterState.builder(currentState);
                newState.metaData(MetaData.builder(currentState.getMetaData())
                    .removeCustom(AutoFollowMetadata.TYPE)
                    .build());
                return newState.build();
            }

            @Override
            public void onFailure(String source, Exception e) {
                latch.countDown();
            }

            @Override
            public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                latch.countDown();
            }
        });
        latch.await();

        clusterGroup.leaderCluster.wipe(Collections.emptySet());
        clusterGroup.followerCluster.wipe(Collections.emptySet());
    }

    @AfterClass
    public static void stopClusters() throws IOException {
        IOUtils.close(clusterGroup);
        clusterGroup = null;
    }

    protected int numberOfNodesPerCluster() {
        return 2;
    }

    protected boolean reuseClusters() {
        return true;
    }

    protected final Client leaderClient() {
        return clusterGroup.leaderCluster.client();
    }

    protected final Client followerClient() {
        return clusterGroup.followerCluster.client();
    }

    protected final InternalTestCluster getLeaderCluster() {
        return clusterGroup.leaderCluster;
    }

    protected final InternalTestCluster getFollowerCluster() {
        return clusterGroup.followerCluster;
    }

    protected final ClusterHealthStatus ensureLeaderYellow(String... indices) {
        return ensureColor(clusterGroup.leaderCluster, ClusterHealthStatus.YELLOW, TimeValue.timeValueSeconds(30), false, indices);
    }

    protected final ClusterHealthStatus ensureLeaderGreen(String... indices) {
        return ensureColor(clusterGroup.leaderCluster, ClusterHealthStatus.GREEN, TimeValue.timeValueSeconds(30), false, indices);
    }

    protected final ClusterHealthStatus ensureFollowerGreen(String... indices) {
        return ensureColor(clusterGroup.followerCluster, ClusterHealthStatus.GREEN, TimeValue.timeValueSeconds(30), false, indices);
    }

    private ClusterHealthStatus ensureColor(TestCluster testCluster,
                                            ClusterHealthStatus clusterHealthStatus,
                                            TimeValue timeout,
                                            boolean waitForNoInitializingShards,
                                            String... indices) {
        String color = clusterHealthStatus.name().toLowerCase(Locale.ROOT);
        String method = "ensure" + Strings.capitalize(color);

        ClusterHealthRequest healthRequest = Requests.clusterHealthRequest(indices)
            .timeout(timeout)
            .waitForStatus(clusterHealthStatus)
            .waitForEvents(Priority.LANGUID)
            .waitForNoRelocatingShards(true)
            .waitForNoInitializingShards(waitForNoInitializingShards)
            .waitForNodes(Integer.toString(testCluster.size()));

        ClusterHealthResponse actionGet = testCluster.client().admin().cluster().health(healthRequest).actionGet();
        if (actionGet.isTimedOut()) {
            logger.info("{} timed out, cluster state:\n{}\n{}",
                method,
                testCluster.client().admin().cluster().prepareState().get().getState(),
                testCluster.client().admin().cluster().preparePendingClusterTasks().get());
            fail("timed out waiting for " + color + " state");
        }
        assertThat("Expected at least " + clusterHealthStatus + " but got " + actionGet.getStatus(),
            actionGet.getStatus().value(), lessThanOrEqualTo(clusterHealthStatus.value()));
        logger.debug("indices {} are {}", indices.length == 0 ? "[_all]" : indices, color);
        return actionGet.getStatus();
    }

    protected final Index resolveLeaderIndex(String index) {
        GetIndexResponse getIndexResponse = leaderClient().admin().indices().prepareGetIndex().setIndices(index).get();
        assertTrue("index " + index + " not found", getIndexResponse.getSettings().containsKey(index));
        String uuid = getIndexResponse.getSettings().get(index).get(IndexMetaData.SETTING_INDEX_UUID);
        return new Index(index, uuid);
    }

    protected final Index resolveFollowerIndex(String index) {
        GetIndexResponse getIndexResponse = followerClient().admin().indices().prepareGetIndex().setIndices(index).get();
        assertTrue("index " + index + " not found", getIndexResponse.getSettings().containsKey(index));
        String uuid = getIndexResponse.getSettings().get(index).get(IndexMetaData.SETTING_INDEX_UUID);
        return new Index(index, uuid);
    }

    protected final RefreshResponse refresh(Client client, String... indices) {
        RefreshResponse actionGet = client.admin().indices().prepareRefresh(indices).execute().actionGet();
        assertNoFailures(actionGet);
        return actionGet;
    }

    static class ClusterGroup implements Closeable {

        final InternalTestCluster leaderCluster;
        final InternalTestCluster followerCluster;

        ClusterGroup(InternalTestCluster leaderCluster, InternalTestCluster followerCluster) {
            this.leaderCluster = leaderCluster;
            this.followerCluster = followerCluster;
        }

        @Override
        public void close() throws IOException {
            IOUtils.close(leaderCluster, followerCluster);
        }
    }
}
