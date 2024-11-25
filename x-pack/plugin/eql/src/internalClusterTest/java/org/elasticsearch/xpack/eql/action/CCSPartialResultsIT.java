/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.eql.action;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.admin.cluster.remote.RemoteInfoRequest;
import org.elasticsearch.action.admin.cluster.remote.TransportRemoteInfoAction;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsAction;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.network.NetworkModule;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.CloseableTestClusterWrapper;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.InternalTestCluster;
import org.elasticsearch.test.MockHttpTransport;
import org.elasticsearch.test.NodeConfigurationSource;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.transport.RemoteClusterAware;
import org.elasticsearch.transport.RemoteConnectionInfo;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.eql.plugin.EqlPlugin;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.elasticsearch.discovery.DiscoveryModule.DISCOVERY_SEED_PROVIDERS_SETTING;
import static org.elasticsearch.discovery.SettingsBasedSeedHostsProvider.DISCOVERY_SEED_HOSTS_SETTING;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

public class CCSPartialResultsIT extends ESTestCase {


    public static final String LOCAL_CLUSTER = RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY;
    public static final String REMOTE_CLUSTER = "remote_cluster";

    private static volatile ClusterGroup clusterGroup;

    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singletonList(LocalStateEQLXPackPlugin.class);
    }

    protected Settings nodeSettings() {
        return Settings.builder()
            .put("cluster.routing.rebalance.enable", "none")
            .build();
    }

    protected final Client localClient() {
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


    @Before
    public final void startClusters() throws Exception {
        if (clusterGroup != null) {
            return;
        }
        stopClusters();
        final Map<String, InternalTestCluster> clusters = new HashMap<>();
        final List<String> clusterAliases = List.of(REMOTE_CLUSTER, LOCAL_CLUSTER);
        for (String clusterAlias : clusterAliases) {
            final String clusterName = clusterAlias.equals(LOCAL_CLUSTER) ? "main-cluster" : clusterAlias;
            final int numberOfNodes = 2;
            final List<Class<? extends Plugin>> mockPlugins = List.of(
                MockHttpTransport.TestPlugin.class,
                MockTransportService.TestPlugin.class,
                getTestTransportPlugin()
            );
            final Collection<Class<? extends Plugin>> nodePlugins = nodePlugins();

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
            cluster.getNodeNames();
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

    protected void configureAndConnectsToRemoteClusters() throws Exception {
        final InternalTestCluster cluster = clusterGroup.getCluster(REMOTE_CLUSTER);
        final String[] allNodes = cluster.getNodeNames();
        configureRemoteCluster(REMOTE_CLUSTER, allNodes[1]);
    }

    protected void configureRemoteCluster(String clusterAlias, String seedNode) throws Exception {
        final String remoteClusterSettingPrefix = "cluster.remote." + clusterAlias + ".";
        Settings.Builder settings = Settings.builder();
        final TransportService transportService = cluster(clusterAlias).getInstance(TransportService.class, seedNode);
        String seedAddress = transportService.boundAddress().publishAddress().toString();

        Settings.Builder builder;
        if (randomBoolean()) {
            builder = settings.putNull(remoteClusterSettingPrefix + "proxy_address")
                .put(remoteClusterSettingPrefix + "mode", "sniff")
                .put(remoteClusterSettingPrefix + "seeds", seedAddress);
        } else {
            builder = settings.putNull(remoteClusterSettingPrefix + "seeds")
                .put(remoteClusterSettingPrefix + "mode", "proxy")
                .put(remoteClusterSettingPrefix + "proxy_address", seedAddress);
        }

        localClient().admin()
            .cluster()
            .prepareUpdateSettings(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT)
            .setPersistentSettings(builder)
            .get();

        assertBusy(() -> {
            List<RemoteConnectionInfo> remoteConnectionInfos = localClient().execute(TransportRemoteInfoAction.TYPE, new RemoteInfoRequest())
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

        @Override
        public void close() throws IOException {
            IOUtils.close(CloseableTestClusterWrapper.wrap(clusters.values()));
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


    public void testFailuresFromRemote() throws ExecutionException, InterruptedException, IOException {
        final Client localClient = localClient();
        final Client remoteClient = client(REMOTE_CLUSTER);

        assertAcked(
            remoteClient.admin().indices().prepareCreate("test-1-remote")
                .setSettings(
                    Settings.builder()
                        .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                        .put("index.routing.allocation.include._name", clusterGroup.clusters.get(REMOTE_CLUSTER).getNodeNames()[0])
                        .build()
                )
                .setMapping("@timestamp", "type=date"),
            TimeValue.timeValueSeconds(60)
        );

        assertAcked(
            remoteClient.admin().indices().prepareCreate("test-2-remote")
                .setSettings(
                    Settings.builder()
                        .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                        .put("index.routing.allocation.exclude._name", clusterGroup.clusters.get(REMOTE_CLUSTER).getNodeNames()[0])
                        .build()
                )
                .setMapping("@timestamp", "type=date"),
            TimeValue.timeValueSeconds(60)
        );

        for (int i = 0; i < 5; i++) {
            int val = i * 2;
            remoteClient.prepareIndex("test-1-remote").setId(Integer.toString(i))
                .setSource("@timestamp", 100000 + val, "event.category", "process", "key", "same", "value", val)
                .get();
        }
        for (int i = 0; i < 5; i++) {
            int val = i * 2 + 1;
            remoteClient.prepareIndex("test-2-remote").setId(Integer.toString(i))
                .setSource("@timestamp", 100000 + val, "event.category", "process", "key", "same", "value", val)
                .get();
        }

        remoteClient.admin().indices().prepareRefresh().get();
        localClient.admin().indices().prepareRefresh().get();

        // ------------------------------------------------------------------------
        // queries with full cluster (no missing shards)
        // ------------------------------------------------------------------------

        // event query
        var request = new EqlSearchRequest().indices(REMOTE_CLUSTER + ":test-*")
            .query("process where true")
            .allowPartialSearchResults(randomBoolean())
            .allowPartialSequenceResults(randomBoolean());
        EqlSearchResponse response = localClient().execute(EqlSearchAction.INSTANCE, request).get();
        assertThat(response.hits().events().size(), equalTo(10));
        for (int i = 0; i < 10; i++) {
            assertThat(response.hits().events().get(i).toString(), containsString("\"value\" : " + i));
        }
        assertThat(response.shardFailures().length, is(0));

        // sequence query on both shards
        request = new EqlSearchRequest().indices(REMOTE_CLUSTER + ":test-*")
            .query("sequence [process where value == 1] [process where value == 2]")
            .allowPartialSearchResults(randomBoolean())
            .allowPartialSequenceResults(randomBoolean());
        response = localClient().execute(EqlSearchAction.INSTANCE, request).get();
        assertThat(response.hits().sequences().size(), equalTo(1));
        EqlSearchResponse.Sequence sequence = response.hits().sequences().get(0);
        assertThat(sequence.events().get(0).toString(), containsString("\"value\" : 1"));
        assertThat(sequence.events().get(1).toString(), containsString("\"value\" : 2"));
        assertThat(response.shardFailures().length, is(0));

        // sequence query on the available shard only
        request = new EqlSearchRequest().indices(REMOTE_CLUSTER + ":test-*")
            .query("sequence [process where value == 1] [process where value == 3]")
            .allowPartialSearchResults(randomBoolean())
            .allowPartialSequenceResults(randomBoolean());
        response = localClient().execute(EqlSearchAction.INSTANCE, request).get();
        assertThat(response.hits().sequences().size(), equalTo(1));
        sequence = response.hits().sequences().get(0);
        assertThat(sequence.events().get(0).toString(), containsString("\"value\" : 1"));
        assertThat(sequence.events().get(1).toString(), containsString("\"value\" : 3"));
        assertThat(response.shardFailures().length, is(0));

        // sequence query on the unavailable shard only
        request = new EqlSearchRequest().indices(REMOTE_CLUSTER + ":test-*")
            .query("sequence [process where value == 0] [process where value == 2]")
            .allowPartialSearchResults(randomBoolean())
            .allowPartialSequenceResults(randomBoolean());
        response = localClient().execute(EqlSearchAction.INSTANCE, request).get();
        assertThat(response.hits().sequences().size(), equalTo(1));
        sequence = response.hits().sequences().get(0);
        assertThat(sequence.events().get(0).toString(), containsString("\"value\" : 0"));
        assertThat(sequence.events().get(1).toString(), containsString("\"value\" : 2"));
        assertThat(response.shardFailures().length, is(0));

        // sequence query with missing event on unavailable shard
        request = new EqlSearchRequest().indices(REMOTE_CLUSTER + ":test-*")
            .query("sequence with maxspan=10s [process where value == 1] ![process where value == 2] [process where value == 3]")
            .allowPartialSearchResults(randomBoolean())
            .allowPartialSequenceResults(randomBoolean());
        response = localClient().execute(EqlSearchAction.INSTANCE, request).get();
        assertThat(response.hits().sequences().size(), equalTo(0));
        assertThat(response.shardFailures().length, is(0));

        // sample query on both shards
        request = new EqlSearchRequest().indices(REMOTE_CLUSTER + ":test-*")
            .query("sample by key [process where value == 2] [process where value == 1]")
            .allowPartialSearchResults(randomBoolean())
            .allowPartialSequenceResults(randomBoolean());
        response = localClient().execute(EqlSearchAction.INSTANCE, request).get();
        assertThat(response.hits().sequences().size(), equalTo(1));
        EqlSearchResponse.Sequence sample = response.hits().sequences().get(0);
        assertThat(sample.events().get(0).toString(), containsString("\"value\" : 2"));
        assertThat(sample.events().get(1).toString(), containsString("\"value\" : 1"));
        assertThat(response.shardFailures().length, is(0));

        // sample query on the available shard only
        request = new EqlSearchRequest().indices(REMOTE_CLUSTER + ":test-*")
            .query("sample by key [process where value == 3] [process where value == 1]")
            .allowPartialSearchResults(randomBoolean())
            .allowPartialSequenceResults(randomBoolean());
        response = localClient().execute(EqlSearchAction.INSTANCE, request).get();
        assertThat(response.hits().sequences().size(), equalTo(1));
        sample = response.hits().sequences().get(0);
        assertThat(sample.events().get(0).toString(), containsString("\"value\" : 3"));
        assertThat(sample.events().get(1).toString(), containsString("\"value\" : 1"));
        assertThat(response.shardFailures().length, is(0));

        // sample query on the unavailable shard only
        request = new EqlSearchRequest().indices(REMOTE_CLUSTER + ":test-*")
            .query("sample by key [process where value == 2] [process where value == 0]")
            .allowPartialSearchResults(randomBoolean())
            .allowPartialSequenceResults(randomBoolean());
        response = localClient().execute(EqlSearchAction.INSTANCE, request).get();
        assertThat(response.hits().sequences().size(), equalTo(1));
        sample = response.hits().sequences().get(0);
        assertThat(sample.events().get(0).toString(), containsString("\"value\" : 2"));
        assertThat(sample.events().get(1).toString(), containsString("\"value\" : 0"));
        assertThat(response.shardFailures().length, is(0));

        // ------------------------------------------------------------------------
        // stop one of the nodes, make one of the shards unavailable
        // ------------------------------------------------------------------------

        cluster(REMOTE_CLUSTER).stopNode(clusterGroup.clusters.get(REMOTE_CLUSTER).getNodeNames()[0]);

        // ------------------------------------------------------------------------
        // same queries, with missing shards and allow_partial_search_results=true
        // and allow_partial_sequence_result=true
        // ------------------------------------------------------------------------

        // event query
        request = new EqlSearchRequest().indices(REMOTE_CLUSTER + ":test-*").query("process where true").allowPartialSearchResults(true);
        response = localClient().execute(EqlSearchAction.INSTANCE, request).get();
        assertThat(response.hits().events().size(), equalTo(5));
        for (int i = 0; i < 5; i++) {
            assertThat(response.hits().events().get(i).toString(), containsString("\"value\" : " + (i * 2 + 1)));
        }
        assertThat(response.shardFailures().length, is(1));

        // sequence query on both shards
        request = new EqlSearchRequest().indices(REMOTE_CLUSTER + ":test-*")
            .query("sequence [process where value == 1] [process where value == 2]")
            .allowPartialSearchResults(true)
            .allowPartialSequenceResults(true);
        response = localClient().execute(EqlSearchAction.INSTANCE, request).get();
        assertThat(response.hits().sequences().size(), equalTo(0));
        assertThat(response.shardFailures().length, is(1));
        assertThat(response.shardFailures()[0].index(), is("test-1-remote"));
        assertThat(response.shardFailures()[0].reason(), containsString("NoShardAvailableActionException"));

        // sequence query on the available shard only
        request = new EqlSearchRequest().indices(REMOTE_CLUSTER + ":test-*")
            .query("sequence [process where value == 1] [process where value == 3]")
            .allowPartialSearchResults(true)
            .allowPartialSequenceResults(true);
        response = localClient().execute(EqlSearchAction.INSTANCE, request).get();
        assertThat(response.hits().sequences().size(), equalTo(1));
        sequence = response.hits().sequences().get(0);
        assertThat(sequence.events().get(0).toString(), containsString("\"value\" : 1"));
        assertThat(sequence.events().get(1).toString(), containsString("\"value\" : 3"));
        assertThat(response.shardFailures().length, is(1));
        assertThat(response.shardFailures()[0].index(), is("test-1-remote"));
        assertThat(response.shardFailures()[0].reason(), containsString("NoShardAvailableActionException"));

        // sequence query on the unavailable shard only
        request = new EqlSearchRequest().indices(REMOTE_CLUSTER + ":test-*")
            .query("sequence [process where value == 0] [process where value == 2]")
            .allowPartialSearchResults(true)
            .allowPartialSequenceResults(true);
        response = localClient().execute(EqlSearchAction.INSTANCE, request).get();
        assertThat(response.hits().sequences().size(), equalTo(0));
        assertThat(response.shardFailures().length, is(1));
        assertThat(response.shardFailures()[0].index(), is("test-1-remote"));
        assertThat(response.shardFailures()[0].reason(), containsString("NoShardAvailableActionException"));

        // sequence query with missing event on unavailable shard. THIS IS A FALSE POSITIVE
        request = new EqlSearchRequest().indices(REMOTE_CLUSTER + ":test-*")
            .query("sequence with maxspan=10s  [process where value == 1] ![process where value == 2] [process where value == 3]")
            .allowPartialSearchResults(true)
            .allowPartialSequenceResults(true);
        response = localClient().execute(EqlSearchAction.INSTANCE, request).get();
        assertThat(response.hits().sequences().size(), equalTo(1));
        sequence = response.hits().sequences().get(0);
        assertThat(sequence.events().get(0).toString(), containsString("\"value\" : 1"));
        assertThat(sequence.events().get(2).toString(), containsString("\"value\" : 3"));
        assertThat(response.shardFailures().length, is(1));
        assertThat(response.shardFailures()[0].index(), is("test-1-remote"));
        assertThat(response.shardFailures()[0].reason(), containsString("NoShardAvailableActionException"));

        // sample query on both shards
        request = new EqlSearchRequest().indices(REMOTE_CLUSTER + ":test-*")
            .query("sample by key [process where value == 2] [process where value == 1]")
            .allowPartialSearchResults(true)
            .allowPartialSequenceResults(true);
        response = localClient().execute(EqlSearchAction.INSTANCE, request).get();
        assertThat(response.hits().sequences().size(), equalTo(0));
        assertThat(response.shardFailures().length, is(1));
        assertThat(response.shardFailures()[0].index(), is("test-1-remote"));
        assertThat(response.shardFailures()[0].reason(), containsString("NoShardAvailableActionException"));

        // sample query on the available shard only
        request = new EqlSearchRequest().indices(REMOTE_CLUSTER + ":test-*")
            .query("sample by key [process where value == 3] [process where value == 1]")
            .allowPartialSearchResults(true)
            .allowPartialSequenceResults(true);
        response = localClient().execute(EqlSearchAction.INSTANCE, request).get();
        assertThat(response.hits().sequences().size(), equalTo(1));
        sample = response.hits().sequences().get(0);
        assertThat(sample.events().get(0).toString(), containsString("\"value\" : 3"));
        assertThat(sample.events().get(1).toString(), containsString("\"value\" : 1"));
        assertThat(response.shardFailures().length, is(1));
        assertThat(response.shardFailures()[0].index(), is("test-1-remote"));
        assertThat(response.shardFailures()[0].reason(), containsString("NoShardAvailableActionException"));

        // sample query on the unavailable shard only
        request = new EqlSearchRequest().indices(REMOTE_CLUSTER + ":test-*")
            .query("sample by key [process where value == 2] [process where value == 0]")
            .allowPartialSearchResults(true)
            .allowPartialSequenceResults(true);
        response = localClient().execute(EqlSearchAction.INSTANCE, request).get();
        assertThat(response.hits().sequences().size(), equalTo(0));
        assertThat(response.shardFailures().length, is(1));
        assertThat(response.shardFailures()[0].index(), is("test-1-remote"));
        assertThat(response.shardFailures()[0].reason(), containsString("NoShardAvailableActionException"));

        // ------------------------------------------------------------------------
        // same queries, with missing shards and allow_partial_search_results=true
        // and default allow_partial_sequence_results (ie. false)
        // ------------------------------------------------------------------------

        // event query
        request = new EqlSearchRequest().indices(REMOTE_CLUSTER + ":test-*").query("process where true").allowPartialSearchResults(true);
        response = localClient().execute(EqlSearchAction.INSTANCE, request).get();
        assertThat(response.hits().events().size(), equalTo(5));
        for (int i = 0; i < 5; i++) {
            assertThat(response.hits().events().get(i).toString(), containsString("\"value\" : " + (i * 2 + 1)));
        }
        assertThat(response.shardFailures().length, is(1));

        // sequence query on both shards
        request = new EqlSearchRequest().indices(REMOTE_CLUSTER + ":test-*")
            .query("sequence [process where value == 1] [process where value == 2]")
            .allowPartialSearchResults(true);
        response = localClient().execute(EqlSearchAction.INSTANCE, request).get();
        assertThat(response.hits().sequences().size(), equalTo(0));
        assertThat(response.shardFailures().length, is(1));
        assertThat(response.shardFailures()[0].index(), is("test-1-remote"));
        assertThat(response.shardFailures()[0].reason(), containsString("NoShardAvailableActionException"));

        // sequence query on the available shard only
        request = new EqlSearchRequest().indices(REMOTE_CLUSTER + ":test-*")
            .query("sequence [process where value == 1] [process where value == 3]")
            .allowPartialSearchResults(true);
        response = localClient().execute(EqlSearchAction.INSTANCE, request).get();
        assertThat(response.hits().sequences().size(), equalTo(0));
        assertThat(response.shardFailures().length, is(1));
        assertThat(response.shardFailures()[0].index(), is("test-1-remote"));
        assertThat(response.shardFailures()[0].reason(), containsString("NoShardAvailableActionException"));

        // sequence query on the unavailable shard only
        request = new EqlSearchRequest().indices(REMOTE_CLUSTER + ":test-*")
            .query("sequence [process where value == 0] [process where value == 2]")
            .allowPartialSearchResults(true);
        response = localClient().execute(EqlSearchAction.INSTANCE, request).get();
        assertThat(response.hits().sequences().size(), equalTo(0));
        assertThat(response.shardFailures().length, is(1));
        assertThat(response.shardFailures()[0].index(), is("test-1-remote"));
        assertThat(response.shardFailures()[0].reason(), containsString("NoShardAvailableActionException"));

        // sequence query with missing event on unavailable shard. THIS IS A FALSE POSITIVE
        request = new EqlSearchRequest().indices(REMOTE_CLUSTER + ":test-*")
            .query("sequence with maxspan=10s  [process where value == 1] ![process where value == 2] [process where value == 3]")
            .allowPartialSearchResults(true);
        response = localClient().execute(EqlSearchAction.INSTANCE, request).get();
        assertThat(response.hits().sequences().size(), equalTo(0));
        assertThat(response.shardFailures().length, is(1));
        assertThat(response.shardFailures()[0].index(), is("test-1-remote"));
        assertThat(response.shardFailures()[0].reason(), containsString("NoShardAvailableActionException"));

        // sample query on both shards
        request = new EqlSearchRequest().indices(REMOTE_CLUSTER + ":test-*")
            .query("sample by key [process where value == 2] [process where value == 1]")
            .allowPartialSearchResults(true);
        response = localClient().execute(EqlSearchAction.INSTANCE, request).get();
        assertThat(response.hits().sequences().size(), equalTo(0));
        assertThat(response.shardFailures().length, is(1));
        assertThat(response.shardFailures()[0].index(), is("test-1-remote"));
        assertThat(response.shardFailures()[0].reason(), containsString("NoShardAvailableActionException"));

        // sample query on the available shard only
        request = new EqlSearchRequest().indices(REMOTE_CLUSTER + ":test-*")
            .query("sample by key [process where value == 3] [process where value == 1]")
            .allowPartialSearchResults(true);
        response = localClient().execute(EqlSearchAction.INSTANCE, request).get();
        assertThat(response.hits().sequences().size(), equalTo(1));
        sample = response.hits().sequences().get(0);
        assertThat(sample.events().get(0).toString(), containsString("\"value\" : 3"));
        assertThat(sample.events().get(1).toString(), containsString("\"value\" : 1"));
        assertThat(response.shardFailures().length, is(1));
        assertThat(response.shardFailures()[0].index(), is("test-1-remote"));
        assertThat(response.shardFailures()[0].reason(), containsString("NoShardAvailableActionException"));

        // sample query on the unavailable shard only
        request = new EqlSearchRequest().indices(REMOTE_CLUSTER + ":test-*")
            .query("sample by key [process where value == 2] [process where value == 0]")
            .allowPartialSearchResults(true);
        response = localClient().execute(EqlSearchAction.INSTANCE, request).get();
        assertThat(response.hits().sequences().size(), equalTo(0));
        assertThat(response.shardFailures().length, is(1));
        assertThat(response.shardFailures()[0].index(), is("test-1-remote"));
        assertThat(response.shardFailures()[0].reason(), containsString("NoShardAvailableActionException"));

        // ------------------------------------------------------------------------
        // same queries, with missing shards and with default xpack.eql.default_allow_partial_results=true
        // ------------------------------------------------------------------------

        cluster(REMOTE_CLUSTER).client().execute(
            ClusterUpdateSettingsAction.INSTANCE,
            new ClusterUpdateSettingsRequest(TimeValue.THIRTY_SECONDS, TimeValue.THIRTY_SECONDS).persistentSettings(
                Settings.builder().put(EqlPlugin.DEFAULT_ALLOW_PARTIAL_SEARCH_RESULTS.getKey(), true)
            )
        ).get();

        // event query
        request = new EqlSearchRequest().indices(REMOTE_CLUSTER + ":test-*").query("process where true");
        response = localClient().execute(EqlSearchAction.INSTANCE, request).get();
        assertThat(response.hits().events().size(), equalTo(5));
        for (int i = 0; i < 5; i++) {
            assertThat(response.hits().events().get(i).toString(), containsString("\"value\" : " + (i * 2 + 1)));
        }
        assertThat(response.shardFailures().length, is(1));

        // sequence query on both shards
        request = new EqlSearchRequest().indices(REMOTE_CLUSTER + ":test-*").query("sequence [process where value == 1] [process where value == 2]");
        response = localClient().execute(EqlSearchAction.INSTANCE, request).get();
        assertThat(response.hits().sequences().size(), equalTo(0));
        assertThat(response.shardFailures().length, is(1));
        assertThat(response.shardFailures()[0].index(), is("test-1-remote"));
        assertThat(response.shardFailures()[0].reason(), containsString("NoShardAvailableActionException"));

        // sequence query on the available shard only
        request = new EqlSearchRequest().indices(REMOTE_CLUSTER + ":test-*").query("sequence [process where value == 1] [process where value == 3]");
        response = localClient().execute(EqlSearchAction.INSTANCE, request).get();
        assertThat(response.hits().sequences().size(), equalTo(0));
        assertThat(response.shardFailures().length, is(1));
        assertThat(response.shardFailures()[0].index(), is("test-1-remote"));
        assertThat(response.shardFailures()[0].reason(), containsString("NoShardAvailableActionException"));

        // sequence query on the unavailable shard only
        request = new EqlSearchRequest().indices(REMOTE_CLUSTER + ":test-*").query("sequence [process where value == 0] [process where value == 2]");
        response = localClient().execute(EqlSearchAction.INSTANCE, request).get();
        assertThat(response.hits().sequences().size(), equalTo(0));
        assertThat(response.shardFailures().length, is(1));
        assertThat(response.shardFailures()[0].index(), is("test-1-remote"));
        assertThat(response.shardFailures()[0].reason(), containsString("NoShardAvailableActionException"));

        // sequence query with missing event on unavailable shard. THIS IS A FALSE POSITIVE
        request = new EqlSearchRequest().indices(REMOTE_CLUSTER + ":test-*")
            .query("sequence with maxspan=10s  [process where value == 1] ![process where value == 2] [process where value == 3]");
        response = localClient().execute(EqlSearchAction.INSTANCE, request).get();
        assertThat(response.hits().sequences().size(), equalTo(0));
        assertThat(response.shardFailures().length, is(1));
        assertThat(response.shardFailures()[0].index(), is("test-1-remote"));
        assertThat(response.shardFailures()[0].reason(), containsString("NoShardAvailableActionException"));

        // sample query on both shards
        request = new EqlSearchRequest().indices(REMOTE_CLUSTER + ":test-*").query("sample by key [process where value == 2] [process where value == 1]");
        response = localClient().execute(EqlSearchAction.INSTANCE, request).get();
        assertThat(response.hits().sequences().size(), equalTo(0));
        assertThat(response.shardFailures().length, is(1));
        assertThat(response.shardFailures()[0].index(), is("test-1-remote"));
        assertThat(response.shardFailures()[0].reason(), containsString("NoShardAvailableActionException"));

        // sample query on the available shard only
        request = new EqlSearchRequest().indices(REMOTE_CLUSTER + ":test-*").query("sample by key [process where value == 3] [process where value == 1]");
        response = localClient().execute(EqlSearchAction.INSTANCE, request).get();
        assertThat(response.hits().sequences().size(), equalTo(1));
        sample = response.hits().sequences().get(0);
        assertThat(sample.events().get(0).toString(), containsString("\"value\" : 3"));
        assertThat(sample.events().get(1).toString(), containsString("\"value\" : 1"));
        assertThat(response.shardFailures().length, is(1));
        assertThat(response.shardFailures()[0].index(), is("test-1-remote"));
        assertThat(response.shardFailures()[0].reason(), containsString("NoShardAvailableActionException"));

        // sample query on the unavailable shard only
        request = new EqlSearchRequest().indices(REMOTE_CLUSTER + ":test-*").query("sample by key [process where value == 2] [process where value == 0]");
        response = localClient().execute(EqlSearchAction.INSTANCE, request).get();
        assertThat(response.hits().sequences().size(), equalTo(0));
        assertThat(response.shardFailures().length, is(1));
        assertThat(response.shardFailures()[0].index(), is("test-1-remote"));
        assertThat(response.shardFailures()[0].reason(), containsString("NoShardAvailableActionException"));

        localClient().execute(
            ClusterUpdateSettingsAction.INSTANCE,
            new ClusterUpdateSettingsRequest(TimeValue.THIRTY_SECONDS, TimeValue.THIRTY_SECONDS).persistentSettings(
                Settings.builder().putNull(EqlPlugin.DEFAULT_ALLOW_PARTIAL_SEARCH_RESULTS.getKey())
            )
        ).get();
    }
}
