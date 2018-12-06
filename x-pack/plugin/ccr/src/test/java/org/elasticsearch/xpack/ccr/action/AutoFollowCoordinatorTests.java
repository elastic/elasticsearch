/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ccr.action;

import org.elasticsearch.Version;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.TestShardRouting;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.ccr.CcrLicenseChecker;
import org.elasticsearch.xpack.ccr.action.AutoFollowCoordinator.AutoFollower;
import org.elasticsearch.xpack.core.ccr.AutoFollowMetadata;
import org.elasticsearch.xpack.core.ccr.AutoFollowMetadata.AutoFollowPattern;
import org.elasticsearch.xpack.core.ccr.AutoFollowStats;
import org.elasticsearch.xpack.core.ccr.action.PutFollowAction;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import static org.elasticsearch.xpack.ccr.action.AutoFollowCoordinator.AutoFollower.recordLeaderIndexAsFollowFunction;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class AutoFollowCoordinatorTests extends ESTestCase {

    public void testAutoFollower() {
        Client client = mock(Client.class);
        ThreadPool threadPool = mockThreadPool();
        when(client.getRemoteClusterClient(anyString())).thenReturn(client);

        ClusterState leaderState = createRemoteClusterState("logs-20190101");

        AutoFollowPattern autoFollowPattern = new AutoFollowPattern("remote", Collections.singletonList("logs-*"),
            null, null, null, null, null, null, null, null, null, null, null);
        Map<String, AutoFollowPattern> patterns = new HashMap<>();
        patterns.put("remote", autoFollowPattern);
        Map<String, List<String>> followedLeaderIndexUUIDS = new HashMap<>();
        followedLeaderIndexUUIDS.put("remote", new ArrayList<>());
        Map<String, Map<String, String>> autoFollowHeaders = new HashMap<>();
        autoFollowHeaders.put("remote", Collections.singletonMap("key", "val"));
        AutoFollowMetadata autoFollowMetadata = new AutoFollowMetadata(patterns, followedLeaderIndexUUIDS, autoFollowHeaders);

        ClusterState currentState = ClusterState.builder(new ClusterName("name"))
            .metaData(MetaData.builder().putCustom(AutoFollowMetadata.TYPE, autoFollowMetadata))
            .build();

        boolean[] invoked = new boolean[]{false};
        Consumer<List<AutoFollowCoordinator.AutoFollowResult>> handler = results -> {
            invoked[0] = true;

            assertThat(results.size(), equalTo(1));
            assertThat(results.get(0).clusterStateFetchException, nullValue());
            List<Map.Entry<Index, Exception>> entries = new ArrayList<>(results.get(0).autoFollowExecutionResults.entrySet());
            assertThat(entries.size(), equalTo(1));
            assertThat(entries.get(0).getKey().getName(), equalTo("logs-20190101"));
            assertThat(entries.get(0).getValue(), nullValue());
        };
        AutoFollower autoFollower = new AutoFollower("remote", threadPool, handler, followerClusterStateSupplier(currentState)) {
            @Override
            void getLeaderClusterState(String remoteCluster,
                                       BiConsumer<ClusterState, Exception> handler) {
                assertThat(remoteCluster, equalTo("remote"));
                handler.accept(leaderState, null);
            }

            @Override
            void createAndFollow(Map<String, String> headers,
                                 PutFollowAction.Request followRequest,
                                 Runnable successHandler,
                                 Consumer<Exception> failureHandler) {
                assertThat(headers, equalTo(autoFollowHeaders.get("remote")));
                assertThat(followRequest.getRemoteCluster(), equalTo("remote"));
                assertThat(followRequest.getLeaderIndex(), equalTo("logs-20190101"));
                assertThat(followRequest.getFollowRequest().getFollowerIndex(), equalTo("logs-20190101"));
                successHandler.run();
            }

            @Override
            void updateAutoFollowMetadata(Function<ClusterState, ClusterState> updateFunction,
                                          Consumer<Exception> handler) {
                ClusterState resultCs = updateFunction.apply(currentState);
                AutoFollowMetadata result = resultCs.metaData().custom(AutoFollowMetadata.TYPE);
                assertThat(result.getFollowedLeaderIndexUUIDs().size(), equalTo(1));
                assertThat(result.getFollowedLeaderIndexUUIDs().get("remote").size(), equalTo(1));
                handler.accept(null);
            }
        };
        autoFollower.autoFollowIndices();
        assertThat(invoked[0], is(true));
    }

    public void testAutoFollowerClusterStateApiFailure() {
        Client client = mock(Client.class);
        ThreadPool threadPool = mockThreadPool();
        when(client.getRemoteClusterClient(anyString())).thenReturn(client);

        AutoFollowPattern autoFollowPattern = new AutoFollowPattern("remote", Collections.singletonList("logs-*"),
                null, null, null, null, null, null, null, null, null, null, null);
        Map<String, AutoFollowPattern> patterns = new HashMap<>();
        patterns.put("remote", autoFollowPattern);
        Map<String, List<String>> followedLeaderIndexUUIDS = new HashMap<>();
        followedLeaderIndexUUIDS.put("remote", new ArrayList<>());
        Map<String, Map<String, String>> headers = new HashMap<>();
        AutoFollowMetadata autoFollowMetadata = new AutoFollowMetadata(patterns, followedLeaderIndexUUIDS, headers);
        ClusterState followerState = ClusterState.builder(new ClusterName("remote"))
            .metaData(MetaData.builder().putCustom(AutoFollowMetadata.TYPE, autoFollowMetadata))
            .build();

        Exception failure = new RuntimeException("failure");
        boolean[] invoked = new boolean[]{false};
        Consumer<List<AutoFollowCoordinator.AutoFollowResult>> handler = results -> {
            invoked[0] = true;

            assertThat(results.size(), equalTo(1));
            assertThat(results.get(0).clusterStateFetchException, sameInstance(failure));
            assertThat(results.get(0).autoFollowExecutionResults.entrySet().size(), equalTo(0));
        };
        AutoFollower autoFollower = new AutoFollower("remote", threadPool, handler, followerClusterStateSupplier(followerState)) {
            @Override
            void getLeaderClusterState(String remoteCluster,
                                       BiConsumer<ClusterState, Exception> handler) {
                handler.accept(null, failure);
            }

            @Override
            void createAndFollow(Map<String, String> headers,
                                 PutFollowAction.Request followRequest,
                                 Runnable successHandler,
                                 Consumer<Exception> failureHandler) {
                fail("should not get here");
            }

            @Override
            void updateAutoFollowMetadata(Function<ClusterState, ClusterState> updateFunction,
                                          Consumer<Exception> handler) {
                fail("should not get here");
            }
        };
        autoFollower.autoFollowIndices();
        assertThat(invoked[0], is(true));
    }

    public void testAutoFollowerUpdateClusterStateFailure() {
        Client client = mock(Client.class);
        ThreadPool threadPool = mockThreadPool();
        when(client.getRemoteClusterClient(anyString())).thenReturn(client);
        ClusterState leaderState = createRemoteClusterState("logs-20190101");

        AutoFollowPattern autoFollowPattern = new AutoFollowPattern("remote", Collections.singletonList("logs-*"),
                null, null, null, null, null, null, null, null, null, null, null);
        Map<String, AutoFollowPattern> patterns = new HashMap<>();
        patterns.put("remote", autoFollowPattern);
        Map<String, List<String>> followedLeaderIndexUUIDS = new HashMap<>();
        followedLeaderIndexUUIDS.put("remote", new ArrayList<>());
        Map<String, Map<String, String>> headers = new HashMap<>();
        AutoFollowMetadata autoFollowMetadata = new AutoFollowMetadata(patterns, followedLeaderIndexUUIDS, headers);
        ClusterState followerState = ClusterState.builder(new ClusterName("remote"))
            .metaData(MetaData.builder().putCustom(AutoFollowMetadata.TYPE, autoFollowMetadata))
            .build();

        Exception failure = new RuntimeException("failure");
        boolean[] invoked = new boolean[]{false};
        Consumer<List<AutoFollowCoordinator.AutoFollowResult>> handler = results -> {
            invoked[0] = true;

            assertThat(results.size(), equalTo(1));
            assertThat(results.get(0).clusterStateFetchException, nullValue());
            List<Map.Entry<Index, Exception>> entries = new ArrayList<>(results.get(0).autoFollowExecutionResults.entrySet());
            assertThat(entries.size(), equalTo(1));
            assertThat(entries.get(0).getKey().getName(), equalTo("logs-20190101"));
            assertThat(entries.get(0).getValue(), sameInstance(failure));
        };
        AutoFollower autoFollower = new AutoFollower("remote", threadPool, handler, followerClusterStateSupplier(followerState)) {
            @Override
            void getLeaderClusterState(String remoteCluster,
                                       BiConsumer<ClusterState, Exception> handler) {
                handler.accept(leaderState, null);
            }

            @Override
            void createAndFollow(Map<String, String> headers,
                                 PutFollowAction.Request followRequest,
                                 Runnable successHandler,
                                 Consumer<Exception> failureHandler) {
                assertThat(followRequest.getRemoteCluster(), equalTo("remote"));
                assertThat(followRequest.getLeaderIndex(), equalTo("logs-20190101"));
                assertThat(followRequest.getFollowRequest().getFollowerIndex(), equalTo("logs-20190101"));
                successHandler.run();
            }

            @Override
            void updateAutoFollowMetadata(Function<ClusterState, ClusterState> updateFunction, Consumer<Exception> handler) {
                handler.accept(failure);
            }
        };
        autoFollower.autoFollowIndices();
        assertThat(invoked[0], is(true));
    }

    public void testAutoFollowerCreateAndFollowApiCallFailure() {
        Client client = mock(Client.class);
        ThreadPool  threadPool = mockThreadPool();
        when(client.getRemoteClusterClient(anyString())).thenReturn(client);
        ClusterState leaderState = createRemoteClusterState("logs-20190101");

        AutoFollowPattern autoFollowPattern = new AutoFollowPattern("remote", Collections.singletonList("logs-*"),
                null, null, null, null, null, null, null, null, null, null, null);
        Map<String, AutoFollowPattern> patterns = new HashMap<>();
        patterns.put("remote", autoFollowPattern);
        Map<String, List<String>> followedLeaderIndexUUIDS = new HashMap<>();
        followedLeaderIndexUUIDS.put("remote", new ArrayList<>());
        Map<String, Map<String, String>> headers = new HashMap<>();
        AutoFollowMetadata autoFollowMetadata = new AutoFollowMetadata(patterns, followedLeaderIndexUUIDS, headers);
        ClusterState followerState = ClusterState.builder(new ClusterName("remote"))
            .metaData(MetaData.builder().putCustom(AutoFollowMetadata.TYPE, autoFollowMetadata))
            .build();

        Exception failure = new RuntimeException("failure");
        boolean[] invoked = new boolean[]{false};
        Consumer<List<AutoFollowCoordinator.AutoFollowResult>> handler = results -> {
            invoked[0] = true;

            assertThat(results.size(), equalTo(1));
            assertThat(results.get(0).clusterStateFetchException, nullValue());
            List<Map.Entry<Index, Exception>> entries = new ArrayList<>(results.get(0).autoFollowExecutionResults.entrySet());
            assertThat(entries.size(), equalTo(1));
            assertThat(entries.get(0).getKey().getName(), equalTo("logs-20190101"));
            assertThat(entries.get(0).getValue(), sameInstance(failure));
        };
        AutoFollower autoFollower = new AutoFollower("remote", threadPool, handler, followerClusterStateSupplier(followerState)) {
            @Override
            void getLeaderClusterState(String remoteCluster,
                                       BiConsumer<ClusterState, Exception> handler) {
                handler.accept(leaderState, null);
            }

            @Override
            void createAndFollow(Map<String, String> headers,
                                 PutFollowAction.Request followRequest,
                                 Runnable successHandler,
                                 Consumer<Exception> failureHandler) {
                assertThat(followRequest.getRemoteCluster(), equalTo("remote"));
                assertThat(followRequest.getLeaderIndex(), equalTo("logs-20190101"));
                assertThat(followRequest.getFollowRequest().getFollowerIndex(), equalTo("logs-20190101"));
                failureHandler.accept(failure);
            }

            @Override
            void updateAutoFollowMetadata(Function<ClusterState, ClusterState> updateFunction,
                                          Consumer<Exception> handler) {
                fail("should not get here");
            }
        };
        autoFollower.autoFollowIndices();
        assertThat(invoked[0], is(true));
    }

    public void testGetLeaderIndicesToFollow() {
        AutoFollowPattern autoFollowPattern = new AutoFollowPattern("remote", Collections.singletonList("metrics-*"),
            null, null, null, null, null, null, null, null, null, null, null);
        Map<String, Map<String, String>> headers = new HashMap<>();
        ClusterState followerState = ClusterState.builder(new ClusterName("remote"))
            .metaData(MetaData.builder().putCustom(AutoFollowMetadata.TYPE,
                new AutoFollowMetadata(Collections.singletonMap("remote", autoFollowPattern), Collections.emptyMap(), headers)))
            .build();

        RoutingTable.Builder routingTableBuilder = RoutingTable.builder();
        MetaData.Builder imdBuilder = MetaData.builder();
        for (int i = 0; i < 5; i++) {
            String indexName = "metrics-" + i;
            Settings.Builder builder = Settings.builder()
                .put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT)
                .put(IndexMetaData.SETTING_INDEX_UUID, indexName)
                .put(IndexSettings.INDEX_SOFT_DELETES_SETTING.getKey(), i % 2 == 0);
            imdBuilder.put(IndexMetaData.builder("metrics-" + i)
                .settings(builder)
                .numberOfShards(1)
                .numberOfReplicas(0));

            ShardRouting shardRouting =
                TestShardRouting.newShardRouting(indexName, 0, "1", true, ShardRoutingState.INITIALIZING).moveToStarted();
            IndexRoutingTable indexRoutingTable = IndexRoutingTable.builder(imdBuilder.get(indexName).getIndex())
                .addShard(shardRouting)
                .build();
            routingTableBuilder.add(indexRoutingTable);
        }

        imdBuilder.put(IndexMetaData.builder("logs-0")
            .settings(settings(Version.CURRENT))
            .numberOfShards(1)
            .numberOfReplicas(0));
        ShardRouting shardRouting =
            TestShardRouting.newShardRouting("logs-0", 0, "1", true, ShardRoutingState.INITIALIZING).moveToStarted();
        IndexRoutingTable indexRoutingTable = IndexRoutingTable.builder(imdBuilder.get("logs-0").getIndex()).addShard(shardRouting).build();
        routingTableBuilder.add(indexRoutingTable);

        ClusterState leaderState = ClusterState.builder(new ClusterName("remote"))
            .metaData(imdBuilder)
            .routingTable(routingTableBuilder.build())
            .build();

        List<Index> result = AutoFollower.getLeaderIndicesToFollow(autoFollowPattern, leaderState, followerState,
            Collections.emptyList());
        result.sort(Comparator.comparing(Index::getName));
        assertThat(result.size(), equalTo(3));
        assertThat(result.get(0).getName(), equalTo("metrics-0"));
        assertThat(result.get(1).getName(), equalTo("metrics-2"));
        assertThat(result.get(2).getName(), equalTo("metrics-4"));

        List<String> followedIndexUUIDs = Collections.singletonList(leaderState.metaData().index("metrics-2").getIndexUUID());
        result = AutoFollower.getLeaderIndicesToFollow(autoFollowPattern, leaderState, followerState, followedIndexUUIDs);
        result.sort(Comparator.comparing(Index::getName));
        assertThat(result.size(), equalTo(2));
        assertThat(result.get(0).getName(), equalTo("metrics-0"));
        assertThat(result.get(1).getName(), equalTo("metrics-4"));
    }

    public void testGetLeaderIndicesToFollow_shardsNotStarted() {
        AutoFollowPattern autoFollowPattern = new AutoFollowPattern("remote", Collections.singletonList("*"),
            null, null, null, null, null, null, null, null, null, null, null);
        Map<String, Map<String, String>> headers = new HashMap<>();
        ClusterState followerState = ClusterState.builder(new ClusterName("remote"))
            .metaData(MetaData.builder().putCustom(AutoFollowMetadata.TYPE,
                new AutoFollowMetadata(Collections.singletonMap("remote", autoFollowPattern), Collections.emptyMap(), headers)))
            .build();

        // 1 shard started and another not started:
        ClusterState leaderState = createRemoteClusterState("index1");
        MetaData.Builder mBuilder= MetaData.builder(leaderState.metaData());
        mBuilder.put(IndexMetaData.builder("index2")
            .settings(settings(Version.CURRENT).put(IndexSettings.INDEX_SOFT_DELETES_SETTING.getKey(), true))
            .numberOfShards(1)
            .numberOfReplicas(0));
        ShardRouting shardRouting =
            TestShardRouting.newShardRouting("index2", 0, "1", true, ShardRoutingState.INITIALIZING);
        IndexRoutingTable indexRoutingTable = IndexRoutingTable.builder(mBuilder.get("index2").getIndex()
        ).addShard(shardRouting).build();
        leaderState = ClusterState.builder(leaderState.getClusterName())
            .metaData(mBuilder)
            .routingTable(RoutingTable.builder(leaderState.routingTable()).add(indexRoutingTable).build())
            .build();

        List<Index> result = AutoFollower.getLeaderIndicesToFollow(autoFollowPattern, leaderState, followerState,
            Collections.emptyList());
        assertThat(result.size(), equalTo(1));
        assertThat(result.get(0).getName(), equalTo("index1"));

        // Start second shard:
        shardRouting = shardRouting.moveToStarted();
        indexRoutingTable = IndexRoutingTable.builder(leaderState.metaData().indices().get("index2").getIndex())
            .addShard(shardRouting).build();
        leaderState = ClusterState.builder(leaderState.getClusterName())
            .metaData(leaderState.metaData())
            .routingTable(RoutingTable.builder(leaderState.routingTable()).add(indexRoutingTable).build())
            .build();

        result = AutoFollower.getLeaderIndicesToFollow(autoFollowPattern, leaderState, followerState, Collections.emptyList());
        assertThat(result.size(), equalTo(2));
        result.sort(Comparator.comparing(Index::getName));
        assertThat(result.get(0).getName(), equalTo("index1"));
        assertThat(result.get(1).getName(), equalTo("index2"));
    }

    public void testRecordLeaderIndexAsFollowFunction() {
        AutoFollowMetadata autoFollowMetadata = new AutoFollowMetadata(Collections.emptyMap(),
            Collections.singletonMap("pattern1", Collections.emptyList()), Collections.emptyMap());
        ClusterState clusterState = new ClusterState.Builder(new ClusterName("name"))
            .metaData(new MetaData.Builder().putCustom(AutoFollowMetadata.TYPE, autoFollowMetadata))
            .build();
        Function<ClusterState, ClusterState> function = recordLeaderIndexAsFollowFunction("pattern1", new Index("index1", "index1"));

        ClusterState result = function.apply(clusterState);
        AutoFollowMetadata autoFollowMetadataResult = result.metaData().custom(AutoFollowMetadata.TYPE);
        assertThat(autoFollowMetadataResult.getFollowedLeaderIndexUUIDs().get("pattern1"), notNullValue());
        assertThat(autoFollowMetadataResult.getFollowedLeaderIndexUUIDs().get("pattern1").size(), equalTo(1));
        assertThat(autoFollowMetadataResult.getFollowedLeaderIndexUUIDs().get("pattern1").get(0), equalTo("index1"));
    }

    public void testRecordLeaderIndexAsFollowFunctionNoEntry() {
        AutoFollowMetadata autoFollowMetadata = new AutoFollowMetadata(Collections.emptyMap(), Collections.emptyMap(),
            Collections.emptyMap());
        ClusterState clusterState = new ClusterState.Builder(new ClusterName("name"))
            .metaData(new MetaData.Builder().putCustom(AutoFollowMetadata.TYPE, autoFollowMetadata))
            .build();
        Function<ClusterState, ClusterState> function = recordLeaderIndexAsFollowFunction("pattern1", new Index("index1", "index1"));

        ClusterState result = function.apply(clusterState);
        assertThat(result, sameInstance(clusterState));
    }

    public void testGetFollowerIndexName() {
        AutoFollowPattern autoFollowPattern = new AutoFollowPattern("remote", Collections.singletonList("metrics-*"), null, null,
            null, null, null, null, null, null, null, null, null);
        assertThat(AutoFollower.getFollowerIndexName(autoFollowPattern, "metrics-0"), equalTo("metrics-0"));

        autoFollowPattern = new AutoFollowPattern("remote", Collections.singletonList("metrics-*"), "eu-metrics-0", null, null,
            null, null, null, null, null, null, null, null);
        assertThat(AutoFollower.getFollowerIndexName(autoFollowPattern, "metrics-0"), equalTo("eu-metrics-0"));

        autoFollowPattern = new AutoFollowPattern("remote", Collections.singletonList("metrics-*"), "eu-{{leader_index}}", null,
            null, null, null, null, null, null, null, null, null);
        assertThat(AutoFollower.getFollowerIndexName(autoFollowPattern, "metrics-0"), equalTo("eu-metrics-0"));
    }

    public void testStats() {
        AutoFollowCoordinator autoFollowCoordinator = new AutoFollowCoordinator(
            null,
            null,
            mock(ClusterService.class),
            new CcrLicenseChecker(() -> true, () -> false)
        );

        autoFollowCoordinator.updateStats(Collections.singletonList(
            new AutoFollowCoordinator.AutoFollowResult("_alias1"))
        );
        AutoFollowStats autoFollowStats = autoFollowCoordinator.getStats();
        assertThat(autoFollowStats.getNumberOfFailedFollowIndices(), equalTo(0L));
        assertThat(autoFollowStats.getNumberOfFailedRemoteClusterStateRequests(), equalTo(0L));
        assertThat(autoFollowStats.getNumberOfSuccessfulFollowIndices(), equalTo(0L));
        assertThat(autoFollowStats.getRecentAutoFollowErrors().size(), equalTo(0));

        autoFollowCoordinator.updateStats(Collections.singletonList(
            new AutoFollowCoordinator.AutoFollowResult("_alias1", new RuntimeException("error")))
        );
        autoFollowStats = autoFollowCoordinator.getStats();
        assertThat(autoFollowStats.getNumberOfFailedFollowIndices(), equalTo(0L));
        assertThat(autoFollowStats.getNumberOfFailedRemoteClusterStateRequests(), equalTo(1L));
        assertThat(autoFollowStats.getNumberOfSuccessfulFollowIndices(), equalTo(0L));
        assertThat(autoFollowStats.getRecentAutoFollowErrors().size(), equalTo(1));
        assertThat(autoFollowStats.getRecentAutoFollowErrors().get("_alias1").getCause().getMessage(), equalTo("error"));

        autoFollowCoordinator.updateStats(Arrays.asList(
            new AutoFollowCoordinator.AutoFollowResult("_alias1",
                Collections.singletonList(Tuple.tuple(new Index("index1", "_na_"), new RuntimeException("error")))),
            new AutoFollowCoordinator.AutoFollowResult("_alias2",
                Collections.singletonList(Tuple.tuple(new Index("index2", "_na_"), new RuntimeException("error"))))
        ));
        autoFollowStats = autoFollowCoordinator.getStats();
        assertThat(autoFollowStats.getNumberOfFailedFollowIndices(), equalTo(2L));
        assertThat(autoFollowStats.getNumberOfFailedRemoteClusterStateRequests(), equalTo(1L));
        assertThat(autoFollowStats.getNumberOfSuccessfulFollowIndices(), equalTo(0L));
        assertThat(autoFollowStats.getRecentAutoFollowErrors().size(), equalTo(3));
        assertThat(autoFollowStats.getRecentAutoFollowErrors().get("_alias1").getCause().getMessage(), equalTo("error"));
        assertThat(autoFollowStats.getRecentAutoFollowErrors().get("_alias1:index1").getCause().getMessage(), equalTo("error"));
        assertThat(autoFollowStats.getRecentAutoFollowErrors().get("_alias2:index2").getCause().getMessage(), equalTo("error"));

        autoFollowCoordinator.updateStats(Arrays.asList(
            new AutoFollowCoordinator.AutoFollowResult("_alias1",
                Collections.singletonList(Tuple.tuple(new Index("index1", "_na_"), null))),
            new AutoFollowCoordinator.AutoFollowResult("_alias2",
                Collections.singletonList(Tuple.tuple(new Index("index2", "_na_"), null)))
        ));
        autoFollowStats = autoFollowCoordinator.getStats();
        assertThat(autoFollowStats.getNumberOfFailedFollowIndices(), equalTo(2L));
        assertThat(autoFollowStats.getNumberOfFailedRemoteClusterStateRequests(), equalTo(1L));
        assertThat(autoFollowStats.getNumberOfSuccessfulFollowIndices(), equalTo(2L));
        assertThat(autoFollowStats.getRecentAutoFollowErrors().size(), equalTo(3));
        assertThat(autoFollowStats.getRecentAutoFollowErrors().get("_alias1").getCause().getMessage(), equalTo("error"));
        assertThat(autoFollowStats.getRecentAutoFollowErrors().get("_alias1:index1").getCause().getMessage(), equalTo("error"));
        assertThat(autoFollowStats.getRecentAutoFollowErrors().get("_alias2:index2").getCause().getMessage(), equalTo("error"));
    }

    private static ClusterState createRemoteClusterState(String indexName) {
        IndexMetaData indexMetaData = IndexMetaData.builder(indexName)
            .settings(settings(Version.CURRENT).put(IndexSettings.INDEX_SOFT_DELETES_SETTING.getKey(), true))
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();
        ClusterState.Builder csBuilder = ClusterState.builder(new ClusterName("remote"))
            .metaData(MetaData.builder().put(indexMetaData, true));

        ShardRouting shardRouting =
            TestShardRouting.newShardRouting(indexName, 0, "1", true, ShardRoutingState.INITIALIZING).moveToStarted();
        IndexRoutingTable indexRoutingTable = IndexRoutingTable.builder(indexMetaData.getIndex()).addShard(shardRouting).build();
        csBuilder.routingTable(RoutingTable.builder().add(indexRoutingTable).build()).build();

        return csBuilder.build();
    }

    private static Supplier<ClusterState> followerClusterStateSupplier(ClusterState... states) {
        final AutoFollowMetadata emptyAutoFollowMetadata =
            new AutoFollowMetadata(Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap());
        final ClusterState lastState = ClusterState.builder(new ClusterName("remote"))
            .metaData(MetaData.builder().putCustom(AutoFollowMetadata.TYPE, emptyAutoFollowMetadata))
            .build();
        final LinkedList<ClusterState> queue = new LinkedList<>(Arrays.asList(states));
        return () -> {
            final ClusterState current = queue.poll();
            if (current != null) {
                return current;
            } else {
                return lastState;
            }
        };
    }

    private static ThreadPool mockThreadPool() {
        ThreadPool threadPool = mock(ThreadPool.class);
        doAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            Runnable task = (Runnable) args[2];
            task.run();
            return null;
        }).when(threadPool).schedule(any(), anyString(), any());
        return threadPool;
    }

}
