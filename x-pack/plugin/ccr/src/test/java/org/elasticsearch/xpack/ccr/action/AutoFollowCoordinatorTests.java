/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ccr.action;

import org.elasticsearch.Version;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
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
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.ccr.Ccr;
import org.elasticsearch.xpack.ccr.CcrLicenseChecker;
import org.elasticsearch.xpack.ccr.CcrSettings;
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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import static org.elasticsearch.xpack.ccr.action.AutoFollowCoordinator.AutoFollower.cleanFollowedRemoteIndices;
import static org.elasticsearch.xpack.ccr.action.AutoFollowCoordinator.AutoFollower.recordLeaderIndexAsFollowFunction;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class AutoFollowCoordinatorTests extends ESTestCase {

    public void testAutoFollower() {
        Client client = mock(Client.class);
        when(client.getRemoteClusterClient(anyString())).thenReturn(client);

        ClusterState remoteState = createRemoteClusterState("logs-20190101", true);

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
        AutoFollower autoFollower = new AutoFollower("remote", handler, localClusterStateSupplier(currentState), () -> 1L, Runnable::run) {
            @Override
            void getRemoteClusterState(String remoteCluster,
                                       long metadataVersion,
                                       BiConsumer<ClusterStateResponse, Exception> handler) {
                assertThat(remoteCluster, equalTo("remote"));
                handler.accept(new ClusterStateResponse(new ClusterName("name"), remoteState, false), null);
            }

            @Override
            void createAndFollow(Map<String, String> headers,
                                 PutFollowAction.Request followRequest,
                                 Runnable successHandler,
                                 Consumer<Exception> failureHandler) {
                assertThat(headers, equalTo(autoFollowHeaders.get("remote")));
                assertThat(followRequest.getRemoteCluster(), equalTo("remote"));
                assertThat(followRequest.getLeaderIndex(), equalTo("logs-20190101"));
                assertThat(followRequest.getFollowerIndex(), equalTo("logs-20190101"));
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

            @Override
            void cleanFollowedRemoteIndices(ClusterState remoteClusterState, List<String> patterns) {
                // Ignore, to avoid invoking updateAutoFollowMetadata(...) twice
            }
        };
        autoFollower.start();
        assertThat(invoked[0], is(true));
    }

    public void testAutoFollowerClusterStateApiFailure() {
        Client client = mock(Client.class);
        when(client.getRemoteClusterClient(anyString())).thenReturn(client);

        AutoFollowPattern autoFollowPattern = new AutoFollowPattern("remote", Collections.singletonList("logs-*"),
                null, null, null, null, null, null, null, null, null, null, null);
        Map<String, AutoFollowPattern> patterns = new HashMap<>();
        patterns.put("remote", autoFollowPattern);
        Map<String, List<String>> followedLeaderIndexUUIDS = new HashMap<>();
        followedLeaderIndexUUIDS.put("remote", new ArrayList<>());
        Map<String, Map<String, String>> headers = new HashMap<>();
        AutoFollowMetadata autoFollowMetadata = new AutoFollowMetadata(patterns, followedLeaderIndexUUIDS, headers);
        ClusterState clusterState = ClusterState.builder(new ClusterName("remote"))
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
        AutoFollower autoFollower = new AutoFollower("remote", handler, localClusterStateSupplier(clusterState), () -> 1L, Runnable::run) {
            @Override
            void getRemoteClusterState(String remoteCluster,
                                       long metadataVersion,
                                       BiConsumer<ClusterStateResponse, Exception> handler) {
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
        autoFollower.start();
        assertThat(invoked[0], is(true));
    }

    public void testAutoFollowerUpdateClusterStateFailure() {
        Client client = mock(Client.class);
        when(client.getRemoteClusterClient(anyString())).thenReturn(client);
        ClusterState remoteState = createRemoteClusterState("logs-20190101", true);

        AutoFollowPattern autoFollowPattern = new AutoFollowPattern("remote", Collections.singletonList("logs-*"),
                null, null, null, null, null, null, null, null, null, null, null);
        Map<String, AutoFollowPattern> patterns = new HashMap<>();
        patterns.put("remote", autoFollowPattern);
        Map<String, List<String>> followedLeaderIndexUUIDS = new HashMap<>();
        followedLeaderIndexUUIDS.put("remote", new ArrayList<>());
        Map<String, Map<String, String>> headers = new HashMap<>();
        AutoFollowMetadata autoFollowMetadata = new AutoFollowMetadata(patterns, followedLeaderIndexUUIDS, headers);
        ClusterState clusterState = ClusterState.builder(new ClusterName("remote"))
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
        AutoFollower autoFollower = new AutoFollower("remote", handler, localClusterStateSupplier(clusterState), () -> 1L, Runnable::run) {
            @Override
            void getRemoteClusterState(String remoteCluster,
                                       long metadataVersion,
                                       BiConsumer<ClusterStateResponse, Exception> handler) {
                handler.accept(new ClusterStateResponse(new ClusterName("name"), remoteState, false), null);
            }

            @Override
            void createAndFollow(Map<String, String> headers,
                                 PutFollowAction.Request followRequest,
                                 Runnable successHandler,
                                 Consumer<Exception> failureHandler) {
                assertThat(followRequest.getRemoteCluster(), equalTo("remote"));
                assertThat(followRequest.getLeaderIndex(), equalTo("logs-20190101"));
                assertThat(followRequest.getFollowerIndex(), equalTo("logs-20190101"));
                successHandler.run();
            }

            @Override
            void updateAutoFollowMetadata(Function<ClusterState, ClusterState> updateFunction, Consumer<Exception> handler) {
                handler.accept(failure);
            }
        };
        autoFollower.start();
        assertThat(invoked[0], is(true));
    }

    public void testAutoFollowerCreateAndFollowApiCallFailure() {
        Client client = mock(Client.class);
        when(client.getRemoteClusterClient(anyString())).thenReturn(client);
        ClusterState remoteState = createRemoteClusterState("logs-20190101", true);

        AutoFollowPattern autoFollowPattern = new AutoFollowPattern("remote", Collections.singletonList("logs-*"),
                null, null, null, null, null, null, null, null, null, null, null);
        Map<String, AutoFollowPattern> patterns = new HashMap<>();
        patterns.put("remote", autoFollowPattern);
        Map<String, List<String>> followedLeaderIndexUUIDS = new HashMap<>();
        followedLeaderIndexUUIDS.put("remote", new ArrayList<>());
        Map<String, Map<String, String>> headers = new HashMap<>();
        AutoFollowMetadata autoFollowMetadata = new AutoFollowMetadata(patterns, followedLeaderIndexUUIDS, headers);
        ClusterState clusterState = ClusterState.builder(new ClusterName("remote"))
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
        AutoFollower autoFollower = new AutoFollower("remote", handler, localClusterStateSupplier(clusterState), () -> 1L, Runnable::run) {
            @Override
            void getRemoteClusterState(String remoteCluster,
                                       long metadataVersion,
                                       BiConsumer<ClusterStateResponse, Exception> handler) {
                handler.accept(new ClusterStateResponse(new ClusterName("name"), remoteState, false), null);
            }

            @Override
            void createAndFollow(Map<String, String> headers,
                                 PutFollowAction.Request followRequest,
                                 Runnable successHandler,
                                 Consumer<Exception> failureHandler) {
                assertThat(followRequest.getRemoteCluster(), equalTo("remote"));
                assertThat(followRequest.getLeaderIndex(), equalTo("logs-20190101"));
                assertThat(followRequest.getFollowerIndex(), equalTo("logs-20190101"));
                failureHandler.accept(failure);
            }

            @Override
            void updateAutoFollowMetadata(Function<ClusterState, ClusterState> updateFunction,
                                          Consumer<Exception> handler) {
                fail("should not get here");
            }

            @Override
            void cleanFollowedRemoteIndices(ClusterState remoteClusterState, List<String> patterns) {
                // Ignore, to avoid invoking updateAutoFollowMetadata(...)
            }
        };
        autoFollower.start();
        assertThat(invoked[0], is(true));
    }

    public void testGetLeaderIndicesToFollow() {
        AutoFollowPattern autoFollowPattern = new AutoFollowPattern("remote", Collections.singletonList("metrics-*"),
            null, null, null, null, null, null, null, null, null, null, null);
        Map<String, Map<String, String>> headers = new HashMap<>();
        ClusterState clusterState = ClusterState.builder(new ClusterName("remote"))
            .metaData(MetaData.builder().putCustom(AutoFollowMetadata.TYPE,
                new AutoFollowMetadata(Collections.singletonMap("remote", autoFollowPattern), Collections.emptyMap(), headers)))
            .build();

        RoutingTable.Builder routingTableBuilder = RoutingTable.builder();
        MetaData.Builder imdBuilder = MetaData.builder();
        for (int i = 0; i < 5; i++) {
            String indexName = "metrics-" + i;
            Settings.Builder builder = Settings.builder()
                .put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT)
                .put(IndexMetaData.SETTING_INDEX_UUID, indexName);
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

        ClusterState remoteState = ClusterState.builder(new ClusterName("remote"))
            .metaData(imdBuilder)
            .routingTable(routingTableBuilder.build())
            .build();

        List<Index> result = AutoFollower.getLeaderIndicesToFollow(autoFollowPattern, remoteState, Collections.emptyList());
        result.sort(Comparator.comparing(Index::getName));
        assertThat(result.size(), equalTo(5));
        assertThat(result.get(0).getName(), equalTo("metrics-0"));
        assertThat(result.get(1).getName(), equalTo("metrics-1"));
        assertThat(result.get(2).getName(), equalTo("metrics-2"));
        assertThat(result.get(3).getName(), equalTo("metrics-3"));
        assertThat(result.get(4).getName(), equalTo("metrics-4"));

        List<String> followedIndexUUIDs = Collections.singletonList(remoteState.metaData().index("metrics-2").getIndexUUID());
        result = AutoFollower.getLeaderIndicesToFollow(autoFollowPattern, remoteState, followedIndexUUIDs);
        result.sort(Comparator.comparing(Index::getName));
        assertThat(result.size(), equalTo(4));
        assertThat(result.get(0).getName(), equalTo("metrics-0"));
        assertThat(result.get(1).getName(), equalTo("metrics-1"));
        assertThat(result.get(2).getName(), equalTo("metrics-3"));
        assertThat(result.get(3).getName(), equalTo("metrics-4"));
    }

    public void testGetLeaderIndicesToFollow_shardsNotStarted() {
        AutoFollowPattern autoFollowPattern = new AutoFollowPattern("remote", Collections.singletonList("*"),
            null, null, null, null, null, null, null, null, null, null, null);
        Map<String, Map<String, String>> headers = new HashMap<>();
        ClusterState clusterState = ClusterState.builder(new ClusterName("remote"))
            .metaData(MetaData.builder().putCustom(AutoFollowMetadata.TYPE,
                new AutoFollowMetadata(Collections.singletonMap("remote", autoFollowPattern), Collections.emptyMap(), headers)))
            .build();

        // 1 shard started and another not started:
        ClusterState remoteState = createRemoteClusterState("index1", true);
        MetaData.Builder mBuilder= MetaData.builder(remoteState.metaData());
        mBuilder.put(IndexMetaData.builder("index2")
            .settings(settings(Version.CURRENT).put(IndexSettings.INDEX_SOFT_DELETES_SETTING.getKey(), true))
            .numberOfShards(1)
            .numberOfReplicas(0));
        ShardRouting shardRouting =
            TestShardRouting.newShardRouting("index2", 0, "1", true, ShardRoutingState.INITIALIZING);
        IndexRoutingTable indexRoutingTable = IndexRoutingTable.builder(mBuilder.get("index2").getIndex()
        ).addShard(shardRouting).build();
        remoteState = ClusterState.builder(remoteState.getClusterName())
            .metaData(mBuilder)
            .routingTable(RoutingTable.builder(remoteState.routingTable()).add(indexRoutingTable).build())
            .build();

        List<Index> result = AutoFollower.getLeaderIndicesToFollow(autoFollowPattern, remoteState, Collections.emptyList());
        assertThat(result.size(), equalTo(1));
        assertThat(result.get(0).getName(), equalTo("index1"));

        // Start second shard:
        shardRouting = shardRouting.moveToStarted();
        indexRoutingTable = IndexRoutingTable.builder(remoteState.metaData().indices().get("index2").getIndex())
            .addShard(shardRouting).build();
        remoteState = ClusterState.builder(remoteState.getClusterName())
            .metaData(remoteState.metaData())
            .routingTable(RoutingTable.builder(remoteState.routingTable()).add(indexRoutingTable).build())
            .build();

        result = AutoFollower.getLeaderIndicesToFollow(autoFollowPattern, remoteState, Collections.emptyList());
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

    public void testCleanFollowedLeaderIndices() {
        AutoFollowMetadata autoFollowMetadata = new AutoFollowMetadata(Collections.emptyMap(),
            Collections.singletonMap("pattern1", Arrays.asList("index1", "index2", "index3")), Collections.emptyMap());
        ClusterState clusterState = new ClusterState.Builder(new ClusterName("name"))
            .metaData(new MetaData.Builder().putCustom(AutoFollowMetadata.TYPE, autoFollowMetadata))
            .build();

        MetaData remoteMetadata = new MetaData.Builder()
            .put(IndexMetaData.builder("index1")
                .settings(settings(Version.CURRENT)
                    .put(IndexSettings.INDEX_SOFT_DELETES_SETTING.getKey(), true)
                    .put(IndexMetaData.SETTING_INDEX_UUID, "index1"))
                .numberOfShards(1)
                .numberOfReplicas(0))
            .put(IndexMetaData.builder("index3")
                .settings(settings(Version.CURRENT)
                    .put(IndexSettings.INDEX_SOFT_DELETES_SETTING.getKey(), true)
                    .put(IndexMetaData.SETTING_INDEX_UUID, "index3"))
                .numberOfShards(1)
                .numberOfReplicas(0))
            .build();

        Function<ClusterState, ClusterState> function = cleanFollowedRemoteIndices(remoteMetadata, Collections.singletonList("pattern1"));
        AutoFollowMetadata result = function.apply(clusterState).metaData().custom(AutoFollowMetadata.TYPE);
        assertThat(result.getFollowedLeaderIndexUUIDs().get("pattern1").size(), equalTo(2));
        assertThat(result.getFollowedLeaderIndexUUIDs().get("pattern1").get(0), equalTo("index1"));
        assertThat(result.getFollowedLeaderIndexUUIDs().get("pattern1").get(1), equalTo("index3"));
    }

    public void testCleanFollowedLeaderIndicesNoChanges() {
        AutoFollowMetadata autoFollowMetadata = new AutoFollowMetadata(Collections.emptyMap(),
            Collections.singletonMap("pattern1", Arrays.asList("index1", "index2", "index3")), Collections.emptyMap());
        ClusterState clusterState = new ClusterState.Builder(new ClusterName("name"))
            .metaData(new MetaData.Builder().putCustom(AutoFollowMetadata.TYPE, autoFollowMetadata))
            .build();

        MetaData remoteMetadata = new MetaData.Builder()
            .put(IndexMetaData.builder("index1")
                .settings(settings(Version.CURRENT)
                    .put(IndexSettings.INDEX_SOFT_DELETES_SETTING.getKey(), true)
                    .put(IndexMetaData.SETTING_INDEX_UUID, "index1"))
                .numberOfShards(1)
                .numberOfReplicas(0))
            .put(IndexMetaData.builder("index2")
                .settings(settings(Version.CURRENT)
                    .put(IndexSettings.INDEX_SOFT_DELETES_SETTING.getKey(), true)
                    .put(IndexMetaData.SETTING_INDEX_UUID, "index2"))
                .numberOfShards(1)
                .numberOfReplicas(0))
            .put(IndexMetaData.builder("index3")
                .settings(settings(Version.CURRENT)
                    .put(IndexSettings.INDEX_SOFT_DELETES_SETTING.getKey(), true)
                    .put(IndexMetaData.SETTING_INDEX_UUID, "index3"))
                .numberOfShards(1)
                .numberOfReplicas(0))
            .build();

        Function<ClusterState, ClusterState> function = cleanFollowedRemoteIndices(remoteMetadata, Collections.singletonList("pattern1"));
        ClusterState result = function.apply(clusterState);
        assertThat(result, sameInstance(clusterState));
    }

    public void testCleanFollowedLeaderIndicesNoEntry() {
        AutoFollowMetadata autoFollowMetadata = new AutoFollowMetadata(Collections.emptyMap(),
            Collections.singletonMap("pattern2", Arrays.asList("index1", "index2", "index3")), Collections.emptyMap());
        ClusterState clusterState = new ClusterState.Builder(new ClusterName("name"))
            .metaData(new MetaData.Builder().putCustom(AutoFollowMetadata.TYPE, autoFollowMetadata))
            .build();

        MetaData remoteMetadata = new MetaData.Builder()
            .put(IndexMetaData.builder("index1")
                .settings(settings(Version.CURRENT).put(IndexSettings.INDEX_SOFT_DELETES_SETTING.getKey(), true))
                .numberOfShards(1)
                .numberOfReplicas(0))
            .build();

        Function<ClusterState, ClusterState> function = cleanFollowedRemoteIndices(remoteMetadata, Collections.singletonList("pattern1"));
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
            Settings.EMPTY,
            null,
            mockClusterService(),
            new CcrLicenseChecker(() -> true, () -> false),
            () -> 1L,
            () -> 1L,
            Runnable::run);

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
        assertThat(autoFollowStats.getRecentAutoFollowErrors().get("_alias1").v2().getCause().getMessage(), equalTo("error"));

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
        assertThat(autoFollowStats.getRecentAutoFollowErrors().get("_alias1").v2().getCause().getMessage(), equalTo("error"));
        assertThat(autoFollowStats.getRecentAutoFollowErrors().get("_alias1:index1").v2().getCause().getMessage(), equalTo("error"));
        assertThat(autoFollowStats.getRecentAutoFollowErrors().get("_alias2:index2").v2().getCause().getMessage(), equalTo("error"));

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
        assertThat(autoFollowStats.getRecentAutoFollowErrors().get("_alias1").v2().getCause().getMessage(), equalTo("error"));
        assertThat(autoFollowStats.getRecentAutoFollowErrors().get("_alias1:index1").v2().getCause().getMessage(), equalTo("error"));
        assertThat(autoFollowStats.getRecentAutoFollowErrors().get("_alias2:index2").v2().getCause().getMessage(), equalTo("error"));
    }

    public void testUpdateAutoFollowers() {
        ClusterService clusterService = mockClusterService();
        // Return a cluster state with no patterns so that the auto followers never really execute:
        ClusterState followerState = ClusterState.builder(new ClusterName("remote"))
            .metaData(MetaData.builder().putCustom(AutoFollowMetadata.TYPE,
                new AutoFollowMetadata(Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap())))
            .build();
        when(clusterService.state()).thenReturn(followerState);
        AutoFollowCoordinator autoFollowCoordinator = new AutoFollowCoordinator(
            Settings.EMPTY,
            null,
            clusterService,
            new CcrLicenseChecker(() -> true, () -> false),
            () -> 1L,
            () -> 1L,
            Runnable::run);
        // Add 3 patterns:
        Map<String, AutoFollowPattern> patterns = new HashMap<>();
        patterns.put("pattern1", new AutoFollowPattern("remote1", Collections.singletonList("logs-*"), null, null, null,
            null, null, null, null, null, null, null, null));
        patterns.put("pattern2", new AutoFollowPattern("remote2", Collections.singletonList("logs-*"), null, null, null,
            null, null, null, null, null, null, null, null));
        patterns.put("pattern3", new AutoFollowPattern("remote2", Collections.singletonList("metrics-*"), null, null, null,
            null, null, null, null, null, null, null, null));
        ClusterState clusterState = ClusterState.builder(new ClusterName("remote"))
            .metaData(MetaData.builder().putCustom(AutoFollowMetadata.TYPE,
                new AutoFollowMetadata(patterns, Collections.emptyMap(), Collections.emptyMap())))
            .build();
        autoFollowCoordinator.updateAutoFollowers(clusterState);
        assertThat(autoFollowCoordinator.getStats().getAutoFollowedClusters().size(), equalTo(2));
        assertThat(autoFollowCoordinator.getStats().getAutoFollowedClusters().get("remote1"), notNullValue());
        assertThat(autoFollowCoordinator.getStats().getAutoFollowedClusters().get("remote2"), notNullValue());
        // Get a reference to auto follower that will get removed, so that we can assert that it has been marked as removed,
        // when pattern 1 and 3 are moved. (To avoid a edge case where multiple auto follow coordinators for the same remote cluster)
        AutoFollowCoordinator.AutoFollower removedAutoFollower1 = autoFollowCoordinator.getAutoFollowers().get("remote1");
        assertThat(removedAutoFollower1.removed, is(false));
        // Remove patterns 1 and 3:
        patterns.remove("pattern1");
        patterns.remove("pattern3");
        clusterState = ClusterState.builder(new ClusterName("remote"))
            .metaData(MetaData.builder().putCustom(AutoFollowMetadata.TYPE,
                new AutoFollowMetadata(patterns, Collections.emptyMap(), Collections.emptyMap())))
            .build();
        autoFollowCoordinator.updateAutoFollowers(clusterState);
        assertThat(autoFollowCoordinator.getStats().getAutoFollowedClusters().size(), equalTo(1));
        assertThat(autoFollowCoordinator.getStats().getAutoFollowedClusters().get("remote2"), notNullValue());
        assertThat(removedAutoFollower1.removed, is(true));
        // Add pattern 4:
        patterns.put("pattern4", new AutoFollowPattern("remote1", Collections.singletonList("metrics-*"), null, null, null,
            null, null, null, null, null, null, null, null));
        clusterState = ClusterState.builder(new ClusterName("remote"))
            .metaData(MetaData.builder().putCustom(AutoFollowMetadata.TYPE,
                new AutoFollowMetadata(patterns, Collections.emptyMap(), Collections.emptyMap())))
            .build();
        autoFollowCoordinator.updateAutoFollowers(clusterState);
        assertThat(autoFollowCoordinator.getStats().getAutoFollowedClusters().size(), equalTo(2));
        assertThat(autoFollowCoordinator.getStats().getAutoFollowedClusters().get("remote1"), notNullValue());
        assertThat(autoFollowCoordinator.getStats().getAutoFollowedClusters().get("remote2"), notNullValue());
        // Get references to auto followers that will get removed, so that we can assert that those have been marked as removed,
        // when pattern 2 and 4 are moved. (To avoid a edge case where multiple auto follow coordinators for the same remote cluster)
        removedAutoFollower1 = autoFollowCoordinator.getAutoFollowers().get("remote1");
        AutoFollower removedAutoFollower2 = autoFollowCoordinator.getAutoFollowers().get("remote2");
        // Remove patterns 2 and 4:
        assertThat(removedAutoFollower1.removed, is(false));
        assertThat(removedAutoFollower2.removed, is(false));
        patterns.remove("pattern2");
        patterns.remove("pattern4");
        clusterState = ClusterState.builder(new ClusterName("remote"))
            .metaData(MetaData.builder().putCustom(AutoFollowMetadata.TYPE,
                new AutoFollowMetadata(patterns, Collections.emptyMap(), Collections.emptyMap())))
            .build();
        autoFollowCoordinator.updateAutoFollowers(clusterState);
        assertThat(autoFollowCoordinator.getStats().getAutoFollowedClusters().size(), equalTo(0));
        assertThat(removedAutoFollower1.removed, is(true));
        assertThat(removedAutoFollower2.removed, is(true));
    }

    public void testUpdateAutoFollowersNoPatterns() {
        AutoFollowCoordinator autoFollowCoordinator = new AutoFollowCoordinator(
            Settings.EMPTY,
            null,
            mockClusterService(),
            new CcrLicenseChecker(() -> true, () -> false),
            () -> 1L,
            () -> 1L,
            Runnable::run);
        ClusterState clusterState = ClusterState.builder(new ClusterName("remote"))
            .metaData(MetaData.builder().putCustom(AutoFollowMetadata.TYPE,
                new AutoFollowMetadata(Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap())))
            .build();
        autoFollowCoordinator.updateAutoFollowers(clusterState);
        assertThat(autoFollowCoordinator.getStats().getAutoFollowedClusters().size(), equalTo(0));
    }

    public void testUpdateAutoFollowersNoAutoFollowMetadata() {
        AutoFollowCoordinator autoFollowCoordinator = new AutoFollowCoordinator(
            Settings.EMPTY,
            null,
            mockClusterService(),
            new CcrLicenseChecker(() -> true, () -> false),
            () -> 1L,
            () -> 1L,
            Runnable::run);
        ClusterState clusterState = ClusterState.builder(new ClusterName("remote")).build();
        autoFollowCoordinator.updateAutoFollowers(clusterState);
        assertThat(autoFollowCoordinator.getStats().getAutoFollowedClusters().size(), equalTo(0));
    }

    public void testWaitForMetadataVersion() {
        Client client = mock(Client.class);
        when(client.getRemoteClusterClient(anyString())).thenReturn(client);

        AutoFollowPattern autoFollowPattern = new AutoFollowPattern("remote", Collections.singletonList("logs-*"),
            null, null, null, null, null, null, null, null, null, null, null);
        Map<String, AutoFollowPattern> patterns = new HashMap<>();
        patterns.put("remote", autoFollowPattern);
        Map<String, List<String>> followedLeaderIndexUUIDS = new HashMap<>();
        followedLeaderIndexUUIDS.put("remote", new ArrayList<>());
        Map<String, Map<String, String>> autoFollowHeaders = new HashMap<>();
        autoFollowHeaders.put("remote", Collections.singletonMap("key", "val"));
        AutoFollowMetadata autoFollowMetadata = new AutoFollowMetadata(patterns, followedLeaderIndexUUIDS, autoFollowHeaders);

        final LinkedList<ClusterState> leaderStates = new LinkedList<>();
        ClusterState[] states = new ClusterState[16];
        for (int i = 0; i < states.length; i++) {
            states[i] = ClusterState.builder(new ClusterName("name"))
                .metaData(MetaData.builder().putCustom(AutoFollowMetadata.TYPE, autoFollowMetadata))
                .build();
            String indexName = "logs-" + i;
            leaderStates.add(i == 0 ? createRemoteClusterState(indexName, true) :
                createRemoteClusterState(leaderStates.get(i - 1), indexName));
        }

        List<AutoFollowCoordinator.AutoFollowResult> allResults = new ArrayList<>();
        Consumer<List<AutoFollowCoordinator.AutoFollowResult>> handler = allResults::addAll;
        AutoFollower autoFollower = new AutoFollower("remote", handler, localClusterStateSupplier(states), () -> 1L, Runnable::run) {

            long previousRequestedMetadataVersion = 0;

            @Override
            void getRemoteClusterState(String remoteCluster,
                                       long metadataVersion,
                                       BiConsumer<ClusterStateResponse, Exception> handler) {
                assertThat(remoteCluster, equalTo("remote"));
                assertThat(metadataVersion, greaterThan(previousRequestedMetadataVersion));
                handler.accept(new ClusterStateResponse(new ClusterName("name"), leaderStates.poll(), false), null);
            }

            @Override
            void createAndFollow(Map<String, String> headers,
                                 PutFollowAction.Request followRequest,
                                 Runnable successHandler,
                                 Consumer<Exception> failureHandler) {
                successHandler.run();
            }

            @Override
            void updateAutoFollowMetadata(Function<ClusterState, ClusterState> updateFunction,
                                          Consumer<Exception> handler) {
                handler.accept(null);
            }
        };
        autoFollower.start();
        assertThat(allResults.size(), equalTo(states.length));
        for (int i = 0; i < states.length; i++) {
            assertThat(allResults.get(i).autoFollowExecutionResults.containsKey(new Index("logs-" + i, "_na_")), is(true));
        }
    }

    public void testWaitForTimeOut() {
        Client client = mock(Client.class);
        when(client.getRemoteClusterClient(anyString())).thenReturn(client);

        AutoFollowPattern autoFollowPattern = new AutoFollowPattern("remote", Collections.singletonList("logs-*"),
            null, null, null, null, null, null, null, null, null, null, null);
        Map<String, AutoFollowPattern> patterns = new HashMap<>();
        patterns.put("remote", autoFollowPattern);
        Map<String, List<String>> followedLeaderIndexUUIDS = new HashMap<>();
        followedLeaderIndexUUIDS.put("remote", new ArrayList<>());
        Map<String, Map<String, String>> autoFollowHeaders = new HashMap<>();
        autoFollowHeaders.put("remote", Collections.singletonMap("key", "val"));
        AutoFollowMetadata autoFollowMetadata = new AutoFollowMetadata(patterns, followedLeaderIndexUUIDS, autoFollowHeaders);

        ClusterState[] states = new ClusterState[16];
        for (int i = 0; i < states.length; i++) {
            states[i] = ClusterState.builder(new ClusterName("name"))
                .metaData(MetaData.builder().putCustom(AutoFollowMetadata.TYPE, autoFollowMetadata))
                .build();
        }
        Consumer<List<AutoFollowCoordinator.AutoFollowResult>> handler = results -> {
            fail("should not be invoked");
        };
        AtomicInteger counter = new AtomicInteger();
        AutoFollower autoFollower = new AutoFollower("remote", handler, localClusterStateSupplier(states), () -> 1L, Runnable::run) {

            long previousRequestedMetadataVersion = 0;

            @Override
            void getRemoteClusterState(String remoteCluster,
                                       long metadataVersion,
                                       BiConsumer<ClusterStateResponse, Exception> handler) {
                counter.incrementAndGet();
                assertThat(remoteCluster, equalTo("remote"));
                assertThat(metadataVersion, greaterThan(previousRequestedMetadataVersion));
                handler.accept(new ClusterStateResponse(new ClusterName("name"), null, true), null);
            }

            @Override
            void createAndFollow(Map<String, String> headers,
                                 PutFollowAction.Request followRequest,
                                 Runnable successHandler,
                                 Consumer<Exception> failureHandler) {
                fail("should not be invoked");
            }

            @Override
            void updateAutoFollowMetadata(Function<ClusterState, ClusterState> updateFunction,
                                          Consumer<Exception> handler) {
                fail("should not be invoked");
            }
        };
        autoFollower.start();
        assertThat(counter.get(), equalTo(states.length));
    }

    public void testAutoFollowerSoftDeletesDisabled() {
        Client client = mock(Client.class);
        when(client.getRemoteClusterClient(anyString())).thenReturn(client);

        ClusterState remoteState = createRemoteClusterState("logs-20190101", false);

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

        List<AutoFollowCoordinator.AutoFollowResult> results = new ArrayList<>();
        Consumer<List<AutoFollowCoordinator.AutoFollowResult>> handler = results::addAll;
        AutoFollower autoFollower = new AutoFollower("remote", handler, localClusterStateSupplier(currentState), () -> 1L, Runnable::run) {
            @Override
            void getRemoteClusterState(String remoteCluster,
                                       long metadataVersion,
                                       BiConsumer<ClusterStateResponse, Exception> handler) {
                assertThat(remoteCluster, equalTo("remote"));
                handler.accept(new ClusterStateResponse(new ClusterName("name"), remoteState, false), null);
            }

            @Override
            void createAndFollow(Map<String, String> headers,
                                 PutFollowAction.Request followRequest,
                                 Runnable successHandler,
                                 Consumer<Exception> failureHandler) {
                fail("soft deletes are disabled; index should not be followed");
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

            @Override
            void cleanFollowedRemoteIndices(ClusterState remoteClusterState, List<String> patterns) {
                // Ignore, to avoid invoking updateAutoFollowMetadata(...) twice
            }
        };
        autoFollower.start();

        assertThat(results.size(), equalTo(1));
        assertThat(results.get(0).clusterStateFetchException, nullValue());
        List<Map.Entry<Index, Exception>> entries = new ArrayList<>(results.get(0).autoFollowExecutionResults.entrySet());
        assertThat(entries.size(), equalTo(1));
        assertThat(entries.get(0).getKey().getName(), equalTo("logs-20190101"));
        assertThat(entries.get(0).getValue(), notNullValue());
        assertThat(entries.get(0).getValue().getMessage(), equalTo("index [logs-20190101] cannot be followed, " +
            "because soft deletes are not enabled"));
    }

    public void testAutoFollowerFollowerIndexAlreadyExists() {
        Client client = mock(Client.class);
        when(client.getRemoteClusterClient(anyString())).thenReturn(client);

        ClusterState remoteState = createRemoteClusterState("logs-20190101", true);

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
            .metaData(MetaData.builder()
                .put(IndexMetaData.builder("logs-20190101")
                    .settings(settings(Version.CURRENT).put(IndexSettings.INDEX_SOFT_DELETES_SETTING.getKey(), true))
                    .putCustom(Ccr.CCR_CUSTOM_METADATA_KEY, Collections.singletonMap(Ccr.CCR_CUSTOM_METADATA_LEADER_INDEX_UUID_KEY,
                        remoteState.metaData().index("logs-20190101").getIndexUUID()))
                    .numberOfShards(1)
                    .numberOfReplicas(0))
                .putCustom(AutoFollowMetadata.TYPE, autoFollowMetadata))
            .build();


        final Object[] resultHolder = new Object[1];
        Consumer<List<AutoFollowCoordinator.AutoFollowResult>> handler = results -> {
            resultHolder[0] = results;
        };
        AutoFollower autoFollower = new AutoFollower("remote", handler, localClusterStateSupplier(currentState), () -> 1L, Runnable::run) {
            @Override
            void getRemoteClusterState(String remoteCluster,
                                       long metadataVersion,
                                       BiConsumer<ClusterStateResponse, Exception> handler) {
                assertThat(remoteCluster, equalTo("remote"));
                handler.accept(new ClusterStateResponse(new ClusterName("name"), remoteState, false), null);
            }

            @Override
            void createAndFollow(Map<String, String> headers,
                                 PutFollowAction.Request followRequest,
                                 Runnable successHandler,
                                 Consumer<Exception> failureHandler) {
                fail("this should not be invoked");
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

            @Override
            void cleanFollowedRemoteIndices(ClusterState remoteClusterState, List<String> patterns) {
                // Ignore, to avoid invoking updateAutoFollowMetadata(...) twice
            }
        };
        autoFollower.start();

        @SuppressWarnings("unchecked")
        List<AutoFollowCoordinator.AutoFollowResult> results = (List<AutoFollowCoordinator.AutoFollowResult>) resultHolder[0];
        assertThat(results, notNullValue());
        assertThat(results.size(), equalTo(1));
        assertThat(results.get(0).clusterStateFetchException, nullValue());
        List<Map.Entry<Index, Exception>> entries = new ArrayList<>(results.get(0).autoFollowExecutionResults.entrySet());
        assertThat(entries.size(), equalTo(1));
        assertThat(entries.get(0).getKey().getName(), equalTo("logs-20190101"));
        assertThat(entries.get(0).getValue(), nullValue());
    }

    /*
     * This tests for a situation where in the face of repeated failures we would be called back on the same thread, and
     * then recurse through the start method again, and eventually stack overflow. Now when we are called back on the
     * same thread, we fork a new thread to avoid this. This test simulates a repeated failure to exercise this logic
     * and ensures that we do not stack overflow. If we did stack overflow, it would go as an uncaught exception and
     * fail the test. We have sufficiently high iterations here to ensure that we would indeed stack overflow were it
     * not for this logic.
     */
    public void testRepeatedFailures() throws InterruptedException {
        final ClusterState clusterState = mock(ClusterState.class);
        final MetaData metaData = mock(MetaData.class);
        when(clusterState.metaData()).thenReturn(metaData);
        final AutoFollowPattern pattern = new AutoFollowPattern(
            "remote",
            List.of("*"),
            "{}",
            0,
            0,
            0,
            0,
            ByteSizeValue.ZERO,
            ByteSizeValue.ZERO,
            0,
            ByteSizeValue.ZERO,
            TimeValue.ZERO,
            TimeValue.ZERO);
        final AutoFollowMetadata autoFollowMetadata = new AutoFollowMetadata(Map.of("remote", pattern), Map.of(), Map.of());
        when(metaData.custom(AutoFollowMetadata.TYPE)).thenReturn(autoFollowMetadata);

        final int iterations = randomIntBetween(16384, 32768); // sufficiently large to exercise that we do not stack overflow
        final AtomicInteger counter = new AtomicInteger();
        final CountDownLatch latch = new CountDownLatch(1);
        final ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            final AutoFollower autoFollower = new AutoFollower("remote", x -> {}, () -> clusterState, () -> 1, executor) {

                @Override
                void getRemoteClusterState(
                    final String remoteCluster,
                    final long metadataVersion,
                    final BiConsumer<ClusterStateResponse, Exception> handler) {
                    counter.incrementAndGet();
                    if (counter.incrementAndGet() > iterations) {
                        this.stop();
                        latch.countDown();
                        /*
                         * Do not call back the handler here, when we unlatch the test thread it will shutdown the
                         * executor which would lead to the execution of the callback facing a rejected execution
                         * exception (from the executor being shutdown).
                         */
                        return;
                    }
                    handler.accept(null, new EsRejectedExecutionException());
                }

                @Override
                void createAndFollow(
                    final Map<String, String> headers,
                    final PutFollowAction.Request followRequest,
                    final Runnable successHandler,
                    final Consumer<Exception> failureHandler) {

                }

                @Override
                void updateAutoFollowMetadata(
                    final Function<ClusterState, ClusterState> updateFunction,
                    final Consumer<Exception> handler) {

                }

            };
            autoFollower.start();
            latch.await();
        } finally {
            executor.shutdown();
        }
    }

    private static ClusterState createRemoteClusterState(String indexName, boolean enableSoftDeletes) {
        Settings.Builder indexSettings;
        indexSettings = settings(Version.CURRENT).put(IndexSettings.INDEX_SOFT_DELETES_SETTING.getKey(), enableSoftDeletes);

        IndexMetaData indexMetaData = IndexMetaData.builder(indexName)
            .settings(indexSettings)
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

    private static ClusterState createRemoteClusterState(ClusterState previous, String indexName) {
        IndexMetaData indexMetaData = IndexMetaData.builder(indexName)
            .settings(settings(Version.CURRENT).put(IndexSettings.INDEX_SOFT_DELETES_SETTING.getKey(), true))
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();
        ClusterState.Builder csBuilder = ClusterState.builder(new ClusterName("remote"))
            .metaData(MetaData.builder(previous.metaData())
                .version(previous.metaData().version() + 1)
                .put(indexMetaData, true));

        ShardRouting shardRouting =
            TestShardRouting.newShardRouting(indexName, 0, "1", true, ShardRoutingState.INITIALIZING).moveToStarted();
        IndexRoutingTable indexRoutingTable = IndexRoutingTable.builder(indexMetaData.getIndex()).addShard(shardRouting).build();
        csBuilder.routingTable(RoutingTable.builder().add(indexRoutingTable).build()).build();

        return csBuilder.build();
    }

    private static Supplier<ClusterState> localClusterStateSupplier(ClusterState... states) {
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

    private ClusterService mockClusterService() {
        ClusterService clusterService = mock(ClusterService.class);
        ClusterSettings clusterSettings =
            new ClusterSettings(Settings.EMPTY, Collections.singleton(CcrSettings.CCR_WAIT_FOR_METADATA_TIMEOUT));
        when(clusterService.getClusterSettings()).thenReturn(clusterSettings);
        return clusterService;
    }

}
