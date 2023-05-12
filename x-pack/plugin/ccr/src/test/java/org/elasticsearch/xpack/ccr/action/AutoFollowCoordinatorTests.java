/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ccr.action;

import org.elasticsearch.Version;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.action.support.replication.ClusterStateCreationUtils;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.DataStreamTestHelper;
import org.elasticsearch.cluster.metadata.IndexAbstraction;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.TestShardRouting;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.VersionUtils;
import org.elasticsearch.xpack.ccr.Ccr;
import org.elasticsearch.xpack.ccr.CcrLicenseChecker;
import org.elasticsearch.xpack.ccr.CcrSettings;
import org.elasticsearch.xpack.ccr.action.AutoFollowCoordinator.AutoFollower;
import org.elasticsearch.xpack.core.ccr.AutoFollowMetadata;
import org.elasticsearch.xpack.core.ccr.AutoFollowMetadata.AutoFollowPattern;
import org.elasticsearch.xpack.core.ccr.AutoFollowStats;
import org.elasticsearch.xpack.core.ccr.action.ActivateAutoFollowPatternAction;
import org.elasticsearch.xpack.core.ccr.action.PutAutoFollowPatternAction;
import org.elasticsearch.xpack.core.ccr.action.PutFollowAction;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.elasticsearch.xpack.ccr.action.AutoFollowCoordinator.AutoFollower.cleanFollowedRemoteIndices;
import static org.elasticsearch.xpack.ccr.action.AutoFollowCoordinator.AutoFollower.recordLeaderIndexAsFollowFunction;
import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.matchesPattern;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.hamcrest.Matchers.startsWith;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class AutoFollowCoordinatorTests extends ESTestCase {

    public void testAutoFollower() {
        Client client = mock(Client.class);
        when(client.getRemoteClusterClient(anyString())).thenReturn(client);

        ClusterState remoteState = createRemoteClusterState("logs-20190101", true);

        AutoFollowPattern autoFollowPattern = createAutoFollowPattern("remote", "logs-*");
        Map<String, AutoFollowPattern> patterns = new HashMap<>();
        patterns.put("remote", autoFollowPattern);
        Map<String, List<String>> followedLeaderIndexUUIDS = new HashMap<>();
        followedLeaderIndexUUIDS.put("remote", new ArrayList<>());
        Map<String, Map<String, String>> autoFollowHeaders = new HashMap<>();
        autoFollowHeaders.put("remote", Map.of("key", "val"));
        AutoFollowMetadata autoFollowMetadata = new AutoFollowMetadata(patterns, followedLeaderIndexUUIDS, autoFollowHeaders);

        ClusterState currentState = ClusterState.builder(new ClusterName("name"))
            .metadata(Metadata.builder().putCustom(AutoFollowMetadata.TYPE, autoFollowMetadata))
            .build();

        boolean[] invoked = new boolean[] { false };
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
            void getRemoteClusterState(String remoteCluster, long metadataVersion, BiConsumer<ClusterStateResponse, Exception> handler) {
                assertThat(remoteCluster, equalTo("remote"));
                handler.accept(new ClusterStateResponse(new ClusterName("name"), remoteState, false), null);
            }

            @Override
            void createAndFollow(
                Map<String, String> headers,
                PutFollowAction.Request followRequest,
                Runnable successHandler,
                Consumer<Exception> failureHandler
            ) {
                assertThat(headers, equalTo(autoFollowHeaders.get("remote")));
                assertThat(followRequest.getRemoteCluster(), equalTo("remote"));
                assertThat(followRequest.getLeaderIndex(), equalTo("logs-20190101"));
                assertThat(followRequest.getFollowerIndex(), equalTo("logs-20190101"));
                assertThat(followRequest.masterNodeTimeout(), equalTo(TimeValue.MAX_VALUE));
                successHandler.run();
            }

            @Override
            void updateAutoFollowMetadata(Function<ClusterState, ClusterState> updateFunction, Consumer<Exception> handler) {
                ClusterState resultCs = updateFunction.apply(currentState);
                AutoFollowMetadata result = resultCs.metadata().custom(AutoFollowMetadata.TYPE);
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

    public void testAutoFollower_dataStream() {
        Client client = mock(Client.class);
        when(client.getRemoteClusterClient(anyString())).thenReturn(client);

        ClusterState remoteState = createRemoteClusterStateWithDataStream("logs-foobar");

        AutoFollowPattern autoFollowPattern = createAutoFollowPattern("remote", "logs-*");
        Map<String, AutoFollowPattern> patterns = new HashMap<>();
        patterns.put("remote", autoFollowPattern);
        Map<String, List<String>> followedLeaderIndexUUIDS = new HashMap<>();
        followedLeaderIndexUUIDS.put("remote", new ArrayList<>());
        Map<String, Map<String, String>> autoFollowHeaders = new HashMap<>();
        autoFollowHeaders.put("remote", Map.of("key", "val"));
        AutoFollowMetadata autoFollowMetadata = new AutoFollowMetadata(patterns, followedLeaderIndexUUIDS, autoFollowHeaders);

        ClusterState currentState = ClusterState.builder(new ClusterName("name"))
            .metadata(Metadata.builder().putCustom(AutoFollowMetadata.TYPE, autoFollowMetadata))
            .build();

        boolean[] invoked = new boolean[] { false };
        Consumer<List<AutoFollowCoordinator.AutoFollowResult>> handler = results -> {
            invoked[0] = true;

            assertThat(results.size(), equalTo(1));
            assertThat(results.get(0).clusterStateFetchException, nullValue());
            List<Map.Entry<Index, Exception>> entries = new ArrayList<>(results.get(0).autoFollowExecutionResults.entrySet());
            assertThat(entries.size(), equalTo(1));
            assertThat(entries.get(0).getKey().getName(), matchesPattern(DataStreamTestHelper.backingIndexPattern("logs-foobar", 1)));
            assertThat(entries.get(0).getValue(), nullValue());
        };
        AutoFollower autoFollower = new AutoFollower("remote", handler, localClusterStateSupplier(currentState), () -> 1L, Runnable::run) {
            @Override
            void getRemoteClusterState(String remoteCluster, long metadataVersion, BiConsumer<ClusterStateResponse, Exception> handler) {
                assertThat(remoteCluster, equalTo("remote"));
                handler.accept(new ClusterStateResponse(new ClusterName("name"), remoteState, false), null);
            }

            @Override
            void createAndFollow(
                Map<String, String> headers,
                PutFollowAction.Request followRequest,
                Runnable successHandler,
                Consumer<Exception> failureHandler
            ) {
                assertThat(headers, equalTo(autoFollowHeaders.get("remote")));
                assertThat(followRequest.getRemoteCluster(), equalTo("remote"));
                assertThat(followRequest.getLeaderIndex(), matchesPattern(DataStreamTestHelper.backingIndexPattern("logs-foobar", 1)));
                assertThat(followRequest.getFollowerIndex(), matchesPattern(DataStreamTestHelper.backingIndexPattern("logs-foobar", 1)));
                assertThat(followRequest.masterNodeTimeout(), equalTo(TimeValue.MAX_VALUE));
                successHandler.run();
            }

            @Override
            void updateAutoFollowMetadata(Function<ClusterState, ClusterState> updateFunction, Consumer<Exception> handler) {
                ClusterState resultCs = updateFunction.apply(currentState);
                AutoFollowMetadata result = resultCs.metadata().custom(AutoFollowMetadata.TYPE);
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

        AutoFollowPattern autoFollowPattern = createAutoFollowPattern("remote", "logs-*");
        Map<String, AutoFollowPattern> patterns = new HashMap<>();
        patterns.put("remote", autoFollowPattern);
        Map<String, List<String>> followedLeaderIndexUUIDS = new HashMap<>();
        followedLeaderIndexUUIDS.put("remote", new ArrayList<>());
        Map<String, Map<String, String>> headers = new HashMap<>();
        AutoFollowMetadata autoFollowMetadata = new AutoFollowMetadata(patterns, followedLeaderIndexUUIDS, headers);
        ClusterState clusterState = ClusterState.builder(new ClusterName("remote"))
            .metadata(Metadata.builder().putCustom(AutoFollowMetadata.TYPE, autoFollowMetadata))
            .build();

        Exception failure = new RuntimeException("failure");
        boolean[] invoked = new boolean[] { false };
        Consumer<List<AutoFollowCoordinator.AutoFollowResult>> handler = results -> {
            invoked[0] = true;

            assertThat(results.size(), equalTo(1));
            assertThat(results.get(0).clusterStateFetchException, sameInstance(failure));
            assertThat(results.get(0).autoFollowExecutionResults.entrySet().size(), equalTo(0));
        };
        AutoFollower autoFollower = new AutoFollower("remote", handler, localClusterStateSupplier(clusterState), () -> 1L, Runnable::run) {
            @Override
            void getRemoteClusterState(String remoteCluster, long metadataVersion, BiConsumer<ClusterStateResponse, Exception> handler) {
                handler.accept(null, failure);
            }

            @Override
            void createAndFollow(
                Map<String, String> headers,
                PutFollowAction.Request followRequest,
                Runnable successHandler,
                Consumer<Exception> failureHandler
            ) {
                fail("should not get here");
            }

            @Override
            void updateAutoFollowMetadata(Function<ClusterState, ClusterState> updateFunction, Consumer<Exception> handler) {
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

        AutoFollowPattern autoFollowPattern = createAutoFollowPattern("remote", "logs-*");
        Map<String, AutoFollowPattern> patterns = new HashMap<>();
        patterns.put("remote", autoFollowPattern);
        Map<String, List<String>> followedLeaderIndexUUIDS = new HashMap<>();
        followedLeaderIndexUUIDS.put("remote", new ArrayList<>());
        Map<String, Map<String, String>> headers = new HashMap<>();
        AutoFollowMetadata autoFollowMetadata = new AutoFollowMetadata(patterns, followedLeaderIndexUUIDS, headers);
        ClusterState clusterState = ClusterState.builder(new ClusterName("remote"))
            .metadata(Metadata.builder().putCustom(AutoFollowMetadata.TYPE, autoFollowMetadata))
            .build();

        Exception failure = new RuntimeException("failure");
        boolean[] invoked = new boolean[] { false };
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
            void getRemoteClusterState(String remoteCluster, long metadataVersion, BiConsumer<ClusterStateResponse, Exception> handler) {
                handler.accept(new ClusterStateResponse(new ClusterName("name"), remoteState, false), null);
            }

            @Override
            void createAndFollow(
                Map<String, String> headers,
                PutFollowAction.Request followRequest,
                Runnable successHandler,
                Consumer<Exception> failureHandler
            ) {
                assertThat(followRequest.getRemoteCluster(), equalTo("remote"));
                assertThat(followRequest.getLeaderIndex(), equalTo("logs-20190101"));
                assertThat(followRequest.getFollowerIndex(), equalTo("logs-20190101"));
                assertThat(followRequest.masterNodeTimeout(), equalTo(TimeValue.MAX_VALUE));
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

    public void testAutoFollowerWithNoActivePatternsDoesNotStart() {
        final String remoteCluster = randomAlphaOfLength(5);

        final Map<String, AutoFollowPattern> autoFollowPatterns = Maps.newMapWithExpectedSize(2);
        autoFollowPatterns.put(
            "pattern_1",
            new AutoFollowPattern(
                remoteCluster,
                List.of("logs-*", "test-*"),
                Collections.emptyList(),
                "copy-",
                Settings.EMPTY,
                false,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null
            )
        );
        autoFollowPatterns.put(
            "pattern_2",
            new AutoFollowPattern(
                remoteCluster,
                List.of("users-*"),
                Collections.emptyList(),
                "copy-",
                Settings.EMPTY,
                false,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null
            )
        );

        final Map<String, List<String>> followedLeaderIndexUUIDs = Maps.newMapWithExpectedSize(2);
        followedLeaderIndexUUIDs.put("pattern_1", List.of("uuid1", "uuid2"));
        followedLeaderIndexUUIDs.put("pattern_2", Collections.emptyList());

        final Map<String, Map<String, String>> headers = Maps.newMapWithExpectedSize(2);
        headers.put("pattern_1", singletonMap("header", "value"));
        headers.put("pattern_2", emptyMap());

        final Supplier<ClusterState> followerClusterStateSupplier = localClusterStateSupplier(
            ClusterState.builder(new ClusterName("test"))
                .metadata(
                    Metadata.builder()
                        .putCustom(AutoFollowMetadata.TYPE, new AutoFollowMetadata(autoFollowPatterns, followedLeaderIndexUUIDs, headers))
                        .build()
                )
                .build()
        );

        final AtomicBoolean invoked = new AtomicBoolean(false);
        final AutoFollower autoFollower = new AutoFollower(
            remoteCluster,
            v -> invoked.set(true),
            followerClusterStateSupplier,
            () -> 1L,
            Runnable::run
        ) {
            @Override
            void getRemoteClusterState(String remote, long metadataVersion, BiConsumer<ClusterStateResponse, Exception> handler) {
                invoked.set(true);
            }

            @Override
            void createAndFollow(
                Map<String, String> headers,
                PutFollowAction.Request request,
                Runnable successHandler,
                Consumer<Exception> failureHandler
            ) {
                invoked.set(true);
                successHandler.run();
            }

            @Override
            void updateAutoFollowMetadata(Function<ClusterState, ClusterState> updateFunction, Consumer<Exception> handler) {
                invoked.set(true);
            }
        };

        autoFollower.start();
        assertThat(invoked.get(), is(false));
    }

    public void testAutoFollowerWithPausedActivePatterns() {
        final String remoteCluster = randomAlphaOfLength(5);

        final AtomicReference<ClusterState> remoteClusterState = new AtomicReference<>(
            createRemoteClusterState("patternLogs-0", true, randomLongBetween(1L, 1_000L))
        );

        final AtomicReference<ClusterState> localClusterState = new AtomicReference<>(
            ClusterState.builder(new ClusterName("local"))
                .metadata(Metadata.builder().putCustom(AutoFollowMetadata.TYPE, new AutoFollowMetadata(emptyMap(), emptyMap(), emptyMap())))
                .build()
        );

        // compute and return the local cluster state, updated with some auto-follow patterns
        final Supplier<ClusterState> localClusterStateSupplier = () -> localClusterState.updateAndGet(currentLocalState -> {
            final int nextClusterStateVersion = (int) (currentLocalState.version() + 1);

            final ClusterState nextLocalClusterState;
            if (nextClusterStateVersion == 1) {
                // cluster state #1 : one pattern is active
                PutAutoFollowPatternAction.Request request = new PutAutoFollowPatternAction.Request();
                request.setName("patternLogs");
                request.setRemoteCluster(remoteCluster);
                request.setLeaderIndexPatterns(singletonList("patternLogs-*"));
                request.setFollowIndexNamePattern("copy-{{leader_index}}");
                nextLocalClusterState = TransportPutAutoFollowPatternAction.innerPut(
                    request,
                    emptyMap(),
                    currentLocalState,
                    remoteClusterState.get()
                );

            } else if (nextClusterStateVersion == 2) {
                // cluster state #2 : still one pattern is active
                nextLocalClusterState = currentLocalState;

            } else if (nextClusterStateVersion == 3) {
                // cluster state #3 : add a new pattern, two patterns are active
                PutAutoFollowPatternAction.Request request = new PutAutoFollowPatternAction.Request();
                request.setName("patternDocs");
                request.setRemoteCluster(remoteCluster);
                request.setLeaderIndexPatterns(singletonList("patternDocs-*"));
                request.setFollowIndexNamePattern("copy-{{leader_index}}");
                nextLocalClusterState = TransportPutAutoFollowPatternAction.innerPut(
                    request,
                    emptyMap(),
                    currentLocalState,
                    remoteClusterState.get()
                );

            } else if (nextClusterStateVersion == 4) {
                // cluster state #4 : still both patterns are active
                nextLocalClusterState = currentLocalState;

            } else if (nextClusterStateVersion == 5) {
                // cluster state #5 : first pattern is paused, second pattern is still active
                ActivateAutoFollowPatternAction.Request request = new ActivateAutoFollowPatternAction.Request("patternLogs", false);
                nextLocalClusterState = TransportActivateAutoFollowPatternAction.innerActivate(request, currentLocalState);

            } else if (nextClusterStateVersion == 6) {
                // cluster state #5 : second pattern is paused, both patterns are inactive
                ActivateAutoFollowPatternAction.Request request = new ActivateAutoFollowPatternAction.Request("patternDocs", false);
                nextLocalClusterState = TransportActivateAutoFollowPatternAction.innerActivate(request, currentLocalState);

            } else {
                return currentLocalState;
            }

            return ClusterState.builder(nextLocalClusterState).version(nextClusterStateVersion).build();
        });

        final Set<String> followedIndices = ConcurrentCollections.newConcurrentSet();
        final List<AutoFollowCoordinator.AutoFollowResult> autoFollowResults = new ArrayList<>();

        final AutoFollower autoFollower = new AutoFollower(
            remoteCluster,
            autoFollowResults::addAll,
            localClusterStateSupplier,
            () -> 1L,
            Runnable::run
        ) {

            int countFetches = 1; // to be aligned with local cluster state updates
            ClusterState lastFetchedRemoteClusterState;

            @Override
            void getRemoteClusterState(String remote, long metadataVersion, BiConsumer<ClusterStateResponse, Exception> handler) {
                assertThat(remote, equalTo(remoteCluster));

                // in this test, every time it fetches the remote cluster state new leader indices to follow appears
                final String[] newLeaderIndices = { "patternLogs-" + countFetches, "patternDocs-" + countFetches };

                if (countFetches == 1) {
                    assertThat("first invocation, it should retrieve the metadata version 1", metadataVersion, equalTo(1L));
                    lastFetchedRemoteClusterState = createRemoteClusterState(remoteClusterState.get(), newLeaderIndices);

                } else if (countFetches == 2 || countFetches == 4) {
                    assertThat(
                        "no patterns changes, it should retrieve the last known metadata version + 1",
                        metadataVersion,
                        equalTo(lastFetchedRemoteClusterState.metadata().version() + 1)
                    );
                    lastFetchedRemoteClusterState = createRemoteClusterState(remoteClusterState.get(), newLeaderIndices);
                    assertThat(
                        "remote cluster state metadata version is aligned with what the auto-follower is requesting",
                        lastFetchedRemoteClusterState.getMetadata().version(),
                        equalTo(metadataVersion)
                    );

                } else if (countFetches == 3 || countFetches == 5) {
                    assertThat(
                        "patterns have changed, it should retrieve the last known metadata version again",
                        metadataVersion,
                        equalTo(lastFetchedRemoteClusterState.metadata().version())
                    );
                    lastFetchedRemoteClusterState = createRemoteClusterState(remoteClusterState.get(), newLeaderIndices);
                    assertThat(
                        "remote cluster state metadata version is incremented",
                        lastFetchedRemoteClusterState.getMetadata().version(),
                        equalTo(metadataVersion + 1)
                    );
                } else {
                    fail("after the 5th invocation there are no more active patterns, the auto-follower should have stopped");
                }

                countFetches = countFetches + 1;
                remoteClusterState.set(lastFetchedRemoteClusterState);
                handler.accept(
                    new ClusterStateResponse(lastFetchedRemoteClusterState.getClusterName(), lastFetchedRemoteClusterState, false),
                    null
                );
            }

            @Override
            void createAndFollow(
                Map<String, String> headers,
                PutFollowAction.Request request,
                Runnable successHandler,
                Consumer<Exception> failureHandler
            ) {
                assertThat(request.getRemoteCluster(), equalTo(remoteCluster));
                assertThat(request.masterNodeTimeout(), equalTo(TimeValue.MAX_VALUE));
                assertThat(request.getFollowerIndex(), startsWith("copy-"));
                followedIndices.add(request.getLeaderIndex());
                successHandler.run();
            }

            @Override
            void updateAutoFollowMetadata(Function<ClusterState, ClusterState> updateFunction, Consumer<Exception> handler) {
                localClusterState.updateAndGet(updateFunction::apply);
                handler.accept(null);
            }

            @Override
            void cleanFollowedRemoteIndices(ClusterState remoteClusterState, List<String> patterns) {
                // Ignore, to avoid invoking updateAutoFollowMetadata(...) twice
            }
        };

        autoFollower.start();

        assertThat(autoFollowResults.size(), equalTo(7));
        assertThat(
            followedIndices,
            containsInAnyOrder(
                "patternLogs-1", // iteration #1 : only pattern "patternLogs" is active in local cluster state
                "patternLogs-2", // iteration #2 : only pattern "patternLogs" is active in local cluster state
                "patternLogs-3", // iteration #3 : both patterns "patternLogs" and "patternDocs" are active in local cluster state
                "patternDocs-3", //
                "patternLogs-4", // iteration #4 : both patterns "patternLogs" and "patternDocs" are active in local cluster state
                "patternDocs-4", //
                "patternDocs-5"  // iteration #5 : only pattern "patternDocs" is active in local cluster state, "patternLogs" is paused
            )
        );

        final ClusterState finalRemoteClusterState = remoteClusterState.get();
        final ClusterState finalLocalClusterState = localClusterState.get();

        AutoFollowMetadata autoFollowMetadata = finalLocalClusterState.metadata().custom(AutoFollowMetadata.TYPE);
        assertThat(autoFollowMetadata.getPatterns().size(), equalTo(2));
        assertThat(autoFollowMetadata.getPatterns().values().stream().noneMatch(AutoFollowPattern::isActive), is(true));

        assertThat(
            autoFollowMetadata.getFollowedLeaderIndexUUIDs().get("patternLogs"),
            containsInAnyOrder(
                finalRemoteClusterState.metadata().index("patternLogs-0").getIndexUUID(),
                finalRemoteClusterState.metadata().index("patternLogs-1").getIndexUUID(),
                finalRemoteClusterState.metadata().index("patternLogs-2").getIndexUUID(),
                finalRemoteClusterState.metadata().index("patternLogs-3").getIndexUUID(),
                finalRemoteClusterState.metadata().index("patternLogs-4").getIndexUUID()
                // patternLogs-5 exists in remote cluster state but patternLogs was paused
            )
        );

        assertThat(
            autoFollowMetadata.getFollowedLeaderIndexUUIDs().get("patternDocs"),
            containsInAnyOrder(
                // patternDocs-0 does not exist in remote cluster state
                finalRemoteClusterState.metadata().index("patternDocs-1").getIndexUUID(),
                finalRemoteClusterState.metadata().index("patternDocs-2").getIndexUUID(),
                finalRemoteClusterState.metadata().index("patternDocs-3").getIndexUUID(),
                finalRemoteClusterState.metadata().index("patternDocs-4").getIndexUUID(),
                finalRemoteClusterState.metadata().index("patternDocs-5").getIndexUUID()
            )
        );
    }

    public void testAutoFollowerCreateAndFollowApiCallFailure() {
        Client client = mock(Client.class);
        when(client.getRemoteClusterClient(anyString())).thenReturn(client);
        ClusterState remoteState = createRemoteClusterState("logs-20190101", true);

        AutoFollowPattern autoFollowPattern = createAutoFollowPattern("remote", "logs-*");
        Map<String, AutoFollowPattern> patterns = new HashMap<>();
        patterns.put("remote", autoFollowPattern);
        Map<String, List<String>> followedLeaderIndexUUIDS = new HashMap<>();
        followedLeaderIndexUUIDS.put("remote", new ArrayList<>());
        Map<String, Map<String, String>> headers = new HashMap<>();
        AutoFollowMetadata autoFollowMetadata = new AutoFollowMetadata(patterns, followedLeaderIndexUUIDS, headers);
        ClusterState clusterState = ClusterState.builder(new ClusterName("remote"))
            .metadata(Metadata.builder().putCustom(AutoFollowMetadata.TYPE, autoFollowMetadata))
            .build();

        Exception failure = new RuntimeException("failure");
        boolean[] invoked = new boolean[] { false };
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
            void getRemoteClusterState(String remoteCluster, long metadataVersion, BiConsumer<ClusterStateResponse, Exception> handler) {
                handler.accept(new ClusterStateResponse(new ClusterName("name"), remoteState, false), null);
            }

            @Override
            void createAndFollow(
                Map<String, String> headers,
                PutFollowAction.Request followRequest,
                Runnable successHandler,
                Consumer<Exception> failureHandler
            ) {
                assertThat(followRequest.getRemoteCluster(), equalTo("remote"));
                assertThat(followRequest.masterNodeTimeout(), equalTo(TimeValue.MAX_VALUE));
                assertThat(followRequest.getLeaderIndex(), equalTo("logs-20190101"));
                assertThat(followRequest.getFollowerIndex(), equalTo("logs-20190101"));
                failureHandler.accept(failure);
            }

            @Override
            void updateAutoFollowMetadata(Function<ClusterState, ClusterState> updateFunction, Consumer<Exception> handler) {
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
        final AutoFollowPattern autoFollowPattern = createAutoFollowPattern("remote", "metrics-*");

        RoutingTable.Builder routingTableBuilder = RoutingTable.builder();
        Metadata.Builder imdBuilder = Metadata.builder();
        for (int i = 0; i < 5; i++) {
            String indexName = "metrics-" + i;
            Settings.Builder builder = Settings.builder()
                .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                .put(IndexMetadata.SETTING_INDEX_UUID, indexName);
            imdBuilder.put(IndexMetadata.builder(indexName).settings(builder).numberOfShards(1).numberOfReplicas(0));

            ShardRouting shardRouting = TestShardRouting.newShardRouting(
                new ShardId(imdBuilder.get(indexName).getIndex(), 0),
                "1",
                true,
                ShardRoutingState.INITIALIZING
            ).moveToStarted(ShardRouting.UNAVAILABLE_EXPECTED_SHARD_SIZE);
            IndexRoutingTable indexRoutingTable = IndexRoutingTable.builder(imdBuilder.get(indexName).getIndex())
                .addShard(shardRouting)
                .build();
            routingTableBuilder.add(indexRoutingTable);
        }

        imdBuilder.put(IndexMetadata.builder("logs-0").settings(settings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(0));
        ShardRouting shardRouting = TestShardRouting.newShardRouting("logs-0", 0, "1", true, ShardRoutingState.INITIALIZING)
            .moveToStarted(ShardRouting.UNAVAILABLE_EXPECTED_SHARD_SIZE);
        IndexRoutingTable indexRoutingTable = IndexRoutingTable.builder(imdBuilder.get("logs-0").getIndex()).addShard(shardRouting).build();
        routingTableBuilder.add(indexRoutingTable);

        ClusterState remoteState = ClusterState.builder(new ClusterName("remote"))
            .metadata(imdBuilder)
            .routingTable(routingTableBuilder.build())
            .build();

        List<Index> result = AutoFollower.getLeaderIndicesToFollow(autoFollowPattern, remoteState, Collections.emptyList());
        result.sort(Index.COMPARE_BY_NAME);
        assertThat(result.size(), equalTo(5));
        assertThat(result.get(0).getName(), equalTo("metrics-0"));
        assertThat(result.get(1).getName(), equalTo("metrics-1"));
        assertThat(result.get(2).getName(), equalTo("metrics-2"));
        assertThat(result.get(3).getName(), equalTo("metrics-3"));
        assertThat(result.get(4).getName(), equalTo("metrics-4"));

        final List<String> followedIndexUUIDs = Collections.singletonList(remoteState.metadata().index("metrics-2").getIndexUUID());
        result = AutoFollower.getLeaderIndicesToFollow(autoFollowPattern, remoteState, followedIndexUUIDs);
        result.sort(Index.COMPARE_BY_NAME);
        assertThat(result.size(), equalTo(4));
        assertThat(result.get(0).getName(), equalTo("metrics-0"));
        assertThat(result.get(1).getName(), equalTo("metrics-1"));
        assertThat(result.get(2).getName(), equalTo("metrics-3"));
        assertThat(result.get(3).getName(), equalTo("metrics-4"));

        final AutoFollowPattern inactiveAutoFollowPattern = new AutoFollowPattern(
            "remote",
            Collections.singletonList("metrics-*"),
            Collections.emptyList(),
            null,
            Settings.EMPTY,
            false,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null
        );

        result = AutoFollower.getLeaderIndicesToFollow(inactiveAutoFollowPattern, remoteState, Collections.emptyList());
        assertThat(result.size(), equalTo(0));

        result = AutoFollower.getLeaderIndicesToFollow(inactiveAutoFollowPattern, remoteState, followedIndexUUIDs);
        assertThat(result.size(), equalTo(0));
    }

    public void testGetLeaderIndicesToFollow_shardsNotStarted() {
        AutoFollowPattern autoFollowPattern = createAutoFollowPattern("remote", "*");

        // 1 shard started and another not started:
        ClusterState remoteState = createRemoteClusterState("index1", true);
        Metadata.Builder mBuilder = Metadata.builder(remoteState.metadata());
        mBuilder.put(IndexMetadata.builder("index2").settings(settings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(0));
        ShardRouting shardRouting = TestShardRouting.newShardRouting("index2", 0, "1", true, ShardRoutingState.INITIALIZING);
        IndexRoutingTable indexRoutingTable = IndexRoutingTable.builder(mBuilder.get("index2").getIndex()).addShard(shardRouting).build();
        remoteState = ClusterState.builder(remoteState.getClusterName())
            .metadata(mBuilder)
            .routingTable(RoutingTable.builder(remoteState.routingTable()).add(indexRoutingTable).build())
            .build();

        List<Index> result = AutoFollower.getLeaderIndicesToFollow(autoFollowPattern, remoteState, Collections.emptyList());
        assertThat(result.size(), equalTo(1));
        assertThat(result.get(0).getName(), equalTo("index1"));

        // Start second shard:
        shardRouting = shardRouting.moveToStarted(ShardRouting.UNAVAILABLE_EXPECTED_SHARD_SIZE);
        indexRoutingTable = IndexRoutingTable.builder(remoteState.metadata().indices().get("index2").getIndex())
            .addShard(shardRouting)
            .build();
        remoteState = ClusterState.builder(remoteState.getClusterName())
            .metadata(remoteState.metadata())
            .routingTable(RoutingTable.builder(remoteState.routingTable()).add(indexRoutingTable).build())
            .build();

        result = AutoFollower.getLeaderIndicesToFollow(autoFollowPattern, remoteState, Collections.emptyList());
        assertThat(result.size(), equalTo(2));
        result.sort(Index.COMPARE_BY_NAME);
        assertThat(result.get(0).getName(), equalTo("index1"));
        assertThat(result.get(1).getName(), equalTo("index2"));
    }

    public void testGetLeaderIndicesToFollowWithClosedIndices() {
        final AutoFollowPattern autoFollowPattern = createAutoFollowPattern("remote", "*");

        // index is opened
        ClusterState remoteState = ClusterStateCreationUtils.stateWithActivePrimary("test-index", true, randomIntBetween(1, 3), 0);
        List<Index> result = AutoFollower.getLeaderIndicesToFollow(autoFollowPattern, remoteState, Collections.emptyList());
        assertThat(result.size(), equalTo(1));
        assertThat(result, hasItem(remoteState.metadata().index("test-index").getIndex()));

        // index is closed
        remoteState = ClusterState.builder(remoteState)
            .metadata(
                Metadata.builder(remoteState.metadata())
                    .put(IndexMetadata.builder(remoteState.metadata().index("test-index")).state(IndexMetadata.State.CLOSE).build(), true)
                    .build()
            )
            .build();
        result = AutoFollower.getLeaderIndicesToFollow(autoFollowPattern, remoteState, Collections.emptyList());
        assertThat(result.size(), equalTo(0));
    }

    public void testRecordLeaderIndexAsFollowFunction() {
        AutoFollowMetadata autoFollowMetadata = new AutoFollowMetadata(
            Collections.emptyMap(),
            Map.of("pattern1", Collections.emptyList()),
            Collections.emptyMap()
        );
        ClusterState clusterState = new ClusterState.Builder(new ClusterName("name")).metadata(
            new Metadata.Builder().putCustom(AutoFollowMetadata.TYPE, autoFollowMetadata)
        ).build();
        Function<ClusterState, ClusterState> function = recordLeaderIndexAsFollowFunction("pattern1", new Index("index1", "index1"));

        ClusterState result = function.apply(clusterState);
        AutoFollowMetadata autoFollowMetadataResult = result.metadata().custom(AutoFollowMetadata.TYPE);
        assertThat(autoFollowMetadataResult.getFollowedLeaderIndexUUIDs().get("pattern1"), notNullValue());
        assertThat(autoFollowMetadataResult.getFollowedLeaderIndexUUIDs().get("pattern1").size(), equalTo(1));
        assertThat(autoFollowMetadataResult.getFollowedLeaderIndexUUIDs().get("pattern1").get(0), equalTo("index1"));
    }

    public void testRecordLeaderIndexAsFollowFunctionNoEntry() {
        AutoFollowMetadata autoFollowMetadata = new AutoFollowMetadata(
            Collections.emptyMap(),
            Collections.emptyMap(),
            Collections.emptyMap()
        );
        ClusterState clusterState = new ClusterState.Builder(new ClusterName("name")).metadata(
            new Metadata.Builder().putCustom(AutoFollowMetadata.TYPE, autoFollowMetadata)
        ).build();
        Function<ClusterState, ClusterState> function = recordLeaderIndexAsFollowFunction("pattern1", new Index("index1", "index1"));

        ClusterState result = function.apply(clusterState);
        assertThat(result, sameInstance(clusterState));
    }

    public void testCleanFollowedLeaderIndices() {
        AutoFollowMetadata autoFollowMetadata = new AutoFollowMetadata(
            Collections.emptyMap(),
            Map.of("pattern1", Arrays.asList("index1", "index2", "index3")),
            Collections.emptyMap()
        );
        ClusterState clusterState = new ClusterState.Builder(new ClusterName("name")).metadata(
            new Metadata.Builder().putCustom(AutoFollowMetadata.TYPE, autoFollowMetadata)
        ).build();

        Metadata remoteMetadata = new Metadata.Builder().put(
            IndexMetadata.builder("index1")
                .settings(settings(Version.CURRENT).put(IndexMetadata.SETTING_INDEX_UUID, "index1"))
                .numberOfShards(1)
                .numberOfReplicas(0)
        )
            .put(
                IndexMetadata.builder("index3")
                    .settings(settings(Version.CURRENT).put(IndexMetadata.SETTING_INDEX_UUID, "index3"))
                    .numberOfShards(1)
                    .numberOfReplicas(0)
            )
            .build();

        Function<ClusterState, ClusterState> function = cleanFollowedRemoteIndices(remoteMetadata, Collections.singletonList("pattern1"));
        AutoFollowMetadata result = function.apply(clusterState).metadata().custom(AutoFollowMetadata.TYPE);
        assertThat(result.getFollowedLeaderIndexUUIDs().get("pattern1").size(), equalTo(2));
        assertThat(result.getFollowedLeaderIndexUUIDs().get("pattern1").get(0), equalTo("index1"));
        assertThat(result.getFollowedLeaderIndexUUIDs().get("pattern1").get(1), equalTo("index3"));
    }

    public void testCleanFollowedLeaderIndicesNoChanges() {
        AutoFollowMetadata autoFollowMetadata = new AutoFollowMetadata(
            Collections.emptyMap(),
            Map.of("pattern1", Arrays.asList("index1", "index2", "index3")),
            Collections.emptyMap()
        );
        ClusterState clusterState = new ClusterState.Builder(new ClusterName("name")).metadata(
            new Metadata.Builder().putCustom(AutoFollowMetadata.TYPE, autoFollowMetadata)
        ).build();

        Metadata remoteMetadata = new Metadata.Builder().put(
            IndexMetadata.builder("index1")
                .settings(settings(Version.CURRENT).put(IndexMetadata.SETTING_INDEX_UUID, "index1"))
                .numberOfShards(1)
                .numberOfReplicas(0)
        )
            .put(
                IndexMetadata.builder("index2")
                    .settings(settings(Version.CURRENT).put(IndexMetadata.SETTING_INDEX_UUID, "index2"))
                    .numberOfShards(1)
                    .numberOfReplicas(0)
            )
            .put(
                IndexMetadata.builder("index3")
                    .settings(settings(Version.CURRENT).put(IndexMetadata.SETTING_INDEX_UUID, "index3"))
                    .numberOfShards(1)
                    .numberOfReplicas(0)
            )
            .build();

        Function<ClusterState, ClusterState> function = cleanFollowedRemoteIndices(remoteMetadata, Collections.singletonList("pattern1"));
        ClusterState result = function.apply(clusterState);
        assertThat(result, sameInstance(clusterState));
    }

    public void testCleanFollowedLeaderIndicesNoEntry() {
        AutoFollowMetadata autoFollowMetadata = new AutoFollowMetadata(
            Collections.emptyMap(),
            Map.of("pattern2", Arrays.asList("index1", "index2", "index3")),
            Collections.emptyMap()
        );
        ClusterState clusterState = new ClusterState.Builder(new ClusterName("name")).metadata(
            new Metadata.Builder().putCustom(AutoFollowMetadata.TYPE, autoFollowMetadata)
        ).build();

        Metadata remoteMetadata = new Metadata.Builder().put(
            IndexMetadata.builder("index1").settings(settings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(0)
        ).build();

        Function<ClusterState, ClusterState> function = cleanFollowedRemoteIndices(remoteMetadata, Collections.singletonList("pattern1"));
        ClusterState result = function.apply(clusterState);
        assertThat(result, sameInstance(clusterState));
    }

    public void testGetFollowerIndexName() {
        AutoFollowPattern autoFollowPattern = createAutoFollowPattern("remote", "metrics-*");

        assertThat(AutoFollower.getFollowerIndexName(autoFollowPattern, "metrics-0"), equalTo("metrics-0"));

        autoFollowPattern = new AutoFollowPattern(
            "remote",
            Collections.singletonList("metrics-*"),
            Collections.emptyList(),
            "eu-metrics-0",
            Settings.EMPTY,
            true,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null
        );
        assertThat(AutoFollower.getFollowerIndexName(autoFollowPattern, "metrics-0"), equalTo("eu-metrics-0"));

        autoFollowPattern = new AutoFollowPattern(
            "remote",
            Collections.singletonList("metrics-*"),
            Collections.emptyList(),
            "eu-{{leader_index}}",
            Settings.EMPTY,
            true,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null
        );
        assertThat(AutoFollower.getFollowerIndexName(autoFollowPattern, "metrics-0"), equalTo("eu-metrics-0"));

        // Test that index of data stream type name works correctly:
        autoFollowPattern = new AutoFollowPattern(
            "remote",
            List.of("logs-*"),
            List.of(),
            "{{leader_index}}_copy",
            Settings.EMPTY,
            true,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null
        );
        assertThat(
            AutoFollower.getFollowerIndexName(autoFollowPattern, ".ds-logs-foo-bar-2022-02-01-123456"),
            equalTo(".ds-logs-foo-bar_copy-2022-02-01-123456")
        );

        autoFollowPattern = new AutoFollowPattern(
            "remote",
            List.of("logs-*"),
            List.of(),
            "prepend_{{leader_index}}",
            Settings.EMPTY,
            true,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null
        );
        assertThat(
            AutoFollower.getFollowerIndexName(autoFollowPattern, ".ds-logs-foo-bar-2022-02-01-123456"),
            equalTo(".ds-prepend_logs-foo-bar-2022-02-01-123456")
        );

    }

    public void testGenerateRequest() {
        // Renaming with a suffix and normal pattern backing indices
        {
            AutoFollowPattern pattern = new AutoFollowPattern(
                "remote",
                List.of("logs-*"),
                List.of(),
                "{{leader_index}}_copy",
                Settings.EMPTY,
                true,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null
            );

            Index index = new Index(".ds-logs-foo-bar-2022-02-01-123456", "uuid");
            IndexAbstraction indexAbstraction = new IndexAbstraction.ConcreteIndex(
                IndexMetadata.builder(index.getName())
                    .settings(
                        Settings.builder()
                            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
                            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                            .put(IndexMetadata.SETTING_INDEX_UUID, index.getUUID())
                            .build()
                    )
                    .build(),
                new DataStream("logs-foo-bar", List.of(index), 1, Map.of(), false, false, false, true, IndexMode.STANDARD)
            );

            PutFollowAction.Request request = AutoFollower.generateRequest("remote", index, indexAbstraction, pattern);
            assertThat(request.getRemoteCluster(), equalTo("remote"));
            assertThat(request.getFollowerIndex(), equalTo(".ds-logs-foo-bar_copy-2022-02-01-123456"));
            assertThat(request.getLeaderIndex(), equalTo(".ds-logs-foo-bar-2022-02-01-123456"));
            assertThat(request.getDataStreamName(), equalTo("logs-foo-bar_copy"));
        }

        // Renaming with a prefix and normal pattern backing indices
        {
            AutoFollowPattern pattern = new AutoFollowPattern(
                "remote",
                List.of("logs-*"),
                List.of(),
                "copy_{{leader_index}}",
                Settings.EMPTY,
                true,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null
            );

            Index index = new Index(".ds-logs-foo-bar-2022-02-01-123456", "uuid");
            IndexAbstraction indexAbstraction = new IndexAbstraction.ConcreteIndex(
                IndexMetadata.builder(index.getName())
                    .settings(
                        Settings.builder()
                            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
                            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                            .put(IndexMetadata.SETTING_INDEX_UUID, index.getUUID())
                            .build()
                    )
                    .build(),
                new DataStream("logs-foo-bar", List.of(index), 1, Map.of(), false, false, false, true, IndexMode.STANDARD)
            );

            PutFollowAction.Request request = AutoFollower.generateRequest("remote", index, indexAbstraction, pattern);
            assertThat(request.getRemoteCluster(), equalTo("remote"));
            assertThat(request.getFollowerIndex(), equalTo(".ds-copy_logs-foo-bar-2022-02-01-123456"));
            assertThat(request.getLeaderIndex(), equalTo(".ds-logs-foo-bar-2022-02-01-123456"));
            assertThat(request.getDataStreamName(), equalTo("copy_logs-foo-bar"));
        }

        // Renaming with a suffix and irregular pattern backing indices
        {
            AutoFollowPattern pattern = new AutoFollowPattern(
                "remote",
                List.of("logs-*"),
                List.of(),
                "{{leader_index}}_copy",
                Settings.EMPTY,
                true,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null
            );

            Index index = new Index("my-backing-index", "uuid");
            IndexAbstraction indexAbstraction = new IndexAbstraction.ConcreteIndex(
                IndexMetadata.builder(index.getName())
                    .settings(
                        Settings.builder()
                            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
                            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                            .put(IndexMetadata.SETTING_INDEX_UUID, index.getUUID())
                            .build()
                    )
                    .build(),
                new DataStream("logs-foo-bar", List.of(index), 1, Map.of(), false, false, false, true, IndexMode.STANDARD)
            );

            PutFollowAction.Request request = AutoFollower.generateRequest("remote", index, indexAbstraction, pattern);
            assertThat(request.getRemoteCluster(), equalTo("remote"));
            assertThat(request.getFollowerIndex(), equalTo("my-backing-index_copy"));
            assertThat(request.getLeaderIndex(), equalTo("my-backing-index"));
            assertThat(request.getDataStreamName(), equalTo("logs-foo-bar_copy"));
        }

        // Renaming with a suffix but not part of a data stream
        {
            AutoFollowPattern pattern = new AutoFollowPattern(
                "remote",
                List.of("logs-*"),
                List.of(),
                "{{leader_index}}_copy",
                Settings.EMPTY,
                true,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null
            );

            Index index = new Index(".ds-logs-foo-bar-2022-02-01-123456", "uuid");
            IndexAbstraction indexAbstraction = new IndexAbstraction.ConcreteIndex(
                IndexMetadata.builder(index.getName())
                    .settings(
                        Settings.builder()
                            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
                            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                            .put(IndexMetadata.SETTING_INDEX_UUID, index.getUUID())
                            .build()
                    )
                    .build(),
                null
            );

            PutFollowAction.Request request = AutoFollower.generateRequest("remote", index, indexAbstraction, pattern);
            assertThat(request.getRemoteCluster(), equalTo("remote"));
            assertThat(request.getFollowerIndex(), equalTo(".ds-logs-foo-bar_copy-2022-02-01-123456"));
            assertThat(request.getLeaderIndex(), equalTo(".ds-logs-foo-bar-2022-02-01-123456"));
            assertThat(request.getDataStreamName(), equalTo(null));
        }

        // Regular backing index, but no renaming
        {
            AutoFollowPattern pattern = new AutoFollowPattern(
                "remote",
                List.of("logs-*"),
                List.of(),
                null,
                Settings.EMPTY,
                true,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null
            );

            Index index = new Index(".ds-logs-foo-bar-2022-02-01-123456", "uuid");
            IndexAbstraction indexAbstraction = new IndexAbstraction.ConcreteIndex(
                IndexMetadata.builder(index.getName())
                    .settings(
                        Settings.builder()
                            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
                            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                            .put(IndexMetadata.SETTING_INDEX_UUID, index.getUUID())
                            .build()
                    )
                    .build(),
                new DataStream("logs-foo-bar", List.of(index), 1, Map.of(), false, false, false, true, IndexMode.STANDARD)
            );

            PutFollowAction.Request request = AutoFollower.generateRequest("remote", index, indexAbstraction, pattern);
            assertThat(request.getRemoteCluster(), equalTo("remote"));
            assertThat(request.getFollowerIndex(), equalTo(".ds-logs-foo-bar-2022-02-01-123456"));
            assertThat(request.getLeaderIndex(), equalTo(".ds-logs-foo-bar-2022-02-01-123456"));
            assertThat(request.getDataStreamName(), equalTo(null));
        }

        // Renaming with a suffix and just the worst named backing indices
        {
            AutoFollowPattern pattern = new AutoFollowPattern(
                "remote",
                List.of("logs-*"),
                List.of(),
                "{{leader_index}}_copy",
                Settings.EMPTY,
                true,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null
            );

            Index index = new Index("my-.ds-backing-index", "uuid");
            IndexAbstraction indexAbstraction = new IndexAbstraction.ConcreteIndex(
                IndexMetadata.builder(index.getName())
                    .settings(
                        Settings.builder()
                            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
                            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                            .put(IndexMetadata.SETTING_INDEX_UUID, index.getUUID())
                            .build()
                    )
                    .build(),
                new DataStream("logs-foo-bar", List.of(index), 1, Map.of(), false, false, false, true, IndexMode.STANDARD)
            );

            IllegalArgumentException e = expectThrows(
                IllegalArgumentException.class,
                () -> AutoFollower.generateRequest("remote", index, indexAbstraction, pattern)
            );
            assertThat(
                e.getMessage(),
                containsString(
                    "unable to determine follower index name from leader index name "
                        + "[my-.ds-backing-index] and follow index pattern: [{{leader_index}}_copy]"
                        + ", index appears to follow a regular data stream backing pattern, but could not be parsed"
                )
            );
        }
    }

    public void testStats() {
        AutoFollowCoordinator autoFollowCoordinator = createAutoFollowCoordinator();

        autoFollowCoordinator.updateStats(Collections.singletonList(new AutoFollowCoordinator.AutoFollowResult("_alias1")));
        AutoFollowStats autoFollowStats = autoFollowCoordinator.getStats();
        assertThat(autoFollowStats.getNumberOfFailedFollowIndices(), equalTo(0L));
        assertThat(autoFollowStats.getNumberOfFailedRemoteClusterStateRequests(), equalTo(0L));
        assertThat(autoFollowStats.getNumberOfSuccessfulFollowIndices(), equalTo(0L));
        assertThat(autoFollowStats.getRecentAutoFollowErrors().size(), equalTo(0));

        autoFollowCoordinator.updateStats(
            Collections.singletonList(new AutoFollowCoordinator.AutoFollowResult("_alias1", new RuntimeException("error")))
        );
        autoFollowStats = autoFollowCoordinator.getStats();
        assertThat(autoFollowStats.getNumberOfFailedFollowIndices(), equalTo(0L));
        assertThat(autoFollowStats.getNumberOfFailedRemoteClusterStateRequests(), equalTo(1L));
        assertThat(autoFollowStats.getNumberOfSuccessfulFollowIndices(), equalTo(0L));
        assertThat(autoFollowStats.getRecentAutoFollowErrors().size(), equalTo(1));
        assertThat(autoFollowStats.getRecentAutoFollowErrors().get("_alias1").v2().getCause().getMessage(), equalTo("error"));

        autoFollowCoordinator.updateStats(
            Arrays.asList(
                new AutoFollowCoordinator.AutoFollowResult(
                    "_alias1",
                    Collections.singletonList(Tuple.tuple(new Index("index1", "_na_"), new RuntimeException("error-1")))
                ),
                new AutoFollowCoordinator.AutoFollowResult(
                    "_alias2",
                    Collections.singletonList(Tuple.tuple(new Index("index2", "_na_"), new RuntimeException("error-2")))
                )
            )
        );
        autoFollowStats = autoFollowCoordinator.getStats();
        assertThat(autoFollowStats.getNumberOfFailedFollowIndices(), equalTo(2L));
        assertThat(autoFollowStats.getNumberOfFailedRemoteClusterStateRequests(), equalTo(1L));
        assertThat(autoFollowStats.getNumberOfSuccessfulFollowIndices(), equalTo(0L));
        assertThat(autoFollowStats.getRecentAutoFollowErrors().size(), equalTo(2));
        assertThat(autoFollowStats.getRecentAutoFollowErrors().get("_alias1"), nullValue());
        assertThat(autoFollowStats.getRecentAutoFollowErrors().get("_alias1:index1").v2().getCause().getMessage(), equalTo("error-1"));
        assertThat(autoFollowStats.getRecentAutoFollowErrors().get("_alias2:index2").v2().getCause().getMessage(), equalTo("error-2"));

        autoFollowCoordinator.updateStats(
            Arrays.asList(
                new AutoFollowCoordinator.AutoFollowResult(
                    "_alias1",
                    Collections.singletonList(Tuple.tuple(new Index("index1", "_na_"), null))
                ),
                new AutoFollowCoordinator.AutoFollowResult(
                    "_alias2",
                    Collections.singletonList(Tuple.tuple(new Index("index2", "_na_"), null))
                )
            )
        );
        autoFollowStats = autoFollowCoordinator.getStats();
        assertThat(autoFollowStats.getNumberOfFailedFollowIndices(), equalTo(2L));
        assertThat(autoFollowStats.getNumberOfFailedRemoteClusterStateRequests(), equalTo(1L));
        assertThat(autoFollowStats.getNumberOfSuccessfulFollowIndices(), equalTo(2L));
        assertThat(autoFollowStats.getRecentAutoFollowErrors().keySet(), empty());

    }

    public void testUpdateAutoFollowers() {
        ClusterService clusterService = mockClusterService();
        // Return a cluster state with no patterns so that the auto followers never really execute:
        ClusterState followerState = ClusterState.builder(new ClusterName("remote"))
            .metadata(
                Metadata.builder()
                    .putCustom(
                        AutoFollowMetadata.TYPE,
                        new AutoFollowMetadata(Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap())
                    )
            )
            .build();
        when(clusterService.state()).thenReturn(followerState);
        AutoFollowCoordinator autoFollowCoordinator = new AutoFollowCoordinator(
            Settings.EMPTY,
            null,
            clusterService,
            new CcrLicenseChecker(() -> true, () -> false),
            () -> 1L,
            () -> 1L,
            Runnable::run
        );
        // Add 3 patterns:
        Map<String, AutoFollowPattern> patterns = new HashMap<>();
        patterns.put("pattern1", createAutoFollowPattern("remote1", "logs-*"));
        patterns.put("pattern2", createAutoFollowPattern("remote2", "logs-*"));
        patterns.put("pattern3", createAutoFollowPattern("remote2", "metrics-*"));
        ClusterState clusterState = ClusterState.builder(new ClusterName("remote"))
            .metadata(
                Metadata.builder()
                    .putCustom(AutoFollowMetadata.TYPE, new AutoFollowMetadata(patterns, Collections.emptyMap(), Collections.emptyMap()))
            )
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
            .metadata(
                Metadata.builder()
                    .putCustom(AutoFollowMetadata.TYPE, new AutoFollowMetadata(patterns, Collections.emptyMap(), Collections.emptyMap()))
            )
            .build();
        autoFollowCoordinator.updateAutoFollowers(clusterState);
        assertThat(autoFollowCoordinator.getStats().getAutoFollowedClusters().size(), equalTo(1));
        assertThat(autoFollowCoordinator.getStats().getAutoFollowedClusters().get("remote2"), notNullValue());
        assertThat(removedAutoFollower1.removed, is(true));
        // Add pattern 4:
        patterns.put("pattern4", createAutoFollowPattern("remote1", "metrics-*"));
        clusterState = ClusterState.builder(new ClusterName("remote"))
            .metadata(
                Metadata.builder()
                    .putCustom(AutoFollowMetadata.TYPE, new AutoFollowMetadata(patterns, Collections.emptyMap(), Collections.emptyMap()))
            )
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
            .metadata(
                Metadata.builder()
                    .putCustom(AutoFollowMetadata.TYPE, new AutoFollowMetadata(patterns, Collections.emptyMap(), Collections.emptyMap()))
            )
            .build();
        autoFollowCoordinator.updateAutoFollowers(clusterState);
        assertThat(autoFollowCoordinator.getStats().getAutoFollowedClusters().size(), equalTo(0));
        assertThat(removedAutoFollower1.removed, is(true));
        assertThat(removedAutoFollower2.removed, is(true));
    }

    public void testUpdateAutoFollowersRevertMetadata() {
        // given coordinator with some initial patterns
        var autoFollowCoordinator = createAutoFollowCoordinator();

        // with some initial patterns
        var pattern1 = createAutoFollowPattern("remote1", "logs-*");
        var pattern2 = createAutoFollowPattern("remote2", "logs-*");
        var pattern3 = createAutoFollowPattern("remote2", "metrics-*");// same remote
        autoFollowCoordinator.updateAutoFollowers(
            createClusterStateWith(Map.of("pattern1", pattern1, "pattern2", pattern2, "pattern3", pattern3))
        );
        var initialAutoFollowers = autoFollowCoordinator.getAutoFollowers();

        // when resetting the state
        autoFollowCoordinator.updateAutoFollowers(createClusterStateWith(null));
        var newAutoFollowers = autoFollowCoordinator.getAutoFollowers();

        // then auto-followers are removed
        assertThat(newAutoFollowers.entrySet(), empty());
        // and auto-followers are stopped
        assertThat(initialAutoFollowers.get("remote1").removed, equalTo(true));
        assertThat(initialAutoFollowers.get("remote2").removed, equalTo(true));
    }

    public void testUpdateAutoFollowersNoPatterns() {
        var autoFollowCoordinator = createAutoFollowCoordinator();
        autoFollowCoordinator.updateAutoFollowers(createClusterStateWith(Map.of()));

        assertThat(autoFollowCoordinator.getAutoFollowers().keySet(), empty());
        assertThat(autoFollowCoordinator.getStats().getAutoFollowedClusters().size(), equalTo(0));
    }

    public void testUpdateAutoFollowersNoAutoFollowMetadata() {
        var autoFollowCoordinator = createAutoFollowCoordinator();
        autoFollowCoordinator.updateAutoFollowers(createClusterStateWith(null));

        assertThat(autoFollowCoordinator.getAutoFollowers().keySet(), empty());
        assertThat(autoFollowCoordinator.getStats().getAutoFollowedClusters().size(), equalTo(0));
    }

    public void testUpdateAutoFollowersNoActivePatterns() {
        final ClusterService clusterService = mockClusterService();
        final AutoFollowCoordinator autoFollowCoordinator = new AutoFollowCoordinator(
            Settings.EMPTY,
            null,
            clusterService,
            new CcrLicenseChecker(() -> true, () -> false),
            () -> 1L,
            () -> 1L,
            Runnable::run
        );

        autoFollowCoordinator.updateAutoFollowers(ClusterState.EMPTY_STATE);
        assertThat(autoFollowCoordinator.getStats().getAutoFollowedClusters().size(), equalTo(0));

        // Add 3 patterns:
        Map<String, AutoFollowPattern> patterns = new HashMap<>();
        patterns.put("pattern1", createAutoFollowPattern("remote1", "logs-*"));
        patterns.put("pattern2", createAutoFollowPattern("remote2", "logs-*"));
        patterns.put("pattern3", createAutoFollowPattern("remote2", "metrics-*"));

        autoFollowCoordinator.updateAutoFollowers(
            ClusterState.builder(new ClusterName("remote"))
                .metadata(
                    Metadata.builder()
                        .putCustom(
                            AutoFollowMetadata.TYPE,
                            new AutoFollowMetadata(patterns, Collections.emptyMap(), Collections.emptyMap())
                        )
                )
                .build()
        );
        assertThat(autoFollowCoordinator.getStats().getAutoFollowedClusters().size(), equalTo(2));
        assertThat(autoFollowCoordinator.getStats().getAutoFollowedClusters().get("remote1"), notNullValue());
        assertThat(autoFollowCoordinator.getStats().getAutoFollowedClusters().get("remote2"), notNullValue());

        AutoFollowCoordinator.AutoFollower removedAutoFollower1 = autoFollowCoordinator.getAutoFollowers().get("remote1");
        assertThat(removedAutoFollower1.removed, is(false));
        AutoFollowCoordinator.AutoFollower removedAutoFollower2 = autoFollowCoordinator.getAutoFollowers().get("remote2");
        assertThat(removedAutoFollower2.removed, is(false));

        // Make pattern 1 and pattern 3 inactive
        patterns.computeIfPresent(
            "pattern1",
            (name, pattern) -> new AutoFollowPattern(
                pattern.getRemoteCluster(),
                pattern.getLeaderIndexPatterns(),
                pattern.getLeaderIndexExclusionPatterns(),
                pattern.getFollowIndexPattern(),
                Settings.EMPTY,
                false,
                pattern.getMaxReadRequestOperationCount(),
                pattern.getMaxWriteRequestOperationCount(),
                pattern.getMaxOutstandingReadRequests(),
                pattern.getMaxOutstandingWriteRequests(),
                pattern.getMaxReadRequestSize(),
                pattern.getMaxWriteRequestSize(),
                pattern.getMaxWriteBufferCount(),
                pattern.getMaxWriteBufferSize(),
                pattern.getMaxRetryDelay(),
                pattern.getReadPollTimeout()
            )
        );
        patterns.computeIfPresent(
            "pattern3",
            (name, pattern) -> new AutoFollowPattern(
                pattern.getRemoteCluster(),
                pattern.getLeaderIndexPatterns(),
                pattern.getLeaderIndexExclusionPatterns(),
                pattern.getFollowIndexPattern(),
                Settings.EMPTY,
                false,
                pattern.getMaxReadRequestOperationCount(),
                pattern.getMaxWriteRequestOperationCount(),
                pattern.getMaxOutstandingReadRequests(),
                pattern.getMaxOutstandingWriteRequests(),
                pattern.getMaxReadRequestSize(),
                pattern.getMaxWriteRequestSize(),
                pattern.getMaxWriteBufferCount(),
                pattern.getMaxWriteBufferSize(),
                pattern.getMaxRetryDelay(),
                pattern.getReadPollTimeout()
            )
        );

        autoFollowCoordinator.updateAutoFollowers(
            ClusterState.builder(new ClusterName("remote"))
                .metadata(
                    Metadata.builder()
                        .putCustom(
                            AutoFollowMetadata.TYPE,
                            new AutoFollowMetadata(patterns, Collections.emptyMap(), Collections.emptyMap())
                        )
                )
                .build()
        );
        assertThat(autoFollowCoordinator.getStats().getAutoFollowedClusters().size(), equalTo(1));
        assertThat(autoFollowCoordinator.getStats().getAutoFollowedClusters().get("remote2"), notNullValue());
        assertThat(removedAutoFollower1.removed, is(true));
        assertThat(removedAutoFollower2.removed, is(false));

        // Add active pattern 4 and make pattern 2 inactive
        patterns.put("pattern4", createAutoFollowPattern("remote1", "metrics-*"));
        patterns.computeIfPresent(
            "pattern2",
            (name, pattern) -> new AutoFollowPattern(
                pattern.getRemoteCluster(),
                pattern.getLeaderIndexPatterns(),
                pattern.getLeaderIndexExclusionPatterns(),
                pattern.getFollowIndexPattern(),
                Settings.EMPTY,
                false,
                pattern.getMaxReadRequestOperationCount(),
                pattern.getMaxWriteRequestOperationCount(),
                pattern.getMaxOutstandingReadRequests(),
                pattern.getMaxOutstandingWriteRequests(),
                pattern.getMaxReadRequestSize(),
                pattern.getMaxWriteRequestSize(),
                pattern.getMaxWriteBufferCount(),
                pattern.getMaxWriteBufferSize(),
                pattern.getMaxRetryDelay(),
                pattern.getReadPollTimeout()
            )
        );

        autoFollowCoordinator.updateAutoFollowers(
            ClusterState.builder(new ClusterName("remote"))
                .metadata(
                    Metadata.builder()
                        .putCustom(
                            AutoFollowMetadata.TYPE,
                            new AutoFollowMetadata(patterns, Collections.emptyMap(), Collections.emptyMap())
                        )
                )
                .build()
        );
        assertThat(autoFollowCoordinator.getStats().getAutoFollowedClusters().size(), equalTo(1));
        assertThat(autoFollowCoordinator.getStats().getAutoFollowedClusters().get("remote1"), notNullValue());

        AutoFollowCoordinator.AutoFollower removedAutoFollower4 = autoFollowCoordinator.getAutoFollowers().get("remote1");
        assertThat(removedAutoFollower4.removed, is(false));
        assertNotSame(removedAutoFollower4, removedAutoFollower1);
        assertThat(removedAutoFollower2.removed, is(true));

        autoFollowCoordinator.updateAutoFollowers(
            ClusterState.builder(new ClusterName("remote"))
                .metadata(
                    Metadata.builder()
                        .putCustom(
                            AutoFollowMetadata.TYPE,
                            new AutoFollowMetadata(Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap())
                        )
                )
                .build()
        );
        assertThat(autoFollowCoordinator.getStats().getAutoFollowedClusters().size(), equalTo(0));
        assertThat(removedAutoFollower1.removed, is(true));
        assertThat(removedAutoFollower2.removed, is(true));
        assertThat(removedAutoFollower4.removed, is(true));
    }

    public void testWaitForMetadataVersion() {
        Client client = mock(Client.class);
        when(client.getRemoteClusterClient(anyString())).thenReturn(client);

        AutoFollowPattern autoFollowPattern = createAutoFollowPattern("remote", "logs-*");
        Map<String, AutoFollowPattern> patterns = new HashMap<>();
        patterns.put("remote", autoFollowPattern);
        Map<String, List<String>> followedLeaderIndexUUIDS = new HashMap<>();
        followedLeaderIndexUUIDS.put("remote", new ArrayList<>());
        Map<String, Map<String, String>> autoFollowHeaders = new HashMap<>();
        autoFollowHeaders.put("remote", Map.of("key", "val"));
        AutoFollowMetadata autoFollowMetadata = new AutoFollowMetadata(patterns, followedLeaderIndexUUIDS, autoFollowHeaders);

        final LinkedList<ClusterState> leaderStates = new LinkedList<>();
        ClusterState[] states = new ClusterState[16];
        for (int i = 0; i < states.length; i++) {
            states[i] = ClusterState.builder(new ClusterName("name"))
                .metadata(Metadata.builder().putCustom(AutoFollowMetadata.TYPE, autoFollowMetadata))
                .build();
            String indexName = "logs-" + i;
            leaderStates.add(
                i == 0 ? createRemoteClusterState(indexName, true) : createRemoteClusterState(leaderStates.get(i - 1), indexName)
            );
        }

        List<AutoFollowCoordinator.AutoFollowResult> allResults = new ArrayList<>();
        Consumer<List<AutoFollowCoordinator.AutoFollowResult>> handler = allResults::addAll;
        AutoFollower autoFollower = new AutoFollower("remote", handler, localClusterStateSupplier(states), () -> 1L, Runnable::run) {

            long previousRequestedMetadataVersion = 0;

            @Override
            void getRemoteClusterState(String remoteCluster, long metadataVersion, BiConsumer<ClusterStateResponse, Exception> handler) {
                assertThat(remoteCluster, equalTo("remote"));
                assertThat(metadataVersion, greaterThan(previousRequestedMetadataVersion));
                handler.accept(new ClusterStateResponse(new ClusterName("name"), leaderStates.poll(), false), null);
            }

            @Override
            void createAndFollow(
                Map<String, String> headers,
                PutFollowAction.Request followRequest,
                Runnable successHandler,
                Consumer<Exception> failureHandler
            ) {
                successHandler.run();
            }

            @Override
            void updateAutoFollowMetadata(Function<ClusterState, ClusterState> updateFunction, Consumer<Exception> handler) {
                handler.accept(null);
            }
        };
        autoFollower.start();
        assertThat(allResults.size(), equalTo(states.length));
        for (int i = 0; i < states.length; i++) {
            final String indexName = "logs-" + i;
            assertThat(
                allResults.get(i).autoFollowExecutionResults.keySet().stream().anyMatch(index -> index.getName().equals(indexName)),
                is(true)
            );
        }
    }

    public void testWaitForTimeOut() {
        Client client = mock(Client.class);
        when(client.getRemoteClusterClient(anyString())).thenReturn(client);

        AutoFollowPattern autoFollowPattern = createAutoFollowPattern("remote", "logs-*");
        Map<String, AutoFollowPattern> patterns = new HashMap<>();
        patterns.put("remote", autoFollowPattern);
        Map<String, List<String>> followedLeaderIndexUUIDS = new HashMap<>();
        followedLeaderIndexUUIDS.put("remote", new ArrayList<>());
        Map<String, Map<String, String>> autoFollowHeaders = new HashMap<>();
        autoFollowHeaders.put("remote", Map.of("key", "val"));
        AutoFollowMetadata autoFollowMetadata = new AutoFollowMetadata(patterns, followedLeaderIndexUUIDS, autoFollowHeaders);

        ClusterState[] states = new ClusterState[16];
        for (int i = 0; i < states.length; i++) {
            states[i] = ClusterState.builder(new ClusterName("name"))
                .metadata(Metadata.builder().putCustom(AutoFollowMetadata.TYPE, autoFollowMetadata))
                .build();
        }
        Consumer<List<AutoFollowCoordinator.AutoFollowResult>> handler = results -> { fail("should not be invoked"); };
        AtomicInteger counter = new AtomicInteger();
        AutoFollower autoFollower = new AutoFollower("remote", handler, localClusterStateSupplier(states), () -> 1L, Runnable::run) {

            long previousRequestedMetadataVersion = 0;

            @Override
            void getRemoteClusterState(String remoteCluster, long metadataVersion, BiConsumer<ClusterStateResponse, Exception> handler) {
                counter.incrementAndGet();
                assertThat(remoteCluster, equalTo("remote"));
                assertThat(metadataVersion, greaterThan(previousRequestedMetadataVersion));
                handler.accept(new ClusterStateResponse(new ClusterName("name"), null, true), null);
            }

            @Override
            void createAndFollow(
                Map<String, String> headers,
                PutFollowAction.Request followRequest,
                Runnable successHandler,
                Consumer<Exception> failureHandler
            ) {
                fail("should not be invoked");
            }

            @Override
            void updateAutoFollowMetadata(Function<ClusterState, ClusterState> updateFunction, Consumer<Exception> handler) {
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

        AutoFollowPattern autoFollowPattern = createAutoFollowPattern("remote", "logs-*");
        Map<String, AutoFollowPattern> patterns = new HashMap<>();
        patterns.put("remote", autoFollowPattern);
        Map<String, List<String>> followedLeaderIndexUUIDS = new HashMap<>();
        followedLeaderIndexUUIDS.put("remote", new ArrayList<>());
        Map<String, Map<String, String>> autoFollowHeaders = new HashMap<>();
        autoFollowHeaders.put("remote", Map.of("key", "val"));
        AutoFollowMetadata autoFollowMetadata = new AutoFollowMetadata(patterns, followedLeaderIndexUUIDS, autoFollowHeaders);

        ClusterState currentState = ClusterState.builder(new ClusterName("name"))
            .metadata(Metadata.builder().putCustom(AutoFollowMetadata.TYPE, autoFollowMetadata))
            .build();

        List<AutoFollowCoordinator.AutoFollowResult> results = new ArrayList<>();
        Consumer<List<AutoFollowCoordinator.AutoFollowResult>> handler = results::addAll;
        AutoFollower autoFollower = new AutoFollower("remote", handler, localClusterStateSupplier(currentState), () -> 1L, Runnable::run) {
            @Override
            void getRemoteClusterState(String remoteCluster, long metadataVersion, BiConsumer<ClusterStateResponse, Exception> handler) {
                assertThat(remoteCluster, equalTo("remote"));
                handler.accept(new ClusterStateResponse(new ClusterName("name"), remoteState, false), null);
            }

            @Override
            void createAndFollow(
                Map<String, String> headers,
                PutFollowAction.Request followRequest,
                Runnable successHandler,
                Consumer<Exception> failureHandler
            ) {
                fail("soft deletes are disabled; index should not be followed");
            }

            @Override
            void updateAutoFollowMetadata(Function<ClusterState, ClusterState> updateFunction, Consumer<Exception> handler) {
                ClusterState resultCs = updateFunction.apply(currentState);
                AutoFollowMetadata result = resultCs.metadata().custom(AutoFollowMetadata.TYPE);
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
        assertThat(
            entries.get(0).getValue().getMessage(),
            equalTo("index [logs-20190101] cannot be followed, " + "because soft deletes are not enabled")
        );
    }

    public void testAutoFollowerFollowerIndexAlreadyExists() {
        Client client = mock(Client.class);
        when(client.getRemoteClusterClient(anyString())).thenReturn(client);

        ClusterState remoteState = createRemoteClusterState("logs-20190101", true);

        AutoFollowPattern autoFollowPattern = createAutoFollowPattern("remote", "logs-*");
        Map<String, AutoFollowPattern> patterns = new HashMap<>();
        patterns.put("remote", autoFollowPattern);
        Map<String, List<String>> followedLeaderIndexUUIDS = new HashMap<>();
        followedLeaderIndexUUIDS.put("remote", new ArrayList<>());
        Map<String, Map<String, String>> autoFollowHeaders = new HashMap<>();
        autoFollowHeaders.put("remote", Map.of("key", "val"));
        AutoFollowMetadata autoFollowMetadata = new AutoFollowMetadata(patterns, followedLeaderIndexUUIDS, autoFollowHeaders);

        ClusterState currentState = ClusterState.builder(new ClusterName("name"))
            .metadata(
                Metadata.builder()
                    .put(
                        IndexMetadata.builder("logs-20190101")
                            .settings(settings(Version.CURRENT))
                            .putCustom(
                                Ccr.CCR_CUSTOM_METADATA_KEY,
                                Map.of(
                                    Ccr.CCR_CUSTOM_METADATA_LEADER_INDEX_UUID_KEY,
                                    remoteState.metadata().index("logs-20190101").getIndexUUID()
                                )
                            )
                            .numberOfShards(1)
                            .numberOfReplicas(0)
                    )
                    .putCustom(AutoFollowMetadata.TYPE, autoFollowMetadata)
            )
            .build();

        final Object[] resultHolder = new Object[1];
        Consumer<List<AutoFollowCoordinator.AutoFollowResult>> handler = results -> { resultHolder[0] = results; };
        AutoFollower autoFollower = new AutoFollower("remote", handler, localClusterStateSupplier(currentState), () -> 1L, Runnable::run) {
            @Override
            void getRemoteClusterState(String remoteCluster, long metadataVersion, BiConsumer<ClusterStateResponse, Exception> handler) {
                assertThat(remoteCluster, equalTo("remote"));
                handler.accept(new ClusterStateResponse(new ClusterName("name"), remoteState, false), null);
            }

            @Override
            void createAndFollow(
                Map<String, String> headers,
                PutFollowAction.Request followRequest,
                Runnable successHandler,
                Consumer<Exception> failureHandler
            ) {
                fail("this should not be invoked");
            }

            @Override
            void updateAutoFollowMetadata(Function<ClusterState, ClusterState> updateFunction, Consumer<Exception> handler) {
                ClusterState resultCs = updateFunction.apply(currentState);
                AutoFollowMetadata result = resultCs.metadata().custom(AutoFollowMetadata.TYPE);
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
        final Metadata metadata = mock(Metadata.class);
        when(clusterState.metadata()).thenReturn(metadata);
        final AutoFollowPattern pattern = new AutoFollowPattern(
            "remote",
            List.of("*"),
            Collections.emptyList(),
            "{}",
            Settings.EMPTY,
            true,
            0,
            0,
            0,
            0,
            ByteSizeValue.ZERO,
            ByteSizeValue.ZERO,
            0,
            ByteSizeValue.ZERO,
            TimeValue.ZERO,
            TimeValue.ZERO
        );
        final AutoFollowMetadata autoFollowMetadata = new AutoFollowMetadata(Map.of("remote", pattern), Map.of(), Map.of());
        when(metadata.custom(AutoFollowMetadata.TYPE)).thenReturn(autoFollowMetadata);

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
                    final BiConsumer<ClusterStateResponse, Exception> handler
                ) {
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
                    final Consumer<Exception> failureHandler
                ) {

                }

                @Override
                void updateAutoFollowMetadata(
                    final Function<ClusterState, ClusterState> updateFunction,
                    final Consumer<Exception> handler
                ) {

                }

            };
            autoFollower.start();
            latch.await();
        } finally {
            executor.shutdown();
        }
    }

    public void testClosedIndicesAreNotAutoFollowed() {
        final Client client = mock(Client.class);
        when(client.getRemoteClusterClient(anyString())).thenReturn(client);

        final String pattern = "pattern1";
        final ClusterState localState = ClusterState.builder(new ClusterName("local"))
            .metadata(
                Metadata.builder()
                    .putCustom(
                        AutoFollowMetadata.TYPE,
                        new AutoFollowMetadata(
                            Map.of(pattern, createAutoFollowPattern("remote", "docs-*")),
                            Map.of(pattern, List.of()),
                            Map.of(pattern, Map.of())
                        )
                    )
            )
            .build();

        ClusterState remoteState = null;
        final int nbLeaderIndices = randomIntBetween(1, 15);
        for (int i = 0; i < nbLeaderIndices; i++) {
            String indexName = "docs-" + i;
            if (remoteState == null) {
                remoteState = createRemoteClusterState(indexName, true);
            } else {
                remoteState = createRemoteClusterState(remoteState, indexName);
            }
            if (randomBoolean()) {
                // randomly close the index
                remoteState = ClusterState.builder(remoteState.getClusterName())
                    .routingTable(remoteState.routingTable())
                    .metadata(
                        Metadata.builder(remoteState.metadata())
                            .put(
                                IndexMetadata.builder(remoteState.metadata().index(indexName)).state(IndexMetadata.State.CLOSE).build(),
                                true
                            )
                            .build()
                    )
                    .build();
            }
        }

        final ClusterState finalRemoteState = remoteState;
        final AtomicReference<ClusterState> lastModifiedClusterState = new AtomicReference<>(localState);
        final List<AutoFollowCoordinator.AutoFollowResult> results = new ArrayList<>();
        final Set<Object> followedIndices = ConcurrentCollections.newConcurrentSet();
        final AutoFollower autoFollower = new AutoFollower(
            "remote",
            results::addAll,
            localClusterStateSupplier(localState),
            () -> 1L,
            Runnable::run
        ) {
            @Override
            void getRemoteClusterState(String remoteCluster, long metadataVersion, BiConsumer<ClusterStateResponse, Exception> handler) {
                assertThat(remoteCluster, equalTo("remote"));
                handler.accept(new ClusterStateResponse(new ClusterName("remote"), finalRemoteState, false), null);
            }

            @Override
            void createAndFollow(
                Map<String, String> headers,
                PutFollowAction.Request followRequest,
                Runnable successHandler,
                Consumer<Exception> failureHandler
            ) {
                followedIndices.add(followRequest.getLeaderIndex());
                successHandler.run();
            }

            @Override
            void updateAutoFollowMetadata(Function<ClusterState, ClusterState> updateFunction, Consumer<Exception> handler) {
                lastModifiedClusterState.updateAndGet(updateFunction::apply);
                handler.accept(null);
            }

            @Override
            void cleanFollowedRemoteIndices(ClusterState remoteClusterState, List<String> patterns) {
                // Ignore, to avoid invoking updateAutoFollowMetadata(...) twice
            }
        };
        autoFollower.start();

        assertThat(results, notNullValue());
        assertThat(results.size(), equalTo(1));

        for (var index : remoteState.metadata().indices().entrySet()) {
            boolean expect = index.getValue().getState() == IndexMetadata.State.OPEN;
            assertThat(results.get(0).autoFollowExecutionResults.containsKey(index.getValue().getIndex()), is(expect));
            assertThat(followedIndices.contains(index.getKey()), is(expect));
        }
    }

    public void testExcludedPatternIndicesAreNotAutoFollowed() {
        final Client client = mock(Client.class);
        when(client.getRemoteClusterClient(anyString())).thenReturn(client);

        final String pattern = "pattern1";
        final ClusterState localState = ClusterState.builder(new ClusterName("local"))
            .metadata(
                Metadata.builder()
                    .putCustom(
                        AutoFollowMetadata.TYPE,
                        new AutoFollowMetadata(
                            Map.of(
                                pattern,
                                new AutoFollowPattern(
                                    "remote",
                                    List.of("docs-*"),
                                    List.of("docs-excluded-*"),
                                    null,
                                    Settings.EMPTY,
                                    true,
                                    null,
                                    null,
                                    null,
                                    null,
                                    null,
                                    null,
                                    null,
                                    null,
                                    null,
                                    null
                                )
                            ),
                            Map.of(pattern, List.of()),
                            Map.of(pattern, Map.of())
                        )
                    )
            )
            .build();

        ClusterState remoteState = ClusterState.EMPTY_STATE;
        final int nbLeaderIndices = randomIntBetween(0, 15);
        for (int i = 0; i < nbLeaderIndices; i++) {
            String indexName = "docs-" + i;
            remoteState = createRemoteClusterState(remoteState, indexName);
        }

        final int nbLeaderExcludedIndices = randomIntBetween(1, 15);
        for (int i = 0; i < nbLeaderExcludedIndices; i++) {
            String indexName = "docs-excluded-" + i;
            remoteState = createRemoteClusterState(remoteState, indexName);
        }

        final ClusterState finalRemoteState = remoteState;
        final AtomicReference<ClusterState> lastModifiedClusterState = new AtomicReference<>(localState);
        final List<AutoFollowCoordinator.AutoFollowResult> results = new ArrayList<>();
        final Set<Object> followedIndices = ConcurrentCollections.newConcurrentSet();
        final AutoFollower autoFollower = new AutoFollower(
            "remote",
            results::addAll,
            localClusterStateSupplier(localState),
            () -> 1L,
            Runnable::run
        ) {
            @Override
            void getRemoteClusterState(String remoteCluster, long metadataVersion, BiConsumer<ClusterStateResponse, Exception> handler) {
                assertThat(remoteCluster, equalTo("remote"));
                handler.accept(new ClusterStateResponse(new ClusterName("remote"), finalRemoteState, false), null);
            }

            @Override
            void createAndFollow(
                Map<String, String> headers,
                PutFollowAction.Request followRequest,
                Runnable successHandler,
                Consumer<Exception> failureHandler
            ) {
                followedIndices.add(followRequest.getLeaderIndex());
                successHandler.run();
            }

            @Override
            void updateAutoFollowMetadata(Function<ClusterState, ClusterState> updateFunction, Consumer<Exception> handler) {
                lastModifiedClusterState.updateAndGet(updateFunction::apply);
                handler.accept(null);
            }

            @Override
            void cleanFollowedRemoteIndices(ClusterState remoteClusterState, List<String> patterns) {
                // Ignore, to avoid invoking updateAutoFollowMetadata(...) twice
            }
        };
        autoFollower.start();

        assertThat(results, notNullValue());
        assertThat(results.size(), equalTo(1));

        AutoFollowMetadata autoFollowMetadata = lastModifiedClusterState.get().metadata().custom(AutoFollowMetadata.TYPE);
        final List<String> autoFollowedIndices = autoFollowMetadata.getFollowedLeaderIndexUUIDs().get(pattern);
        assertThat(autoFollowedIndices.size(), equalTo(nbLeaderIndices));

        for (var index : remoteState.metadata().indices().entrySet()) {
            final Index remoteIndex = index.getValue().getIndex();
            boolean followed = remoteIndex.getName().startsWith("docs-excluded") == false;
            assertThat(results.get(0).autoFollowExecutionResults.containsKey(index.getValue().getIndex()), is(followed));
            assertThat(followedIndices.contains(index.getKey()), is(followed));
            assertThat(autoFollowedIndices.contains(remoteIndex.getUUID()), equalTo(followed));
        }
    }

    public void testSystemIndicesAreNotAutoFollowed() {
        ClusterState clusterState = null;
        final int nbLeaderSystemIndices = randomIntBetween(1, 15);
        for (int i = 0; i < nbLeaderSystemIndices; i++) {
            String indexName = "." + i;
            if (clusterState == null) {
                clusterState = createRemoteClusterState(indexName, true, 0, true);
            } else {
                clusterState = createRemoteClusterState(clusterState, true, indexName);
            }
        }

        Tuple<List<AutoFollowCoordinator.AutoFollowResult>, Set<String>> autoFollowResults = executeAutoFollow(".*", clusterState);
        assertThat(autoFollowResults.v1().size(), equalTo(1));
        assertThat(autoFollowResults.v1().get(0).autoFollowExecutionResults, is(anEmptyMap()));
        assertThat(autoFollowResults.v2(), is(empty()));
    }

    public void testSystemDataStreamsAreNotAutoFollowed() {
        Tuple<List<AutoFollowCoordinator.AutoFollowResult>, Set<String>> autoFollowResults = executeAutoFollow(
            "*.",
            createRemoteClusterStateWithDataStream(".test-data-stream")
        );

        assertThat(autoFollowResults.v1().size(), equalTo(1));
        assertThat(autoFollowResults.v1().get(0).autoFollowExecutionResults, is(anEmptyMap()));
        assertThat(autoFollowResults.v2(), is(empty()));
    }

    public void testFollowerIndexIsCreatedInExecuteAutoFollow() {
        final String indexName = "idx-1";
        ClusterState clusterState = createRemoteClusterState(indexName, true, 0, false);

        Tuple<List<AutoFollowCoordinator.AutoFollowResult>, Set<String>> autoFollowResults = executeAutoFollow("idx-*", clusterState);
        assertThat(autoFollowResults.v1().size(), equalTo(1));
        assertThat(autoFollowResults.v1().get(0).autoFollowExecutionResults.size(), equalTo(1));
        for (Map.Entry<Index, Exception> autoFollowEntry : autoFollowResults.v1().get(0).autoFollowExecutionResults.entrySet()) {
            assertThat(autoFollowEntry.getKey().getName(), equalTo(indexName));
            assertThat(autoFollowEntry.getValue(), nullValue());
        }
        assertThat(autoFollowResults.v2().contains(indexName), equalTo(true));
    }

    public void testRemovesClusterLevelErrorsOnRemovingAutoFollowPattern() {
        // given auto-follow pattern added
        var pattern1 = createAutoFollowPattern("remote1", "logs-*");
        var pattern2 = createAutoFollowPattern("remote2", "logs-*");
        var pattern3 = createAutoFollowPattern("remote2", "metrics-*");// same remote

        var autoFollowCoordinator = createAutoFollowCoordinator();
        autoFollowCoordinator.updateAutoFollowers(
            createClusterStateWith(Map.of("pattern1", pattern1, "pattern2", pattern2, "pattern3", pattern3))
        );

        // and stats are published
        autoFollowCoordinator.updateStats(
            List.of(
                new AutoFollowCoordinator.AutoFollowResult("pattern1", new RuntimeException("ClusterStateFetchException")),
                new AutoFollowCoordinator.AutoFollowResult("pattern2", new RuntimeException("ClusterStateFetchException")),
                new AutoFollowCoordinator.AutoFollowResult("pattern3", new RuntimeException("ClusterStateFetchException"))
            )
        );

        // when auto-follow pattern `pattern3` is removed
        var before = autoFollowCoordinator.getStats();
        autoFollowCoordinator.updateAutoFollowers(createClusterStateWith(Map.of("pattern1", pattern1, "pattern2", pattern2)));
        autoFollowCoordinator.updateStats(List.of());// actually triggers the purge
        var after = autoFollowCoordinator.getStats();

        // then stats are removed as well (but only for the removed pattern)
        assertThat(before.getRecentAutoFollowErrors().keySet(), equalTo(Set.of("pattern1", "pattern2", "pattern3")));
        assertThat(after.getRecentAutoFollowErrors().keySet(), equalTo(Set.of("pattern1", "pattern2")));
    }

    public void testRemovesIndexLevelErrorsOnRemovingAutoFollowPattern() {
        // given auto-follow pattern added
        var pattern1 = createAutoFollowPattern("remote1", "logs-*");
        var pattern2 = createAutoFollowPattern("remote2", "logs-*");
        var pattern3 = createAutoFollowPattern("remote2", "metrics-*");// same remote

        var autoFollowCoordinator = createAutoFollowCoordinator();
        autoFollowCoordinator.updateAutoFollowers(
            createClusterStateWith(Map.of("pattern1", pattern1, "pattern2", pattern2, "pattern3", pattern3))
        );

        // and stats are published
        autoFollowCoordinator.updateStats(
            List.of(
                new AutoFollowCoordinator.AutoFollowResult(
                    "pattern1",
                    List.of(Tuple.tuple(new Index("logs-1", UUIDs.base64UUID()), new RuntimeException("AutoFollowExecutionException")))
                ),
                new AutoFollowCoordinator.AutoFollowResult(
                    "pattern2",
                    List.of(Tuple.tuple(new Index("logs-1", UUIDs.base64UUID()), new RuntimeException("AutoFollowExecutionException")))
                ),
                new AutoFollowCoordinator.AutoFollowResult(
                    "pattern3",
                    List.of(Tuple.tuple(new Index("metrics-1", UUIDs.base64UUID()), new RuntimeException("AutoFollowExecutionException")))
                )
            )
        );

        // when auto-follow pattern `pattern3` is removed
        var before = autoFollowCoordinator.getStats();
        autoFollowCoordinator.updateAutoFollowers(createClusterStateWith(Map.of("pattern1", pattern1, "pattern2", pattern2)));
        autoFollowCoordinator.updateStats(List.of());// actually triggers the purge
        var after = autoFollowCoordinator.getStats();

        // then stats are removed as well (but only for the removed pattern)
        assertThat(
            before.getRecentAutoFollowErrors().keySet(),
            equalTo(Set.of("pattern1:logs-1", "pattern2:logs-1", "pattern3:metrics-1"))
        );
        assertThat(after.getRecentAutoFollowErrors().keySet(), equalTo(Set.of("pattern1:logs-1", "pattern2:logs-1")));
    }

    public void testRemovesErrorsIfPatternContainsColon() {
        // given auto-follow pattern added
        var pattern1 = createAutoFollowPattern("remote1", "logs-*");
        var pattern2 = createAutoFollowPattern("remote2", "logs-*");
        var pattern3 = createAutoFollowPattern("remote2", "metrics-*");// same remote

        var autoFollowCoordinator = createAutoFollowCoordinator();
        autoFollowCoordinator.updateAutoFollowers(
            createClusterStateWith(Map.of("pattern:1", pattern1, "pattern:2", pattern2, "pattern:3", pattern3))
        );

        // and stats are published
        autoFollowCoordinator.updateStats(
            List.of(
                new AutoFollowCoordinator.AutoFollowResult("pattern:1", new RuntimeException("ClusterStateFetchException")),
                new AutoFollowCoordinator.AutoFollowResult("pattern:2", new RuntimeException("ClusterStateFetchException")),
                new AutoFollowCoordinator.AutoFollowResult("pattern:3", new RuntimeException("ClusterStateFetchException"))
            )
        );

        // when auto-follow pattern `pattern:3` is removed
        var before = autoFollowCoordinator.getStats();
        autoFollowCoordinator.updateAutoFollowers(createClusterStateWith(Map.of("pattern:1", pattern1, "pattern:2", pattern2)));
        autoFollowCoordinator.updateStats(List.of());// actually triggers the purge
        var after = autoFollowCoordinator.getStats();

        // then stats are removed as well (but only for the removed pattern)
        assertThat(before.getRecentAutoFollowErrors().keySet(), equalTo(Set.of("pattern:1", "pattern:2", "pattern:3")));
        assertThat(after.getRecentAutoFollowErrors().keySet(), equalTo(Set.of("pattern:1", "pattern:2")));
    }

    private AutoFollowCoordinator createAutoFollowCoordinator() {
        return new AutoFollowCoordinator(
            Settings.EMPTY,
            null,
            mockClusterService(),
            new CcrLicenseChecker(() -> true, () -> false),
            () -> 1L,
            () -> 1L,
            Runnable::run
        );
    }

    private ClusterState createClusterStateWith(Map<String, AutoFollowPattern> patterns) {
        var builder = ClusterState.builder(new ClusterName("remote"));
        if (patterns != null) {
            builder.metadata(Metadata.builder().putCustom(AutoFollowMetadata.TYPE, new AutoFollowMetadata(patterns, Map.of(), Map.of())));
        }
        return builder.build();
    }

    private AutoFollowPattern createAutoFollowPattern(String remoteCluster, String pattern) {
        return new AutoFollowPattern(
            remoteCluster,
            List.of(pattern),
            List.of(),
            null,
            Settings.EMPTY,
            true,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null
        );
    }

    private Tuple<List<AutoFollowCoordinator.AutoFollowResult>, Set<String>> executeAutoFollow(
        String indexPattern,
        ClusterState finalRemoteState
    ) {
        final Client client = mock(Client.class);
        when(client.getRemoteClusterClient(anyString())).thenReturn(client);

        final String pattern = "pattern1";
        final ClusterState localState = ClusterState.builder(new ClusterName("local"))
            .metadata(
                Metadata.builder()
                    .putCustom(
                        AutoFollowMetadata.TYPE,
                        new AutoFollowMetadata(
                            Map.of(pattern, createAutoFollowPattern("remote", indexPattern)),
                            Map.of(pattern, List.of()),
                            Map.of(pattern, Map.of())
                        )
                    )
            )
            .build();

        final AtomicReference<ClusterState> lastModifiedClusterState = new AtomicReference<>(localState);
        final List<AutoFollowCoordinator.AutoFollowResult> results = new ArrayList<>();
        final Set<String> followedIndices = ConcurrentCollections.newConcurrentSet();
        final AutoFollower autoFollower = new AutoFollower(
            "remote",
            results::addAll,
            localClusterStateSupplier(localState),
            () -> 1L,
            Runnable::run
        ) {
            @Override
            void getRemoteClusterState(String remoteCluster, long metadataVersion, BiConsumer<ClusterStateResponse, Exception> handler) {
                assertThat(remoteCluster, equalTo("remote"));
                handler.accept(new ClusterStateResponse(new ClusterName("remote"), finalRemoteState, false), null);
            }

            @Override
            void createAndFollow(
                Map<String, String> headers,
                PutFollowAction.Request followRequest,
                Runnable successHandler,
                Consumer<Exception> failureHandler
            ) {
                followedIndices.add(followRequest.getLeaderIndex());
                successHandler.run();
            }

            @Override
            void updateAutoFollowMetadata(Function<ClusterState, ClusterState> updateFunction, Consumer<Exception> handler) {
                lastModifiedClusterState.updateAndGet(updateFunction::apply);
                handler.accept(null);
            }

            @Override
            void cleanFollowedRemoteIndices(ClusterState remoteClusterState, List<String> patterns) {
                // Ignore, to avoid invoking updateAutoFollowMetadata(...) twice
            }
        };
        autoFollower.start();

        assertThat(results, notNullValue());
        return Tuple.tuple(results, followedIndices);
    }

    private static ClusterState createRemoteClusterState(String indexName, boolean enableSoftDeletes) {
        return createRemoteClusterState(indexName, enableSoftDeletes, 0L);
    }

    private static ClusterState createRemoteClusterState(String indexName, boolean enableSoftDeletes, long metadataVersion) {
        return createRemoteClusterState(indexName, enableSoftDeletes, metadataVersion, false);
    }

    private static ClusterState createRemoteClusterState(
        String indexName,
        boolean enableSoftDeletes,
        long metadataVersion,
        boolean systemIndex
    ) {
        Settings.Builder indexSettings;
        if (enableSoftDeletes == false) {
            indexSettings = settings(VersionUtils.randomPreviousCompatibleVersion(random(), Version.V_8_0_0)).put(
                IndexSettings.INDEX_SOFT_DELETES_SETTING.getKey(),
                false
            );
        } else {
            indexSettings = settings(Version.CURRENT);
        }
        indexSettings.put(IndexMetadata.SETTING_INDEX_UUID, UUIDs.randomBase64UUID(random()));

        IndexMetadata indexMetadata = IndexMetadata.builder(indexName)
            .settings(indexSettings)
            .numberOfShards(1)
            .numberOfReplicas(0)
            .system(systemIndex)
            .build();
        ClusterState.Builder csBuilder = ClusterState.builder(new ClusterName("remote"))
            .metadata(Metadata.builder().put(indexMetadata, true).version(metadataVersion));

        ShardRouting shardRouting = TestShardRouting.newShardRouting(
            new ShardId(indexMetadata.getIndex(), 0),
            "1",
            true,
            ShardRoutingState.INITIALIZING
        ).moveToStarted(ShardRouting.UNAVAILABLE_EXPECTED_SHARD_SIZE);
        IndexRoutingTable indexRoutingTable = IndexRoutingTable.builder(indexMetadata.getIndex()).addShard(shardRouting).build();
        return csBuilder.routingTable(RoutingTable.builder().add(indexRoutingTable).build()).build();
    }

    private static ClusterState createRemoteClusterState(final ClusterState previous, final String... indices) {
        return createRemoteClusterState(previous, false, indices);
    }

    private static ClusterState createRemoteClusterState(final ClusterState previous, boolean systemIndices, final String... indices) {
        if (indices == null) {
            return previous;
        }
        final Metadata.Builder metadataBuilder = Metadata.builder(previous.metadata()).version(previous.metadata().version() + 1);
        final RoutingTable.Builder routingTableBuilder = RoutingTable.builder(previous.routingTable());
        for (String indexName : indices) {
            IndexMetadata indexMetadata = IndexMetadata.builder(indexName)
                .settings(settings(Version.CURRENT).put(IndexMetadata.SETTING_INDEX_UUID, UUIDs.randomBase64UUID(random())))
                .numberOfShards(1)
                .numberOfReplicas(0)
                .system(systemIndices)
                .build();
            metadataBuilder.put(indexMetadata, true);
            routingTableBuilder.add(
                IndexRoutingTable.builder(indexMetadata.getIndex())
                    .addShard(
                        TestShardRouting.newShardRouting(
                            new ShardId(indexMetadata.getIndex(), 0),
                            "1",
                            true,
                            ShardRoutingState.INITIALIZING
                        ).moveToStarted(ShardRouting.UNAVAILABLE_EXPECTED_SHARD_SIZE)
                    )
                    .build()
            );
        }
        return ClusterState.builder(previous.getClusterName())
            .metadata(metadataBuilder.build())
            .routingTable(routingTableBuilder.build())
            .build();
    }

    private static Supplier<ClusterState> localClusterStateSupplier(ClusterState... states) {
        final AutoFollowMetadata emptyAutoFollowMetadata = new AutoFollowMetadata(
            Collections.emptyMap(),
            Collections.emptyMap(),
            Collections.emptyMap()
        );
        final ClusterState lastState = ClusterState.builder(new ClusterName("remote"))
            .metadata(Metadata.builder().putCustom(AutoFollowMetadata.TYPE, emptyAutoFollowMetadata))
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
        ClusterSettings clusterSettings = new ClusterSettings(
            Settings.EMPTY,
            Collections.singleton(CcrSettings.CCR_WAIT_FOR_METADATA_TIMEOUT)
        );
        when(clusterService.getClusterSettings()).thenReturn(clusterSettings);
        return clusterService;
    }

    private static ClusterState createRemoteClusterStateWithDataStream(String dataStreamName) {
        return createRemoteClusterStateWithDataStream(dataStreamName, false);
    }

    private static ClusterState createRemoteClusterStateWithDataStream(String dataStreamName, boolean system) {
        Settings.Builder indexSettings = settings(Version.CURRENT);
        indexSettings.put(IndexMetadata.SETTING_INDEX_UUID, UUIDs.randomBase64UUID(random()));
        indexSettings.put("index.hidden", true);

        IndexMetadata indexMetadata = IndexMetadata.builder(DataStream.getDefaultBackingIndexName(dataStreamName, 1))
            .settings(indexSettings)
            .numberOfShards(1)
            .numberOfReplicas(0)
            .system(system)
            .build();
        DataStream dataStream = new DataStream(
            dataStreamName,
            List.of(indexMetadata.getIndex()),
            1,
            null,
            false,
            false,
            system,
            false,
            null
        );
        ClusterState.Builder csBuilder = ClusterState.builder(new ClusterName("remote"))
            .metadata(Metadata.builder().put(indexMetadata, true).put(dataStream).version(0L));

        ShardRouting shardRouting = TestShardRouting.newShardRouting(
            new ShardId(indexMetadata.getIndex(), 0),
            "1",
            true,
            ShardRoutingState.INITIALIZING
        ).moveToStarted(ShardRouting.UNAVAILABLE_EXPECTED_SHARD_SIZE);
        IndexRoutingTable indexRoutingTable = IndexRoutingTable.builder(indexMetadata.getIndex()).addShard(shardRouting).build();
        return csBuilder.routingTable(RoutingTable.builder().add(indexRoutingTable).build()).build();
    }

}
