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
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ccr.AutoFollowMetadata;
import org.elasticsearch.xpack.core.ccr.AutoFollowMetadata.AutoFollowPattern;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class AutoFollowCoordinatorTests extends ESTestCase {

    public void testAutoFollower() {
        Client client = mock(Client.class);
        when(client.getRemoteClusterClient(anyString())).thenReturn(client);

        ClusterState remoteState = ClusterState.builder(new ClusterName("remote"))
            .metaData(MetaData.builder().put(IndexMetaData.builder("logs-20190101")
                .settings(settings(Version.CURRENT))
                .numberOfShards(1)
                .numberOfReplicas(0)))
            .build();

        AutoFollowPattern autoFollowPattern =
            new AutoFollowPattern(Collections.singletonList("logs-*"), null, null, null, null, null, null, null, null);
        Map<String, AutoFollowPattern> patterns = new HashMap<>();
        patterns.put("remote", autoFollowPattern);
        Map<String, List<String>> followedLeaderIndexUUIDS = new HashMap<>();
        followedLeaderIndexUUIDS.put("remote", new ArrayList<>());
        AutoFollowMetadata autoFollowMetadata = new AutoFollowMetadata(patterns, followedLeaderIndexUUIDS);

        ClusterState currentState = ClusterState.builder(new ClusterName("name"))
            .metaData(MetaData.builder().putCustom(AutoFollowMetadata.TYPE, autoFollowMetadata))
            .build();

        boolean[] invoked = new boolean[]{false};
        Consumer<Exception> handler = e -> {
            invoked[0] = true;
            assertThat(e, nullValue());
        };
        AutoFollowCoordinator.AutoFollower autoFollower = new AutoFollowCoordinator.AutoFollower(client, handler, autoFollowMetadata) {
            @Override
            void clusterStateApiCall(Client remoteClient, BiConsumer<ClusterState, Exception> handler) {
                handler.accept(remoteState, null);
            }

            @Override
            void createAndFollowApiCall(FollowIndexAction.Request followRequest, Consumer<Exception> handler) {
                assertThat(followRequest.getLeaderIndex(), equalTo("remote:logs-20190101"));
                assertThat(followRequest.getFollowerIndex(), equalTo("logs-20190101"));
                handler.accept(null);
            }

            @Override
            void updateAutoMetadata(Function<ClusterState, ClusterState> updateFunction, Consumer<Exception> handler) {
                ClusterState resultCs = updateFunction.apply(currentState);
                AutoFollowMetadata result = resultCs.metaData().custom(AutoFollowMetadata.TYPE);
                assertThat(result.getFollowedLeaderIndexUUIDS().size(), equalTo(1));
                assertThat(result.getFollowedLeaderIndexUUIDS().get("remote").size(), equalTo(1));
                handler.accept(null);
            }
        };
        autoFollower.autoFollowIndices();
        assertThat(invoked[0], is(true));
    }

    public void testAutoFollowerClusterStateApiFailure() {
        Client client = mock(Client.class);
        when(client.getRemoteClusterClient(anyString())).thenReturn(client);

        AutoFollowPattern autoFollowPattern =
            new AutoFollowPattern(Collections.singletonList("logs-*"), null, null, null, null, null, null, null, null);
        Map<String, AutoFollowPattern> patterns = new HashMap<>();
        patterns.put("remote", autoFollowPattern);
        Map<String, List<String>> followedLeaderIndexUUIDS = new HashMap<>();
        followedLeaderIndexUUIDS.put("remote", new ArrayList<>());
        AutoFollowMetadata autoFollowMetadata = new AutoFollowMetadata(patterns, followedLeaderIndexUUIDS);

        Exception failure = new RuntimeException("failure");
        boolean[] invoked = new boolean[]{false};
        Consumer<Exception> handler = e -> {
            invoked[0] = true;
            assertThat(e, sameInstance(failure));
        };
        AutoFollowCoordinator.AutoFollower autoFollower = new AutoFollowCoordinator.AutoFollower(client, handler, autoFollowMetadata) {
            @Override
            void clusterStateApiCall(Client remoteClient, BiConsumer<ClusterState, Exception> handler) {
                handler.accept(null, failure);
            }

            @Override
            void createAndFollowApiCall(FollowIndexAction.Request followRequest, Consumer<Exception> handler) {
                fail("should not get here");
            }

            @Override
            void updateAutoMetadata(Function<ClusterState, ClusterState> updateFunction, Consumer<Exception> handler) {
                fail("should not get here");
            }
        };
        autoFollower.autoFollowIndices();
        assertThat(invoked[0], is(true));
    }

    public void testAutoFollowerUpdateClusterStateFailure() {
        Client client = mock(Client.class);
        when(client.getRemoteClusterClient(anyString())).thenReturn(client);

        ClusterState remoteState = ClusterState.builder(new ClusterName("remote"))
            .metaData(MetaData.builder().put(IndexMetaData.builder("logs-20190101")
                .settings(settings(Version.CURRENT))
                .numberOfShards(1)
                .numberOfReplicas(0)))
            .build();

        AutoFollowPattern autoFollowPattern =
            new AutoFollowPattern(Collections.singletonList("logs-*"), null, null, null, null, null, null, null, null);
        Map<String, AutoFollowPattern> patterns = new HashMap<>();
        patterns.put("remote", autoFollowPattern);
        Map<String, List<String>> followedLeaderIndexUUIDS = new HashMap<>();
        followedLeaderIndexUUIDS.put("remote", new ArrayList<>());
        AutoFollowMetadata autoFollowMetadata = new AutoFollowMetadata(patterns, followedLeaderIndexUUIDS);

        Exception failure = new RuntimeException("failure");
        boolean[] invoked = new boolean[]{false};
        Consumer<Exception> handler = e -> {
            invoked[0] = true;
            assertThat(e, sameInstance(failure));
        };
        AutoFollowCoordinator.AutoFollower autoFollower = new AutoFollowCoordinator.AutoFollower(client, handler, autoFollowMetadata) {
            @Override
            void clusterStateApiCall(Client remoteClient, BiConsumer<ClusterState, Exception> handler) {
                handler.accept(remoteState, null);
            }

            @Override
            void createAndFollowApiCall(FollowIndexAction.Request followRequest, Consumer<Exception> handler) {
                assertThat(followRequest.getLeaderIndex(), equalTo("remote:logs-20190101"));
                assertThat(followRequest.getFollowerIndex(), equalTo("logs-20190101"));
                handler.accept(null);
            }

            @Override
            void updateAutoMetadata(Function<ClusterState, ClusterState> updateFunction, Consumer<Exception> handler) {
                handler.accept(failure);
            }
        };
        autoFollower.autoFollowIndices();
        assertThat(invoked[0], is(true));
    }

    public void testAutoFollowerCreateAndFollowApiCallFailure() {
        Client client = mock(Client.class);
        when(client.getRemoteClusterClient(anyString())).thenReturn(client);

        ClusterState remoteState = ClusterState.builder(new ClusterName("remote"))
            .metaData(MetaData.builder().put(IndexMetaData.builder("logs-20190101")
                .settings(settings(Version.CURRENT))
                .numberOfShards(1)
                .numberOfReplicas(0)))
            .build();

        AutoFollowPattern autoFollowPattern =
            new AutoFollowPattern(Collections.singletonList("logs-*"), null, null, null, null, null, null, null, null);
        Map<String, AutoFollowPattern> patterns = new HashMap<>();
        patterns.put("remote", autoFollowPattern);
        Map<String, List<String>> followedLeaderIndexUUIDS = new HashMap<>();
        followedLeaderIndexUUIDS.put("remote", new ArrayList<>());
        AutoFollowMetadata autoFollowMetadata = new AutoFollowMetadata(patterns, followedLeaderIndexUUIDS);

        Exception failure = new RuntimeException("failure");
        boolean[] invoked = new boolean[]{false};
        Consumer<Exception> handler = e -> {
            invoked[0] = true;
            assertThat(e, sameInstance(failure));
        };
        AutoFollowCoordinator.AutoFollower autoFollower = new AutoFollowCoordinator.AutoFollower(client, handler, autoFollowMetadata) {
            @Override
            void clusterStateApiCall(Client remoteClient, BiConsumer<ClusterState, Exception> handler) {
                handler.accept(remoteState, null);
            }

            @Override
            void createAndFollowApiCall(FollowIndexAction.Request followRequest, Consumer<Exception> handler) {
                assertThat(followRequest.getLeaderIndex(), equalTo("remote:logs-20190101"));
                assertThat(followRequest.getFollowerIndex(), equalTo("logs-20190101"));
                handler.accept(failure);
            }

            @Override
            void updateAutoMetadata(Function<ClusterState, ClusterState> updateFunction, Consumer<Exception> handler) {
                fail("should not get here");
            }
        };
        autoFollower.autoFollowIndices();
        assertThat(invoked[0], is(true));
    }

}
