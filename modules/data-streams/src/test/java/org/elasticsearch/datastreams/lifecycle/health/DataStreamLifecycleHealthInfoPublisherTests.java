/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.datastreams.lifecycle.health;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.replication.ClusterStateCreationUtils;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.datastreams.DataStreamFeatures;
import org.elasticsearch.datastreams.lifecycle.DataStreamLifecycleErrorStore;
import org.elasticsearch.datastreams.lifecycle.DataStreamLifecycleService;
import org.elasticsearch.features.FeatureService;
import org.elasticsearch.health.node.DataStreamLifecycleHealthInfo;
import org.elasticsearch.health.node.DslErrorInfo;
import org.elasticsearch.health.node.UpdateHealthInfoCacheAction;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.client.NoOpClient;
import org.elasticsearch.threadpool.TestThreadPool;
import org.junit.After;
import org.junit.Before;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.elasticsearch.datastreams.lifecycle.DataStreamLifecycleService.DATA_STREAM_MERGE_POLICY_TARGET_FACTOR_SETTING;
import static org.elasticsearch.datastreams.lifecycle.DataStreamLifecycleService.DATA_STREAM_MERGE_POLICY_TARGET_FLOOR_SEGMENT_SETTING;
import static org.elasticsearch.datastreams.lifecycle.DataStreamLifecycleService.DATA_STREAM_SIGNALLING_ERROR_RETRY_INTERVAL_SETTING;
import static org.elasticsearch.test.ClusterServiceUtils.createClusterService;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

public class DataStreamLifecycleHealthInfoPublisherTests extends ESTestCase {

    private long now;
    private ClusterService clusterService;
    private TestThreadPool threadPool;
    private CopyOnWriteArrayList<UpdateHealthInfoCacheAction.Request> clientSeenRequests;
    private DataStreamLifecycleHealthInfoPublisher dslHealthInfoPublisher;
    private final DiscoveryNode node1 = DiscoveryNodeUtils.builder("node_1")
        .roles(Set.of(DiscoveryNodeRole.MASTER_ROLE, DiscoveryNodeRole.DATA_ROLE))
        .build();
    private final DiscoveryNode node2 = DiscoveryNodeUtils.builder("node_2")
        .roles(Set.of(DiscoveryNodeRole.MASTER_ROLE, DiscoveryNodeRole.DATA_ROLE))
        .build();
    private final DiscoveryNode[] allNodes = new DiscoveryNode[] { node1, node2 };
    private DataStreamLifecycleErrorStore errorStore;

    @Before
    public void setupServices() {
        threadPool = new TestThreadPool(getTestName());
        Set<Setting<?>> builtInClusterSettings = new HashSet<>(ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        builtInClusterSettings.add(DataStreamLifecycleService.DATA_STREAM_LIFECYCLE_POLL_INTERVAL_SETTING);
        builtInClusterSettings.add(DATA_STREAM_MERGE_POLICY_TARGET_FLOOR_SEGMENT_SETTING);
        builtInClusterSettings.add(DATA_STREAM_MERGE_POLICY_TARGET_FACTOR_SETTING);
        builtInClusterSettings.add(DATA_STREAM_SIGNALLING_ERROR_RETRY_INTERVAL_SETTING);
        ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, builtInClusterSettings);
        clusterService = createClusterService(threadPool, clusterSettings);

        now = System.currentTimeMillis();
        clientSeenRequests = new CopyOnWriteArrayList<>();

        final Client client = getTransportRequestsRecordingClient();
        errorStore = new DataStreamLifecycleErrorStore(() -> now);
        dslHealthInfoPublisher = new DataStreamLifecycleHealthInfoPublisher(
            Settings.EMPTY,
            client,
            clusterService,
            errorStore,
            new FeatureService(List.of(new DataStreamFeatures()))
        );
    }

    @After
    public void cleanup() {
        clientSeenRequests.clear();
        clusterService.close();
        threadPool.shutdownNow();
    }

    public void testPublishDslErrorEntries() {
        for (int i = 0; i < 11; i++) {
            errorStore.recordError("testIndexOverSignalThreshold", new NullPointerException("ouch"));
        }
        errorStore.recordError("testIndex", new IllegalStateException("bad state"));
        ClusterState stateWithHealthNode = ClusterStateCreationUtils.state(node1, node1, node1, allNodes);
        stateWithHealthNode = ClusterState.builder(stateWithHealthNode)
            .nodeFeatures(
                Map.of(
                    node1.getId(),
                    Set.of(DataStreamLifecycleHealthInfoPublisher.DSL_HEALTH_INFO_FEATURE.id()),
                    node2.getId(),
                    Set.of(DataStreamLifecycleHealthInfoPublisher.DSL_HEALTH_INFO_FEATURE.id())
                )
            )
            .build();
        ClusterServiceUtils.setState(clusterService, stateWithHealthNode);
        dslHealthInfoPublisher.publishDslErrorEntries(new ActionListener<>() {
            @Override
            public void onResponse(AcknowledgedResponse acknowledgedResponse) {}

            @Override
            public void onFailure(Exception e) {

            }
        });

        assertThat(clientSeenRequests.size(), is(1));
        DataStreamLifecycleHealthInfo dslHealthInfo = clientSeenRequests.get(0).getDslHealthInfo();
        assertThat(dslHealthInfo, is(notNullValue()));
        List<DslErrorInfo> dslErrorsInfo = dslHealthInfo.dslErrorsInfo();
        assertThat(dslErrorsInfo.size(), is(1));
        assertThat(dslErrorsInfo.get(0).indexName(), is("testIndexOverSignalThreshold"));
        assertThat(dslHealthInfo.totalErrorEntriesCount(), is(2));
    }

    public void testPublishDslErrorEntriesNoHealthNode() {
        // no requests are being executed
        for (int i = 0; i < 11; i++) {
            errorStore.recordError("testIndexOverSignalThreshold", new NullPointerException("ouch"));
        }
        errorStore.recordError("testIndex", new IllegalStateException("bad state"));

        ClusterState stateNoHealthNode = ClusterStateCreationUtils.state(node1, node1, null, allNodes);
        stateNoHealthNode = ClusterState.builder(stateNoHealthNode)
            .nodeFeatures(
                Map.of(
                    node1.getId(),
                    Set.of(DataStreamLifecycleHealthInfoPublisher.DSL_HEALTH_INFO_FEATURE.id()),
                    node2.getId(),
                    Set.of(DataStreamLifecycleHealthInfoPublisher.DSL_HEALTH_INFO_FEATURE.id())
                )
            )
            .build();
        ClusterServiceUtils.setState(clusterService, stateNoHealthNode);
        dslHealthInfoPublisher.publishDslErrorEntries(new ActionListener<>() {
            @Override
            public void onResponse(AcknowledgedResponse acknowledgedResponse) {}

            @Override
            public void onFailure(Exception e) {

            }
        });

        assertThat(clientSeenRequests.size(), is(0));
    }

    public void testPublishDslErrorEntriesEmptyErrorStore() {
        // publishes the empty error store (this is the "back to healthy" state where all errors have been fixed)
        ClusterState state = ClusterStateCreationUtils.state(node1, node1, node1, allNodes);
        state = ClusterState.builder(state)
            .nodeFeatures(
                Map.of(
                    node1.getId(),
                    Set.of(DataStreamLifecycleHealthInfoPublisher.DSL_HEALTH_INFO_FEATURE.id()),
                    node2.getId(),
                    Set.of(DataStreamLifecycleHealthInfoPublisher.DSL_HEALTH_INFO_FEATURE.id())
                )
            )
            .build();
        ClusterServiceUtils.setState(clusterService, state);
        dslHealthInfoPublisher.publishDslErrorEntries(new ActionListener<>() {
            @Override
            public void onResponse(AcknowledgedResponse acknowledgedResponse) {}

            @Override
            public void onFailure(Exception e) {

            }
        });

        assertThat(clientSeenRequests.size(), is(1));
        DataStreamLifecycleHealthInfo dslHealthInfo = clientSeenRequests.get(0).getDslHealthInfo();
        assertThat(dslHealthInfo, is(notNullValue()));
        List<DslErrorInfo> dslErrorsInfo = dslHealthInfo.dslErrorsInfo();
        assertThat(dslErrorsInfo.size(), is(0));
        assertThat(dslHealthInfo.totalErrorEntriesCount(), is(0));
    }

    private Client getTransportRequestsRecordingClient() {
        return new NoOpClient(threadPool) {
            @Override
            protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
                ActionType<Response> action,
                Request request,
                ActionListener<Response> listener
            ) {
                clientSeenRequests.add((UpdateHealthInfoCacheAction.Request) request);
            }
        };
    }

}
