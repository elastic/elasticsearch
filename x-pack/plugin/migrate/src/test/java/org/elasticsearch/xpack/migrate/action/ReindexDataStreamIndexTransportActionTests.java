/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.migrate.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.ReindexAction;
import org.elasticsearch.index.reindex.ReindexRequest;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.TransportService;
import org.junit.After;
import org.junit.Before;
import org.mockito.Answers;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ReindexDataStreamIndexTransportActionTests extends ESTestCase {

    @Mock(answer = Answers.RETURNS_DEEP_STUBS)
    private TransportService transportService;
    @Mock
    private ClusterService clusterService;
    @Mock
    private ActionFilters actionFilters;
    @Mock
    private Client client;

    @InjectMocks
    private ReindexDataStreamIndexTransportAction action;

    @Captor
    private ArgumentCaptor<ReindexRequest> request;

    private AutoCloseable mocks;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        mocks = MockitoAnnotations.openMocks(this);
    }

    @After
    public void tearDown() throws Exception {
        super.tearDown();
        mocks.close();
    }

    public void testGenerateDestIndexName_noDotPrefix() {
        String sourceIndex = "sourceindex";
        String expectedDestIndex = "migrated-sourceindex";
        String actualDestIndex = ReindexDataStreamIndexTransportAction.generateDestIndexName(sourceIndex);
        assertEquals(expectedDestIndex, actualDestIndex);
    }

    public void testGenerateDestIndexName_withDotPrefix() {
        String sourceIndex = ".sourceindex";
        String expectedDestIndex = ".migrated-sourceindex";
        String actualDestIndex = ReindexDataStreamIndexTransportAction.generateDestIndexName(sourceIndex);
        assertEquals(expectedDestIndex, actualDestIndex);
    }

    public void testGenerateDestIndexName_withHyphen() {
        String sourceIndex = "source-index";
        String expectedDestIndex = "migrated-source-index";
        String actualDestIndex = ReindexDataStreamIndexTransportAction.generateDestIndexName(sourceIndex);
        assertEquals(expectedDestIndex, actualDestIndex);
    }

    public void testGenerateDestIndexName_withUnderscore() {
        String sourceIndex = "source_index";
        String expectedDestIndex = "migrated-source_index";
        String actualDestIndex = ReindexDataStreamIndexTransportAction.generateDestIndexName(sourceIndex);
        assertEquals(expectedDestIndex, actualDestIndex);
    }

    public void testReindexIncludesRateLimit() {
        var targetRateLimit = randomFloatBetween(1, 100, true);
        Settings settings = Settings.builder()
            .put(ReindexDataStreamIndexTransportAction.REINDEX_MAX_REQUESTS_PER_SECOND_SETTING.getKey(), targetRateLimit)
            .build();

        String sourceIndex = randomAlphanumericOfLength(10);
        String destIndex = randomAlphanumericOfLength(10);
        ActionListener<BulkByScrollResponse> listener = ActionListener.noop();
        TaskId taskId = TaskId.EMPTY_TASK_ID;

        when(clusterService.getClusterSettings()).thenReturn(
            new ClusterSettings(
                settings,
                Collections.singleton(ReindexDataStreamIndexTransportAction.REINDEX_MAX_REQUESTS_PER_SECOND_SETTING)
            )
        );

        ClusterState clusterState = mock(ClusterState.class);
        when(clusterState.getNodes()).thenReturn(getTestDiscoveryNodes());
        when(clusterService.state()).thenReturn(clusterState);
        doNothing().when(transportService).sendRequest(any(), eq(ReindexAction.NAME), request.capture(), any());

        action.reindex(sourceIndex, destIndex, listener, taskId);

        ReindexRequest requestValue = request.getValue();

        assertEquals(targetRateLimit, requestValue.getRequestsPerSecond(), 0.0);
    }

    public void testReindexIncludesInfiniteRateLimit() {
        Settings settings = Settings.builder()
            .put(ReindexDataStreamIndexTransportAction.REINDEX_MAX_REQUESTS_PER_SECOND_SETTING.getKey(), "-1")
            .build();

        String sourceIndex = randomAlphanumericOfLength(10);
        String destIndex = randomAlphanumericOfLength(10);
        ActionListener<BulkByScrollResponse> listener = ActionListener.noop();
        TaskId taskId = TaskId.EMPTY_TASK_ID;

        when(clusterService.getClusterSettings()).thenReturn(
            new ClusterSettings(
                settings,
                Collections.singleton(ReindexDataStreamIndexTransportAction.REINDEX_MAX_REQUESTS_PER_SECOND_SETTING)
            )
        );
        ClusterState clusterState = mock(ClusterState.class);
        when(clusterState.getNodes()).thenReturn(getTestDiscoveryNodes());
        when(clusterService.state()).thenReturn(clusterState);
        doNothing().when(transportService).sendRequest(any(), eq(ReindexAction.NAME), request.capture(), any());

        action.reindex(sourceIndex, destIndex, listener, taskId);

        ReindexRequest requestValue = request.getValue();

        assertEquals(Float.POSITIVE_INFINITY, requestValue.getRequestsPerSecond(), 0.0);
    }

    public void testReindexZeroRateLimitThrowsError() {
        Settings settings = Settings.builder()
            .put(ReindexDataStreamIndexTransportAction.REINDEX_MAX_REQUESTS_PER_SECOND_SETTING.getKey(), "0")
            .build();

        String sourceIndex = randomAlphanumericOfLength(10);
        String destIndex = randomAlphanumericOfLength(10);
        ActionListener<BulkByScrollResponse> listener = ActionListener.noop();
        TaskId taskId = TaskId.EMPTY_TASK_ID;

        when(clusterService.getClusterSettings()).thenReturn(
            new ClusterSettings(
                settings,
                Collections.singleton(ReindexDataStreamIndexTransportAction.REINDEX_MAX_REQUESTS_PER_SECOND_SETTING)
            )
        );

        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> action.reindex(sourceIndex, destIndex, listener, taskId)
        );
        assertEquals(
            "Failed to parse value [0.0] for setting [migrate.data_stream_reindex_max_request_per_second]"
                + " must be greater than 0 or -1 for infinite",
            e.getMessage()
        );
    }

    public void testReindexNegativeRateLimitThrowsError() {
        float targetRateLimit = randomFloatBetween(-100, -1, true);
        Settings settings = Settings.builder()
            .put(ReindexDataStreamIndexTransportAction.REINDEX_MAX_REQUESTS_PER_SECOND_SETTING.getKey(), targetRateLimit)
            .build();

        String sourceIndex = randomAlphanumericOfLength(10);
        String destIndex = randomAlphanumericOfLength(10);
        ActionListener<BulkByScrollResponse> listener = ActionListener.noop();
        TaskId taskId = TaskId.EMPTY_TASK_ID;

        when(clusterService.getClusterSettings()).thenReturn(
            new ClusterSettings(
                settings,
                Collections.singleton(ReindexDataStreamIndexTransportAction.REINDEX_MAX_REQUESTS_PER_SECOND_SETTING)
            )
        );

        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> action.reindex(sourceIndex, destIndex, listener, taskId)
        );
        assertEquals(
            "Failed to parse value ["
                + targetRateLimit
                + "] for setting [migrate.data_stream_reindex_max_request_per_second]"
                + " must be greater than 0 or -1 for infinite",
            e.getMessage()
        );
    }

    public void testRoundRobin() {
        /*
         * This tests that the action will round-robin through the list of ingest nodes in the cluster.
         */
        String sourceIndex = randomAlphanumericOfLength(10);
        String destIndex = randomAlphanumericOfLength(10);
        AtomicBoolean failed = new AtomicBoolean(false);
        ActionListener<BulkByScrollResponse> listener = new ActionListener<>() {
            @Override
            public void onResponse(BulkByScrollResponse bulkByScrollResponse) {}

            @Override
            public void onFailure(Exception e) {
                failed.set(true);
            }
        };
        TaskId taskId = TaskId.EMPTY_TASK_ID;

        when(clusterService.getClusterSettings()).thenReturn(
            new ClusterSettings(
                Settings.EMPTY,
                Collections.singleton(ReindexDataStreamIndexTransportAction.REINDEX_MAX_REQUESTS_PER_SECOND_SETTING)
            )
        );

        ClusterState clusterState = mock(ClusterState.class);
        when(clusterState.getNodes()).thenReturn(getTestDiscoveryNodes());
        when(clusterService.state()).thenReturn(clusterState);
        ArgumentCaptor<DiscoveryNode> nodeCaptor = ArgumentCaptor.captor();
        doNothing().when(transportService).sendRequest(nodeCaptor.capture(), eq(ReindexAction.NAME), request.capture(), any());

        action.reindex(sourceIndex, destIndex, listener, taskId);
        DiscoveryNode node1 = nodeCaptor.getValue();
        assertNotNull(node1);

        action.reindex(sourceIndex, destIndex, listener, taskId);
        DiscoveryNode node2 = nodeCaptor.getValue();
        assertNotNull(node2);

        int ingestNodeCount = clusterState.getNodes().getIngestNodes().size();
        if (ingestNodeCount > 1) {
            assertThat(node1.getName(), not(equalTo(node2.getName())));
        }

        // check that if we keep going we eventually get back to the original node:
        DiscoveryNode node = node2;
        for (int i = 0; i < ingestNodeCount - 1; i++) {
            action.reindex(sourceIndex, destIndex, listener, taskId);
            node = nodeCaptor.getValue();
        }
        assertNotNull(node);
        assertThat(node1.getName(), equalTo(node.getName()));
        assertThat(failed.get(), equalTo(false));

        // make sure the listener gets notified of failure if there are no ingest nodes:
        when(clusterState.getNodes()).thenReturn(getTestDiscoveryNodesNoIngest());
        action.reindex(sourceIndex, destIndex, listener, taskId);
        assertThat(failed.get(), equalTo(true));
    }

    private DiscoveryNodes getTestDiscoveryNodes() {
        DiscoveryNodes.Builder builder = DiscoveryNodes.builder();
        boolean nodeHasIngestRole = false;
        int nodeCount = randomIntBetween(1, 10);
        for (int i = 0; i < nodeCount; i++) {
            final DiscoveryNode discoveryNode = new DiscoveryNode(
                "test-name-" + i,
                "test-id-" + i,
                "test-ephemeral-id-" + i,
                "test-hostname-" + i,
                "test-hostaddr",
                buildNewFakeTransportAddress(),
                Map.of(),
                randomSet(
                    1,
                    5,
                    () -> randomFrom(
                        DiscoveryNodeRole.DATA_ROLE,
                        DiscoveryNodeRole.INGEST_ROLE,
                        DiscoveryNodeRole.SEARCH_ROLE,
                        DiscoveryNodeRole.MASTER_ROLE,
                        DiscoveryNodeRole.MASTER_ROLE
                    )
                ),
                null,
                null
            );
            nodeHasIngestRole = nodeHasIngestRole || discoveryNode.getRoles().contains(DiscoveryNodeRole.INGEST_ROLE);
            builder.add(discoveryNode);
        }
        if (nodeHasIngestRole == false) {
            final DiscoveryNode discoveryNode = new DiscoveryNode(
                "test-name-" + nodeCount,
                "test-id-" + nodeCount,
                "test-ephemeral-id-" + nodeCount,
                "test-hostname-" + nodeCount,
                "test-hostaddr",
                buildNewFakeTransportAddress(),
                Map.of(),
                Set.of(DiscoveryNodeRole.INGEST_ROLE),
                null,
                null
            );
            builder.add(discoveryNode);
        }
        return builder.build();
    }

    private DiscoveryNodes getTestDiscoveryNodesNoIngest() {
        DiscoveryNodes.Builder builder = DiscoveryNodes.builder();
        int nodeCount = randomIntBetween(0, 10);
        for (int i = 0; i < nodeCount; i++) {
            final DiscoveryNode discoveryNode = new DiscoveryNode(
                "test-name-" + i,
                "test-id-" + i,
                "test-ephemeral-id-" + i,
                "test-hostname-" + i,
                "test-hostaddr",
                buildNewFakeTransportAddress(),
                Map.of(),
                randomSet(
                    1,
                    4,
                    () -> randomFrom(
                        DiscoveryNodeRole.DATA_ROLE,
                        DiscoveryNodeRole.SEARCH_ROLE,
                        DiscoveryNodeRole.MASTER_ROLE,
                        DiscoveryNodeRole.MASTER_ROLE
                    )
                ),
                null,
                null
            );
            builder.add(discoveryNode);
        }
        return builder.build();
    }
}
