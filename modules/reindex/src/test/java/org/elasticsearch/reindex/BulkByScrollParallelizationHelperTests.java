/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.reindex;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.index.mapper.IdFieldMapper;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.BulkByScrollTask;
import org.elasticsearch.index.reindex.ReindexAction;
import org.elasticsearch.index.reindex.ReindexRequest;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.TransportService;
import org.mockito.ArgumentCaptor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.elasticsearch.reindex.BulkByScrollParallelizationHelper.sliceIntoSubRequests;
import static org.elasticsearch.search.RandomSearchRequestGenerator.randomSearchRequest;
import static org.elasticsearch.search.RandomSearchRequestGenerator.randomSearchSourceBuilder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class BulkByScrollParallelizationHelperTests extends ESTestCase {
    public void testSliceIntoSubRequests() throws IOException {
        SearchRequest searchRequest = randomSearchRequest(
            () -> randomSearchSourceBuilder(() -> null, () -> null, () -> null, Collections::emptyList, () -> null, () -> null)
        );
        if (searchRequest.source() != null) {
            // Clear the slice builder if there is one set. We can't call sliceIntoSubRequests if it is.
            searchRequest.source().slice(null);
        }
        int times = between(2, 100);
        String field = randomBoolean() ? IdFieldMapper.NAME : randomAlphaOfLength(5);
        int currentSliceId = 0;
        for (SearchRequest slice : sliceIntoSubRequests(searchRequest, field, times)) {
            assertEquals(field, slice.source().slice().getField());
            assertEquals(currentSliceId, slice.source().slice().getId());
            assertEquals(times, slice.source().slice().getMax());

            // If you clear the slice then the slice should be the same request as the parent request
            slice.source().slice(null);
            if (searchRequest.source() == null) {
                // Except that adding the slice might have added an empty builder
                searchRequest.source(new SearchSourceBuilder());
            }
            assertEquals(searchRequest, slice);
            currentSliceId++;
        }
    }

    public void testExecuteSlicedAction() {
        ActionType<BulkByScrollResponse> action = ReindexAction.INSTANCE;
        DiscoveryNode localNode = getTestDiscoveryNode(0, randomBoolean());
        String localNodeId = localNode.getId();
        BulkByScrollTask task = new BulkByScrollTask(
            randomLong(),
            randomAlphaOfLength(10),
            action.name(),
            randomAlphaOfLength(10),
            new TaskId(localNodeId, randomLong()),
            Map.of()
        );
        int numberOfSlices = randomIntBetween(2, 100);
        task.setWorkerCount(numberOfSlices);
        ReindexRequest request = new ReindexRequest();
        AtomicBoolean failed = new AtomicBoolean(false);
        ActionListener<BulkByScrollResponse> listener = new ActionListener<>() {
            @Override
            public void onResponse(BulkByScrollResponse bulkByScrollResponse) {}

            @Override
            public void onFailure(Exception e) {
                failed.set(true);
            }
        };
        Client client = mock(Client.class);
        Runnable workerAction = () -> {};
        TransportService transportService = mock(TransportService.class);
        ClusterService clusterService = mock(ClusterService.class);
        ClusterState clusterState = mock(ClusterState.class);
        ArgumentCaptor<DiscoveryNode> nodeCaptor = ArgumentCaptor.captor();
        ArgumentCaptor<ReindexRequest> requestCaptor = ArgumentCaptor.captor();
        doNothing().when(transportService).sendRequest(nodeCaptor.capture(), eq(ReindexAction.NAME), requestCaptor.capture(), any());
        {
            // We have multiple ingest nodes, so we make sure that requests are not always sent to the same one
            when(clusterService.state()).thenReturn(clusterState);
            when(clusterState.getNodes()).thenReturn(getTestDiscoveryNodes(randomIntBetween(2, 10)));
            BulkByScrollParallelizationHelper.executeSlicedAction(
                task,
                request,
                action,
                listener,
                client,
                localNode,
                workerAction,
                transportService,
                clusterService
            );
            verify(transportService, times(numberOfSlices)).sendRequest(any(), any(), any(), any());
            verify(client, times(0)).execute(any(), any(), any());
            List<DiscoveryNode> nodesUsed = nodeCaptor.getAllValues();
            assertThat(nodesUsed.size(), equalTo(numberOfSlices));
            DiscoveryNode firstNode = nodesUsed.get(0);
            assertNotNull(firstNode);
            DiscoveryNode previousNode = firstNode;
            for (int i = 1; i < nodesUsed.size(); i++) {
                DiscoveryNode node = nodesUsed.get(i);
                assertNotNull(node);
                assertThat(node.getId(), not(equalTo(previousNode.getId())));
                previousNode = node;
            }
            assertThat(failed.get(), equalTo(false));
        }
        /*
         * If there are no ingest nodes, we expect it to just use the client. If we have a single ingest node, we have no need of
         * round-robin, so we expect it to use the client
         */
        for (int ingestNodeCount = 0; ingestNodeCount < 2; ingestNodeCount++) {
            when(clusterService.state()).thenReturn(clusterState);
            when(clusterState.getNodes()).thenReturn(getTestDiscoveryNodes(ingestNodeCount));
            reset(client);
            reset(transportService);
            BulkByScrollParallelizationHelper.executeSlicedAction(
                task,
                request,
                action,
                listener,
                client,
                localNode,
                workerAction,
                transportService,
                clusterService
            );
            assertThat(failed.get(), equalTo(false));
            verify(transportService, times(0)).sendRequest(any(), any(), any(), any());
            verify(client, times(numberOfSlices)).execute(any(), any(), any());
            assertThat(failed.get(), equalTo(false));
        }
    }

    private DiscoveryNodes getTestDiscoveryNodes(int ingestNodeCount) {
        DiscoveryNodes.Builder builder = DiscoveryNodes.builder();
        int additionalNodeCount = randomIntBetween(ingestNodeCount == 0 ? 1 : 0, 10);
        List<DiscoveryNode> discoveryNodeList = new ArrayList<>();
        for (int i = 0; i < ingestNodeCount; i++) {
            discoveryNodeList.add(getTestDiscoveryNode(i, true));
        }
        for (int i = 0; i < additionalNodeCount; i++) {
            discoveryNodeList.add(getTestDiscoveryNode(i + ingestNodeCount, false));
        }
        for (DiscoveryNode discoveryNode : discoveryNodeList) {
            builder.add(discoveryNode);
        }
        builder.localNodeId(discoveryNodeList.stream().map(DiscoveryNode::getId).findAny().orElseThrow());
        return builder.build();
    }

    private DiscoveryNode getTestDiscoveryNode(int i, boolean ingestNode) {
        Set<DiscoveryNodeRole> roles = new HashSet<>();
        if (ingestNode) {
            roles.add(DiscoveryNodeRole.INGEST_ROLE);
        }
        Set<DiscoveryNodeRole> otherRoles = randomSet(
            ingestNode ? 0 : 1,
            4,
            () -> randomFrom(
                DiscoveryNodeRole.DATA_ROLE,
                DiscoveryNodeRole.SEARCH_ROLE,
                DiscoveryNodeRole.MASTER_ROLE,
                DiscoveryNodeRole.MASTER_ROLE
            )
        );
        roles.addAll(otherRoles);
        return new DiscoveryNode(
            "test-name-" + i,
            "test-id-" + i,
            "test-ephemeral-id-" + i,
            "test-hostname-" + i,
            "test-hostaddr",
            buildNewFakeTransportAddress(),
            Map.of(),
            roles,
            null,
            null
        );
    }
}
