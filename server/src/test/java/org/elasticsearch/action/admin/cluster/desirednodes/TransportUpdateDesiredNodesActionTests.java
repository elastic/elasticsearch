/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.cluster.desirednodes;

import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.coordination.NoMasterBlockService;
import org.elasticsearch.cluster.desirednodes.VersionConflictException;
import org.elasticsearch.cluster.metadata.DesiredNode;
import org.elasticsearch.cluster.metadata.DesiredNodeWithStatus;
import org.elasticsearch.cluster.metadata.DesiredNodes;
import org.elasticsearch.cluster.metadata.DesiredNodesMetadata;
import org.elasticsearch.cluster.metadata.DesiredNodesTestCase;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.routing.RerouteService;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.test.MockUtils;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Mockito.mock;

public class TransportUpdateDesiredNodesActionTests extends DesiredNodesTestCase {

    public void testWriteBlocks() {
        ThreadPool threadPool = mock(ThreadPool.class);
        TransportService transportService = MockUtils.setupTransportServiceWithThreadpoolExecutor(threadPool);
        final TransportUpdateDesiredNodesAction action = new TransportUpdateDesiredNodesAction(
            transportService,
            mock(ClusterService.class),
            mock(RerouteService.class),
            threadPool,
            mock(ActionFilters.class),
            mock(AllocationService.class)
        );

        final ClusterBlocks blocks = ClusterBlocks.builder()
            .addGlobalBlock(
                randomFrom(
                    Metadata.CLUSTER_READ_ONLY_BLOCK,
                    Metadata.CLUSTER_READ_ONLY_ALLOW_DELETE_BLOCK,
                    NoMasterBlockService.NO_MASTER_BLOCK_WRITES
                )
            )
            .build();
        final ClusterState state = ClusterState.builder(new ClusterName(randomAlphaOfLength(10))).blocks(blocks).build();
        final ClusterBlockException e = action.checkBlock(randomUpdateDesiredNodesRequest(), state);
        assertThat(e, not(nullValue()));
    }

    public void testNoBlocks() {
        ThreadPool threadPool = mock(ThreadPool.class);
        TransportService transportService = MockUtils.setupTransportServiceWithThreadpoolExecutor(threadPool);
        final TransportUpdateDesiredNodesAction action = new TransportUpdateDesiredNodesAction(
            transportService,
            mock(ClusterService.class),
            mock(RerouteService.class),
            threadPool,
            mock(ActionFilters.class),
            mock(AllocationService.class)
        );

        final ClusterBlocks blocks = ClusterBlocks.builder().build();
        final ClusterState state = ClusterState.builder(new ClusterName(randomAlphaOfLength(10))).blocks(blocks).build();
        final ClusterBlockException e = action.checkBlock(randomUpdateDesiredNodesRequest(), state);
        assertThat(e, is(nullValue()));
    }

    public void testUpdateDesiredNodes() {
        final Metadata.Builder metadataBuilder = Metadata.builder();
        boolean containsDesiredNodes = false;
        if (randomBoolean()) {
            containsDesiredNodes = randomBoolean();
            final DesiredNodesMetadata desiredNodesMetadata = containsDesiredNodes
                ? new DesiredNodesMetadata(randomDesiredNodes())
                : DesiredNodesMetadata.EMPTY;
            metadataBuilder.putCustom(DesiredNodesMetadata.TYPE, desiredNodesMetadata);
        }

        final ClusterState currentClusterState = ClusterState.builder(new ClusterName(randomAlphaOfLength(10)))
            .metadata(metadataBuilder)
            .build();

        final UpdateDesiredNodesRequest request;
        final boolean updateSameHistory = containsDesiredNodes && randomBoolean();
        if (updateSameHistory) {
            // increase the version for the current history and maybe modify the nodes
            final DesiredNodes desiredNodes = DesiredNodes.latestFromClusterState(currentClusterState);
            final List<DesiredNode> updatedNodes = randomSubsetOf(randomIntBetween(1, desiredNodes.nodes().size()), desiredNodes.nodes())
                .stream()
                .map(DesiredNodeWithStatus::desiredNode)
                .toList();
            request = new UpdateDesiredNodesRequest(
                TEST_REQUEST_TIMEOUT,
                TEST_REQUEST_TIMEOUT,
                desiredNodes.historyID(),
                desiredNodes.version() + 1,
                updatedNodes,
                false
            );
        } else {
            request = randomUpdateDesiredNodesRequest();
        }

        final ClusterState updatedClusterState = TransportUpdateDesiredNodesAction.replaceDesiredNodes(
            currentClusterState,
            TransportUpdateDesiredNodesAction.updateDesiredNodes(DesiredNodes.latestFromClusterState(currentClusterState), request)
        );
        final DesiredNodesMetadata desiredNodesMetadata = updatedClusterState.metadata().custom(DesiredNodesMetadata.TYPE);
        assertThat(desiredNodesMetadata, is(notNullValue()));

        final DesiredNodes desiredNodes = desiredNodesMetadata.getLatestDesiredNodes();
        assertThat(desiredNodes, is(notNullValue()));
        assertThat(desiredNodes.historyID(), is(equalTo(request.getHistoryID())));
        assertThat(desiredNodes.version(), is(equalTo(request.getVersion())));
        assertThat(
            desiredNodes.nodes().stream().map(DesiredNodeWithStatus::desiredNode).toList(),
            containsInAnyOrder(request.getNodes().toArray())
        );
    }

    public void testUpdatesAreIdempotent() {
        final var updateDesiredNodesRequest = randomUpdateDesiredNodesRequest();
        final var latestDesiredNodes = TransportUpdateDesiredNodesAction.updateDesiredNodes(null, updateDesiredNodesRequest);

        final List<DesiredNode> equivalentDesiredNodesList = new ArrayList<>(updateDesiredNodesRequest.getNodes());
        if (randomBoolean()) {
            Collections.shuffle(equivalentDesiredNodesList, random());
        }
        final UpdateDesiredNodesRequest equivalentDesiredNodesRequest = new UpdateDesiredNodesRequest(
            TEST_REQUEST_TIMEOUT,
            TEST_REQUEST_TIMEOUT,
            updateDesiredNodesRequest.getHistoryID(),
            updateDesiredNodesRequest.getVersion(),
            equivalentDesiredNodesList,
            updateDesiredNodesRequest.isDryRun()
        );

        assertSame(
            latestDesiredNodes,
            TransportUpdateDesiredNodesAction.updateDesiredNodes(latestDesiredNodes, equivalentDesiredNodesRequest)
        );
    }

    public void testUpdateSameHistoryAndVersionWithDifferentContentsFails() {
        final var updateDesiredNodesRequest = randomUpdateDesiredNodesRequest();
        final var latestDesiredNodes = TransportUpdateDesiredNodesAction.updateDesiredNodes(null, updateDesiredNodesRequest);

        final UpdateDesiredNodesRequest request = new UpdateDesiredNodesRequest(
            TEST_REQUEST_TIMEOUT,
            TEST_REQUEST_TIMEOUT,
            latestDesiredNodes.historyID(),
            latestDesiredNodes.version(),
            randomList(1, 10, DesiredNodesTestCase::randomDesiredNode),
            false
        );

        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> TransportUpdateDesiredNodesAction.updateDesiredNodes(latestDesiredNodes, request)
        );
        assertThat(exception.getMessage(), containsString("already exists with a different definition"));
    }

    public void testBackwardUpdatesFails() {
        final var updateDesiredNodesRequest = randomUpdateDesiredNodesRequest();
        final var latestDesiredNodes = TransportUpdateDesiredNodesAction.updateDesiredNodes(null, updateDesiredNodesRequest);
        final UpdateDesiredNodesRequest request = new UpdateDesiredNodesRequest(
            TEST_REQUEST_TIMEOUT,
            TEST_REQUEST_TIMEOUT,
            latestDesiredNodes.historyID(),
            latestDesiredNodes.version() - 1,
            List.copyOf(latestDesiredNodes.nodes().stream().map(DesiredNodeWithStatus::desiredNode).toList()),
            false
        );

        VersionConflictException exception = expectThrows(
            VersionConflictException.class,
            () -> TransportUpdateDesiredNodesAction.updateDesiredNodes(latestDesiredNodes, request)
        );
        assertThat(exception.getMessage(), containsString("has been superseded by version"));
    }
}
