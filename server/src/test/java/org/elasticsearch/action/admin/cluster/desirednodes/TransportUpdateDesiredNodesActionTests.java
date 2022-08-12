/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.desirednodes;

import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.coordination.NoMasterBlockService;
import org.elasticsearch.cluster.desirednodes.DesiredNodesSettingsValidator;
import org.elasticsearch.cluster.desirednodes.VersionConflictException;
import org.elasticsearch.cluster.metadata.DesiredNode;
import org.elasticsearch.cluster.metadata.DesiredNodeWithStatus;
import org.elasticsearch.cluster.metadata.DesiredNodes;
import org.elasticsearch.cluster.metadata.DesiredNodesMetadata;
import org.elasticsearch.cluster.metadata.DesiredNodesTestCase;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.tasks.Task;
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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

public class TransportUpdateDesiredNodesActionTests extends DesiredNodesTestCase {

    public static final DesiredNodesSettingsValidator NO_OP_SETTINGS_VALIDATOR = new DesiredNodesSettingsValidator(null) {
        @Override
        public void validate(List<DesiredNode> desiredNodes) {}
    };

    public void testWriteBlocks() {
        final TransportUpdateDesiredNodesAction action = new TransportUpdateDesiredNodesAction(
            mock(TransportService.class),
            mock(ClusterService.class),
            mock(ThreadPool.class),
            mock(ActionFilters.class),
            mock(IndexNameExpressionResolver.class),
            NO_OP_SETTINGS_VALIDATOR,
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
        final TransportUpdateDesiredNodesAction action = new TransportUpdateDesiredNodesAction(
            mock(TransportService.class),
            mock(ClusterService.class),
            mock(ThreadPool.class),
            mock(ActionFilters.class),
            mock(IndexNameExpressionResolver.class),
            NO_OP_SETTINGS_VALIDATOR,
            mock(AllocationService.class)
        );

        final ClusterBlocks blocks = ClusterBlocks.builder().build();
        final ClusterState state = ClusterState.builder(new ClusterName(randomAlphaOfLength(10))).blocks(blocks).build();
        final ClusterBlockException e = action.checkBlock(randomUpdateDesiredNodesRequest(), state);
        assertThat(e, is(nullValue()));
    }

    public void testSettingsGetValidated() throws Exception {
        DesiredNodesSettingsValidator validator = new DesiredNodesSettingsValidator(null) {
            @Override
            public void validate(List<DesiredNode> desiredNodes) {
                throw new IllegalArgumentException("Invalid settings");
            }
        };
        ClusterService clusterService = mock(ClusterService.class);
        final TransportUpdateDesiredNodesAction action = new TransportUpdateDesiredNodesAction(
            mock(TransportService.class),
            clusterService,
            mock(ThreadPool.class),
            mock(ActionFilters.class),
            mock(IndexNameExpressionResolver.class),
            validator,
            mock(AllocationService.class)
        );

        final ClusterState state = ClusterState.builder(new ClusterName(randomAlphaOfLength(10))).build();

        final PlainActionFuture<UpdateDesiredNodesResponse> future = PlainActionFuture.newFuture();
        action.masterOperation(mock(Task.class), randomUpdateDesiredNodesRequest(), state, future);
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, future::actionGet);
        assertThat(exception.getMessage(), containsString("Invalid settings"));

        verify(clusterService, never()).submitUnbatchedStateUpdateTask(any(), any());
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
            request = new UpdateDesiredNodesRequest(desiredNodes.historyID(), desiredNodes.version() + 1, updatedNodes, false);
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
