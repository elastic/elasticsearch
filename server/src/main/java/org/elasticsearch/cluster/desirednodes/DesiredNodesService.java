/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.desirednodes;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.desirednodes.UpdateDesiredNodesRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.AckedClusterStateUpdateTask;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateTaskExecutor;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Priority;

import java.util.List;

public class DesiredNodesService {
    private final ClusterService clusterService;

    public DesiredNodesService(ClusterService clusterService) {
        this.clusterService = clusterService;
    }

    public void updateDesiredNodes(UpdateDesiredNodesRequest request, ActionListener<AcknowledgedResponse> listener) {
        // TODO: validate settings
        clusterService.submitStateUpdateTask("update-desired-nodes", new AckedClusterStateUpdateTask(Priority.HIGH, request, listener) {
            @Override
            public ClusterState execute(ClusterState currentState) {
                DesiredNodesMetadata currentDesiredNodesMetadata = currentState.metadata().custom(DesiredNodesMetadata.TYPE);
                DesiredNodes currentDesiredNodes = currentDesiredNodesMetadata.getCurrentDesiredNodes();
                DesiredNodes proposedDesiredNodes = new DesiredNodes(request.getHistoryID(), request.getVersion(), request.getNodes());

                if (currentDesiredNodes.isSupersededBy(proposedDesiredNodes) == false) {
                    throw new IllegalArgumentException("Unexpected");
                }

                if (currentDesiredNodes.hasSameVersion(proposedDesiredNodes) && currentDesiredNodes.equals(proposedDesiredNodes) == false) {
                    throw new IllegalArgumentException();
                }

                return ClusterState.builder(currentState)
                    .metadata(
                        Metadata.builder(currentState.metadata())
                            .putCustom(DesiredNodesMetadata.TYPE, new DesiredNodesMetadata(List.of(proposedDesiredNodes)))
                    )
                    .build();
            }
        }, ClusterStateTaskExecutor.unbatched());
    }
}
