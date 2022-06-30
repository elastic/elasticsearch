/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.immutablestate.service;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateTaskExecutor;
import org.elasticsearch.cluster.ClusterStateTaskListener;
import org.elasticsearch.cluster.metadata.ImmutableStateErrorMetadata;
import org.elasticsearch.cluster.metadata.ImmutableStateMetadata;
import org.elasticsearch.cluster.metadata.Metadata;

import java.util.List;

/**
 * Cluster state update task that sets the error state of the immutable cluster state metadata.
 * <p>
 * This is used when an immutable cluster state update encounters error(s) while processing
 * the {@link org.elasticsearch.immutablestate.service.ImmutableClusterStateController.Package}.
 */
public class ImmutableStateUpdateErrorTask implements ClusterStateTaskListener {

    private final ImmutableClusterStateController.ImmutableUpdateErrorState errorState;
    private final ActionListener<ActionResponse.Empty> listener;

    public ImmutableStateUpdateErrorTask(
        ImmutableClusterStateController.ImmutableUpdateErrorState errorState,
        ActionListener<ActionResponse.Empty> listener
    ) {
        this.errorState = errorState;
        this.listener = listener;
    }

    private static final Logger logger = LogManager.getLogger(ImmutableStateUpdateErrorTask.class);

    @Override
    public void onFailure(Exception e) {
        listener.onFailure(e);
    }

    ActionListener<ActionResponse.Empty> listener() {
        return listener;
    }

    ClusterState execute(ClusterState currentState) {
        ClusterState.Builder stateBuilder = new ClusterState.Builder(currentState);
        Metadata.Builder metadataBuilder = Metadata.builder(currentState.metadata());
        ImmutableStateMetadata immutableMetadata = currentState.metadata().immutableStateMetadata().get(errorState.namespace());
        ImmutableStateMetadata.Builder immMetadataBuilder = ImmutableStateMetadata.builder(errorState.namespace(), immutableMetadata);
        immMetadataBuilder.errorMetadata(
            new ImmutableStateErrorMetadata(errorState.version(), errorState.errorKind(), errorState.errors())
        );
        metadataBuilder.put(immMetadataBuilder.build());
        ClusterState newState = stateBuilder.metadata(metadataBuilder).build();

        return newState;
    }

    /**
     * Immutable cluster error state task executor
     */
    public record ImmutableUpdateErrorTaskExecutor() implements ClusterStateTaskExecutor<ImmutableStateUpdateErrorTask> {

        @Override
        public ClusterState execute(ClusterState currentState, List<TaskContext<ImmutableStateUpdateErrorTask>> taskContexts)
            throws Exception {
            for (final var taskContext : taskContexts) {
                currentState = taskContext.getTask().execute(currentState);
                taskContext.success(
                    () -> taskContext.getTask().listener().delegateFailure((l, s) -> l.onResponse(ActionResponse.Empty.INSTANCE))
                );
            }
            return currentState;
        }

        @Override
        public void clusterStatePublished(ClusterState newClusterState) {
            logger.info("Wrote new error state in immutable metadata");
        }
    }
}
