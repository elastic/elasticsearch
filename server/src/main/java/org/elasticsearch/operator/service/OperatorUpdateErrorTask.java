/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.operator.service;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateTaskExecutor;
import org.elasticsearch.cluster.ClusterStateTaskListener;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.OperatorErrorMetadata;
import org.elasticsearch.cluster.metadata.OperatorMetadata;

import java.util.List;

/**
 * Cluster state update task that sets the error state of the operator metadata.
 * This is used when an operator cluster state update encounters error(s) while processing
 * the file.
 */
public class OperatorUpdateErrorTask implements ClusterStateTaskListener {

    private final OperatorClusterStateController.OperatorErrorState errorState;
    private final ActionListener<ActionResponse.Empty> listener;

    public OperatorUpdateErrorTask(
        OperatorClusterStateController.OperatorErrorState errorState,
        ActionListener<ActionResponse.Empty> listener
    ) {
        this.errorState = errorState;
        this.listener = listener;
    }

    private static final Logger logger = LogManager.getLogger(FileSettingsService.class);

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
        OperatorMetadata operatorMetadata = currentState.metadata().operatorState(errorState.namespace());
        OperatorMetadata.Builder operatorMetadataBuilder = OperatorMetadata.builder(errorState.namespace(), operatorMetadata);
        operatorMetadataBuilder.errorMetadata(
            OperatorErrorMetadata.builder()
                .version(errorState.version())
                .errorKind(errorState.errorKind())
                .errors(errorState.errors())
                .build()
        );
        metadataBuilder.putOperatorState(operatorMetadataBuilder.build());
        ClusterState newState = stateBuilder.metadata(metadataBuilder).build();

        return newState;
    }

    /**
     * Operator update cluster state task executor
     */
    public record OperatorUpdateErrorTaskExecutor() implements ClusterStateTaskExecutor<OperatorUpdateErrorTask> {

        @Override
        public ClusterState execute(ClusterState currentState, List<TaskContext<OperatorUpdateErrorTask>> taskContexts) throws Exception {
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
            logger.info("Wrote new error state in operator metadata");
        }
    }
}
