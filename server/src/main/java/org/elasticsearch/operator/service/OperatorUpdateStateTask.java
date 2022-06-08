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
import org.elasticsearch.cluster.metadata.OperatorHandlerMetadata;
import org.elasticsearch.cluster.metadata.OperatorMetadata;
import org.elasticsearch.cluster.routing.RerouteService;
import org.elasticsearch.common.Priority;
import org.elasticsearch.operator.OperatorHandler;
import org.elasticsearch.operator.TransformState;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

import static org.elasticsearch.core.Strings.format;

/**
 * Generic operator cluster state update task
 */
public class OperatorUpdateStateTask implements ClusterStateTaskListener {
    private static final Logger logger = LogManager.getLogger(FileSettingsService.class);

    private final String namespace;
    private final OperatorClusterStateController.SettingsFile operatorStateFileContent;
    private final Map<String, OperatorHandler<?>> handlers;
    private final Collection<String> orderedHandlers;
    private final Consumer<OperatorClusterStateController.OperatorErrorState> recordErrorState;
    private final ActionListener<ActionResponse.Empty> listener;

    public OperatorUpdateStateTask(
        String namespace,
        OperatorClusterStateController.SettingsFile operatorStateFileContent,
        Map<String, OperatorHandler<?>> handlers,
        Collection<String> orderedHandlers,
        Consumer<OperatorClusterStateController.OperatorErrorState> recordErrorState,
        ActionListener<ActionResponse.Empty> listener
    ) {
        this.namespace = namespace;
        this.operatorStateFileContent = operatorStateFileContent;
        this.handlers = handlers;
        this.orderedHandlers = orderedHandlers;
        this.recordErrorState = recordErrorState;
        this.listener = listener;
    }

    @Override
    public void onFailure(Exception e) {
        listener.onFailure(e);
    }

    ActionListener<ActionResponse.Empty> listener() {
        return listener;
    }

    protected ClusterState execute(ClusterState state) {
        OperatorMetadata existingMetadata = state.metadata().operatorState(namespace);
        Map<String, Object> operatorState = operatorStateFileContent.state;
        OperatorStateVersionMetadata stateVersionMetadata = operatorStateFileContent.metadata;

        OperatorMetadata.Builder operatorMetadataBuilder = new OperatorMetadata.Builder(namespace).version(stateVersionMetadata.version());
        List<String> errors = new ArrayList<>();

        for (var handlerKey : orderedHandlers) {
            OperatorHandler<?> handler = handlers.get(handlerKey);
            try {
                Set<String> existingKeys = keysForHandler(existingMetadata, handlerKey);
                TransformState transformState = handler.transform(operatorState.get(handlerKey), new TransformState(state, existingKeys));
                state = transformState.state();
                operatorMetadataBuilder.putHandler(new OperatorHandlerMetadata.Builder(handlerKey).keys(transformState.keys()).build());
            } catch (Exception e) {
                errors.add(format("Error processing %s state change: %s", handler.name(), e.getMessage()));
            }
        }

        if (errors.isEmpty() == false) {
            recordErrorState.accept(
                new OperatorClusterStateController.OperatorErrorState(
                    namespace,
                    stateVersionMetadata.version(),
                    errors,
                    OperatorErrorMetadata.ErrorKind.VALIDATION
                )
            );
            logger.error("Error processing state change request for [{}] with the following errors [{}]", namespace, errors);

            throw new IllegalStateException("Error processing state change request for " + namespace);
        }

        // remove the last error if we had previously encountered any
        operatorMetadataBuilder.errorMetadata(null);

        ClusterState.Builder stateBuilder = new ClusterState.Builder(state);
        Metadata.Builder metadataBuilder = Metadata.builder(state.metadata()).putOperatorState(operatorMetadataBuilder.build());

        return stateBuilder.metadata(metadataBuilder).build();
    }

    private Set<String> keysForHandler(OperatorMetadata operatorMetadata, String handlerKey) {
        if (operatorMetadata == null || operatorMetadata.handlers().get(handlerKey) == null) {
            return Collections.emptySet();
        }

        return operatorMetadata.handlers().get(handlerKey).keys();
    }

    /**
     * Operator update cluster state task executor
     *
     * @param namespace of the state we are updating
     * @param rerouteService instance of RerouteService so we can execute reroute after cluster state is published
     */
    public record OperatorUpdateStateTaskExecutor(String namespace, RerouteService rerouteService)
        implements
            ClusterStateTaskExecutor<OperatorUpdateStateTask> {

        @Override
        public ClusterState execute(ClusterState currentState, List<TaskContext<OperatorUpdateStateTask>> taskContexts) throws Exception {
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
            rerouteService.reroute(
                "reroute after applying operator cluster state for namespace [" + namespace + "]",
                Priority.NORMAL,
                ActionListener.wrap(
                    r -> logger.trace("reroute after applying operator cluster state for [{}] succeeded", namespace),
                    e -> logger.debug("reroute after applying operator cluster state failed", e)
                )
            );
        }
    }
}
