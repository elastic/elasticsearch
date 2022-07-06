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
import org.elasticsearch.cluster.metadata.ImmutableStateHandlerMetadata;
import org.elasticsearch.cluster.metadata.ImmutableStateMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.routing.RerouteService;
import org.elasticsearch.common.Priority;
import org.elasticsearch.immutablestate.ImmutableClusterStateHandler;
import org.elasticsearch.immutablestate.TransformState;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

import static org.elasticsearch.core.Strings.format;

/**
 * Generic immutable cluster state update task
 */
public class ImmutableStateUpdateStateTask implements ClusterStateTaskListener {
    private static final Logger logger = LogManager.getLogger(ImmutableStateUpdateStateTask.class);

    private final String namespace;
    private final ImmutableClusterStateController.Package immutableStatePackage;
    private final Map<String, ImmutableClusterStateHandler<?>> handlers;
    private final Collection<String> orderedHandlers;
    private final Consumer<ImmutableClusterStateController.ImmutableUpdateErrorState> recordErrorState;
    private final ActionListener<ActionResponse.Empty> listener;

    public ImmutableStateUpdateStateTask(
        String namespace,
        ImmutableClusterStateController.Package immutableStatePackage,
        Map<String, ImmutableClusterStateHandler<?>> handlers,
        Collection<String> orderedHandlers,
        Consumer<ImmutableClusterStateController.ImmutableUpdateErrorState> recordErrorState,
        ActionListener<ActionResponse.Empty> listener
    ) {
        this.namespace = namespace;
        this.immutableStatePackage = immutableStatePackage;
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
        ImmutableStateMetadata existingMetadata = state.metadata().immutableStateMetadata().get(namespace);
        Map<String, Object> immutableState = immutableStatePackage.state();
        PackageVersion packageVersion = immutableStatePackage.metadata();

        var immutableMetadataBuilder = new ImmutableStateMetadata.Builder(namespace).version(packageVersion.version());
        List<String> errors = new ArrayList<>();

        for (var handlerName : orderedHandlers) {
            ImmutableClusterStateHandler<?> handler = handlers.get(handlerName);
            try {
                Set<String> existingKeys = keysForHandler(existingMetadata, handlerName);
                TransformState transformState = handler.transform(immutableState.get(handlerName), new TransformState(state, existingKeys));
                state = transformState.state();
                immutableMetadataBuilder.putHandler(new ImmutableStateHandlerMetadata(handlerName, transformState.keys()));
            } catch (Exception e) {
                errors.add(format("Error processing %s state change: %s", handler.name(), e.getMessage()));
            }
        }

        if (errors.isEmpty() == false) {
            // Check if we had previous error metadata with version information, don't spam with cluster state updates, if the
            // version hasn't been updated.
            if (existingMetadata != null
                && existingMetadata.errorMetadata() != null
                && existingMetadata.errorMetadata().version() >= packageVersion.version()) {
                logger.error("Error processing state change request for [{}] with the following errors [{}]", namespace, errors);

                throw new ImmutableClusterStateController.IncompatibleVersionException(
                    format(
                        "Not updating error state because version [%s] is less or equal to the last state error version [%s]",
                        packageVersion.version(),
                        existingMetadata.errorMetadata().version()
                    )
                );
            }

            recordErrorState.accept(
                new ImmutableClusterStateController.ImmutableUpdateErrorState(
                    namespace,
                    packageVersion.version(),
                    errors,
                    ImmutableStateErrorMetadata.ErrorKind.VALIDATION
                )
            );
            logger.error("Error processing state change request for [{}] with the following errors [{}]", namespace, errors);

            throw new IllegalStateException("Error processing state change request for " + namespace);
        }

        // remove the last error if we had previously encountered any
        immutableMetadataBuilder.errorMetadata(null);

        ClusterState.Builder stateBuilder = new ClusterState.Builder(state);
        Metadata.Builder metadataBuilder = Metadata.builder(state.metadata()).put(immutableMetadataBuilder.build());

        return stateBuilder.metadata(metadataBuilder).build();
    }

    private Set<String> keysForHandler(ImmutableStateMetadata immutableStateMetadata, String handlerName) {
        if (immutableStateMetadata == null || immutableStateMetadata.handlers().get(handlerName) == null) {
            return Collections.emptySet();
        }

        return immutableStateMetadata.handlers().get(handlerName).keys();
    }

    /**
     * Immutable cluster state update task executor
     *
     * @param namespace of the state we are updating
     * @param rerouteService instance of {@link RerouteService}, so that we can execute reroute after cluster state is published
     */
    public record ImmutableUpdateStateTaskExecutor(String namespace, RerouteService rerouteService)
        implements
            ClusterStateTaskExecutor<ImmutableStateUpdateStateTask> {

        @Override
        public ClusterState execute(ClusterState currentState, List<TaskContext<ImmutableStateUpdateStateTask>> taskContexts)
            throws Exception {
            for (final var taskContext : taskContexts) {
                currentState = taskContext.getTask().execute(currentState);
                taskContext.success(() -> taskContext.getTask().listener().onResponse(ActionResponse.Empty.INSTANCE));
            }
            return currentState;
        }

        @Override
        public void clusterStatePublished(ClusterState newClusterState) {
            rerouteService.reroute(
                "reroute after applying immutable cluster state for namespace [" + namespace + "]",
                Priority.NORMAL,
                ActionListener.wrap(
                    r -> logger.trace("reroute after applying immutable cluster state for [{}] succeeded", namespace),
                    e -> logger.debug("reroute after applying immutable cluster state failed", e)
                )
            );
        }
    }
}
