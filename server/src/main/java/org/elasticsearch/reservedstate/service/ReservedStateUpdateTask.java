/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.reservedstate.service;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateTaskListener;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ReservedStateErrorMetadata;
import org.elasticsearch.cluster.metadata.ReservedStateHandlerMetadata;
import org.elasticsearch.cluster.metadata.ReservedStateMetadata;
import org.elasticsearch.reservedstate.ReservedClusterStateHandler;
import org.elasticsearch.reservedstate.TransformState;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;

import static org.elasticsearch.ExceptionsHelper.stackTrace;
import static org.elasticsearch.core.Strings.format;

/**
 * Generic task to update and reserve parts of the cluster state
 *
 * <p>
 * Reserved cluster state can only be modified by using the {@link ReservedClusterStateService}. Updating
 * the reserved cluster state through REST APIs is not permitted.
 */
public class ReservedStateUpdateTask implements ClusterStateTaskListener {
    private static final Logger logger = LogManager.getLogger(ReservedStateUpdateTask.class);

    private final String namespace;
    private final ReservedStateChunk stateChunk;
    private final Map<String, ReservedClusterStateHandler<?>> handlers;
    private final Collection<String> orderedHandlers;
    private final BiConsumer<ClusterState, ErrorState> errorReporter;
    private final ActionListener<ActionResponse.Empty> listener;

    public ReservedStateUpdateTask(
        String namespace,
        ReservedStateChunk stateChunk,
        Map<String, ReservedClusterStateHandler<?>> handlers,
        Collection<String> orderedHandlers,
        BiConsumer<ClusterState, ErrorState> errorReporter,
        ActionListener<ActionResponse.Empty> listener
    ) {
        this.namespace = namespace;
        this.stateChunk = stateChunk;
        this.handlers = handlers;
        this.orderedHandlers = orderedHandlers;
        this.errorReporter = errorReporter;
        this.listener = listener;
    }

    @Override
    public void onFailure(Exception e) {
        listener.onFailure(e);
    }

    ActionListener<ActionResponse.Empty> listener() {
        return listener;
    }

    protected ClusterState execute(final ClusterState currentState) {
        ReservedStateMetadata existingMetadata = currentState.metadata().reservedStateMetadata().get(namespace);
        Map<String, Object> reservedState = stateChunk.state();
        ReservedStateVersion reservedStateVersion = stateChunk.metadata();

        if (checkMetadataVersion(namespace, existingMetadata, reservedStateVersion) == false) {
            return currentState;
        }

        var reservedMetadataBuilder = new ReservedStateMetadata.Builder(namespace).version(reservedStateVersion.version());
        List<String> errors = new ArrayList<>();

        ClusterState state = currentState;
        for (var handlerName : orderedHandlers) {
            ReservedClusterStateHandler<?> handler = handlers.get(handlerName);
            try {
                Set<String> existingKeys = keysForHandler(existingMetadata, handlerName);
                TransformState transformState = handler.transform(reservedState.get(handlerName), new TransformState(state, existingKeys));
                state = transformState.state();
                reservedMetadataBuilder.putHandler(new ReservedStateHandlerMetadata(handlerName, transformState.keys()));
            } catch (Exception e) {
                errors.add(format("Error processing %s state change: %s", handler.name(), stackTrace(e)));
            }
        }

        if (errors.isEmpty() == false) {
            // Check if we had previous error metadata with version information, don't spam with cluster state updates, if the
            // version hasn't been updated.
            logger.debug("Error processing state change request for [{}] with the following errors [{}]", namespace, errors);

            var errorState = new ErrorState(
                namespace,
                reservedStateVersion.version(),
                errors,
                ReservedStateErrorMetadata.ErrorKind.VALIDATION
            );

            errorReporter.accept(currentState, errorState);

            throw new IllegalStateException("Error processing state change request for " + namespace + ", errors: " + errorState);
        }

        // remove the last error if we had previously encountered any
        reservedMetadataBuilder.errorMetadata(null);

        ClusterState.Builder stateBuilder = new ClusterState.Builder(state);
        Metadata.Builder metadataBuilder = Metadata.builder(state.metadata()).put(reservedMetadataBuilder.build());

        return stateBuilder.metadata(metadataBuilder).build();
    }

    private Set<String> keysForHandler(ReservedStateMetadata reservedStateMetadata, String handlerName) {
        if (reservedStateMetadata == null || reservedStateMetadata.handlers().get(handlerName) == null) {
            return Collections.emptySet();
        }

        return reservedStateMetadata.handlers().get(handlerName).keys();
    }

    static boolean checkMetadataVersion(
        String namespace,
        ReservedStateMetadata existingMetadata,
        ReservedStateVersion reservedStateVersion
    ) {
        if (Version.CURRENT.before(reservedStateVersion.minCompatibleVersion())) {
            logger.warn(
                () -> format(
                    "Reserved cluster state version [%s] for namespace [%s] is not compatible with this Elasticsearch node",
                    reservedStateVersion.minCompatibleVersion(),
                    namespace
                )
            );
            return false;
        }

        // Version 0 is special, snapshot restores will reset to 0.
        if (reservedStateVersion.version() <= 0L) {
            logger.warn(
                () -> format(
                    "Not updating reserved cluster state for namespace [%s], because version [%s] is less or equal to 0",
                    namespace,
                    reservedStateVersion.version(),
                    existingMetadata.version()
                )
            );
            return false;
        }

        if (existingMetadata != null && existingMetadata.version() >= reservedStateVersion.version()) {
            logger.warn(
                () -> format(
                    "Not updating reserved cluster state for namespace [%s], because version [%s] is less or equal"
                        + " to the current metadata version [%s]",
                    namespace,
                    reservedStateVersion.version(),
                    existingMetadata.version()
                )
            );
            return false;
        }

        return true;
    }
}
