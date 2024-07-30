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
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.reservedstate.ReservedClusterStateHandler;
import org.elasticsearch.reservedstate.TransformState;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

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
    private final Consumer<ErrorState> errorReporter;
    private final ActionListener<ActionResponse.Empty> listener;

    public ReservedStateUpdateTask(
        String namespace,
        ReservedStateChunk stateChunk,
        Map<String, ReservedClusterStateHandler<?>> handlers,
        Collection<String> orderedHandlers,
        Consumer<ErrorState> errorReporter,
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
        if (currentState.blocks().hasGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK)) {
            // If cluster state has become blocked, this task was submitted while the node was master but is now not master.
            // The new master will re-read file settings, so whatever update was to be written here will be handled
            // by the new master.
            return currentState;
        }

        ReservedStateMetadata existingMetadata = currentState.metadata().reservedStateMetadata().get(namespace);
        Map<String, Object> reservedState = stateChunk.state();
        ReservedStateVersion reservedStateVersion = stateChunk.metadata();

        if (checkMetadataVersion(namespace, existingMetadata, reservedStateVersion) == false) {
            return currentState;
        }

        var reservedMetadataBuilder = new ReservedStateMetadata.Builder(namespace).version(reservedStateVersion.version());
        List<String> errors = new ArrayList<>();

        ClusterState state = currentState;
        // Transform the cluster state first
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

        checkAndThrowOnError(errors, reservedStateVersion);

        // Remove the last error if we had previously encountered any in prior processing of reserved state
        reservedMetadataBuilder.errorMetadata(null);

        ClusterState.Builder stateBuilder = new ClusterState.Builder(state);
        Metadata.Builder metadataBuilder = Metadata.builder(state.metadata()).put(reservedMetadataBuilder.build());

        return stateBuilder.metadata(metadataBuilder).build();
    }

    private void checkAndThrowOnError(List<String> errors, ReservedStateVersion reservedStateVersion) {
        // Any errors should be discovered through validation performed in the transform calls
        if (errors.isEmpty() == false) {
            logger.debug("Error processing state change request for [{}] with the following errors [{}]", namespace, errors);

            var errorState = new ErrorState(
                namespace,
                reservedStateVersion.version(),
                errors,
                ReservedStateErrorMetadata.ErrorKind.VALIDATION
            );

            /*
             * It doesn't matter this reporter needs to re-access the base state,
             * any updates set by this task will just be discarded when the below exception is thrown,
             * and we just need to set the error state once
             */
            errorReporter.accept(errorState);

            throw new IllegalStateException("Error processing state change request for " + namespace + ", errors: " + errorState);
        }
    }

    static Set<String> keysForHandler(ReservedStateMetadata reservedStateMetadata, String handlerName) {
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

        if (reservedStateVersion.version().equals(ReservedStateMetadata.EMPTY_VERSION)) {
            return true;
        }

        // require a regular positive version, reject any special version
        if (reservedStateVersion.version() <= 0L) {
            logger.warn(
                () -> format(
                    "Not updating reserved cluster state for namespace [%s], because version [%s] is less or equal to 0",
                    namespace,
                    reservedStateVersion.version()
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
