/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.reservedstate.service;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateTaskListener;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ReservedStateErrorMetadata;
import org.elasticsearch.cluster.metadata.ReservedStateHandlerMetadata;
import org.elasticsearch.cluster.metadata.ReservedStateMetadata;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.reservedstate.ReservedStateHandler;
import org.elasticsearch.reservedstate.TransformState;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;

import static org.elasticsearch.exception.ExceptionsHelper.stackTrace;
import static org.elasticsearch.core.Strings.format;

/**
 * Generic task to update and reserve parts of the cluster state
 *
 * <p>
 * Reserved cluster state can only be modified by using the {@link ReservedClusterStateService}. Updating
 * the reserved cluster state through REST APIs is not permitted.
 */
public abstract class ReservedStateUpdateTask<T extends ReservedStateHandler<?>> implements ClusterStateTaskListener {
    private static final Logger logger = LogManager.getLogger(ReservedStateUpdateTask.class);

    private final String namespace;
    private final ReservedStateChunk stateChunk;
    private final ReservedStateVersionCheck versionCheck;
    private final Map<String, T> handlers;
    private final Collection<String> orderedHandlers;
    private final Consumer<ErrorState> errorReporter;
    private final ActionListener<ActionResponse.Empty> listener;

    public ReservedStateUpdateTask(
        String namespace,
        ReservedStateChunk stateChunk,
        ReservedStateVersionCheck versionCheck,
        Map<String, T> handlers,
        Collection<String> orderedHandlers,
        Consumer<ErrorState> errorReporter,
        ActionListener<ActionResponse.Empty> listener
    ) {
        this.namespace = namespace;
        this.stateChunk = stateChunk;
        this.versionCheck = versionCheck;
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

    protected abstract Optional<ProjectId> projectId();

    protected abstract TransformState transform(T handler, Object state, TransformState transformState) throws Exception;

    abstract ClusterState execute(ClusterState currentState);

    /**
     * Produces a new state {@code S} with the reserved state info in {@code reservedStateMap}
     * @return A tuple of the new state and new reserved state metadata, or {@code null} if no changes are required.
     */
    final Tuple<ClusterState, ReservedStateMetadata> execute(ClusterState state, Map<String, ReservedStateMetadata> reservedStateMap) {
        Map<String, Object> reservedState = stateChunk.state();
        ReservedStateVersion reservedStateVersion = stateChunk.metadata();
        ReservedStateMetadata reservedStateMetadata = reservedStateMap.get(namespace);

        if (checkMetadataVersion(projectId(), namespace, reservedStateMetadata, reservedStateVersion, versionCheck) == false) {
            return null;
        }

        var reservedMetadataBuilder = new ReservedStateMetadata.Builder(namespace).version(reservedStateVersion.version());
        List<String> errors = new ArrayList<>();

        // Transform the cluster state first
        for (var handlerName : orderedHandlers) {
            T handler = handlers.get(handlerName);
            try {
                Set<String> existingKeys = keysForHandler(reservedStateMetadata, handlerName);
                TransformState transformState = transform(handler, reservedState.get(handlerName), new TransformState(state, existingKeys));
                state = transformState.state();
                reservedMetadataBuilder.putHandler(new ReservedStateHandlerMetadata(handlerName, transformState.keys()));
            } catch (Exception e) {
                errors.add(format("Error processing %s state change: %s", handler.name(), stackTrace(e)));
            }
        }

        checkAndThrowOnError(errors, reservedStateVersion, versionCheck);

        // Remove the last error if we had previously encountered any in prior processing of reserved state
        reservedMetadataBuilder.errorMetadata(null);

        return Tuple.tuple(state, reservedMetadataBuilder.build());
    }

    private void checkAndThrowOnError(List<String> errors, ReservedStateVersion version, ReservedStateVersionCheck versionCheck) {
        // Any errors should be discovered through validation performed in the transform calls
        if (errors.isEmpty() == false) {
            logger.debug("Error processing state change request for [{}] with the following errors [{}]", namespace, errors);

            var errorState = new ErrorState(
                projectId(),
                namespace,
                version.version(),
                versionCheck,
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
        Optional<ProjectId> projectId,
        String namespace,
        ReservedStateMetadata existingMetadata,
        ReservedStateVersion reservedStateVersion,
        ReservedStateVersionCheck versionCheck
    ) {
        if (reservedStateVersion.buildVersion().isFutureVersion()) {
            logger.warn(
                () -> format(
                    "Reserved %s version [%s] for namespace [%s] is not compatible with this Elasticsearch node",
                    projectId.map(p -> "project state [" + p + "]").orElse("cluster state"),
                    reservedStateVersion.buildVersion(),
                    namespace
                )
            );
            return false;
        }

        Long newVersion = reservedStateVersion.version();
        if (newVersion.equals(ReservedStateMetadata.EMPTY_VERSION)) {
            return true;
        }

        // require a regular positive version, reject any special version
        if (newVersion <= 0L) {
            logger.warn(
                () -> format(
                    "Not updating reserved %s for namespace [%s], because version [%s] is less or equal to 0",
                    projectId.map(p -> "project state [" + p + "]").orElse("cluster state"),
                    namespace,
                    newVersion
                )
            );
            return false;
        }

        if (existingMetadata == null) {
            return true;
        }

        Long currentVersion = existingMetadata.version();
        if (versionCheck.test(currentVersion, newVersion)) {
            return true;
        }

        logger.warn(
            () -> format(
                "Not updating reserved %s for namespace [%s], because version [%s] is %s the current metadata version [%s]",
                projectId.map(p -> "project state [" + p + "]").orElse("cluster state"),
                namespace,
                newVersion,
                switch (versionCheck) {
                    case HIGHER_OR_SAME_VERSION -> "less than";
                    case HIGHER_VERSION_ONLY -> "less than or equal to";
                },
                currentVersion
            )
        );
        return false;
    }

}
