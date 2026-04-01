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
import java.util.HashSet;
import java.util.LinkedHashSet;
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
    private final ReservedStateVersionCheck versionCheck;
    private final Map<String, ReservedClusterStateHandler<?>> handlers;
    private final Collection<String> updateSequence;
    private final Consumer<ErrorState> errorReporter;
    private final ActionListener<ActionResponse.Empty> listener;

    /**
     * @param updateSequence the names of handlers corresponding to configuration sections present in the source,
     *                       in the order they should be processed according to their dependencies.
     */
    public ReservedStateUpdateTask(
        String namespace,
        ReservedStateChunk stateChunk,
        ReservedStateVersionCheck versionCheck,
        Map<String, ReservedClusterStateHandler<?>> handlers,
        Collection<String> updateSequence,
        Consumer<ErrorState> errorReporter,
        ActionListener<ActionResponse.Empty> listener
    ) {
        this.namespace = namespace;
        this.stateChunk = stateChunk;
        this.versionCheck = versionCheck;
        this.handlers = handlers;
        this.updateSequence = updateSequence;
        this.errorReporter = errorReporter;
        this.listener = listener;

        // We can't assert the order here, even if we'd like to, because in general,
        // there is not necessarily one unique correct order.
        // But we can at least assert that updateSequence has the right elements.
        assert Set.copyOf(updateSequence).equals(stateChunk.state().keySet())
            : "updateSequence is supposed to be computed from stateChunk.state().keySet(): "
                + updateSequence
                + " vs "
                + stateChunk.state().keySet();

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
        var reservedStateMetadata = currentState.getMetadata().reservedStateMetadata().get(namespace);

        if (checkMetadataVersion(namespace, existingMetadata, reservedStateVersion, versionCheck) == false) {
            return currentState;
        }

        var reservedMetadataBuilder = new ReservedStateMetadata.Builder(namespace).version(reservedStateVersion.version());
        List<String> errors = new ArrayList<>();

        ClusterState state = currentState;

        // First apply the updates to transform the cluster state
        for (var handlerName : updateSequence) {
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

        // Now, any existing handler not listed in updateSequence must have been removed.
        // We do removals after updates in case one of the updated handlers depends on one of these,
        // to give that handler a chance to clean up before its dependency vanishes.
        if (reservedStateMetadata != null) {
            Set<String> toRemove = new HashSet<>(reservedStateMetadata.handlers().keySet());
            toRemove.removeAll(updateSequence);
            var reverseRemovalSequence = List.copyOf(orderedStateHandlers(toRemove, handlers));
            for (var iter = reverseRemovalSequence.listIterator(reverseRemovalSequence.size()); iter.hasPrevious();) {
                String handlerName = iter.previous();
                var handler = handlers.get(handlerName);
                try {
                    Set<String> existingKeys = keysForHandler(reservedStateMetadata, handlerName);
                    state = handler.remove(new TransformState(state, existingKeys));
                } catch (Exception e) {
                    errors.add(format("Error processing %s state removal: %s", handler.name(), stackTrace(e)));
                }
            }
        }

        checkAndThrowOnError(errors, reservedStateVersion, versionCheck);

        // Remove the last error if we had previously encountered any in prior processing of reserved state
        reservedMetadataBuilder.errorMetadata(null);

        ClusterState.Builder stateBuilder = new ClusterState.Builder(state);
        Metadata.Builder metadataBuilder = Metadata.builder(state.metadata()).put(reservedMetadataBuilder.build());

        return stateBuilder.metadata(metadataBuilder).build();
    }

    private void checkAndThrowOnError(List<String> errors, ReservedStateVersion version, ReservedStateVersionCheck versionCheck) {
        // Any errors should be discovered through validation performed in the transform calls
        if (errors.isEmpty() == false) {
            logger.debug("Error processing state change request for [{}] with the following errors [{}]", namespace, errors);

            var errorState = new ErrorState(
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
        String namespace,
        ReservedStateMetadata existingMetadata,
        ReservedStateVersion reservedStateVersion,
        ReservedStateVersionCheck versionCheck
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

        Long newVersion = reservedStateVersion.version();
        if (newVersion.equals(ReservedStateMetadata.EMPTY_VERSION)) {
            return true;
        }

        // require a regular positive version, reject any special version
        if (newVersion <= 0L) {
            logger.warn(
                () -> format(
                    "Not updating reserved cluster state for namespace [%s], because version [%s] is less or equal to 0",
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
                "Not updating reserved cluster state for namespace [%s], because version [%s] is %s the current metadata version [%s]",
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

    /**
     * Returns an ordered set ({@link LinkedHashSet}) of the cluster state handlers that need to
     * execute for a given list of handler names supplied through the {@link ReservedStateChunk}.
     *
     * @param handlerNames Names of handlers found in the {@link ReservedStateChunk}
     */
    static LinkedHashSet<String> orderedStateHandlers(
        Collection<String> handlerNames,
        Map<String, ReservedClusterStateHandler<?>> handlers
    ) {
        LinkedHashSet<String> orderedHandlers = new LinkedHashSet<>();

        for (String key : handlerNames) {
            addStateHandler(handlers, key, handlerNames, orderedHandlers, new LinkedHashSet<String>());
        }

        assert Set.copyOf(handlerNames).equals(orderedHandlers);
        return orderedHandlers;
    }

    private static void addStateHandler(
        Map<String, ReservedClusterStateHandler<?>> handlers,
        String key,
        Collection<String> keys,
        LinkedHashSet<String> ordered,
        LinkedHashSet<String> inProgress
    ) {
        if (inProgress.contains(key)) {
            StringBuilder msg = new StringBuilder("Cycle found in settings dependencies: ");
            inProgress.forEach(s -> {
                msg.append(s);
                msg.append(" -> ");
            });
            msg.append(key);
            throw new IllegalStateException(msg.toString());
        }

        if (ordered.contains(key)) {
            // already added by another dependent handler
            return;
        }

        inProgress.add(key);
        ReservedClusterStateHandler<?> handler = handlers.get(key);

        if (handler == null) {
            throw new IllegalStateException("Unknown handler type: " + key);
        }

        for (String dependency : handler.dependencies()) {
            if (keys.contains(dependency) == false) {
                throw new IllegalStateException("Missing handler dependency definition: " + key + " -> " + dependency);
            }
            addStateHandler(handlers, dependency, keys, ordered, inProgress);
        }

        for (String dependency : handler.optionalDependencies()) {
            if (keys.contains(dependency)) {
                addStateHandler(handlers, dependency, keys, ordered, inProgress);
            }
        }

        inProgress.remove(key);
        ordered.add(key);
    }

}
