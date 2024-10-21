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
import org.elasticsearch.cluster.metadata.ReservedStateErrorMetadata;
import org.elasticsearch.cluster.metadata.ReservedStateMetadata;
import org.elasticsearch.cluster.routing.RerouteService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.cluster.service.MasterServiceTaskQueue;
import org.elasticsearch.common.Priority;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.reservedstate.ReservedClusterStateHandler;
import org.elasticsearch.reservedstate.TransformState;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentParser;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.elasticsearch.ExceptionsHelper.stackTrace;
import static org.elasticsearch.cluster.metadata.ReservedStateMetadata.EMPTY_VERSION;
import static org.elasticsearch.core.Strings.format;
import static org.elasticsearch.reservedstate.service.ReservedStateErrorTask.checkErrorVersion;
import static org.elasticsearch.reservedstate.service.ReservedStateErrorTask.isNewError;
import static org.elasticsearch.reservedstate.service.ReservedStateUpdateTask.checkMetadataVersion;
import static org.elasticsearch.reservedstate.service.ReservedStateUpdateTask.keysForHandler;

/**
 * Controller class for storing and reserving a portion of the {@link ClusterState}
 * <p>
 * This class contains the logic about validation, ordering and applying of
 * the cluster state specified in a file or through plugins/modules. Reserved cluster state
 * cannot be modified through the REST APIs, only through this controller class.
 */
public class ReservedClusterStateService {
    private static final Logger logger = LogManager.getLogger(ReservedClusterStateService.class);

    public static final ParseField STATE_FIELD = new ParseField("state");
    public static final ParseField METADATA_FIELD = new ParseField("metadata");

    final Map<String, ReservedClusterStateHandler<?>> handlers;
    final ClusterService clusterService;
    private final MasterServiceTaskQueue<ReservedStateUpdateTask> updateTaskQueue;
    private final MasterServiceTaskQueue<ReservedStateErrorTask> errorTaskQueue;

    @SuppressWarnings("unchecked")
    private final ConstructingObjectParser<ReservedStateChunk, Void> stateChunkParser = new ConstructingObjectParser<>(
        "reserved_state_chunk",
        a -> {
            List<Tuple<String, Object>> tuples = (List<Tuple<String, Object>>) a[0];
            Map<String, Object> stateMap = new HashMap<>();
            for (var tuple : tuples) {
                stateMap.put(tuple.v1(), tuple.v2());
            }

            return new ReservedStateChunk(stateMap, (ReservedStateVersion) a[1]);
        }
    );

    /**
     * Controller class for saving and reserving {@link ClusterState}.
     * @param clusterService for fetching and saving the modified state
     * @param handlerList a list of reserved state handlers, which we use to transform the state
     */
    public ReservedClusterStateService(
        ClusterService clusterService,
        RerouteService rerouteService,
        List<ReservedClusterStateHandler<?>> handlerList
    ) {
        this.clusterService = clusterService;
        this.updateTaskQueue = clusterService.createTaskQueue(
            "reserved state update",
            Priority.URGENT,
            new ReservedStateUpdateTaskExecutor(rerouteService)
        );
        this.errorTaskQueue = clusterService.createTaskQueue("reserved state error", Priority.URGENT, new ReservedStateErrorTaskExecutor());
        this.handlers = handlerList.stream().collect(Collectors.toMap(ReservedClusterStateHandler::name, Function.identity()));
        stateChunkParser.declareNamedObjects(ConstructingObjectParser.constructorArg(), (p, c, name) -> {
            if (handlers.containsKey(name) == false) {
                throw new IllegalStateException("Missing handler definition for content key [" + name + "]");
            }
            p.nextToken();
            return new Tuple<>(name, handlers.get(name).fromXContent(p));
        }, STATE_FIELD);
        stateChunkParser.declareObject(ConstructingObjectParser.constructorArg(), (p, c) -> ReservedStateVersion.parse(p), METADATA_FIELD);
    }

    ReservedStateChunk parse(String namespace, XContentParser parser) {
        try {
            return stateChunkParser.apply(parser, null);
        } catch (Exception e) {
            ErrorState errorState = new ErrorState(
                namespace,
                EMPTY_VERSION,
                ReservedStateVersionCheck.HIGHER_VERSION_ONLY,
                e,
                ReservedStateErrorMetadata.ErrorKind.PARSING
            );
            updateErrorState(errorState);
            logger.debug("error processing state change request for [{}] with the following errors [{}]", namespace, errorState);

            throw new IllegalStateException("Error processing state change request for " + namespace + ", errors: " + errorState, e);
        }
    }

    /**
     * Saves and reserves a chunk of the cluster state under a given 'namespace' from {@link XContentParser}
     *
     * @param namespace the namespace under which we'll store the reserved keys in the cluster state metadata
     * @param parser the XContentParser to process
     * @param versionCheck determines if current and new versions of reserved state require processing or should be skipped
     * @param errorListener a consumer called with {@link IllegalStateException} if the content has errors and the
     *        cluster state cannot be correctly applied, null if successful or state couldn't be applied because of incompatible version.
     */
    public void process(
        String namespace,
        XContentParser parser,
        ReservedStateVersionCheck versionCheck,
        Consumer<Exception> errorListener
    ) {
        ReservedStateChunk stateChunk;

        try {
            stateChunk = parse(namespace, parser);
        } catch (Exception e) {
            ErrorState errorState = new ErrorState(namespace, EMPTY_VERSION, versionCheck, e, ReservedStateErrorMetadata.ErrorKind.PARSING);
            updateErrorState(errorState);
            logger.debug("error processing state change request for [{}] with the following errors [{}]", namespace, errorState);

            errorListener.accept(
                new IllegalStateException("Error processing state change request for " + namespace + ", errors: " + errorState, e)
            );
            return;
        }

        process(namespace, stateChunk, versionCheck, errorListener);
    }

    public void initEmpty(String namespace, ActionListener<ActionResponse.Empty> listener) {
        var missingVersion = new ReservedStateVersion(EMPTY_VERSION, Version.CURRENT);
        var emptyState = new ReservedStateChunk(Map.of(), missingVersion);
        updateTaskQueue.submitTask(
            "empty initial cluster state [" + namespace + "]",
            new ReservedStateUpdateTask(
                namespace,
                emptyState,
                ReservedStateVersionCheck.HIGHER_VERSION_ONLY,
                Map.of(),
                List.of(),
                // error state should not be possible since there is no metadata being parsed or processed
                errorState -> { throw new AssertionError(); },
                listener
            ),
            null
        );

    }

    /**
     * Saves and reserves a chunk of the cluster state under a given 'namespace' from {@link XContentParser}
     *
     * @param namespace the namespace under which we'll store the reserved keys in the cluster state metadata
     * @param reservedStateChunk a {@link ReservedStateChunk} composite state object to process
     * @param errorListener a consumer called with {@link IllegalStateException} if the content has errors and the
     *        cluster state cannot be correctly applied, null if successful or the state failed to apply because of incompatible version.
     */
    public void process(
        String namespace,
        ReservedStateChunk reservedStateChunk,
        ReservedStateVersionCheck versionCheck,
        Consumer<Exception> errorListener
    ) {
        Map<String, Object> reservedState = reservedStateChunk.state();
        ReservedStateVersion reservedStateVersion = reservedStateChunk.metadata();

        LinkedHashSet<String> orderedHandlers;
        try {
            orderedHandlers = orderedStateHandlers(reservedState.keySet());
        } catch (Exception e) {
            ErrorState errorState = new ErrorState(
                namespace,
                reservedStateVersion.version(),
                versionCheck,
                e,
                ReservedStateErrorMetadata.ErrorKind.PARSING
            );

            updateErrorState(errorState);
            logger.debug("error processing state change request for [{}] with the following errors [{}]", namespace, errorState);

            errorListener.accept(
                new IllegalStateException("Error processing state change request for " + namespace + ", errors: " + errorState, e)
            );
            return;
        }

        ClusterState state = clusterService.state();
        ReservedStateMetadata existingMetadata = state.metadata().reservedStateMetadata().get(namespace);

        // We check if we should exit early on the state version from clusterService. The ReservedStateUpdateTask
        // will check again with the most current state version if this continues.
        if (checkMetadataVersion(namespace, existingMetadata, reservedStateVersion, versionCheck) == false) {
            errorListener.accept(null);
            return;
        }

        // We trial run all handler validations to ensure that we can process all of the cluster state error free.
        var trialRunErrors = trialRun(namespace, state, reservedStateChunk, orderedHandlers);
        // this is not using the modified trial state above, but that doesn't matter, we're just setting errors here
        var error = checkAndReportError(namespace, trialRunErrors, reservedStateVersion, versionCheck);

        if (error != null) {
            errorListener.accept(error);
            return;
        }
        updateTaskQueue.submitTask(
            "reserved cluster state [" + namespace + "]",
            new ReservedStateUpdateTask(
                namespace,
                reservedStateChunk,
                versionCheck,
                handlers,
                orderedHandlers,
                ReservedClusterStateService.this::updateErrorState,
                new ActionListener<>() {
                    @Override
                    public void onResponse(ActionResponse.Empty empty) {
                        logger.info("Successfully applied new reserved cluster state for namespace [{}]", namespace);
                        errorListener.accept(null);
                    }

                    @Override
                    public void onFailure(Exception e) {
                        // Don't spam the logs on repeated errors
                        if (isNewError(existingMetadata, reservedStateVersion.version(), versionCheck)) {
                            logger.debug("Failed to apply reserved cluster state", e);
                            errorListener.accept(e);
                        } else {
                            errorListener.accept(null);
                        }
                    }
                }
            ),
            null
        );
    }

    // package private for testing
    Exception checkAndReportError(
        String namespace,
        List<String> errors,
        ReservedStateVersion reservedStateVersion,
        ReservedStateVersionCheck versionCheck
    ) {
        // Any errors should be discovered through validation performed in the transform calls
        if (errors.isEmpty() == false) {
            logger.debug("Error processing state change request for [{}] with the following errors [{}]", namespace, errors);

            var errorState = new ErrorState(
                namespace,
                reservedStateVersion.version(),
                versionCheck,
                errors,
                ReservedStateErrorMetadata.ErrorKind.VALIDATION
            );

            updateErrorState(errorState);

            return new IllegalStateException("Error processing state change request for " + namespace + ", errors: " + errorState);
        }

        return null;
    }

    // package private for testing
    void updateErrorState(ErrorState errorState) {
        // optimistic check here - the cluster state might change after this, so also need to re-check later
        if (checkErrorVersion(clusterService.state(), errorState) == false) {
            // nothing to update
            return;
        }

        submitErrorUpdateTask(errorState);
    }

    private void submitErrorUpdateTask(ErrorState errorState) {
        errorTaskQueue.submitTask(
            "reserved cluster state update error for [ " + errorState.namespace() + "]",
            new ReservedStateErrorTask(errorState, new ActionListener<>() {
                @Override
                public void onResponse(ActionResponse.Empty empty) {
                    logger.info("Successfully applied new reserved error state for namespace [{}]", errorState.namespace());
                }

                @Override
                public void onFailure(Exception e) {
                    logger.error("Failed to apply reserved error cluster state", e);
                }
            }),
            null
        );
    }

    /**
     * Goes through all of the handlers, runs the validation and the transform part of the cluster state.
     * <p>
     * The trial run does not result in an update of the cluster state, it's only purpose is to verify
     * if we can correctly perform a cluster state update with the given reserved state chunk.
     *
     * Package private for testing
     * @return Any errors that occured
     */
    List<String> trialRun(
        String namespace,
        ClusterState currentState,
        ReservedStateChunk stateChunk,
        LinkedHashSet<String> orderedHandlers
    ) {
        ReservedStateMetadata existingMetadata = currentState.metadata().reservedStateMetadata().get(namespace);
        Map<String, Object> reservedState = stateChunk.state();

        List<String> errors = new ArrayList<>();

        ClusterState state = currentState;

        for (var handlerName : orderedHandlers) {
            ReservedClusterStateHandler<?> handler = handlers.get(handlerName);
            try {
                Set<String> existingKeys = keysForHandler(existingMetadata, handlerName);
                TransformState transformState = handler.transform(reservedState.get(handlerName), new TransformState(state, existingKeys));
                state = transformState.state();
            } catch (Exception e) {
                errors.add(format("Error processing %s state change: %s", handler.name(), stackTrace(e)));
            }
        }

        return errors;
    }

    /**
     * Returns an ordered set ({@link LinkedHashSet}) of the cluster state handlers that need to
     * execute for a given list of handler names supplied through the {@link ReservedStateChunk}.
     * @param handlerNames Names of handlers found in the {@link ReservedStateChunk}
     */
    LinkedHashSet<String> orderedStateHandlers(Set<String> handlerNames) {
        LinkedHashSet<String> orderedHandlers = new LinkedHashSet<>();
        LinkedHashSet<String> dependencyStack = new LinkedHashSet<>();

        for (String key : handlerNames) {
            addStateHandler(key, handlerNames, orderedHandlers, dependencyStack);
        }

        return orderedHandlers;
    }

    private void addStateHandler(String key, Set<String> keys, LinkedHashSet<String> ordered, LinkedHashSet<String> visited) {
        if (visited.contains(key)) {
            StringBuilder msg = new StringBuilder("Cycle found in settings dependencies: ");
            visited.forEach(s -> {
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

        visited.add(key);
        ReservedClusterStateHandler<?> handler = handlers.get(key);

        if (handler == null) {
            throw new IllegalStateException("Unknown handler type: " + key);
        }

        for (String dependency : handler.dependencies()) {
            if (keys.contains(dependency) == false) {
                throw new IllegalStateException("Missing handler dependency definition: " + key + " -> " + dependency);
            }
            addStateHandler(dependency, keys, ordered, visited);
        }

        for (String dependency : handler.optionalDependencies()) {
            if (keys.contains(dependency)) {
                addStateHandler(dependency, keys, ordered, visited);
            }
        }

        visited.remove(key);
        ordered.add(key);
    }

    /**
     * Adds additional {@link ReservedClusterStateHandler} to the handler registry
     * @param handler an additional reserved state handler to be added
     */
    public void installStateHandler(ReservedClusterStateHandler<?> handler) {
        this.handlers.put(handler.name(), handler);
    }
}
