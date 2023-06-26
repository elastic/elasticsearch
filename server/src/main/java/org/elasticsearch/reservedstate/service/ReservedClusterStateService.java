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
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.support.RefCountingListener;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.ReservedStateErrorMetadata;
import org.elasticsearch.cluster.metadata.ReservedStateMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.cluster.service.MasterServiceTaskQueue;
import org.elasticsearch.common.Priority;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.reservedstate.NonStateTransformResult;
import org.elasticsearch.reservedstate.ReservedClusterStateHandler;
import org.elasticsearch.reservedstate.TransformState;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentParser;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.elasticsearch.ExceptionsHelper.stackTrace;
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
    public ReservedClusterStateService(ClusterService clusterService, List<ReservedClusterStateHandler<?>> handlerList) {
        this.clusterService = clusterService;
        this.updateTaskQueue = clusterService.createTaskQueue(
            "reserved state update",
            Priority.URGENT,
            new ReservedStateUpdateTaskExecutor(clusterService.getRerouteService())
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
            ErrorState errorState = new ErrorState(namespace, -1L, e, ReservedStateErrorMetadata.ErrorKind.PARSING);
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
     * @param errorListener a consumer called with {@link IllegalStateException} if the content has errors and the
     *        cluster state cannot be correctly applied, null if successful or state couldn't be applied because of incompatible version.
     */
    public void process(String namespace, XContentParser parser, Consumer<Exception> errorListener) {
        ReservedStateChunk stateChunk;

        try {
            stateChunk = parse(namespace, parser);
        } catch (Exception e) {
            ErrorState errorState = new ErrorState(namespace, -1L, e, ReservedStateErrorMetadata.ErrorKind.PARSING);
            updateErrorState(errorState);
            logger.debug("error processing state change request for [{}] with the following errors [{}]", namespace, errorState);

            errorListener.accept(
                new IllegalStateException("Error processing state change request for " + namespace + ", errors: " + errorState, e)
            );
            return;
        }

        process(namespace, stateChunk, errorListener);
    }

    /**
     * Saves and reserves a chunk of the cluster state under a given 'namespace' from {@link XContentParser}
     *
     * @param namespace the namespace under which we'll store the reserved keys in the cluster state metadata
     * @param reservedStateChunk a {@link ReservedStateChunk} composite state object to process
     * @param errorListener a consumer called with {@link IllegalStateException} if the content has errors and the
     *        cluster state cannot be correctly applied, null if successful or the state failed to apply because of incompatible version.
     */
    public void process(String namespace, ReservedStateChunk reservedStateChunk, Consumer<Exception> errorListener) {
        Map<String, Object> reservedState = reservedStateChunk.state();
        final ReservedStateVersion reservedStateVersion = reservedStateChunk.metadata();

        LinkedHashSet<String> orderedHandlers;
        try {
            orderedHandlers = orderedStateHandlers(reservedState.keySet());
        } catch (Exception e) {
            ErrorState errorState = new ErrorState(
                namespace,
                reservedStateVersion.version(),
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
        if (checkMetadataVersion(namespace, existingMetadata, reservedStateVersion) == false) {
            errorListener.accept(null);
            return;
        }

        // We trial run all handler validations to ensure that we can process all of the cluster state error free. During
        // the trial run we collect 'consumers' (functions) for any non cluster state transforms that need to run.
        var trialRunResult = trialRun(namespace, state, reservedStateChunk, orderedHandlers);
        // this is not using the modified trial state above, but that doesn't matter, we're just setting errors here
        var error = checkAndReportError(namespace, trialRunResult.errors, reservedStateVersion);

        if (error != null) {
            errorListener.accept(error);
            return;
        }

        // Since we have validated that the cluster state update can be correctly performed in the trial run, we now
        // execute the non cluster state transforms. These are assumed to be async and we continue with the cluster state update
        // after all have completed. This part of reserved cluster state update is non-atomic, some or all of the non-state
        // transformations can succeed, and we can fail to eventually write the reserved cluster state.
        executeNonStateTransformationSteps(trialRunResult.nonStateTransforms, new ActionListener<>() {
            @Override
            public void onResponse(Collection<NonStateTransformResult> nonStateTransformResults) {
                // Once all of the non-state transformation results complete, we can proceed to
                // do the final save of the cluster state. The non-state transformation reserved keys are applied
                // to the reserved state after all other key handlers.
                updateTaskQueue.submitTask(
                    "reserved cluster state [" + namespace + "]",
                    new ReservedStateUpdateTask(
                        namespace,
                        reservedStateChunk,
                        nonStateTransformResults,
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
                                if (isNewError(existingMetadata, reservedStateVersion.version())) {
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

            @Override
            public void onFailure(Exception e) {
                // If we encounter an error while runnin the non-state transforms, we avoid saving any cluster state.
                errorListener.accept(checkAndReportError(namespace, List.of(e.getMessage()), reservedStateVersion));
            }
        });
    }

    // package private for testing
    Exception checkAndReportError(String namespace, List<String> errors, ReservedStateVersion reservedStateVersion) {
        // Any errors should be discovered through validation performed in the transform calls
        if (errors.isEmpty() == false) {
            logger.debug("Error processing state change request for [{}] with the following errors [{}]", namespace, errors);

            var errorState = new ErrorState(
                namespace,
                reservedStateVersion.version(),
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
     * While running the handlers we also collect any non cluster state transformation consumer actions that
     * need to be performed asynchronously before we attempt to save the cluster state. The trial run does not
     * result in an update of the cluster state, it's only purpose is to verify if we can correctly perform a
     * cluster state update with the given reserved state chunk.
     *
     * Package private for testing
     */
    TrialRunResult trialRun(
        String namespace,
        ClusterState currentState,
        ReservedStateChunk stateChunk,
        LinkedHashSet<String> orderedHandlers
    ) {
        ReservedStateMetadata existingMetadata = currentState.metadata().reservedStateMetadata().get(namespace);
        Map<String, Object> reservedState = stateChunk.state();

        List<String> errors = new ArrayList<>();
        List<Consumer<ActionListener<NonStateTransformResult>>> nonStateTransforms = new ArrayList<>();

        ClusterState state = currentState;

        for (var handlerName : orderedHandlers) {
            ReservedClusterStateHandler<?> handler = handlers.get(handlerName);
            try {
                Set<String> existingKeys = keysForHandler(existingMetadata, handlerName);
                TransformState transformState = handler.transform(reservedState.get(handlerName), new TransformState(state, existingKeys));
                state = transformState.state();
                if (transformState.nonStateTransform() != null) {
                    nonStateTransforms.add(transformState.nonStateTransform());
                }
            } catch (Exception e) {
                errors.add(format("Error processing %s state change: %s", handler.name(), stackTrace(e)));
            }
        }

        return new TrialRunResult(nonStateTransforms, errors);
    }

    /**
     * Runs the non cluster state transformations asynchronously, collecting the {@link NonStateTransformResult} objects.
     * <p>
     * Once all non cluster state transformations have completed, we submit the cluster state update task, which
     * updates all of the handler state, including the keys produced by the non cluster state transforms. The new reserved
     * state version isn't written to the cluster state until the cluster state task runs.
     *
     * Package private for testing
     */
    void executeNonStateTransformationSteps(
        List<Consumer<ActionListener<NonStateTransformResult>>> nonStateTransforms,
        ActionListener<Collection<NonStateTransformResult>> listener
    ) {
        final List<NonStateTransformResult> result = Collections.synchronizedList(new ArrayList<>(nonStateTransforms.size()));
        try (var listeners = new RefCountingListener(listener.map(ignored -> result))) {
            for (var transform : nonStateTransforms) {
                // non cluster state transforms don't modify the cluster state, they however are given a chance to return a more
                // up-to-date version of the modified keys we should save in the reserved state. These calls are
                // async and report back when they are done through the postTasksListener.
                transform.accept(listeners.acquire(result::add));
            }
        }
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

    /**
     * Helper record class to combine the result of a trial run, non cluster state actions and any errors
     */
    record TrialRunResult(List<Consumer<ActionListener<NonStateTransformResult>>> nonStateTransforms, List<String> errors) {}
}
