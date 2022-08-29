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
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateTaskConfig;
import org.elasticsearch.cluster.metadata.ReservedStateErrorMetadata;
import org.elasticsearch.cluster.metadata.ReservedStateMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Priority;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.reservedstate.ReservedClusterStateHandler;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentParser;

import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.elasticsearch.core.Strings.format;

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
    private final ReservedStateUpdateTaskExecutor updateStateTaskExecutor;
    private final ReservedStateErrorTaskExecutor errorStateTaskExecutor;

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
        this.updateStateTaskExecutor = new ReservedStateUpdateTaskExecutor(clusterService.getRerouteService());
        this.errorStateTaskExecutor = new ReservedStateErrorTaskExecutor();
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
            stateChunk = stateChunkParser.apply(parser, null);
        } catch (Exception e) {
            ErrorState errorState = new ErrorState(namespace, -1L, e, ReservedStateErrorMetadata.ErrorKind.PARSING);
            saveErrorState(clusterService.state(), errorState);
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

            saveErrorState(clusterService.state(), errorState);
            logger.debug("error processing state change request for [{}] with the following errors [{}]", namespace, errorState);

            errorListener.accept(
                new IllegalStateException("Error processing state change request for " + namespace + ", errors: " + errorState, e)
            );
            return;
        }

        ClusterState state = clusterService.state();
        ReservedStateMetadata existingMetadata = state.metadata().reservedStateMetadata().get(namespace);

        clusterService.submitStateUpdateTask(
            "reserved cluster state [" + namespace + "]",
            new ReservedStateUpdateTask(
                namespace,
                reservedStateChunk,
                handlers,
                orderedHandlers,
                (clusterState, errorState) -> saveErrorState(clusterState, errorState),
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
            ClusterStateTaskConfig.build(Priority.URGENT),
            updateStateTaskExecutor
        );
    }

    // package private for testing
    static boolean isNewError(ReservedStateMetadata existingMetadata, Long newStateVersion) {
        return (existingMetadata == null
            || existingMetadata.errorMetadata() == null
            || existingMetadata.errorMetadata().version() < newStateVersion);
    }

    private void saveErrorState(ClusterState clusterState, ErrorState errorState) {
        ReservedStateMetadata existingMetadata = clusterState.metadata().reservedStateMetadata().get(errorState.namespace());

        if (isNewError(existingMetadata, errorState.version()) == false) {
            logger.info(
                () -> format(
                    "Not updating error state because version [%s] is less or equal to the last state error version [%s]",
                    errorState.version(),
                    existingMetadata.errorMetadata().version()
                )
            );

            return;
        }

        clusterService.submitStateUpdateTask(
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
            ClusterStateTaskConfig.build(Priority.URGENT),
            errorStateTaskExecutor
        );
    }

    /**
     * Returns an ordered set ({@link LinkedHashSet}) of the cluster state handlers that need to
     * execute for a given list of handler names supplied through the {@link ReservedStateChunk}.
     * @param handlerNames Names of handlers found in the {@link ReservedStateChunk}
     * @return
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

        visited.remove(key);
        ordered.add(key);
    }
}
