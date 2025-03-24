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
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.metadata.ReservedStateErrorMetadata;
import org.elasticsearch.cluster.metadata.ReservedStateMetadata;
import org.elasticsearch.cluster.routing.RerouteService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.core.FixForMultiProject;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.env.BuildVersion;
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
import java.util.Optional;
import java.util.SequencedSet;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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

    private final Map<String, ReservedClusterStateHandler<?, ?>> allHandlers;
    private final Map<String, ReservedClusterStateHandler<ClusterState, ?>> clusterHandlers;
    private final Map<String, ReservedClusterStateHandler<ProjectMetadata, ?>> projectHandlers;
    private final ClusterService clusterService;
    private final ReservedStateUpdateTaskExecutor updateTaskExecutor;
    private final ReservedStateErrorTaskExecutor errorTaskExecutor;

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

    private static <T> ReservedClusterStateHandler<ClusterState, T> adaptForDefaultProject(
        ReservedClusterStateHandler<ProjectMetadata, T> handler
    ) {
        return new ProjectClusterStateHandlerAdapter<>(Metadata.DEFAULT_PROJECT_ID, handler);
    }

    /**
     * As part of checking and verifying the update, code needs access to a {@code ProjectMetadata} for a project
     * that may not exist in cluster state yet. This method does that.
     */
    static ProjectMetadata getPotentiallyNewProject(ClusterState state, ProjectId projectId) {
        return state.metadata().hasProject(projectId) ? state.metadata().getProject(projectId) : ProjectMetadata.builder(projectId).build();
    }

    /**
     * Controller class for saving and reserving {@link ClusterState}.
     */
    public ReservedClusterStateService(
        ClusterService clusterService,
        RerouteService rerouteService,
        List<ReservedClusterStateHandler<ClusterState, ?>> clusterHandlerList,
        List<ReservedClusterStateHandler<ProjectMetadata, ?>> projectHandlerList
    ) {
        this.clusterService = clusterService;
        this.updateTaskExecutor = new ReservedStateUpdateTaskExecutor(rerouteService);
        this.errorTaskExecutor = new ReservedStateErrorTaskExecutor();

        allHandlers = Stream.concat(clusterHandlerList.stream(), projectHandlerList.stream())
            .collect(Collectors.toMap(ReservedClusterStateHandler::name, Function.identity(), (v1, v2) -> {
                throw new IllegalArgumentException("Duplicate handler name: [" + v1.name() + "]");
            }));
        // project handlers also need to be cluster state handlers for the default project,
        // to handle the case where this is a default single-project install
        clusterHandlers = Stream.concat(
            clusterHandlerList.stream(),
            projectHandlerList.stream().map(ReservedClusterStateService::adaptForDefaultProject)
        ).collect(Collectors.toMap(ReservedClusterStateHandler::name, Function.identity()));
        projectHandlers = projectHandlerList.stream().collect(Collectors.toMap(ReservedClusterStateHandler::name, Function.identity()));

        stateChunkParser.declareNamedObjects(ConstructingObjectParser.constructorArg(), (p, c, name) -> {
            ReservedClusterStateHandler<?, ?> handler = allHandlers.get(name);
            if (handler == null) {
                throw new IllegalStateException("Missing handler definition for content key [" + name + "]");
            }
            p.nextToken();
            return new Tuple<>(name, handler.fromXContent(p));
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

    public ReservedStateChunk parse(ProjectId projectId, String namespace, XContentParser parser) {
        try {
            return stateChunkParser.apply(parser, null);
        } catch (Exception e) {
            ErrorState errorState = new ErrorState(
                projectId,
                namespace,
                EMPTY_VERSION,
                ReservedStateVersionCheck.HIGHER_VERSION_ONLY,
                e,
                ReservedStateErrorMetadata.ErrorKind.PARSING
            );
            updateErrorState(errorState);
            logger.debug(
                "error processing project [{}] change request for [{}] with the following errors [{}]",
                projectId,
                namespace,
                errorState
            );

            throw new IllegalStateException(
                "Error processing project " + projectId + " change request for " + namespace + ", errors: " + errorState,
                e
            );
        }
    }

    /**
     * Saves and reserves a chunk of the project state under a given 'namespace' from {@link XContentParser}
     *
     * @param projectId the project this state is for
     * @param namespace the namespace under which we'll store the reserved keys in the project state metadata
     * @param parser the XContentParser to process
     * @param versionCheck determines if current and new versions of reserved state require processing or should be skipped
     * @param errorListener a consumer called with {@link IllegalStateException} if the content has errors and the
     *        cluster state cannot be correctly applied, null if successful or state couldn't be applied because of incompatible version.
     */
    public void process(
        ProjectId projectId,
        String namespace,
        XContentParser parser,
        ReservedStateVersionCheck versionCheck,
        Consumer<Exception> errorListener
    ) {
        ReservedStateChunk stateChunk;

        try {
            stateChunk = parse(projectId, namespace, parser);
        } catch (Exception e) {
            ErrorState errorState = new ErrorState(
                projectId,
                namespace,
                EMPTY_VERSION,
                versionCheck,
                e,
                ReservedStateErrorMetadata.ErrorKind.PARSING
            );
            updateErrorState(errorState);
            logger.debug(
                "error processing project [{}] change request for [{}] with the following errors [{}]",
                projectId,
                namespace,
                errorState
            );

            errorListener.accept(
                new IllegalStateException(
                    "Error processing project " + projectId + " change request for " + namespace + ", errors: " + errorState,
                    e
                )
            );
            return;
        }

        process(projectId, namespace, stateChunk, versionCheck, errorListener);
    }

    @FixForMultiProject // this also needs to work for project-scoped reserved state
    public void initEmpty(String namespace, ActionListener<ActionResponse.Empty> listener) {
        var missingVersion = new ReservedStateVersion(EMPTY_VERSION, BuildVersion.current());
        var emptyState = new ReservedStateChunk(Map.of(), missingVersion);
        submitUpdateTask(
            "empty initial cluster state [" + namespace + "]",
            new ReservedClusterStateUpdateTask(
                namespace,
                emptyState,
                ReservedStateVersionCheck.HIGHER_VERSION_ONLY,
                Map.of(),
                List.of(),
                // error state should not be possible since there is no metadata being parsed or processed
                errorState -> { throw new AssertionError(); },
                listener
            )
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

        SequencedSet<String> orderedHandlers;
        try {
            orderedHandlers = orderedClusterStateHandlers(reservedState.keySet());
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
        if (checkMetadataVersion(Optional.empty(), namespace, existingMetadata, reservedStateVersion, versionCheck) == false) {
            errorListener.accept(null);
            return;
        }

        // We trial run all handler validations to ensure that we can process all of the cluster state error free.
        var trialRunErrors = trialRun(namespace, state, reservedStateChunk, orderedHandlers);
        // this is not using the modified trial state above, but that doesn't matter, we're just setting errors here
        var error = checkAndReportError(Optional.empty(), namespace, trialRunErrors, reservedStateVersion, versionCheck);

        if (error != null) {
            errorListener.accept(error);
            return;
        }
        submitUpdateTask(
            "reserved cluster state [" + namespace + "]",
            new ReservedClusterStateUpdateTask(
                namespace,
                reservedStateChunk,
                versionCheck,
                clusterHandlers,
                orderedHandlers,
                this::updateErrorState,
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
            )
        );
    }

    /**
     * Saves and reserves a chunk of the cluster state under a given 'namespace' from {@link XContentParser}
     *
     * @param projectId the project state to modify
     * @param namespace the namespace under which we'll store the reserved keys in the cluster state metadata
     * @param reservedStateChunk a {@link ReservedStateChunk} composite state object to process
     * @param versionCheck  Enum representing whether a reserved state should be processed based on the current and new versions
     * @param errorListener a consumer called with {@link IllegalStateException} if the content has errors and the
     *        cluster state cannot be correctly applied, null if successful or the state failed to apply because of incompatible version.
     */
    public void process(
        ProjectId projectId,
        String namespace,
        ReservedStateChunk reservedStateChunk,
        ReservedStateVersionCheck versionCheck,
        Consumer<Exception> errorListener
    ) {
        process(projectId, namespace, List.of(reservedStateChunk), versionCheck, errorListener);
    }

    /**
     * Saves and reserves a chunk of the cluster state under a given 'namespace' from {@link XContentParser} by combining several chunks
     * into one
     *
     * @param projectId the project state to modify
     * @param namespace the namespace under which we'll store the reserved keys in the cluster state metadata
     * @param reservedStateChunks a list of {@link ReservedStateChunk} composite state objects to process
     * @param versionCheck  Enum representing whether a reserved state should be processed based on the current and new versions
     * @param errorListener a consumer called with {@link IllegalStateException} if the content has errors and the
     *        cluster state cannot be correctly applied, null if successful or the state failed to apply because of incompatible version.
     */
    public void process(
        ProjectId projectId,
        String namespace,
        List<ReservedStateChunk> reservedStateChunks,
        ReservedStateVersionCheck versionCheck,
        Consumer<Exception> errorListener
    ) {
        ReservedStateChunk reservedStateChunk;
        ReservedStateVersion reservedStateVersion;
        LinkedHashSet<String> orderedHandlers;

        try {
            reservedStateChunk = mergeReservedStateChunks(reservedStateChunks);
            Map<String, Object> reservedState = reservedStateChunk.state();
            reservedStateVersion = reservedStateChunk.metadata();
            orderedHandlers = orderedProjectStateHandlers(reservedState.keySet());
        } catch (Exception e) {
            ErrorState errorState = new ErrorState(
                projectId,
                namespace,
                reservedStateChunks.isEmpty() ? EMPTY_VERSION : reservedStateChunks.getFirst().metadata().version(),
                versionCheck,
                e,
                ReservedStateErrorMetadata.ErrorKind.PARSING
            );

            updateErrorState(errorState);
            logger.debug(
                "error processing project [{}] change request for [{}] with the following errors [{}]",
                projectId,
                namespace,
                errorState
            );

            errorListener.accept(
                new IllegalStateException(
                    "Error processing project " + projectId + " change request for " + namespace + ", errors: " + errorState,
                    e
                )
            );
            return;
        }

        ClusterState state = clusterService.state();
        ProjectMetadata projectMetadata = getPotentiallyNewProject(state, projectId);
        ReservedStateMetadata existingMetadata = projectMetadata.reservedStateMetadata().get(namespace);

        // We check if we should exit early on the state version from clusterService. The ReservedStateUpdateTask
        // will check again with the most current state version if this continues.
        if (checkMetadataVersion(Optional.of(projectId), namespace, existingMetadata, reservedStateVersion, versionCheck) == false) {
            errorListener.accept(null);
            return;
        }

        // We trial run all handler validations to ensure that we can process all of the cluster state error free.
        var trialRunErrors = trialRun(namespace, projectMetadata, reservedStateChunk, orderedHandlers);
        // this is not using the modified trial state above, but that doesn't matter, we're just setting errors here
        var error = checkAndReportError(Optional.of(projectId), namespace, trialRunErrors, reservedStateVersion, versionCheck);

        if (error != null) {
            errorListener.accept(error);
            return;
        }
        submitUpdateTask(
            "reserved cluster state [" + namespace + "]",
            new ReservedProjectStateUpdateTask(
                projectId,
                namespace,
                reservedStateChunk,
                versionCheck,
                projectHandlers,
                orderedHandlers,
                this::updateErrorState,
                new ActionListener<>() {
                    @Override
                    public void onResponse(ActionResponse.Empty empty) {
                        logger.info(
                            "Successfully applied new reserved cluster state for project [{}] namespace [{}]",
                            projectId,
                            namespace
                        );
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
            )
        );
    }

    private static ReservedStateChunk mergeReservedStateChunks(List<ReservedStateChunk> chunks) {
        if (chunks.isEmpty()) {
            throw new IllegalArgumentException("No chunks provided");
        }

        if (chunks.size() == 1) {
            return chunks.getFirst();
        }

        ReservedStateVersion reservedStateVersion = chunks.getFirst().metadata();
        Map<String, Object> mergedChunks = new HashMap<>(chunks.size());
        for (var chunk : chunks) {
            Set<String> duplicateKeys = Sets.intersection(chunk.state().keySet(), mergedChunks.keySet());
            if (chunk.metadata().equals(reservedStateVersion) == false) {
                throw new IllegalStateException(
                    "Failed to merge reserved state chunks because of version mismatch: ["
                        + reservedStateVersion
                        + "] != ["
                        + chunk.metadata()
                        + "]"
                );
            } else if (duplicateKeys.isEmpty() == false) {
                throw new IllegalStateException("Failed to merge reserved state chunks because of duplicate keys: " + duplicateKeys);
            }
            mergedChunks.putAll(chunk.state());
        }

        return new ReservedStateChunk(mergedChunks, reservedStateVersion);
    }

    // package private for testing
    Exception checkAndReportError(
        Optional<ProjectId> projectId,
        String namespace,
        List<String> errors,
        ReservedStateVersion reservedStateVersion,
        ReservedStateVersionCheck versionCheck
    ) {
        // Any errors should be discovered through validation performed in the transform calls
        if (errors.isEmpty() == false) {
            logger.debug("Error processing state change request for [{}] with the following errors [{}]", namespace, errors);

            var errorState = new ErrorState(
                projectId,
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

    void submitUpdateTask(String source, ReservedStateUpdateTask<?> task) {
        var updateTaskQueue = clusterService.createTaskQueue("reserved state update", Priority.URGENT, updateTaskExecutor);
        updateTaskQueue.submitTask(source, task, null);
    }

    // package private for testing
    void updateErrorState(ErrorState errorState) {
        // optimistic check here - the cluster state might change after this, so also need to re-check later
        if (checkErrorVersion(clusterService.state(), errorState) == false) {
            // nothing to update
            return;
        }

        if (errorState.projectId().isPresent() && clusterService.state().metadata().hasProject(errorState.projectId().get()) == false) {
            // Can't update error state for a project that doesn't exist yet
            return;
        }

        submitErrorUpdateTask(errorState);
    }

    private void submitErrorUpdateTask(ErrorState errorState) {
        var errorTaskQueue = clusterService.createTaskQueue("reserved state error", Priority.URGENT, errorTaskExecutor);
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
     * @return Any errors that occurred
     */
    List<String> trialRun(
        String namespace,
        ClusterState currentState,
        ReservedStateChunk stateChunk,
        SequencedSet<String> orderedHandlers
    ) {
        return trialRun(
            currentState.metadata().reservedStateMetadata().get(namespace),
            currentState,
            stateChunk,
            clusterHandlers,
            orderedHandlers
        );
    }

    /**
     * Goes through all of the handlers, runs the validation and the transform part of the cluster state.
     * <p>
     * The trial run does not result in an update of the cluster state, it's only purpose is to verify
     * if we can correctly perform a cluster state update with the given reserved state chunk.
     *
     * Package private for testing
     * @return Any errors that occurred
     */
    List<String> trialRun(
        String namespace,
        ProjectMetadata currentState,
        ReservedStateChunk stateChunk,
        SequencedSet<String> orderedHandlers
    ) {
        return trialRun(currentState.reservedStateMetadata().get(namespace), currentState, stateChunk, projectHandlers, orderedHandlers);
    }

    private static <S> List<String> trialRun(
        ReservedStateMetadata existingMetadata,
        S currentMetadata,
        ReservedStateChunk stateChunk,
        Map<String, ReservedClusterStateHandler<S, ?>> handlers,
        SequencedSet<String> orderedHandlers
    ) {
        Map<String, Object> reservedState = stateChunk.state();

        List<String> errors = new ArrayList<>();

        S metadata = currentMetadata;

        for (var handlerName : orderedHandlers) {
            ReservedClusterStateHandler<S, ?> handler = handlers.get(handlerName);
            try {
                Set<String> existingKeys = keysForHandler(existingMetadata, handlerName);
                TransformState<S> transformState = transform(
                    handler,
                    reservedState.get(handlerName),
                    new TransformState<>(metadata, existingKeys)
                );
                metadata = transformState.state();
            } catch (Exception e) {
                errors.add(format("Error processing %s state change: %s", handler.name(), stackTrace(e)));
            }
        }

        return errors;
    }

    @SuppressWarnings("unchecked")
    static <S, T> TransformState<S> transform(ReservedClusterStateHandler<S, T> handler, Object state, TransformState<S> transform)
        throws Exception {
        return handler.transform((T) state, transform);
    }

    /**
     * Returns an ordered set ({@link LinkedHashSet}) of the cluster state handlers that need to
     * execute for a given list of handler names supplied through the {@link ReservedStateChunk}.
     * @param handlerNames Names of handlers found in the {@link ReservedStateChunk}
     */
    SequencedSet<String> orderedClusterStateHandlers(Set<String> handlerNames) {
        LinkedHashSet<String> orderedHandlers = new LinkedHashSet<>();
        LinkedHashSet<String> dependencyStack = new LinkedHashSet<>();

        for (String key : handlerNames) {
            addStateHandler(clusterHandlers, key, handlerNames, orderedHandlers, dependencyStack);
        }

        return orderedHandlers;
    }

    /**
     * Returns an ordered set ({@link LinkedHashSet}) of the cluster state handlers that need to
     * execute for a given list of handler names supplied through the {@link ReservedStateChunk}.
     * @param handlerNames Names of handlers found in the {@link ReservedStateChunk}
     */
    LinkedHashSet<String> orderedProjectStateHandlers(Set<String> handlerNames) {
        LinkedHashSet<String> orderedHandlers = new LinkedHashSet<>();
        LinkedHashSet<String> dependencyStack = new LinkedHashSet<>();

        for (String key : handlerNames) {
            addStateHandler(projectHandlers, key, handlerNames, orderedHandlers, dependencyStack);
        }

        return orderedHandlers;
    }

    private void addStateHandler(
        Map<String, ? extends ReservedClusterStateHandler<?, ?>> handlers,
        String key,
        Set<String> keys,
        SequencedSet<String> ordered,
        SequencedSet<String> visited
    ) {
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
        ReservedClusterStateHandler<?, ?> handler = handlers.get(key);

        if (handler == null) {
            throw new IllegalStateException("Unknown handler type: " + key);
        }

        for (String dependency : handler.dependencies()) {
            if (keys.contains(dependency) == false) {
                throw new IllegalStateException("Missing handler dependency definition: " + key + " -> " + dependency);
            }
            addStateHandler(handlers, dependency, keys, ordered, visited);
        }

        for (String dependency : handler.optionalDependencies()) {
            if (keys.contains(dependency)) {
                addStateHandler(handlers, dependency, keys, ordered, visited);
            }
        }

        visited.remove(key);
        ordered.add(key);
    }

    /**
     * Adds additional {@link ReservedClusterStateHandler} to the handler registry
     * @param handler an additional reserved state handler to be added
     */
    public void installClusterStateHandler(ReservedClusterStateHandler<ClusterState, ?> handler) {
        allHandlers.put(handler.name(), handler);
        clusterHandlers.put(handler.name(), handler);
    }

    /**
     * Adds additional {@link ReservedClusterStateHandler} to the handler registry
     * @param handler an additional reserved state handler to be added
     */
    public void installProjectStateHandler(ReservedClusterStateHandler<ProjectMetadata, ?> handler) {
        allHandlers.put(handler.name(), handler);
        projectHandlers.put(handler.name(), handler);
        clusterHandlers.put(handler.name(), adaptForDefaultProject(handler));
    }
}
