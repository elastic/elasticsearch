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
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateTaskConfig;
import org.elasticsearch.cluster.ClusterStateTaskExecutor;
import org.elasticsearch.cluster.metadata.ImmutableStateErrorMetadata;
import org.elasticsearch.cluster.metadata.ImmutableStateMetadata;
import org.elasticsearch.cluster.routing.RerouteService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Priority;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.immutablestate.ImmutableClusterStateHandler;
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
 * Controller class for applying immutable state to ClusterState.
 * <p>
 * This class contains the logic about validation, ordering and applying of
 * the cluster state specified in a file or through plugins/modules.
 */
public class ImmutableClusterStateController {
    private static final Logger logger = LogManager.getLogger(ImmutableClusterStateController.class);

    public static final ParseField STATE_FIELD = new ParseField("state");
    public static final ParseField METADATA_FIELD = new ParseField("metadata");

    final Map<String, ImmutableClusterStateHandler<?>> handlers;
    final ClusterService clusterService;
    private final ImmutableUpdateStateTaskExecutor updateStateTaskExecutor;
    private final ImmutableUpdateErrorTaskExecutor errorStateTaskExecutor;

    @SuppressWarnings("unchecked")
    private final ConstructingObjectParser<Package, Void> packageParser = new ConstructingObjectParser<>("immutable_cluster_package", a -> {
        List<Tuple<String, Object>> tuples = (List<Tuple<String, Object>>) a[0];
        Map<String, Object> stateMap = new HashMap<>();
        for (var tuple : tuples) {
            stateMap.put(tuple.v1(), tuple.v2());
        }

        return new Package(stateMap, (PackageVersion) a[1]);
    });

    /**
     * Controller class for saving immutable ClusterState.
     * @param clusterService for fetching and saving the modified state
     */
    public ImmutableClusterStateController(ClusterService clusterService, List<ImmutableClusterStateHandler<?>> handlerList) {
        this.clusterService = clusterService;
        this.updateStateTaskExecutor = new ImmutableUpdateStateTaskExecutor(clusterService.getRerouteService());
        this.errorStateTaskExecutor = new ImmutableUpdateErrorTaskExecutor();
        this.handlers = handlerList.stream().collect(Collectors.toMap(ImmutableClusterStateHandler::name, Function.identity()));
        packageParser.declareNamedObjects(ConstructingObjectParser.constructorArg(), (p, c, name) -> {
            if (handlers.containsKey(name) == false) {
                throw new IllegalStateException("Missing handler definition for content key [" + name + "]");
            }
            p.nextToken();
            return new Tuple<>(name, handlers.get(name).fromXContent(p));
        }, STATE_FIELD);
        packageParser.declareObject(ConstructingObjectParser.constructorArg(), PackageVersion::parse, METADATA_FIELD);
    }

    /**
     * A package class containing the composite immutable cluster state
     * <p>
     * Apart from the cluster state we want to store as immutable, the package requires that
     * you supply the version metadata. This version metadata (see {@link PackageVersion}) is checked to ensure
     * that the update is safe, and it's not unnecessarily repeated.
     */
    public record Package(Map<String, Object> state, PackageVersion metadata) {}

    /**
     * Saves an immutable cluster state for a given 'namespace' from {@link XContentParser}
     *
     * @param namespace the namespace under which we'll store the immutable keys in the cluster state metadata
     * @param parser the XContentParser to process
     * @param errorListener a consumer called with IllegalStateException if the content has errors and the
     *        cluster state cannot be correctly applied, IncompatibleVersionException if the content is stale or
     *        incompatible with this node {@link Version}, null if successful.
     */
    public void process(String namespace, XContentParser parser, Consumer<Exception> errorListener) {
        Package immutableStatePackage;

        try {
            immutableStatePackage = packageParser.apply(parser, null);
        } catch (Exception e) {
            List<String> errors = List.of(e.getMessage());
            recordErrorState(new ImmutableUpdateErrorState(namespace, -1L, errors, ImmutableStateErrorMetadata.ErrorKind.PARSING));
            logger.error("Error processing state change request for [{}] with the following errors [{}]", namespace, errors);

            errorListener.accept(new IllegalStateException("Error processing state change request for " + namespace, e));
            return;
        }

        process(namespace, immutableStatePackage, errorListener);
    }

    /**
     * Saves an immutable cluster state for a given 'namespace' from {@link Package}
     *
     * @param namespace the namespace under which we'll store the immutable keys in the cluster state metadata
     * @param immutableStateFilePackage a {@link Package} composite state object to process
     * @param errorListener a consumer called with IllegalStateException if the content has errors and the
     *        cluster state cannot be correctly applied, IncompatibleVersionException if the content is stale or
     *        incompatible with this node {@link Version}, null if successful.
     */
    public void process(String namespace, Package immutableStateFilePackage, Consumer<Exception> errorListener) {
        Map<String, Object> immutableState = immutableStateFilePackage.state;
        PackageVersion packageVersion = immutableStateFilePackage.metadata;

        LinkedHashSet<String> orderedHandlers;
        try {
            orderedHandlers = orderedStateHandlers(immutableState.keySet());
        } catch (Exception e) {
            List<String> errors = List.of(e.getMessage());
            recordErrorState(
                new ImmutableUpdateErrorState(namespace, packageVersion.version(), errors, ImmutableStateErrorMetadata.ErrorKind.PARSING)
            );
            logger.error("Error processing state change request for [{}] with the following errors [{}]", namespace, errors);

            errorListener.accept(new IllegalStateException("Error processing state change request for " + namespace, e));
            return;
        }

        ClusterState state = clusterService.state();
        ImmutableStateMetadata existingMetadata = state.metadata().immutableStateMetadata().get(namespace);
        if (checkMetadataVersion(existingMetadata, packageVersion, errorListener) == false) {
            return;
        }

        clusterService.submitStateUpdateTask(
            "immutable cluster state [" + namespace + "]",
            new ImmutableStateUpdateStateTask(
                namespace,
                immutableStateFilePackage,
                handlers,
                orderedHandlers,
                (errorState) -> recordErrorState(errorState),
                new ActionListener<>() {
                    @Override
                    public void onResponse(ActionResponse.Empty empty) {
                        logger.info("Successfully applied new cluster state for namespace [{}]", namespace);
                        errorListener.accept(null);
                    }

                    @Override
                    public void onFailure(Exception e) {
                        logger.error("Failed to apply immutable cluster state", e);
                        errorListener.accept(e);
                    }
                }
            ),
            ClusterStateTaskConfig.build(Priority.URGENT),
            updateStateTaskExecutor
        );
    }

    // package private for testing
    static boolean checkMetadataVersion(
        ImmutableStateMetadata existingMetadata,
        PackageVersion packageVersion,
        Consumer<Exception> errorListener
    ) {
        if (Version.CURRENT.before(packageVersion.minCompatibleVersion())) {
            errorListener.accept(
                new IncompatibleVersionException(
                    format(
                        "Cluster state version [%s] is not compatible with this Elasticsearch node",
                        packageVersion.minCompatibleVersion()
                    )
                )
            );
            return false;
        }

        if (existingMetadata != null && existingMetadata.version() >= packageVersion.version()) {
            errorListener.accept(
                new IncompatibleVersionException(
                    format(
                        "Not updating cluster state because version [%s] is less or equal to the current metadata version [%s]",
                        packageVersion.version(),
                        existingMetadata.version()
                    )
                )
            );
            return false;
        }

        return true;
    }

    record ImmutableUpdateErrorState(
        String namespace,
        Long version,
        List<String> errors,
        ImmutableStateErrorMetadata.ErrorKind errorKind
    ) {}

    private void recordErrorState(ImmutableUpdateErrorState state) {
        clusterService.submitStateUpdateTask(
            "immutable cluster state update error for [ " + state.namespace + "]",
            new ImmutableStateUpdateErrorTask(state, new ActionListener<>() {
                @Override
                public void onResponse(ActionResponse.Empty empty) {
                    logger.info("Successfully applied new immutable error state for namespace [{}]", state.namespace);
                }

                @Override
                public void onFailure(Exception e) {
                    logger.error("Failed to apply immutable error cluster state", e);
                }
            }),
            ClusterStateTaskConfig.build(Priority.URGENT),
            errorStateTaskExecutor
        );
    }

    // package private for testing
    LinkedHashSet<String> orderedStateHandlers(Set<String> keys) {
        LinkedHashSet<String> orderedHandlers = new LinkedHashSet<>();
        LinkedHashSet<String> dependencyStack = new LinkedHashSet<>();

        for (String key : keys) {
            addStateHandler(key, keys, orderedHandlers, dependencyStack);
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
        ImmutableClusterStateHandler<?> handler = handlers.get(key);

        if (handler == null) {
            throw new IllegalStateException("Unknown settings definition type: " + key);
        }

        for (String dependency : handler.dependencies()) {
            if (keys.contains(dependency) == false) {
                throw new IllegalStateException("Missing settings dependency definition: " + key + " -> " + dependency);
            }
            addStateHandler(dependency, keys, ordered, visited);
        }

        visited.remove(key);
        ordered.add(key);
    }

    /**
     * {@link IncompatibleVersionException} is thrown when we try to update the cluster state
     * without changing the update version id, or if we try to update cluster state on
     * an incompatible Elasticsearch version in mixed cluster mode.
     */
    public static class IncompatibleVersionException extends RuntimeException {
        public IncompatibleVersionException(String message) {
            super(message);
        }
    }

    /**
     * Immutable cluster state update task executor
     *
     * @param rerouteService instance of {@link RerouteService}, so that we can execute reroute after cluster state is published
     */
    public record ImmutableUpdateStateTaskExecutor(RerouteService rerouteService)
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
                "reroute after applying immutable cluster state",
                Priority.NORMAL,
                ActionListener.wrap(
                    r -> logger.trace("reroute after applying immutable cluster state succeeded"),
                    e -> logger.debug("reroute after applying immutable cluster state failed", e)
                )
            );
        }
    }

    /**
     * Immutable cluster error state task executor
     * <p>
     * We use this task executor to record any errors while updating immutable cluster state
     */
    public record ImmutableUpdateErrorTaskExecutor() implements ClusterStateTaskExecutor<ImmutableStateUpdateErrorTask> {
        @Override
        public ClusterState execute(ClusterState currentState, List<TaskContext<ImmutableStateUpdateErrorTask>> taskContexts)
            throws Exception {
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
            logger.info("Wrote new error state in immutable metadata");
        }
    }
}
