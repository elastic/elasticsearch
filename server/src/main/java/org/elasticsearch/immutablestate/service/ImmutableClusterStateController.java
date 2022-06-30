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
import org.elasticsearch.cluster.metadata.ImmutableStateErrorMetadata;
import org.elasticsearch.cluster.metadata.ImmutableStateMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Priority;
import org.elasticsearch.immutablestate.ImmutableClusterStateHandler;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentParser;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.elasticsearch.core.Strings.format;

/**
 * Controller class for applying file based settings to ClusterState.
 * This class contains the logic about validation, ordering and applying of
 * the cluster state specified in a file.
 */
public class ImmutableClusterStateController {
    private static final Logger logger = LogManager.getLogger(ImmutableClusterStateController.class);

    public static final String SETTINGS = "settings";
    public static final String METADATA = "metadata";

    Map<String, ImmutableClusterStateHandler<?>> handlers = null;
    final ClusterService clusterService;

    public ImmutableClusterStateController(ClusterService clusterService) {
        this.clusterService = clusterService;
    }

    /**
     * Initializes the controller with the currently implemented state handlers
     *
     * @param handlerList the list of supported immutable cluster state handlers
     */
    public void initHandlers(List<ImmutableClusterStateHandler<?>> handlerList) {
        handlers = handlerList.stream().collect(Collectors.toMap(ImmutableClusterStateHandler::name, Function.identity()));
    }

    /**
     * A package class containing the composite immutable cluster state
     * <p>
     * Apart from the cluster state we want to store as immutable, the package requires that
     * you supply the version metadata. This version metadata (see {@link StateVersionMetadata}) is checked to ensure
     * that the update is safe, and it's not unnecessarily repeated.
     */
    public static class Package {
        public static final ParseField STATE_FIELD = new ParseField("state");
        public static final ParseField METADATA_FIELD = new ParseField("metadata");
        @SuppressWarnings("unchecked")
        private static final ConstructingObjectParser<Package, Void> PARSER = new ConstructingObjectParser<>(
            "immutable_cluster_state",
            a -> new Package((Map<String, Object>) a[0], (StateVersionMetadata) a[1])
        );
        static {
            PARSER.declareObject(ConstructingObjectParser.constructorArg(), (p, c) -> p.map(), STATE_FIELD);
            PARSER.declareObject(ConstructingObjectParser.constructorArg(), StateVersionMetadata::parse, METADATA_FIELD);
        }

        Map<String, Object> state;
        StateVersionMetadata metadata;

        /**
         * A package class containing the composite immutable cluster state
         * @param state a {@link Map} of immutable state handler name and data
         * @param metadata a version metadata
         */
        public Package(Map<String, Object> state, StateVersionMetadata metadata) {
            this.state = state;
            this.metadata = metadata;
        }
    }

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
            immutableStatePackage = Package.PARSER.apply(parser, null);
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
        StateVersionMetadata stateVersionMetadata = immutableStateFilePackage.metadata;

        LinkedHashSet<String> orderedHandlers;
        try {
            orderedHandlers = orderedStateHandlers(immutableState.keySet());
        } catch (Exception e) {
            List<String> errors = List.of(e.getMessage());
            recordErrorState(
                new ImmutableUpdateErrorState(
                    namespace,
                    stateVersionMetadata.version(),
                    errors,
                    ImmutableStateErrorMetadata.ErrorKind.PARSING
                )
            );
            logger.error("Error processing state change request for [{}] with the following errors [{}]", namespace, errors);

            errorListener.accept(new IllegalStateException("Error processing state change request for " + namespace, e));
            return;
        }

        ClusterState state = clusterService.state();
        ImmutableStateMetadata existingMetadata = state.metadata().immutableStateMetadata().get(namespace);
        if (checkMetadataVersion(existingMetadata, stateVersionMetadata, errorListener) == false) {
            return;
        }

        // Do we need to retry this, or it retries automatically?
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
            new ImmutableStateUpdateStateTask.ImmutableUpdateStateTaskExecutor(namespace, clusterService.getRerouteService())
        );
    }

    // package private for testing
    static boolean checkMetadataVersion(
        ImmutableStateMetadata existingMetadata,
        StateVersionMetadata stateVersionMetadata,
        Consumer<Exception> errorListener
    ) {
        if (Version.CURRENT.before(stateVersionMetadata.minCompatibleVersion())) {
            errorListener.accept(
                new IncompatibleVersionException(
                    format(
                        "Cluster state version [%s] is not compatible with this Elasticsearch node",
                        stateVersionMetadata.minCompatibleVersion()
                    )
                )
            );
            return false;
        }

        if (existingMetadata != null && existingMetadata.version() >= stateVersionMetadata.version()) {
            errorListener.accept(
                new IncompatibleVersionException(
                    format(
                        "Not updating cluster state because version [%s] is less or equal to the current metadata version [%s]",
                        stateVersionMetadata.version(),
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
            new ImmutableStateUpdateErrorTask.ImmutableUpdateErrorTaskExecutor()
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
}
