/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.operator.service;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateTaskConfig;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.OperatorHandlerMetadata;
import org.elasticsearch.cluster.metadata.OperatorMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Priority;
import org.elasticsearch.operator.OperatorHandler;
import org.elasticsearch.operator.TransformState;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Controller class for applying file based settings to ClusterState.
 * This class contains the logic about validation, ordering and applying of
 * the cluster state specified in a file.
 */
public class OperatorClusterStateController {
    private static final Logger logger = LogManager.getLogger(FileSettingsService.class);

    public static final String SETTINGS = "settings";
    public static final String METADATA = "metadata";

    Map<String, OperatorHandler<?>> handlers = null;
    final ClusterService clusterService;

    public OperatorClusterStateController(ClusterService clusterService) {
        this.clusterService = clusterService;
    }

    public void initHandlers(List<OperatorHandler<?>> handlerList) {
        handlers = handlerList.stream().collect(Collectors.toMap(OperatorHandler::key, Function.identity()));
    }

    static class SettingsFile {
        public static final ParseField STATE_FIELD = new ParseField("state");
        public static final ParseField METADATA_FIELD = new ParseField("metadata");
        @SuppressWarnings("unchecked")
        private static final ConstructingObjectParser<SettingsFile, Void> PARSER = new ConstructingObjectParser<>(
            "operator_state",
            a -> new SettingsFile((Map<String, Object>) a[0], (OperatorStateVersionMetadata) a[1])
        );
        static {
            PARSER.declareObject(ConstructingObjectParser.constructorArg(), (p, c) -> p.map(), STATE_FIELD);
            PARSER.declareObject(ConstructingObjectParser.constructorArg(), OperatorStateVersionMetadata::parse, METADATA_FIELD);
        }

        Map<String, Object> state;
        OperatorStateVersionMetadata metadata;

        SettingsFile(Map<String, Object> state, OperatorStateVersionMetadata metadata) {
            this.state = state;
            this.metadata = metadata;
        }
    }

    public ClusterState process(String namespace, XContentParser parser) throws IOException {
        SettingsFile operatorStateFileContent = SettingsFile.PARSER.apply(parser, null);
        Map<String, Object> operatorState = operatorStateFileContent.state;
        OperatorStateVersionMetadata stateVersionMetadata = operatorStateFileContent.metadata;

        LinkedHashSet<String> orderedHandlers = orderedStateHandlers(operatorState.keySet());

        ClusterState state = clusterService.state();
        OperatorMetadata existingMetadata = state.metadata().operatorState(namespace);
        if (checkMetadataVersion(existingMetadata, stateVersionMetadata) == false) {
            return state;
        }

        OperatorMetadata.Builder operatorMetadataBuilder = new OperatorMetadata.Builder(namespace).version(stateVersionMetadata.version());
        List<String> errors = new ArrayList<>();

        for (var handlerKey : orderedHandlers) {
            OperatorHandler<?> handler = handlers.get(handlerKey);
            try {
                Set<String> existingKeys = keysForHandler(existingMetadata, handlerKey);
                TransformState transformState = handler.transform(operatorState.get(handlerKey), new TransformState(state, existingKeys));
                state = transformState.state();
                operatorMetadataBuilder.putHandler(new OperatorHandlerMetadata.Builder(handlerKey).keys(transformState.keys()).build());
            } catch (Exception e) {
                errors.add(String.format("Error processing %s state change: %s", handler.key(), e.getMessage()));
            }
        }

        if (errors.isEmpty() == false) {
            clusterService.submitStateUpdateTask("operator state error for [ " + namespace + "]",
                new OperatorUpdateErrorTask(new ActionListener<>() {
                    @Override
                    public void onResponse(ActionResponse.Empty empty) {
                        logger.info("Successfully applied new operator error state for namespace [{}]", namespace);
                    }

                    @Override
                    public void onFailure(Exception e) {
                        logger.error("Failed to apply operator error cluster state", e);
                    }
                }),
                ClusterStateTaskConfig.build(Priority.URGENT),
                new OperatorUpdateErrorTask.OperatorUpdateErrorTaskExecutor(namespace, stateVersionMetadata.version(), errors)
            );

            logger.error("Error processing state change request for [{}] with the following errors [{}]", namespace, errors);

            throw new IllegalStateException("Error processing state change request for " + namespace);
        }

        // remove the last error if we had previously encountered any
        operatorMetadataBuilder.errorMetadata(null);

        ClusterState.Builder stateBuilder = new ClusterState.Builder(state);
        Metadata.Builder metadataBuilder = Metadata.builder(state.metadata()).putOperatorState(operatorMetadataBuilder.build());
        state = stateBuilder.metadata(metadataBuilder).build();

        // TODO: Retry, maybe use RetryableAction?
        clusterService.submitStateUpdateTask("operator state [ " + namespace + "]", new OperatorUpdateStateTask(new ActionListener<>() {
            @Override
            public void onResponse(ActionResponse.Empty empty) {
                logger.info("Successfully applied new cluster state for namespace [{}]", namespace);
            }

            @Override
            public void onFailure(Exception e) {
                logger.error("Failed to apply operator cluster state", e);
            }
        }),
            ClusterStateTaskConfig.build(Priority.URGENT),
            new OperatorUpdateStateTask.OperatorUpdateStateTaskExecutor(namespace, state, clusterService.getRerouteService())
        );

        return state;
    }

    private Set<String> keysForHandler(OperatorMetadata operatorMetadata, String handlerKey) {
        if (operatorMetadata == null || operatorMetadata.handlers().get(handlerKey) == null) {
            return Collections.emptySet();
        }

        return operatorMetadata.handlers().get(handlerKey).keys();
    }

    boolean checkMetadataVersion(OperatorMetadata existingMetadata, OperatorStateVersionMetadata stateVersionMetadata) {
        if (Version.CURRENT.before(stateVersionMetadata.minCompatibleVersion())) {
           logger.info(
               "Cluster state version [{}] is not compatible with this Elasticsearch node",
               stateVersionMetadata.minCompatibleVersion()
           );
           return false;
        }

        if (existingMetadata != null && existingMetadata.version() >= stateVersionMetadata.version()) {
            logger.info(
                "Not updating cluster state because version [{}] is less or equal to the current metadata version [{}]",
                stateVersionMetadata.version(), existingMetadata.version()
            );
            return false;
        }

        return true;
    }

    LinkedHashSet<String> orderedStateHandlers(Set<String> keys) {
        LinkedHashSet<String> orderedHandlers = new LinkedHashSet<>();
        LinkedHashSet<String> dependencyStack = new LinkedHashSet<>();

        for (String key : keys) {
            addStateHandler(key, keys, orderedHandlers, dependencyStack);
        }

        return orderedHandlers;
    }

    void addStateHandler(String key, Set<String> keys, LinkedHashSet<String> ordered, LinkedHashSet<String> visited) {
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
        OperatorHandler<?> handler = handlers.get(key);

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
}
