/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.action;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.OriginSettingClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.InferenceFieldMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.protocol.xpack.XPackUsageRequest;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.action.XPackUsageFeatureAction;
import org.elasticsearch.xpack.core.action.XPackUsageFeatureResponse;
import org.elasticsearch.xpack.core.action.XPackUsageFeatureTransportAction;
import org.elasticsearch.xpack.core.inference.InferenceFeatureSetUsage;
import org.elasticsearch.xpack.core.inference.action.GetInferenceModelAction;
import org.elasticsearch.xpack.core.inference.usage.ModelStats;
import org.elasticsearch.xpack.inference.registry.ModelRegistry;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.elasticsearch.xpack.core.ClientHelper.ML_ORIGIN;

public class TransportInferenceUsageAction extends XPackUsageFeatureTransportAction {

    private final Logger logger = LogManager.getLogger(TransportInferenceUsageAction.class);

    private final ModelRegistry modelRegistry;
    private final Client client;

    @Inject
    public TransportInferenceUsageAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        ModelRegistry modelRegistry,
        Client client
    ) {
        super(XPackUsageFeatureAction.INFERENCE.name(), transportService, clusterService, threadPool, actionFilters);
        this.modelRegistry = modelRegistry;
        this.client = new OriginSettingClient(client, ML_ORIGIN);
    }

    @Override
    protected void localClusterStateOperation(
        Task task,
        XPackUsageRequest request,
        ClusterState state,
        ActionListener<XPackUsageFeatureResponse> listener
    ) {
        GetInferenceModelAction.Request getInferenceModelAction = new GetInferenceModelAction.Request("_all", TaskType.ANY, false);
        client.execute(GetInferenceModelAction.INSTANCE, getInferenceModelAction, ActionListener.wrap(response -> {
            listener.onResponse(
                new XPackUsageFeatureResponse(collectUsage(response.getEndpoints(), state.getMetadata().indicesAllProjects()))
            );
        }, e -> {
            logger.warn(Strings.format("Retrieving inference usage failed with error: %s", e.getMessage()), e);
            listener.onResponse(new XPackUsageFeatureResponse(InferenceFeatureSetUsage.EMPTY));
        }));
    }

    private InferenceFeatureSetUsage collectUsage(List<ModelConfigurations> endpoints, Iterable<IndexMetadata> indicesMetadata) {
        Map<ServiceAndTaskType, Map<String, List<InferenceFieldMetadata>>> inferenceFieldsByIndexServiceAndTask =
            mapInferenceFieldsByIndexServiceAndTask(indicesMetadata, endpoints);
        Map<String, ModelStats> endpointStats = new TreeMap<>();
        addStatsByServiceAndTask(inferenceFieldsByIndexServiceAndTask, endpoints, endpointStats);
        addTopLevelSemanticTextStatsByTask(inferenceFieldsByIndexServiceAndTask, endpointStats);
        addStatsForDefaultModels(inferenceFieldsByIndexServiceAndTask, endpoints, endpointStats);
        return new InferenceFeatureSetUsage(endpointStats.values().stream().filter(stats -> stats.count() > 0).toList());
    }

    private static Map<ServiceAndTaskType, Map<String, List<InferenceFieldMetadata>>> mapInferenceFieldsByIndexServiceAndTask(
        Iterable<IndexMetadata> indicesMetadata,
        List<ModelConfigurations> endpoints
    ) {
        Map<String, ModelConfigurations> inferenceIdToEndpoint = endpoints.stream()
            .collect(Collectors.toMap(ModelConfigurations::getInferenceEntityId, Function.identity()));
        Map<ServiceAndTaskType, Map<String, List<InferenceFieldMetadata>>> inferenceFieldByIndexServiceAndTask = new HashMap<>();
        for (IndexMetadata indexMetadata : indicesMetadata) {
            if (indexMetadata.isSystem()) {
                // Usage for system indices should be reported through the corresponding application usage
                continue;
            }
            indexMetadata.getInferenceFields()
                .values()
                .stream()
                .filter(field -> inferenceIdToEndpoint.containsKey(field.getInferenceId()))
                .forEach(field -> {
                    ModelConfigurations endpoint = inferenceIdToEndpoint.get(field.getInferenceId());
                    Map<String, List<InferenceFieldMetadata>> fieldsByIndex = inferenceFieldByIndexServiceAndTask.computeIfAbsent(
                        new ServiceAndTaskType(endpoint.getService(), endpoint.getTaskType()),
                        key -> new HashMap<>()
                    );
                    fieldsByIndex.computeIfAbsent(indexMetadata.getIndex().getName(), key -> new ArrayList<>()).add(field);
                });
        }
        return inferenceFieldByIndexServiceAndTask;
    }

    private static void addStatsByServiceAndTask(
        Map<ServiceAndTaskType, Map<String, List<InferenceFieldMetadata>>> inferenceFieldsByIndexServiceAndTask,
        List<ModelConfigurations> endpoints,
        Map<String, ModelStats> endpointStats
    ) {
        for (ModelConfigurations model : endpoints) {
            endpointStats.computeIfAbsent(
                new ServiceAndTaskType(model.getService(), model.getTaskType()).toString(),
                key -> new ModelStats(model.getService(), model.getTaskType())
            ).add();

            endpointStats.computeIfAbsent(
                new ServiceAndTaskType(Metadata.ALL, model.getTaskType()).toString(),
                key -> new ModelStats(Metadata.ALL, model.getTaskType())
            ).add();
        }

        inferenceFieldsByIndexServiceAndTask.forEach(
            (serviceAndTaskType, inferenceFieldsByIndex) -> addSemanticTextStats(
                inferenceFieldsByIndex,
                endpointStats.get(serviceAndTaskType.toString())
            )
        );
    }

    private static void addTopLevelSemanticTextStatsByTask(
        Map<ServiceAndTaskType, Map<String, List<InferenceFieldMetadata>>> inferenceFieldsByIndexServiceAndTask,
        Map<String, ModelStats> endpointStats
    ) {
        for (TaskType taskType : Arrays.stream(TaskType.values()).filter(t -> t != TaskType.ANY).toList()) {
            ModelStats allStatsForTaskType = endpointStats.computeIfAbsent(
                new ServiceAndTaskType(Metadata.ALL, taskType).toString(),
                key -> new ModelStats(Metadata.ALL, taskType)
            );
            Map<String, List<InferenceFieldMetadata>> inferenceFieldsByIndex = inferenceFieldsByIndexServiceAndTask.entrySet()
                .stream()
                .filter(e -> e.getKey().taskType == taskType)
                .flatMap(m -> m.getValue().entrySet().stream())
                .collect(
                    Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (l1, l2) -> Stream.concat(l1.stream(), l2.stream()).toList())
                );
            addSemanticTextStats(inferenceFieldsByIndex, allStatsForTaskType);
        }
    }

    private static void addSemanticTextStats(Map<String, List<InferenceFieldMetadata>> inferenceFieldsByIndex, ModelStats stat) {
        Set<String> inferenceIds = new HashSet<>();
        for (List<InferenceFieldMetadata> inferenceFields : inferenceFieldsByIndex.values()) {
            stat.semanticTextStats().addFieldCount(inferenceFields.size());
            stat.semanticTextStats().incIndicesCount();
            inferenceFields.forEach(field -> inferenceIds.add(field.getInferenceId()));
        }
        stat.semanticTextStats().setInferenceIdCount(inferenceIds.size());
    }

    private void addStatsForDefaultModels(
        Map<ServiceAndTaskType, Map<String, List<InferenceFieldMetadata>>> inferenceFieldsByIndexServiceAndTask,
        List<ModelConfigurations> endpoints,
        Map<String, ModelStats> endpointStats
    ) {
        Map<String, String> endpointIdToModelId = endpoints.stream()
            .filter(endpoint -> endpoint.getServiceSettings().modelId() != null)
            .collect(Collectors.toMap(ModelConfigurations::getInferenceEntityId, e -> e.getServiceSettings().modelId()));
        Map<DefaultModelStatsKey, Long> defaultModelsToEndpointCount = createDefaultStatsKeysWithEndpointCounts(endpoints);
        for (Map.Entry<DefaultModelStatsKey, Long> defaultModelStatsKeyToEndpointCount : defaultModelsToEndpointCount.entrySet()) {
            DefaultModelStatsKey statKey = defaultModelStatsKeyToEndpointCount.getKey();
            Map<String, List<InferenceFieldMetadata>> fieldsByIndex = inferenceFieldsByIndexServiceAndTask.getOrDefault(
                new ServiceAndTaskType(statKey.service, statKey.taskType),
                Map.of()
            );
            fieldsByIndex = filterFields(fieldsByIndex, f -> statKey.modelId.equals(endpointIdToModelId.get(f.getInferenceId())));
            ModelStats stats = new ModelStats(statKey.toString(), statKey.taskType, defaultModelStatsKeyToEndpointCount.getValue());
            addSemanticTextStats(fieldsByIndex, stats);
            endpointStats.put(statKey.toString(), stats);
        }
    }

    private Map<DefaultModelStatsKey, Long> createDefaultStatsKeysWithEndpointCounts(List<ModelConfigurations> endpoints) {
        Set<String> modelIds = endpoints.stream()
            .filter(endpoint -> modelRegistry.containsDefaultConfigId(endpoint.getInferenceEntityId()))
            .map(endpoint -> endpoint.getServiceSettings().modelId())
            .collect(Collectors.toSet());
        return endpoints.stream()
            .filter(endpoint -> modelIds.contains(endpoint.getServiceSettings().modelId()))
            .map(
                endpoint -> new DefaultModelStatsKey(endpoint.getService(), endpoint.getTaskType(), endpoint.getServiceSettings().modelId())
            )
            .collect(Collectors.groupingBy(Function.identity(), Collectors.counting()));
    }

    private static Map<String, List<InferenceFieldMetadata>> filterFields(
        Map<String, List<InferenceFieldMetadata>> fieldsByIndex,
        Predicate<InferenceFieldMetadata> predicate
    ) {
        Map<String, List<InferenceFieldMetadata>> filtered = new HashMap<>();
        for (Map.Entry<String, List<InferenceFieldMetadata>> entry : fieldsByIndex.entrySet()) {
            List<InferenceFieldMetadata> filteredFields = entry.getValue().stream().filter(predicate).toList();
            if (filteredFields.isEmpty() == false) {
                filtered.put(entry.getKey(), filteredFields);
            }
        }
        return filtered;
    }

    private record DefaultModelStatsKey(String service, TaskType taskType, String modelId) {

        // Some of the default models have optimized variants for linux that will have the following suffix.
        private static final String MODEL_ID_LINUX_SUFFIX = "_linux-x86_64";

        @Override
        public String toString() {
            // Inference ids cannot start with '_'. Thus, default stats do to avoid conflicts with user-defined inference ids.
            return "_" + service + "_" + stripLinuxSuffix(modelId).replace('.', '_');
        }

        private static String stripLinuxSuffix(String modelId) {
            if (modelId.endsWith(MODEL_ID_LINUX_SUFFIX)) {
                return modelId.substring(0, modelId.length() - MODEL_ID_LINUX_SUFFIX.length());
            }
            return modelId;
        }
    }

    private record ServiceAndTaskType(String service, TaskType taskType) {

        @Override
        public String toString() {
            return service + ":" + taskType.name();
        }
    }
}
