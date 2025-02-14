/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.utils;

import org.apache.lucene.util.Counter;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.ingest.IngestMetadata;
import org.elasticsearch.transport.Transports;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

import static org.elasticsearch.inference.InferenceResults.MODEL_ID_RESULTS_FIELD;
import static org.elasticsearch.ingest.Pipeline.ON_FAILURE_KEY;
import static org.elasticsearch.ingest.Pipeline.PROCESSORS_KEY;

/**
 * Utilities for extracting information around inference processors from IngestMetadata
 */
public final class InferenceProcessorInfoExtractor {
    private static final String FOREACH_PROCESSOR_NAME = "foreach";
    // Any more than 10 nestings of processors, we stop searching for inference processor definitions
    private static final int MAX_INFERENCE_PROCESSOR_SEARCH_RECURSIONS = 10;

    private InferenceProcessorInfoExtractor() {}

    /**
     * @param state The current cluster state
     * @return The current count of inference processors
     */
    @SuppressWarnings("unchecked")
    public static int countInferenceProcessors(ClusterState state) {
        Metadata metadata = state.getMetadata();
        if (metadata == null) {
            return 0;
        }
        IngestMetadata ingestMetadata = metadata.custom(IngestMetadata.TYPE);
        if (ingestMetadata == null) {
            return 0;
        }
        Counter counter = Counter.newCounter();
        ingestMetadata.getPipelines().forEach((pipelineId, configuration) -> {
            Map<String, Object> configMap = configuration.getConfig();
            List<Map<String, Object>> processorConfigs = (List<Map<String, Object>>) configMap.get(PROCESSORS_KEY);
            for (Map<String, Object> processorConfigWithKey : processorConfigs) {
                for (Map.Entry<String, Object> entry : processorConfigWithKey.entrySet()) {
                    addModelsAndPipelines(entry.getKey(), pipelineId, entry.getValue(), pam -> counter.addAndGet(1), 0);
                }
            }
        });
        return (int) counter.get();
    }

    /**
     * @param ingestMetadata The ingestMetadata of current ClusterState
     * @return The set of model IDs referenced by inference processors
     */
    public static Set<String> getModelIdsFromInferenceProcessors(IngestMetadata ingestMetadata) {
        if (ingestMetadata == null) {
            return Set.of();
        }

        Set<String> modelIds = new LinkedHashSet<>();
        ingestMetadata.getPipelines().forEach((pipelineId, configuration) -> {
            Map<String, Object> configMap = configuration.getConfig();
            List<Map<String, Object>> processorConfigs = readList(configMap, PROCESSORS_KEY);
            for (Map<String, Object> processorConfigWithKey : processorConfigs) {
                for (Map.Entry<String, Object> entry : processorConfigWithKey.entrySet()) {
                    addModelsAndPipelines(entry.getKey(), pipelineId, entry.getValue(), pam -> modelIds.add(pam.modelIdOrAlias()), 0);
                }
            }
        });
        return modelIds;
    }

    /**
     * @param state Current cluster state
     * @return a map from Model or Deployment IDs or Aliases to each pipeline referencing them.
     */
    public static Map<String, Set<String>> pipelineIdsByResource(ClusterState state, Set<String> ids) {
        assert Transports.assertNotTransportThread("non-trivial nested loops over cluster state structures");
        Map<String, Set<String>> pipelineIdsByModelIds = new HashMap<>();
        Metadata metadata = state.metadata();
        if (metadata == null) {
            return pipelineIdsByModelIds;
        }
        IngestMetadata ingestMetadata = metadata.custom(IngestMetadata.TYPE);
        if (ingestMetadata == null) {
            return pipelineIdsByModelIds;
        }
        ingestMetadata.getPipelines().forEach((pipelineId, configuration) -> {
            Map<String, Object> configMap = configuration.getConfig();
            List<Map<String, Object>> processorConfigs = readList(configMap, PROCESSORS_KEY);
            for (Map<String, Object> processorConfigWithKey : processorConfigs) {
                for (Map.Entry<String, Object> entry : processorConfigWithKey.entrySet()) {
                    addModelsAndPipelines(entry.getKey(), pipelineId, entry.getValue(), pam -> {
                        if (ids.contains(pam.modelIdOrAlias)) {
                            pipelineIdsByModelIds.computeIfAbsent(pam.modelIdOrAlias, m -> new LinkedHashSet<>()).add(pipelineId);
                        }
                    }, 0);
                }
            }
        });
        return pipelineIdsByModelIds;
    }

    /**
     * @param state Current {@link ClusterState}
     * @return a map from Model or Deployment IDs or Aliases to each pipeline referencing them.
     */
    public static Set<String> pipelineIdsForResource(ClusterState state, Set<String> ids) {
        assert Transports.assertNotTransportThread("non-trivial nested loops over cluster state structures");
        Set<String> pipelineIds = new HashSet<>();
        Metadata metadata = state.metadata();
        if (metadata == null) {
            return pipelineIds;
        }
        IngestMetadata ingestMetadata = metadata.custom(IngestMetadata.TYPE);
        if (ingestMetadata == null) {
            return pipelineIds;
        }
        ingestMetadata.getPipelines().forEach((pipelineId, configuration) -> {
            Map<String, Object> configMap = configuration.getConfig();
            List<Map<String, Object>> processorConfigs = readList(configMap, PROCESSORS_KEY);
            for (Map<String, Object> processorConfigWithKey : processorConfigs) {
                for (Map.Entry<String, Object> entry : processorConfigWithKey.entrySet()) {
                    addModelsAndPipelines(entry.getKey(), pipelineId, entry.getValue(), pam -> {
                        if (ids.contains(pam.modelIdOrAlias)) {
                            pipelineIds.add(pipelineId);
                        }
                    }, 0);
                }
            }
        });
        return pipelineIds;
    }

    @SuppressWarnings("unchecked")
    private static void addModelsAndPipelines(
        String processorType,
        String pipelineId,
        Object processorDefinition,
        Consumer<PipelineAndModel> handler,
        int level
    ) {
        // arbitrary, but we must limit this somehow
        if (level > MAX_INFERENCE_PROCESSOR_SEARCH_RECURSIONS) {
            return;
        }
        if (processorType == null || processorDefinition == null) {
            return;
        }
        if (InferenceProcessorConstants.TYPE.equals(processorType)) {
            if (processorDefinition instanceof Map<?, ?> definitionMap) {
                String modelId = (String) definitionMap.get(MODEL_ID_RESULTS_FIELD);
                if (modelId != null) {
                    handler.accept(new PipelineAndModel(pipelineId, modelId));
                }
            }
            return;
        }
        if (FOREACH_PROCESSOR_NAME.equals(processorType) && processorDefinition instanceof Map<?, ?> definitionMap) {
            Map<String, Object> innerProcessor = (Map<String, Object>) definitionMap.get("processor");
            if (innerProcessor != null) {
                // a foreach processor should only have a SINGLE nested processor. Iteration is for simplicity's sake.
                for (Map.Entry<String, Object> innerProcessorWithName : innerProcessor.entrySet()) {
                    addModelsAndPipelines(
                        innerProcessorWithName.getKey(),
                        pipelineId,
                        innerProcessorWithName.getValue(),
                        handler,
                        level + 1
                    );
                }
            }
            return;
        }
        if (processorDefinition instanceof Map<?, ?> definitionMap && definitionMap.containsKey(ON_FAILURE_KEY)) {
            List<Map<String, Object>> onFailureConfigs = readList(definitionMap, ON_FAILURE_KEY);
            onFailureConfigs.stream()
                .flatMap(map -> map.entrySet().stream())
                .forEach(entry -> addModelsAndPipelines(entry.getKey(), pipelineId, entry.getValue(), handler, level + 1));
        }
    }

    private record PipelineAndModel(String pipelineId, String modelIdOrAlias) {}

    /**
     * A local alternative to ConfigurationUtils.readList(...) that reads list properties out of the processor configuration map,
     * but doesn't rely on mutating the configuration map.
     */
    @SuppressWarnings("unchecked")
    private static List<Map<String, Object>> readList(Map<?, ?> processorConfig, String key) {
        Object val = processorConfig.get(key);
        if (val == null) {
            throw new IllegalArgumentException("Missing required property [" + key + "]");
        }
        return (List<Map<String, Object>>) val;
    }
}
