/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.utils;

import org.apache.lucene.util.Counter;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.ingest.ConfigurationUtils;
import org.elasticsearch.ingest.IngestMetadata;
import org.elasticsearch.ingest.Pipeline;

import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

import static org.elasticsearch.ingest.Pipeline.PROCESSORS_KEY;
import static org.elasticsearch.xpack.core.ml.inference.results.InferenceResults.MODEL_ID_RESULTS_FIELD;
import static org.elasticsearch.xpack.ml.inference.ingest.InferenceProcessor.TYPE;

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
            Map<String, Object> configMap = configuration.getConfigAsMap();
            List<Map<String, Object>> processorConfigs = ConfigurationUtils.readList(null, null, configMap, PROCESSORS_KEY);
            for (Map<String, Object> processorConfigWithKey : processorConfigs) {
                for (Map.Entry<String, Object> entry : processorConfigWithKey.entrySet()) {
                    addModelsAndPipelines(
                        entry.getKey(),
                        pipelineId,
                        (Map<String, Object>) entry.getValue(),
                        pam -> counter.addAndGet(1),
                        0
                    );
                }
            }
        });
        return (int) counter.get();
    }

    /**
     * @param state Current cluster state
     * @return a map from Model or Deployment IDs or Aliases to each pipeline referencing them.
     */
    @SuppressWarnings("unchecked")
    public static Map<String, Set<String>> pipelineIdsByResource(ClusterState state, Set<String> ids) {
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
            Map<String, Object> configMap = configuration.getConfigAsMap();
            List<Map<String, Object>> processorConfigs = ConfigurationUtils.readList(null, null, configMap, PROCESSORS_KEY);
            for (Map<String, Object> processorConfigWithKey : processorConfigs) {
                for (Map.Entry<String, Object> entry : processorConfigWithKey.entrySet()) {
                    addModelsAndPipelines(entry.getKey(), pipelineId, (Map<String, Object>) entry.getValue(), pam -> {
                        if (ids.contains(pam.modelIdOrAlias)) {
                            pipelineIdsByModelIds.computeIfAbsent(pam.modelIdOrAlias, m -> new LinkedHashSet<>()).add(pipelineId);
                        }
                    }, 0);
                }
            }
        });
        return pipelineIdsByModelIds;
    }

    @SuppressWarnings("unchecked")
    private static void addModelsAndPipelines(
        String processorType,
        String pipelineId,
        Map<String, Object> processorDefinition,
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
        if (TYPE.equals(processorType)) {
            String modelId = (String) processorDefinition.get(MODEL_ID_RESULTS_FIELD);
            if (modelId != null) {
                handler.accept(new PipelineAndModel(pipelineId, modelId));
            }
            return;
        }
        if (FOREACH_PROCESSOR_NAME.equals(processorType)) {
            Map<String, Object> innerProcessor = (Map<String, Object>) processorDefinition.get("processor");
            if (innerProcessor != null) {
                // a foreach processor should only have a SINGLE nested processor. Iteration is for simplicity's sake.
                for (Map.Entry<String, Object> innerProcessorWithName : innerProcessor.entrySet()) {
                    addModelsAndPipelines(
                        innerProcessorWithName.getKey(),
                        pipelineId,
                        (Map<String, Object>) innerProcessorWithName.getValue(),
                        handler,
                        level + 1
                    );
                }
            }
            return;
        }
        if (processorDefinition.containsKey(Pipeline.ON_FAILURE_KEY)) {
            List<Map<String, Object>> onFailureConfigs = ConfigurationUtils.readList(
                null,
                null,
                processorDefinition,
                Pipeline.ON_FAILURE_KEY
            );
            onFailureConfigs.stream()
                .flatMap(map -> map.entrySet().stream())
                .forEach(
                    entry -> addModelsAndPipelines(entry.getKey(), pipelineId, (Map<String, Object>) entry.getValue(), handler, level + 1)
                );
        }
    }

    private record PipelineAndModel(String pipelineId, String modelIdOrAlias) {}

}
