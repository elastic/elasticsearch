/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.ingest;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.inference.InferenceResults;
import org.elasticsearch.ingest.IngestMetadata;
import org.elasticsearch.ingest.PipelineConfiguration;
import org.elasticsearch.xpack.core.ml.inference.ModelAliasMetadata;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Shared utilities for discovering model identifiers referenced by ingest pipeline inference processors.
 */
public final class IngestPipelineModelReferences {

    private IngestPipelineModelReferences() {}

    /**
     * Resolve the model identifiers referenced by ingest pipelines for a given project.
     * @param state the cluster state
     * @param projectId the project ID
     * @return the set of model identifiers
     */
    public static Set<String> resolveReferencedModelsForProject(ClusterState state, ProjectId projectId) {
        IngestMetadata ingestMetadata = state.metadata().getProject(projectId).custom(IngestMetadata.TYPE);
        Set<String> referencedKeys = collectReferencedModelKeys(ingestMetadata);
        if (referencedKeys.isEmpty()) {
            return referencedKeys;
        }
        ModelAliasMetadata modelAliasMetadata = modelAliasMetadataForProject(state, projectId);
        Set<String> resolved = new HashSet<>(referencedKeys.size());
        for (String modelKey : referencedKeys) {
            String modelId = modelAliasMetadata.getModelId(modelKey);
            resolved.add(modelId == null ? modelKey : modelId);
        }
        return resolved;
    }

    /**
     * Collect the model identifiers referenced by ingest pipelines from a given ingest metadata.
     * @param ingestMetadata the ingest metadata
     * @return the set of model identifiers
     */
    public static Set<String> collectReferencedModelKeys(IngestMetadata ingestMetadata) {
        Set<String> referencedModelKeys = new HashSet<>();
        if (ingestMetadata == null) {
            return referencedModelKeys;
        }
        ingestMetadata.getPipelines()
            .forEach((pipelineId, pipelineConfiguration) -> collectFromPipeline(pipelineConfiguration, referencedModelKeys));
        return referencedModelKeys;
    }

    private static void collectFromPipeline(PipelineConfiguration pipelineConfiguration, Set<String> referencedModelKeys) {
        Object processors = pipelineConfiguration.getConfig().get("processors");
        if (processors instanceof List<?> processorList) {
            for (Object processor : processorList) {
                collectModelKeyFromProcessor(processor, referencedModelKeys);
            }
        }
    }

    private static void collectModelKeyFromProcessor(Object processor, Set<String> referencedModelKeys) {
        if (processor instanceof Map<?, ?> processorMap) {
            Object processorConfig = processorMap.get(InferenceProcessor.TYPE);
            if (processorConfig instanceof Map<?, ?> inferenceConfig) {
                Object modelId = inferenceConfig.get(InferenceResults.MODEL_ID_RESULTS_FIELD);
                if (modelId != null) {
                    assert modelId instanceof String;
                    referencedModelKeys.add(modelId.toString());
                }
            }
        }
    }

    private static ModelAliasMetadata modelAliasMetadataForProject(ClusterState state, ProjectId projectId) {
        ModelAliasMetadata modelAliasMetadata = state.metadata().getProject(projectId).custom(ModelAliasMetadata.NAME);
        return modelAliasMetadata == null ? ModelAliasMetadata.EMPTY : modelAliasMetadata;
    }
}
