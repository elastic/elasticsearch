/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.ingest;

import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.SimulateBulkRequest;
import org.elasticsearch.cluster.metadata.ProjectId;

import java.util.HashMap;
import java.util.Map;

/**
 * This is an implementation of IngestService that allows us to substitute pipeline definitions so that users can simulate ingest using
 * pipelines that they define on the fly.
 */
public class SimulateIngestService extends IngestService {
    private final Map<String, Pipeline> pipelineSubstitutions;

    public SimulateIngestService(IngestService ingestService, BulkRequest request) {
        super(ingestService);
        if (request instanceof SimulateBulkRequest simulateBulkRequest) {
            try {
                pipelineSubstitutions = getPipelineSubstitutions(simulateBulkRequest.getPipelineSubstitutions(), ingestService);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        } else {
            throw new IllegalArgumentException("Expecting a SimulateBulkRequest but got " + request.getClass());
        }
    }

    /**
     * This transforms the pipeline substitutions from a SimulateBulkRequest into a new map, where the key is the pipelineId and the
     * value is the Pipeline instance. The Pipeline is created using the Processor.Factories and the ScriptService of the given
     * ingestService.
     * @param rawPipelineSubstitutions The pipeline substitutions map received from a SimulateBulkRequest
     * @param ingestService The ingestService beoing used
     * @return A transformed version of rawPipelineSubstitutions, where the values are Pipeline objects
     * @throws Exception
     */
    private Map<String, Pipeline> getPipelineSubstitutions(
        Map<String, Map<String, Object>> rawPipelineSubstitutions,
        IngestService ingestService
    ) throws Exception {
        Map<String, Pipeline> parsedPipelineSubstitutions = new HashMap<>();
        if (rawPipelineSubstitutions != null) {
            for (Map.Entry<String, Map<String, Object>> entry : rawPipelineSubstitutions.entrySet()) {
                String pipelineId = entry.getKey();
                Pipeline pipeline = Pipeline.create(
                    pipelineId,
                    entry.getValue(),
                    ingestService.getProcessorFactories(),
                    ingestService.getScriptService(),
                    ingestService.getProjectResolver().getProjectId(),
                    (nodeFeature) -> ingestService.getFeatureService()
                        .clusterHasFeature(ingestService.getClusterService().state(), nodeFeature)
                );
                parsedPipelineSubstitutions.put(pipelineId, pipeline);
            }
        }
        return parsedPipelineSubstitutions;
    }

    /**
     * This method returns the Pipeline for the given pipelineId. If a substitute definition of the pipeline has been defined for the
     * current simulate, then that pipeline is returned. Otherwise, the pipeline stored in the cluster state is returned.
     */
    @Override
    public Pipeline getPipeline(String pipelineId) {
        Pipeline pipeline = pipelineSubstitutions.get(pipelineId);
        if (pipeline == null) {
            pipeline = super.getPipeline(pipelineId);
        }
        return pipeline;
    }

    /**
     * This method returns the Pipeline for the given pipelineId. If a substitute definition of the pipeline has been defined for the
     * current simulate, then that pipeline is returned. Otherwise, the pipeline stored in the cluster state is returned.
     */
    @Override
    public Pipeline getPipeline(ProjectId projectId, String id) {
        Pipeline pipeline = pipelineSubstitutions.get(id);
        if (pipeline != null) {
            return pipeline;
        }
        return super.getPipeline(projectId, id);
    }
}
