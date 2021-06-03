/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.action.ingest.DeletePipelineRequest;
import org.elasticsearch.client.core.PageParams;
import org.elasticsearch.client.feature.ResetFeaturesRequest;
import org.elasticsearch.client.ml.GetTrainedModelsStatsRequest;

import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Cleans up ML resources created during tests
 */
public class MlTestStateCleaner {

    private final Logger logger;
    private final RestHighLevelClient client;

    public MlTestStateCleaner(Logger logger, RestHighLevelClient client) {
        this.logger = logger;
        this.client = client;
    }

    public void clearMlMetadata() throws IOException {
        deleteAllTrainedModelIngestPipelines();
        // This resets all features, not just ML, but they should have been getting reset between tests anyway so it shouldn't matter
        client.features().resetFeatures(new ResetFeaturesRequest(), RequestOptions.DEFAULT);
    }

    @SuppressWarnings("unchecked")
    private void deleteAllTrainedModelIngestPipelines() throws IOException {
        Set<String> pipelinesWithModels = client.machineLearning().getTrainedModelsStats(
            new GetTrainedModelsStatsRequest("_all").setPageParams(new PageParams(0, 10_000)), RequestOptions.DEFAULT
        ).getTrainedModelStats()
            .stream()
            .flatMap(stats -> {
                Map<String, Object> ingestStats = stats.getIngestStats();
                if (ingestStats == null || ingestStats.isEmpty()) {
                    return Stream.empty();
                }
                Map<String, Object> pipelines = (Map<String, Object>)ingestStats.get("pipelines");
                if (pipelines == null || pipelines.isEmpty()) {
                    return Stream.empty();
                }
                return pipelines.keySet().stream();
            })
            .collect(Collectors.toSet());
        for (String pipelineId : pipelinesWithModels) {
            try {
                client.ingest().deletePipeline(new DeletePipelineRequest(pipelineId), RequestOptions.DEFAULT);
            } catch (Exception ex) {
                logger.warn(() -> new ParameterizedMessage("failed to delete pipeline [{}]", pipelineId), ex);
            }
        }
    }
}
