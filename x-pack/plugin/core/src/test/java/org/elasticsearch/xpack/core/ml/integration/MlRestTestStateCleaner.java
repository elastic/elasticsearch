/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.integration;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.test.rest.ESRestTestCase;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class MlRestTestStateCleaner {

    private final Logger logger;
    private final RestClient adminClient;

    public MlRestTestStateCleaner(Logger logger, RestClient adminClient) {
        this.logger = logger;
        this.adminClient = adminClient;
    }

    public void resetFeatures() throws IOException {
        deleteAllTrainedModelIngestPipelines();
        // This resets all features, not just ML, but they should have been getting reset between tests anyway so it shouldn't matter
        adminClient.performRequest(new Request("POST", "/_features/_reset"));
    }

    @SuppressWarnings("unchecked")
    private void deleteAllTrainedModelIngestPipelines() throws IOException {
        final Request getAllTrainedModelStats = new Request("GET", "/_ml/trained_models/_stats");
        getAllTrainedModelStats.addParameter("size", "10000");
        final Response trainedModelsStatsResponse = adminClient.performRequest(getAllTrainedModelStats);

        final List<Map<String, Object>> pipelines = (List<Map<String, Object>>) XContentMapValues.extractValue(
            "trained_model_stats.ingest.pipelines",
            ESRestTestCase.entityAsMap(trainedModelsStatsResponse)
        );
        Set<String> pipelineIds = pipelines.stream().flatMap(m -> m.keySet().stream()).collect(Collectors.toSet());
        for (String pipelineId : pipelineIds) {
            try {
                adminClient.performRequest(new Request("DELETE", "/_ingest/pipeline/" + pipelineId));
            } catch (Exception ex) {
                logger.warn(() -> "failed to delete pipeline [" + pipelineId + "]", ex);
            }
        }
    }
}
