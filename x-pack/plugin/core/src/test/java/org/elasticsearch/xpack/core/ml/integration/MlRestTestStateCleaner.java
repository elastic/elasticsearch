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
import org.elasticsearch.test.rest.ESRestTestCase;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;

public class MlRestTestStateCleaner {

    private final Logger logger;
    private final RestClient adminClient;

    public MlRestTestStateCleaner(Logger logger, RestClient adminClient) {
        this.logger = logger;
        this.adminClient = adminClient;
    }

    public void resetFeatures() throws IOException {
        deletePipelinesWithInferenceProcessors();
        // This resets all features, not just ML, but they should have been getting reset between tests anyway so it shouldn't matter
        adminClient.performRequest(new Request("POST", "/_features/_reset"));
    }

    @SuppressWarnings("unchecked")
    private void deletePipelinesWithInferenceProcessors() throws IOException {
        final Response pipelinesResponse = adminClient.performRequest(new Request("GET", "/_ingest/pipeline"));
        final Map<String, Object> pipelines = ESRestTestCase.entityAsMap(pipelinesResponse);

        var pipelinesWithInferenceProcessors = new HashSet<String>();
        for (var entry : pipelines.entrySet()) {
            var pipelineDef = (Map<String, Object>) entry.getValue(); // each top level object is a separate pipeline
            var processors = (List<Map<String, Object>>) pipelineDef.get("processors");
            for (var processor : processors) {
                assertThat(processor.entrySet(), hasSize(1));
                if ("inference".equals(processor.keySet().iterator().next())) {
                    pipelinesWithInferenceProcessors.add(entry.getKey());
                }
            }
        }

        for (String pipelineId : pipelinesWithInferenceProcessors) {
            try {
                adminClient.performRequest(new Request("DELETE", "/_ingest/pipeline/" + pipelineId));
            } catch (Exception ex) {
                logger.warn(() -> "failed to delete pipeline [" + pipelineId + "]", ex);
            }
        }
    }
}
