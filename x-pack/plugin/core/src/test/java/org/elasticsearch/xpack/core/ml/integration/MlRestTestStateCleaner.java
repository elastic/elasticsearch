/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.integration;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.test.rest.ESRestTestCase;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class MlRestTestStateCleaner {

    private final Logger logger;
    private final RestClient adminClient;
    private final ESRestTestCase testCase;

    public MlRestTestStateCleaner(Logger logger, RestClient adminClient, ESRestTestCase testCase) {
        this.logger = logger;
        this.adminClient = adminClient;
        this.testCase = testCase;
    }

    public void clearMlMetadata() throws IOException {
        deleteAllDatafeeds();
        deleteAllJobs();
        // indices will be deleted by the ESIntegTestCase class
    }

    @SuppressWarnings("unchecked")
    private void deleteAllDatafeeds() throws IOException {
        final Request datafeedsRequest = new Request("GET", "/_xpack/ml/datafeeds");
        datafeedsRequest.addParameter("filter_path", "datafeeds");
        final Response datafeedsResponse = adminClient.performRequest(datafeedsRequest);
        @SuppressWarnings("unchecked")
        final List<Map<String, Object>> datafeeds =
                (List<Map<String, Object>>) XContentMapValues.extractValue("datafeeds", testCase.entityAsMap(datafeedsResponse));
        if (datafeeds == null) {
            return;
        }

        try {
            int statusCode = adminClient.performRequest("POST", "/_xpack/ml/datafeeds/_all/_stop")
                    .getStatusLine().getStatusCode();
            if (statusCode != 200) {
                logger.error("Got status code " + statusCode + " when stopping datafeeds");
            }
        } catch (Exception e1) {
            logger.warn("failed to stop all datafeeds. Forcing stop", e1);
            try {
                int statusCode = adminClient
                        .performRequest("POST", "/_xpack/ml/datafeeds/_all/_stop?force=true")
                        .getStatusLine().getStatusCode();
                if (statusCode != 200) {
                    logger.error("Got status code " + statusCode + " when stopping datafeeds");
                }
            } catch (Exception e2) {
                logger.warn("Force-closing all data feeds failed", e2);
            }
            throw new RuntimeException(
                    "Had to resort to force-stopping datafeeds, something went wrong?", e1);
        }

        for (Map<String, Object> datafeed : datafeeds) {
            String datafeedId = (String) datafeed.get("datafeed_id");
            int statusCode = adminClient.performRequest("DELETE", "/_xpack/ml/datafeeds/" + datafeedId).getStatusLine().getStatusCode();
            if (statusCode != 200) {
                logger.error("Got status code " + statusCode + " when deleting datafeed " + datafeedId);
            }
        }
    }

    private void deleteAllJobs() throws IOException {
        final Request jobsRequest = new Request("GET", "/_xpack/ml/anomaly_detectors");
        jobsRequest.addParameter("filter_path", "jobs");
        final Response response = adminClient.performRequest(jobsRequest);
        @SuppressWarnings("unchecked")
        final List<Map<String, Object>> jobConfigs =
                (List<Map<String, Object>>) XContentMapValues.extractValue("jobs", testCase.entityAsMap(response));
        if (jobConfigs == null) {
            return;
        }

        try {
            int statusCode = adminClient
                    .performRequest("POST", "/_xpack/ml/anomaly_detectors/_all/_close")
                    .getStatusLine().getStatusCode();
            if (statusCode != 200) {
                logger.error("Got status code " + statusCode + " when closing all jobs");
            }
        } catch (Exception e1) {
            logger.warn("failed to close all jobs. Forcing closed", e1);
            try {
                adminClient.performRequest("POST",
                        "/_xpack/ml/anomaly_detectors/_all/_close?force=true");
            } catch (Exception e2) {
                logger.warn("Force-closing all jobs failed", e2);
            }
            throw new RuntimeException("Had to resort to force-closing jobs, something went wrong?",
                    e1);
        }

        for (Map<String, Object> jobConfig : jobConfigs) {
            String jobId = (String) jobConfig.get("job_id");
            int statusCode = adminClient.performRequest("DELETE", "/_xpack/ml/anomaly_detectors/" + jobId).getStatusLine().getStatusCode();
            if (statusCode != 200) {
                logger.error("Got status code " + statusCode + " when deleting job " + jobId);
            }
        }
    }
}
