/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.integration;

import org.apache.logging.log4j.Logger;
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
        deleteDotML();
    }

    @SuppressWarnings("unchecked")
    private void deleteAllDatafeeds() throws IOException {
        Map<String, Object> clusterStateAsMap = testCase.entityAsMap(adminClient.performRequest("GET", "/_cluster/state",
                Collections.singletonMap("filter_path", "metadata.ml.datafeeds")));
        List<Map<String, Object>> datafeeds =
                (List<Map<String, Object>>) XContentMapValues.extractValue("metadata.ml.datafeeds", clusterStateAsMap);
        if (datafeeds == null) {
            return;
        }

        for (Map<String, Object> datafeed : datafeeds) {
            String datafeedId = (String) datafeed.get("datafeed_id");
            try {
                int statusCode = adminClient.performRequest("POST",
                        "/_xpack/ml/datafeeds/" + datafeedId + "/_stop").getStatusLine().getStatusCode();
                if (statusCode != 200) {
                    logger.error("Got status code " + statusCode + " when stopping datafeed " + datafeedId);
                }
            } catch (Exception e) {
                if (e.getMessage().contains("datafeed already stopped, expected datafeed state [started], but got [stopped]")) {
                    logger.debug("failed to stop datafeed [" + datafeedId + "]", e);
                } else {
                    logger.warn("failed to stop datafeed [" + datafeedId + "]", e);
                }
            }
            int statusCode = adminClient.performRequest("DELETE", "/_xpack/ml/datafeeds/" + datafeedId).getStatusLine().getStatusCode();
            if (statusCode != 200) {
                logger.error("Got status code " + statusCode + " when deleting datafeed " + datafeedId);
            }
        }
    }

    private void deleteAllJobs() throws IOException {
        Map<String, Object> clusterStateAsMap = testCase.entityAsMap(adminClient.performRequest("GET", "/_cluster/state",
                Collections.singletonMap("filter_path", "metadata.ml.jobs")));
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> jobConfigs =
                (List<Map<String, Object>>) XContentMapValues.extractValue("metadata.ml.jobs", clusterStateAsMap);
        if (jobConfigs == null) {
            return;
        }

        for (Map<String, Object> jobConfig : jobConfigs) {
            String jobId = (String) jobConfig.get("job_id");
            try {
                int statusCode = adminClient.performRequest("POST",
                        "/_xpack/ml/anomaly_detectors/" + jobId + "/_close").getStatusLine().getStatusCode();
                if (statusCode != 200) {
                    logger.error("Got status code " + statusCode + " when closing job " + jobId);
                }
            } catch (Exception e) {
                if (e.getMessage().contains("cannot close job, expected job state [opened], but got [closed]")
                        || e.getMessage().contains("cannot close job, expected job state [opened], but got [closing]")) {
                    logger.debug("job [" + jobId + "] has already been closed", e);
                } else {
                    logger.warn("failed to close job [" + jobId + "]", e);
                }
            }
            int statusCode = adminClient.performRequest("DELETE", "/_xpack/ml/anomaly_detectors/" + jobId).getStatusLine().getStatusCode();
            if (statusCode != 200) {
                logger.error("Got status code " + statusCode + " when deleting job " + jobId);
            }
        }
    }

    private void deleteDotML() throws IOException {
        int statusCode = adminClient.performRequest("DELETE",  ".ml-*?ignore_unavailable=true").getStatusLine().getStatusCode();
        if (statusCode != 200) {
            logger.error("Got status code " + statusCode + " when deleting .ml-* indexes");
        }
    }
}
