/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.integration;

import org.apache.http.HttpStatus;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.admin.cluster.node.tasks.list.ListTasksAction;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.rest.ESRestTestCase;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;


public class MlRestTestStateCleaner {

    private final Logger logger;
    private final RestClient adminClient;
    private final ESRestTestCase testCase;

    public MlRestTestStateCleaner(Logger logger, RestClient adminClient, ESRestTestCase testCase) {
        this.logger = logger;
        this.adminClient = adminClient;
        this.testCase = testCase;
    }

    public void clearMlMetadata() throws Exception {
        deleteAllDatafeeds();
        deleteAllJobs();
        waitForPendingTasks();
        // indices will be deleted by the ESIntegTestCase class
    }

    private void waitForPendingTasks() throws Exception {
        ESTestCase.assertBusy(() -> {
            try {
                Response response = adminClient.performRequest("GET", "/_cat/tasks",
                        Collections.singletonMap("detailed", "true"));
                // Check to see if there are tasks still active. We exclude the
                // list tasks
                // actions tasks form this otherwise we will always fail
                if (response.getStatusLine().getStatusCode() == HttpStatus.SC_OK) {
                    try (BufferedReader responseReader = new BufferedReader(
                            new InputStreamReader(response.getEntity().getContent(), StandardCharsets.UTF_8))) {
                        int activeTasks = 0;
                        String line;
                        StringBuilder tasksListString = new StringBuilder();
                        while ((line = responseReader.readLine()) != null) {
                            if (line.startsWith(ListTasksAction.NAME) == false) {
                                activeTasks++;
                                tasksListString.append(line);
                                tasksListString.append('\n');
                            }
                        }
                        assertEquals(activeTasks + " active tasks found:\n" + tasksListString, 0, activeTasks);
                    }
                }
            } catch (IOException e) {
                throw new AssertionError("Error getting active tasks list", e);
            }
        });
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
        Map<String, Object> clusterStateAsMap = testCase.entityAsMap(adminClient.performRequest("GET", "/_cluster/state",
                Collections.singletonMap("filter_path", "metadata.ml.jobs")));
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> jobConfigs =
                (List<Map<String, Object>>) XContentMapValues.extractValue("metadata.ml.jobs", clusterStateAsMap);
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
