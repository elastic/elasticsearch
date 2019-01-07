/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.dataframe.integration;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xpack.core.dataframe.DataFrameField;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

abstract class DataFrameRestTestCase extends ESRestTestCase {

    protected static final String DATAFRAME_ENDPOINT = DataFrameField.REST_BASE_PATH + "jobs/";

    DataFrameRestTestCase() {
        super();
    }

    @SuppressWarnings("unchecked")
    private static List<Map<String, Object>> getDataFrameJobs() throws IOException {
        Response response = adminClient().performRequest(new Request("GET", DATAFRAME_ENDPOINT + "_all"));
        Map<String, Object> jobs = entityAsMap(response);
        List<Map<String, Object>> jobConfigs = (List<Map<String, Object>>) XContentMapValues.extractValue("jobs", jobs);

        return jobConfigs == null ? Collections.emptyList() : jobConfigs;
    }

    protected static String getDataFrameIndexerState(String jobId) throws IOException {
        Response statsResponse = client().performRequest(new Request("GET", DATAFRAME_ENDPOINT + jobId + "/_stats"));

        Map<?, ?> jobStatsAsMap = (Map<?, ?>) ((List<?>) entityAsMap(statsResponse).get("jobs")).get(0);
        return (String) XContentMapValues.extractValue("state.job_state", jobStatsAsMap);
    }

    protected static void wipeDataFrameJobs() throws IOException, InterruptedException {
        List<Map<String, Object>> jobConfigs = getDataFrameJobs();

        for (Map<String, Object> jobConfig : jobConfigs) {
            String jobId = (String) jobConfig.get("id");
            Request request = new Request("POST", DATAFRAME_ENDPOINT + jobId + "/_stop");
            request.addParameter("wait_for_completion", "true");
            request.addParameter("timeout", "10s");
            request.addParameter("ignore", "404");
            adminClient().performRequest(request);
            assertEquals("stopped", getDataFrameIndexerState(jobId));
        }

        for (Map<String, Object> jobConfig : jobConfigs) {
            String jobId = (String) jobConfig.get("id");
            Request request = new Request("DELETE", DATAFRAME_ENDPOINT + jobId);
            request.addParameter("ignore", "404"); // Ignore 404s because they imply someone was racing us to delete this
            adminClient().performRequest(request);
        }

        // jobs should be all gone
        jobConfigs = getDataFrameJobs();
        assertTrue(jobConfigs.isEmpty());
    }

    protected static void waitForPendingDataFrameTasks() throws Exception {
        waitForPendingTasks(adminClient(), taskName -> taskName.startsWith(DataFrameField.TASK_NAME) == false);
    }

    protected static void wipeIndices() throws IOException {
        try {
            adminClient().performRequest(new Request("DELETE", "*"));
        } catch (ResponseException e) {
            // 404 here just means we had no indexes
            if (e.getResponse().getStatusLine().getStatusCode() != 404) {
                throw e;
            }
        }
    }
}