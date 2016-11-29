/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.prelert.integration;

import org.apache.http.HttpHost;
import org.apache.http.entity.StringEntity;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xpack.prelert.PrelertPlugin;
import org.junit.After;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class ScheduledJobIT extends ESRestTestCase {

    public void testStartJobScheduler_GivenMissingJob() {
        ResponseException e = expectThrows(ResponseException.class,
                () -> client().performRequest("post", PrelertPlugin.BASE_PATH + "schedulers/invalid-job/_start"));
        assertThat(e.getResponse().getStatusLine().getStatusCode(), equalTo(404));
    }

    public void testStartJobScheduler_GivenNonScheduledJob() throws Exception {
        String jobId = "_id1";
        createNonScheduledJob(jobId);

        ResponseException e = expectThrows(ResponseException.class,
                () -> client().performRequest("post", PrelertPlugin.BASE_PATH + "schedulers/" + jobId + "/_start"));
        assertThat(e.getResponse().getStatusLine().getStatusCode(), equalTo(400));
        String responseAsString = responseEntityToString(e.getResponse());
        assertThat(responseAsString, containsString("\"reason\":\"There is no job '" + jobId + "' with a scheduler configured\""));
    }

    public void testStartJobScheduler_GivenLookbackOnly() throws Exception {
        String jobId = "_id2";
        createAirlineDataIndex();
        createScheduledJob(jobId);

        Response startSchedulerRequest = client().performRequest("post",
                PrelertPlugin.BASE_PATH + "schedulers/" + jobId + "/_start?start=2016-06-01T00:00:00Z&end=2016-06-02T00:00:00Z");
        assertThat(startSchedulerRequest.getStatusLine().getStatusCode(), equalTo(200));
        assertThat(responseEntityToString(startSchedulerRequest), equalTo("{\"acknowledged\":true}"));
        waitForSchedulerStartedState(jobId);

        assertBusy(() -> {
            try {
                Response getJobResponse = client().performRequest("get", PrelertPlugin.BASE_PATH + "jobs/" + jobId,
                        Collections.singletonMap("metric", "data_counts"));
                assertThat(responseEntityToString(getJobResponse), containsString("\"input_record_count\":2"));
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });

        waitForSchedulerStoppedState(client(), jobId);
    }

    public void testStartJobScheduler_GivenRealtime() throws Exception {
        String jobId = "_id3";
        createAirlineDataIndex();
        createScheduledJob(jobId);

        Response response = client().performRequest("post",
                PrelertPlugin.BASE_PATH + "schedulers/" + jobId + "/_start?start=2016-06-01T00:00:00Z");
        assertThat(response.getStatusLine().getStatusCode(), equalTo(200));
        assertThat(responseEntityToString(response), equalTo("{\"acknowledged\":true}"));
        waitForSchedulerStartedState(jobId);
        assertBusy(() -> {
            try {
                Response getJobResponse = client().performRequest("get", PrelertPlugin.BASE_PATH + "jobs/" + jobId,
                        Collections.singletonMap("metric", "status,data_counts"));
                String responseAsString = responseEntityToString(getJobResponse);
                assertThat(responseAsString, containsString("\"status\":\"RUNNING\""));
                assertThat(responseAsString, containsString("\"input_record_count\":2"));
            } catch (Exception e1) {
                throw new RuntimeException(e1);
            }
        });

        ResponseException e = expectThrows(ResponseException.class,
                () -> client().performRequest("delete", PrelertPlugin.BASE_PATH + "jobs/" + jobId));
        response = e.getResponse();
        assertThat(response.getStatusLine().getStatusCode(), equalTo(409));
        assertThat(responseEntityToString(response), containsString("Cannot delete job '" + jobId + "' while the scheduler is running"));

        response = client().performRequest("post", PrelertPlugin.BASE_PATH + "schedulers/" + jobId + "/_stop");
        assertThat(response.getStatusLine().getStatusCode(), equalTo(200));
        assertThat(responseEntityToString(response), equalTo("{\"acknowledged\":true}"));
        waitForSchedulerStoppedState(client(), jobId);

        client().performRequest("POST", "/_xpack/prelert/data/" + jobId + "/_close");
        response = client().performRequest("delete", PrelertPlugin.BASE_PATH + "jobs/" + jobId);
        assertThat(response.getStatusLine().getStatusCode(), equalTo(200));
        assertThat(responseEntityToString(response), equalTo("{\"acknowledged\":true}"));
    }

    private void createAirlineDataIndex() throws Exception {
        String airlineDataMappings = "{" + "  \"mappings\": {" + "    \"response\": {" + "      \"properties\": {"
                + "        \"time\": { \"type\":\"date\"}," + "        \"airline\": { \"type\":\"keyword\"},"
                + "        \"responsetime\": { \"type\":\"float\"}" + "      }" + "    }" + "  }" + "}";
        client().performRequest("put", "airline-data", Collections.emptyMap(), new StringEntity(airlineDataMappings));

        client().performRequest("put", "airline-data/response/1", Collections.emptyMap(),
                new StringEntity("{\"time\":\"2016-06-01T00:00:00Z\",\"airline\":\"AAA\",\"responsetime\":135.22}"));
        client().performRequest("put", "airline-data/response/2", Collections.emptyMap(),
                new StringEntity("{\"time\":\"2016-06-01T01:59:00Z\",\"airline\":\"AAA\",\"responsetime\":541.76}"));

        client().performRequest("post", "airline-data/_refresh");
    }

    private Response createNonScheduledJob(String id) throws Exception {
        String job = "{\n" + "    \"jobId\":\"" + id + "\",\n" + "    \"description\":\"Analysis of response time by airline\",\n"
                + "    \"analysisConfig\" : {\n" + "        \"bucketSpan\":3600,\n"
                + "        \"detectors\" :[{\"function\":\"mean\",\"fieldName\":\"responsetime\",\"byFieldName\":\"airline\"}]\n"
                + "    },\n" + "    \"dataDescription\" : {\n" + "        \"fieldDelimiter\":\",\",\n" + "        \"timeField\":\"time\",\n"
                + "        \"timeFormat\":\"yyyy-MM-dd'T'HH:mm:ssX\"\n" + "    }\n" + "}";

        return client().performRequest("put", PrelertPlugin.BASE_PATH + "jobs", Collections.emptyMap(), new StringEntity(job));
    }

    private Response createScheduledJob(String id) throws Exception {
        HttpHost httpHost = getClusterHosts().get(0);
        String job = "{\n" + "    \"jobId\":\"" + id + "\",\n" + "    \"description\":\"Analysis of response time by airline\",\n"
                + "    \"analysisConfig\" : {\n" + "        \"bucketSpan\":3600,\n"
                + "        \"detectors\" :[{\"function\":\"mean\",\"fieldName\":\"responsetime\",\"byFieldName\":\"airline\"}]\n"
                + "    },\n" + "    \"dataDescription\" : {\n" + "        \"format\":\"ELASTICSEARCH\",\n"
                + "        \"timeField\":\"time\",\n" + "        \"timeFormat\":\"yyyy-MM-dd'T'HH:mm:ssX\"\n" + "    },\n"
                + "    \"schedulerConfig\" : {\n" + "        \"dataSource\":\"ELASTICSEARCH\",\n"
                + "        \"baseUrl\":\"" + httpHost.toURI() + "\",\n" + "        \"indexes\":[\"airline-data\"],\n"
                + "        \"types\":[\"response\"],\n" + "        \"retrieveWholeSource\":true\n" + "    }\n" + "}";

        return client().performRequest("put", PrelertPlugin.BASE_PATH + "jobs", Collections.emptyMap(), new StringEntity(job));
    }

    private static String responseEntityToString(Response response) throws Exception {
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(response.getEntity().getContent(), StandardCharsets.UTF_8))) {
            return reader.lines().collect(Collectors.joining("\n"));
        }
    }

    private static void waitForSchedulerStoppedState(RestClient client, String jobId) throws Exception {
        try {
            assertBusy(() -> {
                try {
                    Response getJobResponse = client.performRequest("get", PrelertPlugin.BASE_PATH + "jobs/" + jobId,
                            Collections.singletonMap("metric", "scheduler_state"));
                    assertThat(responseEntityToString(getJobResponse), containsString("\"status\":\"STOPPED\""));
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
        } catch (AssertionError e) {
            Response response = client.performRequest("get", "/_nodes/hotthreads");
            Logger logger = Loggers.getLogger(ScheduledJobIT.class);
            logger.info("hot_threads: {}", responseEntityToString(response));
        }
    }

    private void waitForSchedulerStartedState(String jobId) throws Exception {
        try {
            assertBusy(() -> {
                try {
                    Response getJobResponse = client().performRequest("get", PrelertPlugin.BASE_PATH + "jobs/" + jobId,
                            Collections.singletonMap("metric", "scheduler_state"));
                    assertThat(responseEntityToString(getJobResponse), containsString("\"status\":\"STARTED\""));
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
        } catch (AssertionError e) {
            Response response = client().performRequest("get", "/_nodes/hotthreads");
            logger.info("hot_threads: {}", responseEntityToString(response));
        }
    }

    @After
    public void clearPrelertState() throws IOException {
        clearPrelertMetadata(adminClient());
    }

    public static void clearPrelertMetadata(RestClient client) throws IOException {
        Map<String, Object> clusterStateAsMap = entityAsMap(client.performRequest("GET", "/_cluster/state",
                Collections.singletonMap("filter_path", "metadata.prelert.jobs")));
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> jobConfigs =
                (List<Map<String, Object>>) XContentMapValues.extractValue("metadata.prelert.jobs", clusterStateAsMap);
        if (jobConfigs == null) {
            return;
        }

        for (Map<String, Object> jobConfig : jobConfigs) {
            String jobId = (String) jobConfig.get("jobId");
            try {
                client.performRequest("POST", "/_xpack/prelert/schedulers/" + jobId + "/_stop");
                waitForSchedulerStoppedState(client, jobId);
            } catch (Exception e) {
                // ignore
            }
            try {
                client.performRequest("POST", "/_xpack/prelert/data/" + jobId + "/_close");
            } catch (Exception e) {
                // ignore
            }
            client.performRequest("DELETE", "/_xpack/prelert/jobs/" + jobId);
        }
    }
}
