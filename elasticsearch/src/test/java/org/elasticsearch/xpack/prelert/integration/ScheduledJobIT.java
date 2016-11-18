/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.prelert.integration;

import org.apache.http.HttpHost;
import org.apache.http.entity.StringEntity;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.RestClient;
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
import java.util.concurrent.TimeUnit;
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
        createNonScheduledJob();

        ResponseException e = expectThrows(ResponseException.class,
                () -> client().performRequest("post", PrelertPlugin.BASE_PATH + "schedulers/non-scheduled/_start"));
        assertThat(e.getResponse().getStatusLine().getStatusCode(), equalTo(400));
        String responseAsString = responseEntityToString(e.getResponse());
        assertThat(responseAsString, containsString("\"reason\":\"There is no job 'non-scheduled' with a scheduler configured\""));
    }

    @AwaitsFix(bugUrl = "The lookback is sometimes too quick and then we fail to see that the scheduler_state to see is STARTED. " +
            "We need to find a different way to assert this.")
    public void testStartJobScheduler_GivenLookbackOnly() throws Exception {
        createAirlineDataIndex();
        createScheduledJob();

        Response response = client().performRequest("post",
                PrelertPlugin.BASE_PATH + "schedulers/scheduled/_start?start=2016-06-01T00:00:00Z&end=2016-06-02T00:00:00Z");
        assertThat(response.getStatusLine().getStatusCode(), equalTo(200));
        assertThat(responseEntityToString(response), equalTo("{\"acknowledged\":true}"));

        assertBusy(() -> {
            try {
                Response response2 = client().performRequest("get", "/_cluster/state",
                        Collections.singletonMap("filter_path", "metadata.prelert.allocations.scheduler_state"));
                assertThat(responseEntityToString(response2), containsString("\"status\":\"STARTED\""));
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });

        waitForSchedulerToBeStopped();
    }

    public void testStartJobScheduler_GivenRealtime() throws Exception {
        createAirlineDataIndex();
        createScheduledJob();

        Response response = client().performRequest("post",
                PrelertPlugin.BASE_PATH + "schedulers/scheduled/_start?start=2016-06-01T00:00:00Z");
        assertThat(response.getStatusLine().getStatusCode(), equalTo(200));
        assertThat(responseEntityToString(response), equalTo("{\"acknowledged\":true}"));

        assertBusy(() -> {
            try {
                Response response2 = client().performRequest("get", "/_cluster/state",
                        Collections.singletonMap("filter_path", "metadata.prelert.allocations.scheduler_state"));
                assertThat(responseEntityToString(response2), containsString("\"status\":\"STARTED\""));
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });

        response = client().performRequest("post", PrelertPlugin.BASE_PATH + "schedulers/scheduled/_stop");
        assertThat(response.getStatusLine().getStatusCode(), equalTo(200));
        assertThat(responseEntityToString(response), equalTo("{\"acknowledged\":true}"));

        waitForSchedulerToBeStopped();

    }

    private void createAirlineDataIndex() throws Exception {
        String airlineDataMappings = "{" + "  \"mappings\": {" + "    \"response\": {" + "      \"properties\": {"
                + "        \"time\": { \"type\":\"date\"}," + "        \"airline\": { \"type\":\"keyword\"},"
                + "        \"responsetime\": { \"type\":\"float\"}" + "      }" + "    }" + "  }" + "}";
        client().performRequest("put", "airline-data", Collections.emptyMap(), new StringEntity(airlineDataMappings));

        client().performRequest("put", "airline-data/response/1", Collections.emptyMap(),
                new StringEntity("{\"time\":\"2016-10-01T00:00:00Z\",\"airline\":\"AAA\",\"responsetime\":135.22}"));
        client().performRequest("put", "airline-data/response/2", Collections.emptyMap(),
                new StringEntity("{\"time\":\"2016-10-01T01:59:00Z\",\"airline\":\"AAA\",\"responsetime\":541.76}"));

        client().performRequest("post", "airline-data/_refresh");
    }

    private Response createNonScheduledJob() throws Exception {
        String job = "{\n" + "    \"jobId\":\"non-scheduled\",\n" + "    \"description\":\"Analysis of response time by airline\",\n"
                + "    \"analysisConfig\" : {\n" + "        \"bucketSpan\":3600,\n"
                + "        \"detectors\" :[{\"function\":\"mean\",\"fieldName\":\"responsetime\",\"byFieldName\":\"airline\"}]\n"
                + "    },\n" + "    \"dataDescription\" : {\n" + "        \"fieldDelimiter\":\",\",\n" + "        \"timeField\":\"time\",\n"
                + "        \"timeFormat\":\"yyyy-MM-dd'T'HH:mm:ssX\"\n" + "    }\n" + "}";

        return client().performRequest("put", PrelertPlugin.BASE_PATH + "jobs", Collections.emptyMap(), new StringEntity(job));
    }

    private Response createScheduledJob() throws Exception {
        HttpHost httpHost = getClusterHosts().get(0);
        String job = "{\n" + "    \"jobId\":\"scheduled\",\n" + "    \"description\":\"Analysis of response time by airline\",\n"
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

    private void waitForSchedulerToBeStopped() throws Exception {
        assertBusy(() -> {
            try {
                Response response = client().performRequest("get", "/_cluster/state",
                        Collections.singletonMap("filter_path", "metadata.prelert.allocations.scheduler_state"));
                assertThat(responseEntityToString(response), containsString("\"status\":\"STOPPED\""));
            } catch (Exception e) {
                fail();
            }
        }, 1500, TimeUnit.MILLISECONDS);
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
