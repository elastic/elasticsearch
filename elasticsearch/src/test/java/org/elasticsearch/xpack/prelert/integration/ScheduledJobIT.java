/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.prelert.integration;

import org.apache.http.entity.StringEntity;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xpack.prelert.PrelertPlugin;
import org.junit.After;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class ScheduledJobIT extends ESRestTestCase {

    public void testStartJobScheduler_GivenLookbackOnly() throws Exception {
        String jobId = "job-1";
        createAirlineDataIndex();
        createJob(jobId);
        String schedulerId = "sched-1";
        createScheduler(schedulerId, jobId);
        openJob(client(), jobId);

        Response startSchedulerRequest = client().performRequest("post",
                PrelertPlugin.BASE_PATH + "schedulers/" + schedulerId + "/_start?start=2016-06-01T00:00:00Z&end=2016-06-02T00:00:00Z");
        assertThat(startSchedulerRequest.getStatusLine().getStatusCode(), equalTo(200));
        assertThat(responseEntityToString(startSchedulerRequest), containsString("{\"task\":\""));
        assertBusy(() -> {
            try {
                Response getJobResponse = client().performRequest("get",
                        PrelertPlugin.BASE_PATH + "anomaly_detectors/" + jobId + "/_stats");
                assertThat(responseEntityToString(getJobResponse), containsString("\"input_record_count\":2"));
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }

    public void testStartJobScheduler_GivenRealtime() throws Exception {
        String jobId = "job-2";
        createAirlineDataIndex();
        createJob(jobId);
        String schedulerId = "sched-2";
        createScheduler(schedulerId, jobId);
        openJob(client(), jobId);

        Response response = client().performRequest("post",
                PrelertPlugin.BASE_PATH + "schedulers/" + schedulerId + "/_start?start=2016-06-01T00:00:00Z");
        assertThat(response.getStatusLine().getStatusCode(), equalTo(200));
        assertThat(responseEntityToString(response), containsString("{\"task\":\""));
        assertBusy(() -> {
            try {
                Response getJobResponse = client().performRequest("get",
                        PrelertPlugin.BASE_PATH + "anomaly_detectors/" + jobId + "/_stats");
                String responseAsString = responseEntityToString(getJobResponse);
                assertThat(responseAsString, containsString("\"input_record_count\":2"));
            } catch (Exception e1) {
                throw new RuntimeException(e1);
            }
        });

        ResponseException e = expectThrows(ResponseException.class,
                () -> client().performRequest("delete", PrelertPlugin.BASE_PATH + "anomaly_detectors/" + jobId));
        response = e.getResponse();
        assertThat(response.getStatusLine().getStatusCode(), equalTo(409));
        assertThat(responseEntityToString(response), containsString("Cannot delete job [" + jobId + "] while scheduler [" + schedulerId
                + "] refers to it"));

        response = client().performRequest("post", PrelertPlugin.BASE_PATH + "schedulers/" + schedulerId + "/_stop");
        assertThat(response.getStatusLine().getStatusCode(), equalTo(200));
        assertThat(responseEntityToString(response), equalTo("{\"acknowledged\":true}"));

        client().performRequest("POST", "/_xpack/ml/anomaly_detectors/" + jobId + "/_close");

        response = client().performRequest("delete", PrelertPlugin.BASE_PATH + "schedulers/" + schedulerId);
        assertThat(response.getStatusLine().getStatusCode(), equalTo(200));
        assertThat(responseEntityToString(response), equalTo("{\"acknowledged\":true}"));

        response = client().performRequest("delete", PrelertPlugin.BASE_PATH + "anomaly_detectors/" + jobId);
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

    private Response createJob(String id) throws Exception {
        String job = "{\n" + "    \"job_id\":\"" + id + "\",\n" + "    \"description\":\"Analysis of response time by airline\",\n"
                + "    \"analysis_config\" : {\n" + "        \"bucket_span\":3600,\n"
                + "        \"detectors\" :[{\"function\":\"mean\",\"field_name\":\"responsetime\",\"by_field_name\":\"airline\"}]\n"
                + "    },\n" + "    \"data_description\" : {\n" + "        \"format\":\"ELASTICSEARCH\",\n"
                + "        \"time_field\":\"time\",\n" + "        \"time_format\":\"yyyy-MM-dd'T'HH:mm:ssX\"\n" + "    }\n"
                + "}";

        return client().performRequest("put", PrelertPlugin.BASE_PATH + "anomaly_detectors",
                Collections.emptyMap(), new StringEntity(job));
    }

    private Response createScheduler(String schedulerId, String jobId) throws IOException {
        String schedulerConfig = "{" + "\"job_id\": \"" + jobId + "\",\n" + "\"indexes\":[\"airline-data\"],\n"
                + "\"types\":[\"response\"]\n" + "}";
        return client().performRequest("put", PrelertPlugin.BASE_PATH + "schedulers/" + schedulerId, Collections.emptyMap(),
                new StringEntity(schedulerConfig));
    }

    private static String responseEntityToString(Response response) throws Exception {
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(response.getEntity().getContent(), StandardCharsets.UTF_8))) {
            return reader.lines().collect(Collectors.joining("\n"));
        }
    }

    public static void openJob(RestClient client, String jobId) throws IOException {
        Response response = client.performRequest("post", PrelertPlugin.BASE_PATH + "anomaly_detectors/" + jobId + "/_open");
        assertThat(response.getStatusLine().getStatusCode(), equalTo(200));
    }

    @After
    public void clearPrelertState() throws Exception {
        new PrelertRestTestStateCleaner(client(), this).clearPrelertMetadata();
    }
}
