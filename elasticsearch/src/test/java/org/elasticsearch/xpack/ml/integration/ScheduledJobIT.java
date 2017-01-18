/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.integration;

import org.apache.http.entity.StringEntity;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xpack.ml.MlPlugin;
import org.junit.After;
import org.junit.Before;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class ScheduledJobIT extends ESRestTestCase {

    @Before
    public void setUpData() throws Exception {
        // Create index with source = enabled, doc_values = enabled, stored = false
        String mappings = "{"
                + "  \"mappings\": {"
                + "    \"response\": {"
                + "      \"properties\": {"
                + "        \"time stamp\": { \"type\":\"date\"}," // space in 'time stamp' is intentional
                + "        \"airline\": { \"type\":\"keyword\"},"
                + "        \"responsetime\": { \"type\":\"float\"}"
                + "      }"
                + "    }"
                + "  }"
                + "}";
        client().performRequest("put", "airline-data", Collections.emptyMap(), new StringEntity(mappings));

        client().performRequest("put", "airline-data/response/1", Collections.emptyMap(),
                new StringEntity("{\"time stamp\":\"2016-06-01T00:00:00Z\",\"airline\":\"AAA\",\"responsetime\":135.22}"));
        client().performRequest("put", "airline-data/response/2", Collections.emptyMap(),
                new StringEntity("{\"time stamp\":\"2016-06-01T01:59:00Z\",\"airline\":\"AAA\",\"responsetime\":541.76}"));

        // Create index with source = enabled, doc_values = disabled (except time), stored = false
        mappings = "{"
                + "  \"mappings\": {"
                + "    \"response\": {"
                + "      \"properties\": {"
                + "        \"time stamp\": { \"type\":\"date\"},"
                + "        \"airline\": { \"type\":\"keyword\", \"doc_values\":false},"
                + "        \"responsetime\": { \"type\":\"float\", \"doc_values\":false}"
                + "      }"
                + "    }"
                + "  }"
                + "}";
        client().performRequest("put", "airline-data-disabled-doc-values", Collections.emptyMap(), new StringEntity(mappings));

        client().performRequest("put", "airline-data-disabled-doc-values/response/1", Collections.emptyMap(),
                new StringEntity("{\"time stamp\":\"2016-06-01T00:00:00Z\",\"airline\":\"AAA\",\"responsetime\":135.22}"));
        client().performRequest("put", "airline-data-disabled-doc-values/response/2", Collections.emptyMap(),
                new StringEntity("{\"time stamp\":\"2016-06-01T01:59:00Z\",\"airline\":\"AAA\",\"responsetime\":541.76}"));

        // Create index with source = disabled, doc_values = enabled (except time), stored = true
        mappings = "{"
                + "  \"mappings\": {"
                + "    \"response\": {"
                + "      \"_source\":{\"enabled\":false},"
                + "      \"properties\": {"
                + "        \"time stamp\": { \"type\":\"date\", \"store\":true},"
                + "        \"airline\": { \"type\":\"keyword\", \"store\":true},"
                + "        \"responsetime\": { \"type\":\"float\", \"store\":true}"
                + "      }"
                + "    }"
                + "  }"
                + "}";
        client().performRequest("put", "airline-data-disabled-source", Collections.emptyMap(), new StringEntity(mappings));

        client().performRequest("put", "airline-data-disabled-source/response/1", Collections.emptyMap(),
                new StringEntity("{\"time stamp\":\"2016-06-01T00:00:00Z\",\"airline\":\"AAA\",\"responsetime\":135.22}"));
        client().performRequest("put", "airline-data-disabled-source/response/2", Collections.emptyMap(),
                new StringEntity("{\"time stamp\":\"2016-06-01T01:59:00Z\",\"airline\":\"AAA\",\"responsetime\":541.76}"));

        // Create index with nested documents
        mappings = "{"
                + "  \"mappings\": {"
                + "    \"response\": {"
                + "      \"properties\": {"
                + "        \"time\": { \"type\":\"date\"}"
                + "      }"
                + "    }"
                + "  }"
                + "}";
        client().performRequest("put", "nested-data", Collections.emptyMap(), new StringEntity(mappings));

        client().performRequest("put", "nested-data/response/1", Collections.emptyMap(),
                new StringEntity("{\"time\":\"2016-06-01T00:00:00Z\", \"responsetime\":{\"millis\":135.22}}"));
        client().performRequest("put", "nested-data/response/2", Collections.emptyMap(),
                new StringEntity("{\"time\":\"2016-06-01T01:59:00Z\",\"responsetime\":{\"millis\":222.0}}"));

        // Ensure all data is searchable
        client().performRequest("post", "_refresh");
    }

    public void testLookbackOnly() throws Exception {
        new LookbackOnlyTestHelper("lookback-1", "airline-data").setShouldSucceedProcessing(true).execute();
    }

    public void testLookbackOnlyWithSchedulerSourceEnabled() throws Exception {
        new LookbackOnlyTestHelper("lookback-2", "airline-data").setEnableSchedulerSource(true).execute();
    }

    public void testLookbackOnlyWithDocValuesDisabledAndSchedulerSourceDisabled() throws Exception {
        new LookbackOnlyTestHelper("lookback-3", "airline-data-disabled-doc-values").setShouldSucceedInput(false)
                .setShouldSucceedProcessing(false).execute();
    }

    public void testLookbackOnlyWithDocValuesDisabledAndSchedulerSourceEnabled() throws Exception {
        new LookbackOnlyTestHelper("lookback-4", "airline-data-disabled-doc-values").setEnableSchedulerSource(true).execute();
    }

    public void testLookbackOnlyWithSourceDisabled() throws Exception {
        new LookbackOnlyTestHelper("lookback-5", "airline-data-disabled-source").execute();
    }

    public void testLookbackOnlyWithScriptFields() throws Exception {
        new LookbackOnlyTestHelper("lookback-6", "airline-data-disabled-source").setAddScriptedFields(true).execute();
    }

    public void testLookbackOnlyWithNestedFieldsAndSchedulerSourceDisabled() throws Exception {
        executeTestLookbackOnlyWithNestedFields("lookback-7", false);
    }

    public void testLookbackOnlyWithNestedFieldsAndSchedulerSourceEnabled() throws Exception {
        executeTestLookbackOnlyWithNestedFields("lookback-8", true);
    }

    public void testRealtime() throws Exception {
        String jobId = "job-realtime-1";
        createJob(jobId);
        String schedulerId = jobId + "-scheduler";
        createScheduler(schedulerId, jobId, "airline-data", false, false);
        openJob(client(), jobId);

        Response response = client().performRequest("post",
                MlPlugin.BASE_PATH + "schedulers/" + schedulerId + "/_start?start=2016-06-01T00:00:00Z");
        assertThat(response.getStatusLine().getStatusCode(), equalTo(200));
        assertThat(responseEntityToString(response), containsString("{\"task\":\""));
        assertBusy(() -> {
            try {
                Response getJobResponse = client().performRequest("get",
                        MlPlugin.BASE_PATH + "anomaly_detectors/" + jobId + "/_stats");
                String responseAsString = responseEntityToString(getJobResponse);
                assertThat(responseAsString, containsString("\"processed_record_count\":2"));
            } catch (Exception e1) {
                throw new RuntimeException(e1);
            }
        });

        ResponseException e = expectThrows(ResponseException.class,
                () -> client().performRequest("delete", MlPlugin.BASE_PATH + "anomaly_detectors/" + jobId));
        response = e.getResponse();
        assertThat(response.getStatusLine().getStatusCode(), equalTo(409));
        assertThat(responseEntityToString(response), containsString("Cannot delete job [" + jobId + "] while scheduler [" + schedulerId
                + "] refers to it"));

        response = client().performRequest("post", MlPlugin.BASE_PATH + "schedulers/" + schedulerId + "/_stop");
        assertThat(response.getStatusLine().getStatusCode(), equalTo(200));
        assertThat(responseEntityToString(response), equalTo("{\"acknowledged\":true}"));

        client().performRequest("POST", "/_xpack/ml/anomaly_detectors/" + jobId + "/_close");

        response = client().performRequest("delete", MlPlugin.BASE_PATH + "schedulers/" + schedulerId);
        assertThat(response.getStatusLine().getStatusCode(), equalTo(200));
        assertThat(responseEntityToString(response), equalTo("{\"acknowledged\":true}"));

        response = client().performRequest("delete", MlPlugin.BASE_PATH + "anomaly_detectors/" + jobId);
        assertThat(response.getStatusLine().getStatusCode(), equalTo(200));
        assertThat(responseEntityToString(response), equalTo("{\"acknowledged\":true}"));
    }

    private class LookbackOnlyTestHelper {
        private String jobId;
        private String dataIndex;
        private boolean addScriptedFields;
        private boolean enableSchedulerSource;
        private boolean shouldSucceedInput;
        private boolean shouldSucceedProcessing;

        public LookbackOnlyTestHelper(String jobId, String dataIndex) {
            this.jobId = jobId;
            this.dataIndex = dataIndex;
            this.shouldSucceedInput = true;
            this.shouldSucceedProcessing = true;
        }

        public LookbackOnlyTestHelper setAddScriptedFields(boolean value) {
            addScriptedFields = value;
            return this;
        }

        public LookbackOnlyTestHelper setEnableSchedulerSource(boolean value) {
            enableSchedulerSource = value;
            return this;
        }

        public LookbackOnlyTestHelper setShouldSucceedInput(boolean value) {
            shouldSucceedInput = value;
            return this;
        }

        public LookbackOnlyTestHelper setShouldSucceedProcessing(boolean value) {
            shouldSucceedProcessing = value;
            return this;
        }

        public void execute() throws Exception {
            createJob(jobId);
            String schedulerId = "scheduler-" + jobId;
            createScheduler(schedulerId, jobId, dataIndex, enableSchedulerSource, addScriptedFields);
            openJob(client(), jobId);

            startSchedulerAndWaitUntilStopped(schedulerId);
            Response jobStatsResponse = client().performRequest("get", MlPlugin.BASE_PATH + "anomaly_detectors/" + jobId + "/_stats");
            String jobStatsResponseAsString = responseEntityToString(jobStatsResponse);
            if (shouldSucceedInput) {
                assertThat(jobStatsResponseAsString, containsString("\"input_record_count\":2"));
            } else {
                assertThat(jobStatsResponseAsString, containsString("\"input_record_count\":0"));
            }
            if (shouldSucceedProcessing) {
                assertThat(jobStatsResponseAsString, containsString("\"processed_record_count\":2"));
            } else {
                assertThat(jobStatsResponseAsString, containsString("\"processed_record_count\":0"));
            }
            assertThat(jobStatsResponseAsString, containsString("\"missing_field_count\":0"));
        }
    }

    private void startSchedulerAndWaitUntilStopped(String schedulerId) throws Exception {
        Response startSchedulerRequest = client().performRequest("post",
                MlPlugin.BASE_PATH + "schedulers/" + schedulerId + "/_start?start=2016-06-01T00:00:00Z&end=2016-06-02T00:00:00Z");
        assertThat(startSchedulerRequest.getStatusLine().getStatusCode(), equalTo(200));
        assertThat(responseEntityToString(startSchedulerRequest), containsString("{\"task\":\""));
        assertBusy(() -> {
            try {
                Response schedulerStatsResponse = client().performRequest("get",
                        MlPlugin.BASE_PATH + "schedulers/" + schedulerId + "/_stats");
                assertThat(responseEntityToString(schedulerStatsResponse), containsString("\"status\":\"STOPPED\""));
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }

    private Response createJob(String id) throws Exception {
        String job = "{\n" + "    \"description\":\"Analysis of response time by airline\",\n"
                + "    \"analysis_config\" : {\n" + "        \"bucket_span\":3600,\n"
                + "        \"detectors\" :[{\"function\":\"mean\",\"field_name\":\"responsetime\",\"by_field_name\":\"airline\"}]\n"
                + "    },\n" + "    \"data_description\" : {\n" + "        \"format\":\"JSON\",\n"
                + "        \"time_field\":\"time stamp\",\n" + "        \"time_format\":\"yyyy-MM-dd'T'HH:mm:ssX\"\n" + "    }\n"
                + "}";

        return client().performRequest("put", MlPlugin.BASE_PATH + "anomaly_detectors/" + id,
                Collections.emptyMap(), new StringEntity(job));
    }

    private Response createScheduler(String schedulerId, String jobId, String dataIndex, boolean source, boolean addScriptedFields)
            throws IOException {
        String schedulerConfig = "{" + "\"job_id\": \"" + jobId + "\",\n" + "\"indexes\":[\"" + dataIndex + "\"],\n"
                + "\"types\":[\"response\"]" + (source ? ",\"_source\":true" : "") + (addScriptedFields ?
                        ",\"script_fields\":{\"airline\":{\"script\":{\"lang\":\"painless\",\"inline\":\"doc['airline'].value\"}}}" : "")
                +"}";
        return client().performRequest("put", MlPlugin.BASE_PATH + "schedulers/" + schedulerId, Collections.emptyMap(),
                new StringEntity(schedulerConfig));
    }

    private static String responseEntityToString(Response response) throws Exception {
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(response.getEntity().getContent(), StandardCharsets.UTF_8))) {
            return reader.lines().collect(Collectors.joining("\n"));
        }
    }

    public static void openJob(RestClient client, String jobId) throws IOException {
        Response response = client.performRequest("post", MlPlugin.BASE_PATH + "anomaly_detectors/" + jobId + "/_open");
        assertThat(response.getStatusLine().getStatusCode(), equalTo(200));
    }

    private void executeTestLookbackOnlyWithNestedFields(String jobId, boolean source) throws Exception {
        String job = "{\"description\":\"Nested job\", \"analysis_config\" : {\"bucket_span\":3600,\"detectors\" :"
                + "[{\"function\":\"mean\",\"field_name\":\"responsetime.millis\"}]}, \"data_description\" : {\"time_field\":\"time\"}"
                + "}";
        client().performRequest("put", MlPlugin.BASE_PATH + "anomaly_detectors/" + jobId, Collections.emptyMap(), new StringEntity(job));

        String schedulerId = jobId + "-scheduler";
        createScheduler(schedulerId, jobId, "nested-data", source, false);
        openJob(client(), jobId);

        startSchedulerAndWaitUntilStopped(schedulerId);
        Response jobStatsResponse = client().performRequest("get", MlPlugin.BASE_PATH + "anomaly_detectors/" + jobId + "/_stats");
        String jobStatsResponseAsString = responseEntityToString(jobStatsResponse);
        assertThat(jobStatsResponseAsString, containsString("\"input_record_count\":2"));
        assertThat(jobStatsResponseAsString, containsString("\"processed_record_count\":2"));
        assertThat(jobStatsResponseAsString, containsString("\"missing_field_count\":0"));
    }

    @After
    public void clearMlState() throws Exception {
        new MlRestTestStateCleaner(client(), this).clearMlMetadata();
    }
}
