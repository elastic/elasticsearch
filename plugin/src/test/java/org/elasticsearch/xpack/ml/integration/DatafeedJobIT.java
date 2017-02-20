/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.integration;

import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xpack.ml.MachineLearning;
import org.elasticsearch.xpack.security.authc.support.SecuredString;
import org.junit.After;
import org.junit.Before;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.security.authc.support.UsernamePasswordToken.basicAuthHeaderValue;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class DatafeedJobIT extends ESRestTestCase {


    private static final String BASIC_AUTH_VALUE = basicAuthHeaderValue("elastic", new SecuredString("changeme".toCharArray()));

    @Override
    protected Settings restClientSettings() {
        return Settings.builder().put(ThreadContext.PREFIX + ".Authorization", BASIC_AUTH_VALUE).build();
    }

    @Before
    public void setUpData() throws Exception {
        // Create empty index
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
        client().performRequest("put", "airline-data-empty", Collections.emptyMap(),
                new StringEntity(mappings, ContentType.APPLICATION_JSON));

        // Create index with source = enabled, doc_values = enabled, stored = false
        mappings = "{"
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
        client().performRequest("put", "airline-data", Collections.emptyMap(), new StringEntity(mappings, ContentType.APPLICATION_JSON));

        client().performRequest("put", "airline-data/response/1", Collections.emptyMap(),
                new StringEntity("{\"time stamp\":\"2016-06-01T00:00:00Z\",\"airline\":\"AAA\",\"responsetime\":135.22}",
                        ContentType.APPLICATION_JSON));
        client().performRequest("put", "airline-data/response/2", Collections.emptyMap(),
                new StringEntity("{\"time stamp\":\"2016-06-01T01:59:00Z\",\"airline\":\"AAA\",\"responsetime\":541.76}",
                        ContentType.APPLICATION_JSON));

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
        client().performRequest("put", "airline-data-disabled-doc-values", Collections.emptyMap(),
                new StringEntity(mappings, ContentType.APPLICATION_JSON));

        client().performRequest("put", "airline-data-disabled-doc-values/response/1", Collections.emptyMap(),
                new StringEntity("{\"time stamp\":\"2016-06-01T00:00:00Z\",\"airline\":\"AAA\",\"responsetime\":135.22}",
                        ContentType.APPLICATION_JSON));
        client().performRequest("put", "airline-data-disabled-doc-values/response/2", Collections.emptyMap(),
                new StringEntity("{\"time stamp\":\"2016-06-01T01:59:00Z\",\"airline\":\"AAA\",\"responsetime\":541.76}",
                        ContentType.APPLICATION_JSON));

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
        client().performRequest("put", "airline-data-disabled-source", Collections.emptyMap(),
                new StringEntity(mappings, ContentType.APPLICATION_JSON));

        client().performRequest("put", "airline-data-disabled-source/response/1", Collections.emptyMap(),
                new StringEntity("{\"time stamp\":\"2016-06-01T00:00:00Z\",\"airline\":\"AAA\",\"responsetime\":135.22}",
                        ContentType.APPLICATION_JSON));
        client().performRequest("put", "airline-data-disabled-source/response/2", Collections.emptyMap(),
                new StringEntity("{\"time stamp\":\"2016-06-01T01:59:00Z\",\"airline\":\"AAA\",\"responsetime\":541.76}",
                        ContentType.APPLICATION_JSON));

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
        client().performRequest("put", "nested-data", Collections.emptyMap(), new StringEntity(mappings, ContentType.APPLICATION_JSON));

        client().performRequest("put", "nested-data/response/1", Collections.emptyMap(),
                new StringEntity("{\"time\":\"2016-06-01T00:00:00Z\", \"responsetime\":{\"millis\":135.22}}",
                        ContentType.APPLICATION_JSON));
        client().performRequest("put", "nested-data/response/2", Collections.emptyMap(),
                new StringEntity("{\"time\":\"2016-06-01T01:59:00Z\",\"responsetime\":{\"millis\":222.0}}",
                        ContentType.APPLICATION_JSON));

        // Create index with multiple docs per time interval for aggregation testing
        mappings = "{"
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
        client().performRequest("put", "airline-data-aggs", Collections.emptyMap(),
                new StringEntity(mappings, ContentType.APPLICATION_JSON));

        client().performRequest("put", "airline-data-aggs/response/1", Collections.emptyMap(),
                new StringEntity("{\"time stamp\":\"2016-06-01T00:00:00Z\",\"airline\":\"AAA\",\"responsetime\":100.0}",
                        ContentType.APPLICATION_JSON));
        client().performRequest("put", "airline-data-aggs/response/2", Collections.emptyMap(),
                new StringEntity("{\"time stamp\":\"2016-06-01T00:01:00Z\",\"airline\":\"AAA\",\"responsetime\":200.0}",
                        ContentType.APPLICATION_JSON));
        client().performRequest("put", "airline-data-aggs/response/3", Collections.emptyMap(),
                new StringEntity("{\"time stamp\":\"2016-06-01T00:00:00Z\",\"airline\":\"BBB\",\"responsetime\":1000.0}",
                        ContentType.APPLICATION_JSON));
        client().performRequest("put", "airline-data-aggs/response/4", Collections.emptyMap(),
                new StringEntity("{\"time stamp\":\"2016-06-01T00:01:00Z\",\"airline\":\"BBB\",\"responsetime\":2000.0}",
                        ContentType.APPLICATION_JSON));
        client().performRequest("put", "airline-data-aggs/response/5", Collections.emptyMap(),
                new StringEntity("{\"time stamp\":\"2016-06-01T01:00:00Z\",\"airline\":\"AAA\",\"responsetime\":300.0}",
                        ContentType.APPLICATION_JSON));
        client().performRequest("put", "airline-data-aggs/response/6", Collections.emptyMap(),
                new StringEntity("{\"time stamp\":\"2016-06-01T01:01:00Z\",\"airline\":\"AAA\",\"responsetime\":400.0}",
                        ContentType.APPLICATION_JSON));
        client().performRequest("put", "airline-data-aggs/response/7", Collections.emptyMap(),
                new StringEntity("{\"time stamp\":\"2016-06-01T01:00:00Z\",\"airline\":\"BBB\",\"responsetime\":3000.0}",
                        ContentType.APPLICATION_JSON));
        client().performRequest("put", "airline-data-aggs/response/8", Collections.emptyMap(),
                new StringEntity("{\"time stamp\":\"2016-06-01T01:01:00Z\",\"airline\":\"BBB\",\"responsetime\":4000.0}",
                        ContentType.APPLICATION_JSON));

        // Ensure all data is searchable
        client().performRequest("post", "_refresh");
    }

    public void testLookbackOnly() throws Exception {
        new LookbackOnlyTestHelper("lookback-1", "airline-data").setShouldSucceedProcessing(true).execute();
    }

    public void testLookbackOnlyWithDatafeedSourceEnabled() throws Exception {
        new LookbackOnlyTestHelper("lookback-2", "airline-data").setEnableDatafeedSource(true).execute();
    }

    public void testLookbackOnlyWithDocValuesDisabledAndDatafeedSourceDisabled() throws Exception {
        new LookbackOnlyTestHelper("lookback-3", "airline-data-disabled-doc-values").setShouldSucceedInput(false)
                .setShouldSucceedProcessing(false).execute();
    }

    public void testLookbackOnlyWithDocValuesDisabledAndDatafeedSourceEnabled() throws Exception {
        new LookbackOnlyTestHelper("lookback-4", "airline-data-disabled-doc-values").setEnableDatafeedSource(true).execute();
    }

    public void testLookbackOnlyWithSourceDisabled() throws Exception {
        new LookbackOnlyTestHelper("lookback-5", "airline-data-disabled-source").execute();
    }

    @AwaitsFix(bugUrl = "This test uses painless which is not available in the integTest phase")
    public void testLookbackOnlyWithScriptFields() throws Exception {
        new LookbackOnlyTestHelper("lookback-6", "airline-data-disabled-source").setAddScriptedFields(true).execute();
    }

    public void testLookbackOnlyWithNestedFieldsAndDatafeedSourceDisabled() throws Exception {
        executeTestLookbackOnlyWithNestedFields("lookback-7", false);
    }

    public void testLookbackOnlyWithNestedFieldsAndDatafeedSourceEnabled() throws Exception {
        executeTestLookbackOnlyWithNestedFields("lookback-8", true);
    }

    public void testLookbackOnlyGivenEmptyIndex() throws Exception {
        new LookbackOnlyTestHelper("lookback-9", "airline-data-empty").setShouldSucceedInput(false).setShouldSucceedProcessing(false)
                .execute();
    }

    public void testLookbackOnlyGivenAggregationsWithHistogram() throws Exception {
        String jobId = "aggs-histogram-job";
        String job = "{\"description\":\"Aggs job\",\"analysis_config\" :{\"bucket_span\":3600,\"summary_count_field_name\":\"doc_count\","
                + "\"detectors\":[{\"function\":\"mean\",\"field_name\":\"responsetime\",\"by_field_name\":\"airline\"}]},"
                + "\"data_description\" : {\"time_field\":\"time stamp\"}"
                + "}";
        client().performRequest("put", MachineLearning.BASE_PATH + "anomaly_detectors/" + jobId, Collections.emptyMap(),
                new StringEntity(job, ContentType.APPLICATION_JSON));

        String datafeedId = "datafeed-" + jobId;
        String aggregations = "{\"time stamp\":{\"histogram\":{\"field\":\"time stamp\",\"interval\":3600000},"
                + "\"aggregations\":{\"airline\":{\"terms\":{\"field\":\"airline\",\"size\":10},"
                + "\"aggregations\":{\"responsetime\":{\"avg\":{\"field\":\"responsetime\"}}}}}}}";
        new DatafeedBuilder(datafeedId, jobId, "airline-data-aggs", "response").setAggregations(aggregations).build();
        openJob(client(), jobId);

        startDatafeedAndWaitUntilStopped(datafeedId);
        waitUntilJobIsClosed(jobId);
        Response jobStatsResponse = client().performRequest("get", MachineLearning.BASE_PATH + "anomaly_detectors/" + jobId + "/_stats");
        String jobStatsResponseAsString = responseEntityToString(jobStatsResponse);
        assertThat(jobStatsResponseAsString, containsString("\"input_record_count\":4"));
        assertThat(jobStatsResponseAsString, containsString("\"processed_record_count\":4"));
        assertThat(jobStatsResponseAsString, containsString("\"missing_field_count\":0"));
    }

    public void testLookbackOnlyGivenAggregationsWithDateHistogram() throws Exception {
        String jobId = "aggs-date-histogram-job";
        String job = "{\"description\":\"Aggs job\",\"analysis_config\" :{\"bucket_span\":3600,\"summary_count_field_name\":\"doc_count\","
                + "\"detectors\":[{\"function\":\"mean\",\"field_name\":\"responsetime\",\"by_field_name\":\"airline\"}]},"
                + "\"data_description\" : {\"time_field\":\"time stamp\"}"
                + "}";
        client().performRequest("put", MachineLearning.BASE_PATH + "anomaly_detectors/" + jobId, Collections.emptyMap(),
                new StringEntity(job, ContentType.APPLICATION_JSON));

        String datafeedId = "datafeed-" + jobId;
        String aggregations = "{\"time stamp\":{\"date_histogram\":{\"field\":\"time stamp\",\"interval\":\"1h\"},"
                + "\"aggregations\":{\"airline\":{\"terms\":{\"field\":\"airline\",\"size\":10},"
                + "\"aggregations\":{\"responsetime\":{\"avg\":{\"field\":\"responsetime\"}}}}}}}";
        new DatafeedBuilder(datafeedId, jobId, "airline-data-aggs", "response").setAggregations(aggregations).build();
        openJob(client(), jobId);

        startDatafeedAndWaitUntilStopped(datafeedId);
        waitUntilJobIsClosed(jobId);
        Response jobStatsResponse = client().performRequest("get", MachineLearning.BASE_PATH + "anomaly_detectors/" + jobId + "/_stats");
        String jobStatsResponseAsString = responseEntityToString(jobStatsResponse);
        assertThat(jobStatsResponseAsString, containsString("\"input_record_count\":4"));
        assertThat(jobStatsResponseAsString, containsString("\"processed_record_count\":4"));
        assertThat(jobStatsResponseAsString, containsString("\"missing_field_count\":0"));
    }

    public void testRealtime() throws Exception {
        String jobId = "job-realtime-1";
        createJob(jobId);
        String datafeedId = jobId + "-datafeed";
        new DatafeedBuilder(datafeedId, jobId, "airline-data", "response").build();
        openJob(client(), jobId);

        Response response = client().performRequest("post",
                MachineLearning.BASE_PATH + "datafeeds/" + datafeedId + "/_start?start=2016-06-01T00:00:00Z");
        assertThat(response.getStatusLine().getStatusCode(), equalTo(200));
        assertThat(responseEntityToString(response), equalTo("{\"started\":true}"));
        assertBusy(() -> {
            try {
                Response getJobResponse = client().performRequest("get",
                        MachineLearning.BASE_PATH + "anomaly_detectors/" + jobId + "/_stats");
                String responseAsString = responseEntityToString(getJobResponse);
                assertThat(responseAsString, containsString("\"processed_record_count\":2"));
                assertThat(responseAsString, containsString("\"state\":\"opened\""));
            } catch (Exception e1) {
                throw new RuntimeException(e1);
            }
        });

        ResponseException e = expectThrows(ResponseException.class,
                () -> client().performRequest("delete", MachineLearning.BASE_PATH + "anomaly_detectors/" + jobId));
        response = e.getResponse();
        assertThat(response.getStatusLine().getStatusCode(), equalTo(409));
        assertThat(responseEntityToString(response), containsString("Cannot delete job [" + jobId + "] while datafeed [" + datafeedId
                + "] refers to it"));

        response = client().performRequest("post", MachineLearning.BASE_PATH + "datafeeds/" + datafeedId + "/_stop");
        assertThat(response.getStatusLine().getStatusCode(), equalTo(200));
        assertThat(responseEntityToString(response), equalTo("{\"acknowledged\":true}"));

        client().performRequest("POST", "/_xpack/ml/anomaly_detectors/" + jobId + "/_close");

        response = client().performRequest("delete", MachineLearning.BASE_PATH + "datafeeds/" + datafeedId);
        assertThat(response.getStatusLine().getStatusCode(), equalTo(200));
        assertThat(responseEntityToString(response), equalTo("{\"acknowledged\":true}"));

        response = client().performRequest("delete", MachineLearning.BASE_PATH + "anomaly_detectors/" + jobId);
        assertThat(response.getStatusLine().getStatusCode(), equalTo(200));
        assertThat(responseEntityToString(response), equalTo("{\"acknowledged\":true}"));
    }

    private class LookbackOnlyTestHelper {
        private String jobId;
        private String dataIndex;
        private boolean addScriptedFields;
        private boolean enableDatafeedSource;
        private boolean shouldSucceedInput;
        private boolean shouldSucceedProcessing;

        LookbackOnlyTestHelper(String jobId, String dataIndex) {
            this.jobId = jobId;
            this.dataIndex = dataIndex;
            this.shouldSucceedInput = true;
            this.shouldSucceedProcessing = true;
        }

        public LookbackOnlyTestHelper setAddScriptedFields(boolean value) {
            addScriptedFields = value;
            return this;
        }

        public LookbackOnlyTestHelper setEnableDatafeedSource(boolean value) {
            enableDatafeedSource = value;
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
            String datafeedId = "datafeed-" + jobId;
            new DatafeedBuilder(datafeedId, jobId, dataIndex, "response")
                    .setSource(enableDatafeedSource)
                    .setScriptedFields(addScriptedFields ?
                            "{\"airline\":{\"script\":{\"lang\":\"painless\",\"inline\":\"doc['airline'].value\"}}}" : null)
                    .build();
            openJob(client(), jobId);

            startDatafeedAndWaitUntilStopped(datafeedId);
            waitUntilJobIsClosed(jobId);

            Response jobStatsResponse = client().performRequest("get",
                    MachineLearning.BASE_PATH + "anomaly_detectors/" + jobId + "/_stats");
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

    private void startDatafeedAndWaitUntilStopped(String datafeedId) throws Exception {
        Response startDatafeedRequest = client().performRequest("post",
                MachineLearning.BASE_PATH + "datafeeds/" + datafeedId + "/_start?start=2016-06-01T00:00:00Z&end=2016-06-02T00:00:00Z");
        assertThat(startDatafeedRequest.getStatusLine().getStatusCode(), equalTo(200));
        assertThat(responseEntityToString(startDatafeedRequest), equalTo("{\"started\":true}"));
        assertBusy(() -> {
            try {
                Response datafeedStatsResponse = client().performRequest("get",
                        MachineLearning.BASE_PATH + "datafeeds/" + datafeedId + "/_stats");
                assertThat(responseEntityToString(datafeedStatsResponse), containsString("\"state\":\"stopped\""));
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }

    private void waitUntilJobIsClosed(String jobId) throws Exception {
        assertBusy(() -> {
            try {
                Response jobStatsResponse = client().performRequest("get",
                        MachineLearning.BASE_PATH + "anomaly_detectors/" + jobId + "/_stats");
                assertThat(responseEntityToString(jobStatsResponse), containsString("\"state\":\"closed\""));
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
        return client().performRequest("put", MachineLearning.BASE_PATH + "anomaly_detectors/" + id,
                Collections.emptyMap(), new StringEntity(job, ContentType.APPLICATION_JSON));
    }

    private static String responseEntityToString(Response response) throws Exception {
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(response.getEntity().getContent(), StandardCharsets.UTF_8))) {
            return reader.lines().collect(Collectors.joining("\n"));
        }
    }

    public static void openJob(RestClient client, String jobId) throws IOException {
        Response response = client.performRequest("post", MachineLearning.BASE_PATH + "anomaly_detectors/" + jobId + "/_open");
        assertThat(response.getStatusLine().getStatusCode(), equalTo(200));
    }

    private void executeTestLookbackOnlyWithNestedFields(String jobId, boolean source) throws Exception {
        String job = "{\"description\":\"Nested job\", \"analysis_config\" : {\"bucket_span\":3600,\"detectors\" :"
                + "[{\"function\":\"mean\",\"field_name\":\"responsetime.millis\"}]}, \"data_description\" : {\"time_field\":\"time\"}"
                + "}";
        client().performRequest("put", MachineLearning.BASE_PATH + "anomaly_detectors/" + jobId, Collections.emptyMap(),
                new StringEntity(job, ContentType.APPLICATION_JSON));

        String datafeedId = jobId + "-datafeed";
        new DatafeedBuilder(datafeedId, jobId, "nested-data", "response").setSource(source).build();
        openJob(client(), jobId);

        startDatafeedAndWaitUntilStopped(datafeedId);
        waitUntilJobIsClosed(jobId);
        Response jobStatsResponse = client().performRequest("get", MachineLearning.BASE_PATH + "anomaly_detectors/" + jobId + "/_stats");
        String jobStatsResponseAsString = responseEntityToString(jobStatsResponse);
        assertThat(jobStatsResponseAsString, containsString("\"input_record_count\":2"));
        assertThat(jobStatsResponseAsString, containsString("\"processed_record_count\":2"));
        assertThat(jobStatsResponseAsString, containsString("\"missing_field_count\":0"));
    }

    @After
    public void clearMlState() throws Exception {
        new MlRestTestStateCleaner(logger, client(), this).clearMlMetadata();
    }

    private static class DatafeedBuilder {
        String datafeedId;
        String jobId;
        String index;
        String type;
        boolean source;
        String scriptedFields;
        String aggregations;

        DatafeedBuilder(String datafeedId, String jobId, String index, String type) {
            this.datafeedId = datafeedId;
            this.jobId = jobId;
            this.index = index;
            this.type = type;
        }

        DatafeedBuilder setSource(boolean enableSource) {
            this.source = enableSource;
            return this;
        }

        DatafeedBuilder setScriptedFields(String scriptedFields) {
            this.scriptedFields = scriptedFields;
            return this;
        }

        DatafeedBuilder setAggregations(String aggregations) {
            this.aggregations = aggregations;
            return this;
        }

        Response build() throws IOException {
            String datafeedConfig = "{"
                    + "\"job_id\": \"" + jobId + "\",\"indexes\":[\"" + index + "\"],\"types\":[\"" + type + "\"]"
                    + (source ? ",\"_source\":true" : "")
                    + (scriptedFields == null ? "" : ",\"script_fields\":" + scriptedFields)
                    + (aggregations == null ? "" : ",\"aggs\":" + aggregations)
                    + "}";
            return client().performRequest("put", MachineLearning.BASE_PATH + "datafeeds/" + datafeedId, Collections.emptyMap(),
                    new StringEntity(datafeedConfig, ContentType.APPLICATION_JSON));
        }
    }
}
