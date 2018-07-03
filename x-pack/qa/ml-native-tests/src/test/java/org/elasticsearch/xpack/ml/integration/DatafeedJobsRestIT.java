/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.integration;

import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.message.BasicHeader;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.SecuritySettingsSourceField;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xpack.core.ml.integration.MlRestTestStateCleaner;
import org.elasticsearch.xpack.ml.MachineLearning;
import org.elasticsearch.xpack.core.ml.notifications.AuditorField;
import org.elasticsearch.xpack.test.rest.XPackRestTestHelper;
import org.junit.After;
import org.junit.Before;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken.basicAuthHeaderValue;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class DatafeedJobsRestIT extends ESRestTestCase {

    private static final String BASIC_AUTH_VALUE_SUPER_USER =
            basicAuthHeaderValue("x_pack_rest_user", SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING);
    private static final String BASIC_AUTH_VALUE_ML_ADMIN =
            basicAuthHeaderValue("ml_admin", SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING);
    private static final String BASIC_AUTH_VALUE_ML_ADMIN_WITH_SOME_DATA_ACCESS =
            basicAuthHeaderValue("ml_admin_plus_data", SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING);

    @Override
    protected Settings restClientSettings() {
        return Settings.builder().put(ThreadContext.PREFIX + ".Authorization", BASIC_AUTH_VALUE_SUPER_USER).build();
    }

    @Override
    protected boolean preserveTemplatesUponCompletion() {
        return true;
    }

    private void setupDataAccessRole(String index) throws IOException {
        String json = "{"
                + "  \"indices\" : ["
                + "    { \"names\": [\"" + index + "\"], \"privileges\": [\"read\"] }"
                + "  ]"
                + "}";

        client().performRequest("put", "_xpack/security/role/test_data_access", Collections.emptyMap(),
                new StringEntity(json, ContentType.APPLICATION_JSON));
    }

    private void setupUser(String user, List<String> roles) throws IOException {
        String password = new String(SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING.getChars());

        String json = "{"
                + "  \"password\" : \"" + password + "\","
                + "  \"roles\" : [ " + roles.stream().map(unquoted -> "\"" + unquoted + "\"").collect(Collectors.joining(", ")) + " ]"
                + "}";

        client().performRequest("put", "_xpack/security/user/" + user, Collections.emptyMap(),
                new StringEntity(json, ContentType.APPLICATION_JSON));
    }

    @Before
    public void setUpData() throws Exception {
        setupDataAccessRole("network-data");
        // This user has admin rights on machine learning, but (importantly for the tests) no rights
        // on any of the data indexes
        setupUser("ml_admin", Collections.singletonList("machine_learning_admin"));
        // This user has admin rights on machine learning, and read access to the network-data index
        setupUser("ml_admin_plus_data", Arrays.asList("machine_learning_admin", "test_data_access"));
        addAirlineData();
        addNetworkData("network-data");
    }

    private void addAirlineData() throws IOException {
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

        // Create index with source = enabled, doc_values = enabled, stored = false + multi-field
        mappings = "{"
                + "  \"mappings\": {"
                + "    \"response\": {"
                + "      \"properties\": {"
                + "        \"time stamp\": { \"type\":\"date\"}," // space in 'time stamp' is intentional
                + "        \"airline\": {"
                + "          \"type\":\"text\","
                + "          \"fields\":{"
                + "            \"text\":{\"type\":\"text\"},"
                + "            \"keyword\":{\"type\":\"keyword\"}"
                + "           }"
                + "         },"
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

    private void addNetworkData(String index) throws IOException {

        // Create index with source = enabled, doc_values = enabled, stored = false + multi-field
        String mappings = "{"
                + "  \"mappings\": {"
                + "    \"doc\": {"
                + "      \"properties\": {"
                + "        \"timestamp\": { \"type\":\"date\"},"
                + "        \"host\": {"
                + "          \"type\":\"text\","
                + "          \"fields\":{"
                + "            \"text\":{\"type\":\"text\"},"
                + "            \"keyword\":{\"type\":\"keyword\"}"
                + "           }"
                + "         },"
                + "        \"network_bytes_out\": { \"type\":\"long\"}"
                + "      }"
                + "    }"
                + "  }"
                + "}";
        client().performRequest("put", index, Collections.emptyMap(), new StringEntity(mappings, ContentType.APPLICATION_JSON));

        String docTemplate = "{\"timestamp\":%d,\"host\":\"%s\",\"network_bytes_out\":%d}";
        Date date = new Date(1464739200735L);
        for (int i=0; i<120; i++) {
            long byteCount = randomNonNegativeLong();
            String jsonDoc = String.format(Locale.ROOT, docTemplate, date.getTime(), "hostA", byteCount);
            client().performRequest("post", index + "/doc", Collections.emptyMap(),
                    new StringEntity(jsonDoc, ContentType.APPLICATION_JSON));

            byteCount = randomNonNegativeLong();
            jsonDoc = String.format(Locale.ROOT, docTemplate, date.getTime(), "hostB", byteCount);
            client().performRequest("post", index + "/doc", Collections.emptyMap(),
                    new StringEntity(jsonDoc, ContentType.APPLICATION_JSON));

            date = new Date(date.getTime() + 10_000);
        }

        // Ensure all data is searchable
        client().performRequest("post", "_refresh");
    }

    public void testLookbackOnlyWithMixedTypes() throws Exception {
        new LookbackOnlyTestHelper("test-lookback-only-with-mixed-types", "airline-data")
                .setShouldSucceedProcessing(true).execute();
    }

    public void testLookbackOnlyWithKeywordMultiField() throws Exception {
        new LookbackOnlyTestHelper("test-lookback-only-with-keyword-multi-field", "airline-data")
                .setAirlineVariant("airline.keyword").setShouldSucceedProcessing(true).execute();
    }

    public void testLookbackOnlyWithTextMultiField() throws Exception {
        new LookbackOnlyTestHelper("test-lookback-only-with-keyword-multi-field", "airline-data")
                .setAirlineVariant("airline.text").setShouldSucceedProcessing(true).execute();
    }

    public void testLookbackOnlyWithDocValuesDisabled() throws Exception {
        new LookbackOnlyTestHelper("test-lookback-only-with-doc-values-disabled", "airline-data-disabled-doc-values").execute();
    }

    public void testLookbackOnlyWithSourceDisabled() throws Exception {
        new LookbackOnlyTestHelper("test-lookback-only-with-source-disabled", "airline-data-disabled-source").execute();
    }

    @AwaitsFix(bugUrl = "This test uses painless which is not available in the integTest phase")
    public void testLookbackOnlyWithScriptFields() throws Exception {
        new LookbackOnlyTestHelper("test-lookback-only-with-script-fields", "airline-data-disabled-source")
                .setAddScriptedFields(true).execute();
    }

    public void testLookbackOnlyWithNestedFields() throws Exception {
        String jobId = "test-lookback-only-with-nested-fields";
        String job = "{\"description\":\"Nested job\", \"analysis_config\" : {\"bucket_span\":\"1h\",\"detectors\" :"
                + "[{\"function\":\"mean\",\"field_name\":\"responsetime.millis\"}]}, \"data_description\" : {\"time_field\":\"time\"}"
                + "}";
        client().performRequest("put", MachineLearning.BASE_PATH + "anomaly_detectors/" + jobId, Collections.emptyMap(),
                new StringEntity(job, ContentType.APPLICATION_JSON));

        String datafeedId = jobId + "-datafeed";
        new DatafeedBuilder(datafeedId, jobId, "nested-data", "response").build();
        openJob(client(), jobId);

        startDatafeedAndWaitUntilStopped(datafeedId);
        waitUntilJobIsClosed(jobId);
        Response jobStatsResponse = client().performRequest("get", MachineLearning.BASE_PATH + "anomaly_detectors/" + jobId + "/_stats");
        String jobStatsResponseAsString = responseEntityToString(jobStatsResponse);
        assertThat(jobStatsResponseAsString, containsString("\"input_record_count\":2"));
        assertThat(jobStatsResponseAsString, containsString("\"processed_record_count\":2"));
        assertThat(jobStatsResponseAsString, containsString("\"missing_field_count\":0"));
    }

    public void testLookbackOnlyGivenEmptyIndex() throws Exception {
        new LookbackOnlyTestHelper("test-lookback-only-given-empty-index", "airline-data-empty")
                .setShouldSucceedInput(false).setShouldSucceedProcessing(false).execute();
    }

    public void testInsufficientSearchPrivilegesOnPut() throws Exception {
        String jobId = "privs-put-job";
        String job = "{\"description\":\"Aggs job\",\"analysis_config\" :{\"bucket_span\":\"1h\","
                + "\"summary_count_field_name\":\"doc_count\","
                + "\"detectors\":[{\"function\":\"mean\","
                + "\"field_name\":\"responsetime\",\"by_field_name\":\"airline\"}]},"
                + "\"data_description\" : {\"time_field\":\"time stamp\"}"
                + "}";
        client().performRequest("put", MachineLearning.BASE_PATH + "anomaly_detectors/" + jobId,
                Collections.emptyMap(), new StringEntity(job, ContentType.APPLICATION_JSON));

        String datafeedId = "datafeed-" + jobId;
        // This should be disallowed, because even though the ml_admin user has permission to
        // create a datafeed they DON'T have permission to search the index the datafeed is
        // configured to read
        ResponseException e = expectThrows(ResponseException.class, () ->
                new DatafeedBuilder(datafeedId, jobId, "airline-data-aggs", "response")
                        .setAuthHeader(BASIC_AUTH_VALUE_ML_ADMIN)
                        .build());

        assertThat(e.getMessage(), containsString("Cannot create datafeed"));
        assertThat(e.getMessage(),
                containsString("user ml_admin lacks permissions on the indices to be searched"));
    }

    public void testInsufficientSearchPrivilegesOnPreview() throws Exception {
        String jobId = "privs-preview-job";
        String job = "{\"description\":\"Aggs job\",\"analysis_config\" :{\"bucket_span\":\"1h\","
                + "\"summary_count_field_name\":\"doc_count\","
                + "\"detectors\":[{\"function\":\"mean\","
                + "\"field_name\":\"responsetime\",\"by_field_name\":\"airline\"}]},"
                + "\"data_description\" : {\"time_field\":\"time stamp\"}"
                + "}";
        client().performRequest("put", MachineLearning.BASE_PATH + "anomaly_detectors/" + jobId,
                Collections.emptyMap(), new StringEntity(job, ContentType.APPLICATION_JSON));

        String datafeedId = "datafeed-" + jobId;
        new DatafeedBuilder(datafeedId, jobId, "airline-data-aggs", "response").build();

        // This should be disallowed, because ml_admin is trying to preview a datafeed created by
        // by another user (x_pack_rest_user in this case) that will reveal the content of an index they
        // don't have permission to search directly
        ResponseException e = expectThrows(ResponseException.class, () ->
                client().performRequest("get",
                        MachineLearning.BASE_PATH + "datafeeds/" + datafeedId + "/_preview",
                        new BasicHeader("Authorization", BASIC_AUTH_VALUE_ML_ADMIN)));

        assertThat(e.getMessage(),
                containsString("[indices:data/read/field_caps] is unauthorized for user [ml_admin]"));
    }

    public void testLookbackOnlyGivenAggregationsWithHistogram() throws Exception {
        String jobId = "aggs-histogram-job";
        String job = "{\"description\":\"Aggs job\",\"analysis_config\" :{\"bucket_span\":\"1h\","
                + "\"summary_count_field_name\":\"doc_count\","
                + "\"detectors\":[{\"function\":\"mean\",\"field_name\":\"responsetime\",\"by_field_name\":\"airline\"}]},"
                + "\"data_description\" : {\"time_field\":\"time stamp\"}"
                + "}";
        client().performRequest("put", MachineLearning.BASE_PATH + "anomaly_detectors/" + jobId, Collections.emptyMap(),
                new StringEntity(job, ContentType.APPLICATION_JSON));

        String datafeedId = "datafeed-" + jobId;
        String aggregations = "{\"buckets\":{\"histogram\":{\"field\":\"time stamp\",\"interval\":3600000},"
                + "\"aggregations\":{"
                + "\"time stamp\":{\"max\":{\"field\":\"time stamp\"}},"
                + "\"airline\":{\"terms\":{\"field\":\"airline\",\"size\":10},"
                + "  \"aggregations\":{\"responsetime\":{\"avg\":{\"field\":\"responsetime\"}}}}}}}";
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
        String job = "{\"description\":\"Aggs job\",\"analysis_config\" :{\"bucket_span\":\"3600s\","
                + "\"summary_count_field_name\":\"doc_count\","
                + "\"detectors\":[{\"function\":\"mean\",\"field_name\":\"responsetime\",\"by_field_name\":\"airline\"}]},"
                + "\"data_description\" : {\"time_field\":\"time stamp\"}"
                + "}";
        client().performRequest("put", MachineLearning.BASE_PATH + "anomaly_detectors/" + jobId, Collections.emptyMap(),
                new StringEntity(job, ContentType.APPLICATION_JSON));

        String datafeedId = "datafeed-" + jobId;
        String aggregations = "{\"time stamp\":{\"date_histogram\":{\"field\":\"time stamp\",\"interval\":\"1h\"},"
                + "\"aggregations\":{"
                + "\"time stamp\":{\"max\":{\"field\":\"time stamp\"}},"
                + "\"airline\":{\"terms\":{\"field\":\"airline\",\"size\":10},"
                + "  \"aggregations\":{\"responsetime\":{\"avg\":{\"field\":\"responsetime\"}}}}}}}";
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

    public void testLookbackUsingDerivativeAggWithLargerHistogramBucketThanDataRate() throws Exception {
        String jobId = "derivative-agg-network-job";
        String job = "{\"analysis_config\" :{\"bucket_span\":\"300s\","
                + "\"summary_count_field_name\":\"doc_count\","
                + "\"detectors\":[{\"function\":\"mean\",\"field_name\":\"bytes-delta\",\"by_field_name\":\"hostname\"}]},"
                + "\"data_description\" : {\"time_field\":\"timestamp\"}"
                + "}";
        client().performRequest("put", MachineLearning.BASE_PATH + "anomaly_detectors/" + jobId, Collections.emptyMap(),
                new StringEntity(job, ContentType.APPLICATION_JSON));

        String datafeedId = "datafeed-" + jobId;
        String aggregations =
                 "{\"hostname\": {\"terms\" : {\"field\": \"host.keyword\", \"size\":10},"
                    + "\"aggs\": {\"buckets\": {\"date_histogram\":{\"field\":\"timestamp\",\"interval\":\"60s\"},"
                        + "\"aggs\": {\"timestamp\":{\"max\":{\"field\":\"timestamp\"}},"
                            + "\"bytes-delta\":{\"derivative\":{\"buckets_path\":\"avg_bytes_out\"}},"
                            + "\"avg_bytes_out\":{\"avg\":{\"field\":\"network_bytes_out\"}} }}}}}";
        new DatafeedBuilder(datafeedId, jobId, "network-data", "doc")
                .setAggregations(aggregations)
                .setChunkingTimespan("300s")
                .build();

        openJob(client(), jobId);

        startDatafeedAndWaitUntilStopped(datafeedId);
        waitUntilJobIsClosed(jobId);
        Response jobStatsResponse = client().performRequest("get", MachineLearning.BASE_PATH + "anomaly_detectors/" + jobId + "/_stats");
        String jobStatsResponseAsString = responseEntityToString(jobStatsResponse);
        assertThat(jobStatsResponseAsString, containsString("\"input_record_count\":40"));
        assertThat(jobStatsResponseAsString, containsString("\"processed_record_count\":40"));
        assertThat(jobStatsResponseAsString, containsString("\"out_of_order_timestamp_count\":0"));
        assertThat(jobStatsResponseAsString, containsString("\"bucket_count\":3"));
        // The derivative agg won't have values for the first bucket of each host
        assertThat(jobStatsResponseAsString, containsString("\"missing_field_count\":2"));
    }

    public void testLookbackUsingDerivativeAggWithSmallerHistogramBucketThanDataRate() throws Exception {
        String jobId = "derivative-agg-network-job";
        String job = "{\"analysis_config\" :{\"bucket_span\":\"300s\","
                + "\"summary_count_field_name\":\"doc_count\","
                + "\"detectors\":[{\"function\":\"mean\",\"field_name\":\"bytes-delta\",\"by_field_name\":\"hostname\"}]},"
                + "\"data_description\" : {\"time_field\":\"timestamp\"}"
                + "}";
        client().performRequest("put", MachineLearning.BASE_PATH + "anomaly_detectors/" + jobId, Collections.emptyMap(),
                new StringEntity(job, ContentType.APPLICATION_JSON));

        String datafeedId = "datafeed-" + jobId;
        String aggregations =
                "{\"hostname\": {\"terms\" : {\"field\": \"host.keyword\", \"size\":10},"
                        + "\"aggs\": {\"buckets\": {\"date_histogram\":{\"field\":\"timestamp\",\"interval\":\"5s\"},"
                        + "\"aggs\": {\"timestamp\":{\"max\":{\"field\":\"timestamp\"}},"
                        + "\"bytes-delta\":{\"derivative\":{\"buckets_path\":\"avg_bytes_out\"}},"
                        + "\"avg_bytes_out\":{\"avg\":{\"field\":\"network_bytes_out\"}} }}}}}";
        new DatafeedBuilder(datafeedId, jobId, "network-data", "doc")
                .setAggregations(aggregations)
                .setChunkingTimespan("300s")
                .build();

        openJob(client(), jobId);

        startDatafeedAndWaitUntilStopped(datafeedId);
        waitUntilJobIsClosed(jobId);
        Response jobStatsResponse = client().performRequest("get", MachineLearning.BASE_PATH + "anomaly_detectors/" + jobId + "/_stats");
        String jobStatsResponseAsString = responseEntityToString(jobStatsResponse);
        assertThat(jobStatsResponseAsString, containsString("\"input_record_count\":240"));
        assertThat(jobStatsResponseAsString, containsString("\"processed_record_count\":240"));
    }

    public void testLookbackWithoutPermissions() throws Exception {
        String jobId = "permission-test-network-job";
        String job = "{\"analysis_config\" :{\"bucket_span\":\"300s\","
                + "\"summary_count_field_name\":\"doc_count\","
                + "\"detectors\":[{\"function\":\"mean\",\"field_name\":\"bytes-delta\",\"by_field_name\":\"hostname\"}]},"
                + "\"data_description\" : {\"time_field\":\"timestamp\"}"
                + "}";
        client().performRequest("put", MachineLearning.BASE_PATH + "anomaly_detectors/" + jobId, Collections.emptyMap(),
                new StringEntity(job, ContentType.APPLICATION_JSON));

        String datafeedId = "datafeed-" + jobId;
        String aggregations =
                "{\"hostname\": {\"terms\" : {\"field\": \"host.keyword\", \"size\":10},"
                        + "\"aggs\": {\"buckets\": {\"date_histogram\":{\"field\":\"timestamp\",\"interval\":\"5s\"},"
                        + "\"aggs\": {\"timestamp\":{\"max\":{\"field\":\"timestamp\"}},"
                        + "\"bytes-delta\":{\"derivative\":{\"buckets_path\":\"avg_bytes_out\"}},"
                        + "\"avg_bytes_out\":{\"avg\":{\"field\":\"network_bytes_out\"}} }}}}}";

        // At the time we create the datafeed the user can access the network-data index that we have access to
        new DatafeedBuilder(datafeedId, jobId, "network-data", "doc")
                .setAggregations(aggregations)
                .setChunkingTimespan("300s")
                .setAuthHeader(BASIC_AUTH_VALUE_ML_ADMIN_WITH_SOME_DATA_ACCESS)
                .build();

        // Change the role so that the user can no longer access network-data
        setupDataAccessRole("some-other-data");

        openJob(client(), jobId);

        startDatafeedAndWaitUntilStopped(datafeedId, BASIC_AUTH_VALUE_ML_ADMIN_WITH_SOME_DATA_ACCESS);
        waitUntilJobIsClosed(jobId);
        Response jobStatsResponse = client().performRequest("get", MachineLearning.BASE_PATH + "anomaly_detectors/" + jobId + "/_stats");
        String jobStatsResponseAsString = responseEntityToString(jobStatsResponse);
        // We expect that no data made it through to the job
        assertThat(jobStatsResponseAsString, containsString("\"input_record_count\":0"));
        assertThat(jobStatsResponseAsString, containsString("\"processed_record_count\":0"));

        // There should be a notification saying that there was a problem extracting data
        client().performRequest("post", "_refresh");
        Response notificationsResponse = client().performRequest("get", AuditorField.NOTIFICATIONS_INDEX + "/_search?q=job_id:" + jobId);
        String notificationsResponseAsString = responseEntityToString(notificationsResponse);
        assertThat(notificationsResponseAsString, containsString("\"message\":\"Datafeed is encountering errors extracting data: " +
                "action [indices:data/read/search] is unauthorized for user [ml_admin_plus_data]\""));
    }

    public void testLookbackWithPipelineBucketAgg() throws Exception {
        String jobId = "pipeline-bucket-agg-job";
        String job = "{\"analysis_config\" :{\"bucket_span\":\"1h\","
                + "\"summary_count_field_name\":\"doc_count\","
                + "\"detectors\":[{\"function\":\"mean\",\"field_name\":\"percentile95_airlines_count\"}]},"
                + "\"data_description\" : {\"time_field\":\"time stamp\"}"
                + "}";
        client().performRequest("put", MachineLearning.BASE_PATH + "anomaly_detectors/" + jobId, Collections.emptyMap(),
                new StringEntity(job, ContentType.APPLICATION_JSON));

        String datafeedId = "datafeed-" + jobId;
        String aggregations = "{\"buckets\":{\"date_histogram\":{\"field\":\"time stamp\",\"interval\":\"15m\"},"
                + "\"aggregations\":{"
                    + "\"time stamp\":{\"max\":{\"field\":\"time stamp\"}},"
                    + "\"airlines\":{\"terms\":{\"field\":\"airline.keyword\",\"size\":10}},"
                    + "\"percentile95_airlines_count\":{\"percentiles_bucket\":" +
                        "{\"buckets_path\":\"airlines._count\", \"percents\": [95]}}}}}";
        new DatafeedBuilder(datafeedId, jobId, "airline-data", "response").setAggregations(aggregations).build();

        openJob(client(), jobId);

        startDatafeedAndWaitUntilStopped(datafeedId);
        waitUntilJobIsClosed(jobId);
        Response jobStatsResponse = client().performRequest("get", MachineLearning.BASE_PATH + "anomaly_detectors/" + jobId + "/_stats");
        String jobStatsResponseAsString = responseEntityToString(jobStatsResponse);
        assertThat(jobStatsResponseAsString, containsString("\"input_record_count\":2"));
        assertThat(jobStatsResponseAsString, containsString("\"input_field_count\":4"));
        assertThat(jobStatsResponseAsString, containsString("\"processed_record_count\":2"));
        assertThat(jobStatsResponseAsString, containsString("\"processed_field_count\":4"));
        assertThat(jobStatsResponseAsString, containsString("\"out_of_order_timestamp_count\":0"));
        assertThat(jobStatsResponseAsString, containsString("\"missing_field_count\":0"));
    }

    public void testRealtime() throws Exception {
        String jobId = "job-realtime-1";
        createJob(jobId, "airline");
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

        // Model state should be persisted at the end of lookback
        // test a model snapshot is present
        assertBusy(() -> {
            try {
                Response getJobResponse = client().performRequest("get",
                        MachineLearning.BASE_PATH + "anomaly_detectors/" + jobId + "/model_snapshots");
                String responseAsString = responseEntityToString(getJobResponse);
                assertThat(responseAsString, containsString("\"count\":1"));
            } catch (Exception e1) {
                throw new RuntimeException(e1);
            }
        });

        ResponseException e = expectThrows(ResponseException.class,
                () -> client().performRequest("delete", MachineLearning.BASE_PATH + "anomaly_detectors/" + jobId));
        response = e.getResponse();
        assertThat(response.getStatusLine().getStatusCode(), equalTo(409));
        assertThat(responseEntityToString(response), containsString("Cannot delete job [" + jobId + "] because datafeed [" + datafeedId
                + "] refers to it"));

        response = client().performRequest("post", MachineLearning.BASE_PATH + "datafeeds/" + datafeedId + "/_stop");
        assertThat(response.getStatusLine().getStatusCode(), equalTo(200));
        assertThat(responseEntityToString(response), equalTo("{\"stopped\":true}"));

        client().performRequest("POST", "/_xpack/ml/anomaly_detectors/" + jobId + "/_close");

        response = client().performRequest("delete", MachineLearning.BASE_PATH + "datafeeds/" + datafeedId);
        assertThat(response.getStatusLine().getStatusCode(), equalTo(200));
        assertThat(responseEntityToString(response), equalTo("{\"acknowledged\":true}"));

        response = client().performRequest("delete", MachineLearning.BASE_PATH + "anomaly_detectors/" + jobId);
        assertThat(response.getStatusLine().getStatusCode(), equalTo(200));
        assertThat(responseEntityToString(response), equalTo("{\"acknowledged\":true}"));
    }

    public void testForceDeleteWhileDatafeedIsRunning() throws Exception {
        String jobId = "job-realtime-2";
        createJob(jobId, "airline");
        String datafeedId = jobId + "-datafeed";
        new DatafeedBuilder(datafeedId, jobId, "airline-data", "response").build();
        openJob(client(), jobId);

        Response response = client().performRequest("post",
                MachineLearning.BASE_PATH + "datafeeds/" + datafeedId + "/_start?start=2016-06-01T00:00:00Z");
        assertThat(response.getStatusLine().getStatusCode(), equalTo(200));
        assertThat(responseEntityToString(response), equalTo("{\"started\":true}"));

        ResponseException e = expectThrows(ResponseException.class,
                () -> client().performRequest("delete", MachineLearning.BASE_PATH + "datafeeds/" + datafeedId));
        response = e.getResponse();
        assertThat(response.getStatusLine().getStatusCode(), equalTo(409));
        assertThat(responseEntityToString(response), containsString("Cannot delete datafeed [" + datafeedId
                + "] while its status is started"));

        response = client().performRequest("delete",
                MachineLearning.BASE_PATH + "datafeeds/" + datafeedId + "?force=true");
        assertThat(response.getStatusLine().getStatusCode(), equalTo(200));
        assertThat(responseEntityToString(response), equalTo("{\"acknowledged\":true}"));

        expectThrows(ResponseException.class,
                () -> client().performRequest("get", "/_xpack/ml/datafeeds/" + datafeedId));
    }

    private class LookbackOnlyTestHelper {
        private String jobId;
        private String airlineVariant;
        private String dataIndex;
        private boolean addScriptedFields;
        private boolean shouldSucceedInput;
        private boolean shouldSucceedProcessing;

        LookbackOnlyTestHelper(String jobId, String dataIndex) {
            this.jobId = jobId;
            this.dataIndex = dataIndex;
            this.shouldSucceedInput = true;
            this.shouldSucceedProcessing = true;
            this.airlineVariant = "airline";
        }

        public LookbackOnlyTestHelper setAddScriptedFields(boolean value) {
            addScriptedFields = value;
            return this;
        }

        public LookbackOnlyTestHelper setAirlineVariant(String airlineVariant) {
            this.airlineVariant = airlineVariant;
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
            createJob(jobId, airlineVariant);
            String datafeedId = "datafeed-" + jobId;
            new DatafeedBuilder(datafeedId, jobId, dataIndex, "response")
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
        startDatafeedAndWaitUntilStopped(datafeedId, BASIC_AUTH_VALUE_SUPER_USER);
    }

    private void startDatafeedAndWaitUntilStopped(String datafeedId, String authHeader) throws Exception {
        Response startDatafeedRequest = client().performRequest("post",
                MachineLearning.BASE_PATH + "datafeeds/" + datafeedId + "/_start?start=2016-06-01T00:00:00Z&end=2016-06-02T00:00:00Z",
                new BasicHeader("Authorization", authHeader));
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

    private Response createJob(String id, String airlineVariant) throws Exception {
        String job = "{\n" + "    \"description\":\"Analysis of response time by airline\",\n"
                + "    \"analysis_config\" : {\n" + "        \"bucket_span\":\"1h\",\n"
                + "        \"detectors\" :[\n"
                + "          {\"function\":\"mean\",\"field_name\":\"responsetime\",\"by_field_name\":\"" + airlineVariant + "\"}]\n"
                + "    },\n" + "    \"data_description\" : {\n"
                + "        \"format\":\"xcontent\",\n"
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

    @After
    public void clearMlState() throws Exception {
        new MlRestTestStateCleaner(logger, adminClient()).clearMlMetadata();
        XPackRestTestHelper.waitForPendingTasks(adminClient());
    }

    private static class DatafeedBuilder {
        String datafeedId;
        String jobId;
        String index;
        String type;
        boolean source;
        String scriptedFields;
        String aggregations;
        String authHeader = BASIC_AUTH_VALUE_SUPER_USER;
        String chunkingTimespan;

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

        DatafeedBuilder setAuthHeader(String authHeader) {
            this.authHeader = authHeader;
            return this;
        }

        DatafeedBuilder setChunkingTimespan(String timespan) {
            chunkingTimespan = timespan;
            return this;
        }

        Response build() throws IOException {
            String datafeedConfig = "{"
                    + "\"job_id\": \"" + jobId + "\",\"indexes\":[\"" + index + "\"],\"types\":[\"" + type + "\"]"
                    + (source ? ",\"_source\":true" : "")
                    + (scriptedFields == null ? "" : ",\"script_fields\":" + scriptedFields)
                    + (aggregations == null ? "" : ",\"aggs\":" + aggregations)
                    + (chunkingTimespan == null ? "" :
                            ",\"chunking_config\":{\"mode\":\"MANUAL\",\"time_span\":\"" + chunkingTimespan + "\"}")
                    + "}";
            return client().performRequest("put", MachineLearning.BASE_PATH + "datafeeds/" + datafeedId, Collections.emptyMap(),
                    new StringEntity(datafeedConfig, ContentType.APPLICATION_JSON),
                    new BasicHeader("Authorization", authHeader));
        }
    }
}
