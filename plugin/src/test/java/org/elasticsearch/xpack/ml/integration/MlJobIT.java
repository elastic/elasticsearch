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
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xpack.ml.MachineLearning;
import org.elasticsearch.xpack.ml.job.persistence.AnomalyDetectorsIndex;
import org.junit.After;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.security.authc.support.UsernamePasswordToken.basicAuthHeaderValue;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.isEmptyString;
import static org.hamcrest.Matchers.not;

public class MlJobIT extends ESRestTestCase {

    private static final String BASIC_AUTH_VALUE = basicAuthHeaderValue("elastic", new SecureString("changeme".toCharArray()));

    @Override
    protected Settings restClientSettings() {
        return Settings.builder().put(super.restClientSettings()).put(ThreadContext.PREFIX + ".Authorization", BASIC_AUTH_VALUE).build();
    }

    @Override
    protected boolean preserveTemplatesUponCompletion() {
        return true;
    }

    private static final String RESULT_MAPPING = "{ \"mappings\": {\"result\": { \"properties\": { " +
            "\"result_type\": { \"type\" : \"keyword\" }," +
            "\"timestamp\": { \"type\" : \"date\" }, " +
            "\"anomaly_score\": { \"type\" : \"double\" }, " +
            "\"record_score\": { \"type\" : \"double\" }, " +
            "\"over_field_value\": { \"type\" : \"keyword\" }, " +
            "\"partition_field_value\": { \"type\" : \"keyword\" }, " +
            "\"by_field_value\": { \"type\" : \"keyword\" }, " +
            "\"field_name\": { \"type\" : \"keyword\" }, " +
            "\"function\": { \"type\" : \"keyword\" } " +
            "} } } }";

    public void testPutJob_GivenFarequoteConfig() throws Exception {
        Response response = createFarequoteJob("given-farequote-config-job");

        assertThat(response.getStatusLine().getStatusCode(), equalTo(200));
        String responseAsString = responseEntityToString(response);
        assertThat(responseAsString, containsString("\"job_id\":\"given-farequote-config-job\""));
    }

    public void testGetJob_GivenNoSuchJob() throws Exception {
        ResponseException e = expectThrows(ResponseException.class,
                () -> client().performRequest("get", MachineLearning.BASE_PATH + "anomaly_detectors/non-existing-job/_stats"));

        assertThat(e.getResponse().getStatusLine().getStatusCode(), equalTo(404));
        assertThat(e.getMessage(), containsString("No known job with id 'non-existing-job'"));
    }

    public void testGetJob_GivenJobExists() throws Exception {
        createFarequoteJob("get-job_given-job-exists-job");

        Response response = client().performRequest("get",
                MachineLearning.BASE_PATH + "anomaly_detectors/get-job_given-job-exists-job/_stats");

        assertThat(response.getStatusLine().getStatusCode(), equalTo(200));
        String responseAsString = responseEntityToString(response);
        assertThat(responseAsString, containsString("\"count\":1"));
        assertThat(responseAsString, containsString("\"job_id\":\"get-job_given-job-exists-job\""));
    }

    public void testGetJobs_GivenSingleJob() throws Exception {
        String jobId = "get-jobs_given-single-job-job";
        createFarequoteJob(jobId);

        // Explicit _all
        Response response = client().performRequest("get", MachineLearning.BASE_PATH + "anomaly_detectors/_all");

        assertThat(response.getStatusLine().getStatusCode(), equalTo(200));
        String responseAsString = responseEntityToString(response);
        assertThat(responseAsString, containsString("\"count\":1"));
        assertThat(responseAsString, containsString("\"job_id\":\"" + jobId + "\""));

        // Implicit _all
        response = client().performRequest("get", MachineLearning.BASE_PATH + "anomaly_detectors");

        assertThat(response.getStatusLine().getStatusCode(), equalTo(200));
        responseAsString = responseEntityToString(response);
        assertThat(responseAsString, containsString("\"count\":1"));
        assertThat(responseAsString, containsString("\"job_id\":\"" + jobId + "\""));
    }

    public void testGetJobs_GivenMultipleJobs() throws Exception {
        createFarequoteJob("given-multiple-jobs-job-1");
        createFarequoteJob("given-multiple-jobs-job-2");
        createFarequoteJob("given-multiple-jobs-job-3");

        // Explicit _all
        Response response = client().performRequest("get", MachineLearning.BASE_PATH + "anomaly_detectors/_all");

        assertThat(response.getStatusLine().getStatusCode(), equalTo(200));
        String responseAsString = responseEntityToString(response);
        assertThat(responseAsString, containsString("\"count\":3"));
        assertThat(responseAsString, containsString("\"job_id\":\"given-multiple-jobs-job-1\""));
        assertThat(responseAsString, containsString("\"job_id\":\"given-multiple-jobs-job-2\""));
        assertThat(responseAsString, containsString("\"job_id\":\"given-multiple-jobs-job-3\""));

        // Implicit _all
        response = client().performRequest("get", MachineLearning.BASE_PATH + "anomaly_detectors");

        assertThat(response.getStatusLine().getStatusCode(), equalTo(200));
        responseAsString = responseEntityToString(response);
        assertThat(responseAsString, containsString("\"count\":3"));
        assertThat(responseAsString, containsString("\"job_id\":\"given-multiple-jobs-job-1\""));
        assertThat(responseAsString, containsString("\"job_id\":\"given-multiple-jobs-job-2\""));
        assertThat(responseAsString, containsString("\"job_id\":\"given-multiple-jobs-job-3\""));
    }

    private Response createFarequoteJob(String jobId) throws Exception {
        String job = "{\n" + "    \"description\":\"Analysis of response time by airline\",\n"
                + "    \"analysis_config\" : {\n" + "        \"bucket_span\": \"3600s\",\n"
                + "        \"detectors\" :[{\"function\":\"metric\",\"field_name\":\"responsetime\",\"by_field_name\":\"airline\"}]\n"
                + "    },\n" + "    \"data_description\" : {\n" + "        \"field_delimiter\":\",\",\n" + "        " +
                "\"time_field\":\"time\",\n"
                + "        \"time_format\":\"yyyy-MM-dd HH:mm:ssX\"\n" + "    }\n" + "}";

        return client().performRequest("put", MachineLearning.BASE_PATH + "anomaly_detectors/" + jobId,
                Collections.emptyMap(), new StringEntity(job, ContentType.APPLICATION_JSON));
    }

    public void testGetBucketResults() throws Exception {
        Map<String, String> params = new HashMap<>();
        params.put("start", "1200"); // inclusive
        params.put("end", "1400"); // exclusive

        String jobId = "get-bucket-results-job";

        ResponseException e = expectThrows(ResponseException.class,
                () -> client().performRequest("get",
                        MachineLearning.BASE_PATH + "anomaly_detectors/" + jobId + "/results/buckets", params));
        assertThat(e.getResponse().getStatusLine().getStatusCode(), equalTo(404));
        assertThat(e.getMessage(), containsString("No known job with id '" + jobId + "'"));

        createFarequoteJob(jobId);

        addBucketResult(jobId, "1234", 1);
        addBucketResult(jobId, "1235", 1);
        addBucketResult(jobId, "1236", 1);
        Response response = client().performRequest("get",
                MachineLearning.BASE_PATH + "anomaly_detectors/" + jobId + "/results/buckets", params);
        assertThat(response.getStatusLine().getStatusCode(), equalTo(200));
        String responseAsString = responseEntityToString(response);
        assertThat(responseAsString, containsString("\"count\":3"));

        params.put("end", "1235");
        response = client().performRequest("get", MachineLearning.BASE_PATH + "anomaly_detectors/" + jobId + "/results/buckets", params);
        assertThat(response.getStatusLine().getStatusCode(), equalTo(200));
        responseAsString = responseEntityToString(response);
        assertThat(responseAsString, containsString("\"count\":1"));

        e = expectThrows(ResponseException.class, () -> client().performRequest("get", MachineLearning.BASE_PATH
                + "anomaly_detectors/2/results/buckets/1234"));
        assertThat(e.getResponse().getStatusLine().getStatusCode(), equalTo(404));
        assertThat(e.getMessage(), containsString("No known job with id '2'"));

        e = expectThrows(ResponseException.class, () -> client().performRequest("get",
                MachineLearning.BASE_PATH + "anomaly_detectors/" + jobId + "/results/buckets/1"));
        assertThat(e.getResponse().getStatusLine().getStatusCode(), equalTo(404));

        response = client().performRequest("get", MachineLearning.BASE_PATH + "anomaly_detectors/" + jobId + "/results/buckets/1234");
        assertThat(response.getStatusLine().getStatusCode(), equalTo(200));
        responseAsString = responseEntityToString(response);
        assertThat(responseAsString, not(isEmptyString()));
    }

    public void testGetRecordResults() throws Exception {
        Map<String, String> params = new HashMap<>();
        params.put("start", "1200"); // inclusive
        params.put("end", "1400"); // exclusive

        String jobId = "get-record-results-job";
        ResponseException e = expectThrows(ResponseException.class,
                () -> client().performRequest("get",
                        MachineLearning.BASE_PATH + "anomaly_detectors/" + jobId + "/results/records", params));
        assertThat(e.getResponse().getStatusLine().getStatusCode(), equalTo(404));
        assertThat(e.getMessage(), containsString("No known job with id '" + jobId + "'"));

        createFarequoteJob(jobId);

        addRecordResult(jobId, "1234", 1, 1);
        addRecordResult(jobId, "1235", 1, 2);
        addRecordResult(jobId, "1236", 1, 3);
        Response response = client().performRequest("get",
                MachineLearning.BASE_PATH + "anomaly_detectors/" + jobId + "/results/records", params);
        assertThat(response.getStatusLine().getStatusCode(), equalTo(200));
        String responseAsString = responseEntityToString(response);
        assertThat(responseAsString, containsString("\"count\":3"));

        params.put("end", "1235");
        response = client().performRequest("get", MachineLearning.BASE_PATH + "anomaly_detectors/" + jobId + "/results/records", params);
        assertThat(response.getStatusLine().getStatusCode(), equalTo(200));
        responseAsString = responseEntityToString(response);
        assertThat(responseAsString, containsString("\"count\":1"));
    }

    public void testCantCreateJobWithSameID() throws Exception {
        String jobTemplate = "{\n" +
                "  \"analysis_config\" : {\n" +
                "        \"detectors\" :[{\"function\":\"metric\",\"field_name\":\"responsetime\"}]\n" +
                "    },\n" +
                "  \"data_description\": {},\n" +
                "  \"results_index_name\" : \"%s\"}";

        String jobConfig = String.format(Locale.ROOT, jobTemplate, "index-1");

        String jobId = "cant-create-job-with-same-id-job";
        Response response = client().performRequest("put", MachineLearning.BASE_PATH + "anomaly_detectors/" + jobId ,
                Collections.emptyMap(),
                new StringEntity(jobConfig, ContentType.APPLICATION_JSON));
        assertEquals(200, response.getStatusLine().getStatusCode());

        final String jobConfig2 = String.format(Locale.ROOT, jobTemplate, "index-2");
        ResponseException e = expectThrows(ResponseException.class,
                () ->client().performRequest("put", MachineLearning.BASE_PATH + "anomaly_detectors/" + jobId,
                        Collections.emptyMap(), new StringEntity(jobConfig2, ContentType.APPLICATION_JSON)));

        assertThat(e.getResponse().getStatusLine().getStatusCode(), equalTo(400));
        assertThat(e.getMessage(), containsString("The job cannot be created with the Id '" + jobId + "'. The Id is already used."));
    }

    public void testCreateJobsWithIndexNameOption() throws Exception {
        String jobTemplate = "{\n" +
                "  \"analysis_config\" : {\n" +
                "        \"detectors\" :[{\"function\":\"metric\",\"field_name\":\"responsetime\"}]\n" +
                "    },\n" +
                "  \"data_description\": {},\n" +
                "  \"results_index_name\" : \"%s\"}";

        String jobId1 = "create-jobs-with-index-name-option-job-1";
        String indexName = "non-default-index";
        String jobConfig = String.format(Locale.ROOT, jobTemplate, indexName);

        Response response = client().performRequest("put", MachineLearning.BASE_PATH
                        + "anomaly_detectors/" + jobId1, Collections.emptyMap(), new StringEntity(jobConfig, ContentType.APPLICATION_JSON));
        assertEquals(200, response.getStatusLine().getStatusCode());

        String jobId2 = "create-jobs-with-index-name-option-job-2";
        response = client().performRequest("put", MachineLearning.BASE_PATH
                        + "anomaly_detectors/" + jobId2, Collections.emptyMap(), new StringEntity(jobConfig, ContentType.APPLICATION_JSON));
        assertEquals(200, response.getStatusLine().getStatusCode());

        response = client().performRequest("get", "_aliases");
        assertEquals(200, response.getStatusLine().getStatusCode());
        String responseAsString = responseEntityToString(response);

        assertThat(responseAsString, containsString("\"" + AnomalyDetectorsIndex.jobResultsAliasedName("custom-" + indexName)
                + "\":{\"aliases\":{\"" + AnomalyDetectorsIndex.jobResultsAliasedName(jobId1)
                + "\":{\"filter\":{\"term\":{\"job_id\":{\"value\":\"" + jobId1 + "\",\"boost\":1.0}}}},\"" +
                AnomalyDetectorsIndex.jobResultsAliasedName(jobId2)));

        response = client().performRequest("get", "_cat/indices");
        assertEquals(200, response.getStatusLine().getStatusCode());
        responseAsString = responseEntityToString(response);
        assertThat(responseAsString, containsString(AnomalyDetectorsIndex.RESULTS_INDEX_PREFIX + "custom-" + indexName));
        assertThat(responseAsString, not(containsString(AnomalyDetectorsIndex.jobResultsAliasedName(jobId1))));
        assertThat(responseAsString, not(containsString(AnomalyDetectorsIndex.jobResultsAliasedName(jobId2))));

        String bucketResult = String.format(Locale.ROOT,
                "{\"job_id\":\"%s\", \"timestamp\": \"%s\", \"result_type\":\"bucket\", \"bucket_span\": \"%s\"}",
                jobId1, "1234", 1);
        String id = String.format(Locale.ROOT,
                "%s_%s_%s", jobId1, "1234", 1);
        response = client().performRequest("put", AnomalyDetectorsIndex.jobResultsAliasedName(jobId1) + "/result/" + id,
                Collections.emptyMap(), new StringEntity(bucketResult, ContentType.APPLICATION_JSON));
        assertEquals(201, response.getStatusLine().getStatusCode());

        bucketResult = String.format(Locale.ROOT,
                "{\"job_id\":\"%s\", \"timestamp\": \"%s\", \"result_type\":\"bucket\", \"bucket_span\": \"%s\"}",
                jobId1, "1236", 1);
        id = String.format(Locale.ROOT,
                "%s_%s_%s", jobId1, "1236", 1);
        response = client().performRequest("put", AnomalyDetectorsIndex.jobResultsAliasedName(jobId1) + "/result/" + id,
                Collections.emptyMap(), new StringEntity(bucketResult, ContentType.APPLICATION_JSON));
        assertEquals(201, response.getStatusLine().getStatusCode());

        client().performRequest("post", "_refresh");

        response = client().performRequest("get", MachineLearning.BASE_PATH + "anomaly_detectors/" + jobId1 + "/results/buckets");
        assertThat(response.getStatusLine().getStatusCode(), equalTo(200));
        responseAsString = responseEntityToString(response);
        assertThat(responseAsString, containsString("\"count\":2"));

        response = client().performRequest("get", AnomalyDetectorsIndex.jobResultsAliasedName(jobId1) + "/result/_search");
        assertThat(response.getStatusLine().getStatusCode(), equalTo(200));
        responseAsString = responseEntityToString(response);
        assertThat(responseAsString, containsString("\"total\":2"));

        response = client().performRequest("delete", MachineLearning.BASE_PATH + "anomaly_detectors/" + jobId1);
        assertThat(response.getStatusLine().getStatusCode(), equalTo(200));

        // check that indices still exist, but are empty and aliases are gone
        response = client().performRequest("get", "_aliases");
        assertEquals(200, response.getStatusLine().getStatusCode());
        responseAsString = responseEntityToString(response);
        assertThat(responseAsString, not(containsString(AnomalyDetectorsIndex.jobResultsAliasedName(jobId1))));
        assertThat(responseAsString, containsString(AnomalyDetectorsIndex.jobResultsAliasedName(jobId2))); //job2 still exists

        response = client().performRequest("get", "_cat/indices");
        assertEquals(200, response.getStatusLine().getStatusCode());
        responseAsString = responseEntityToString(response);
        assertThat(responseAsString, containsString(AnomalyDetectorsIndex.RESULTS_INDEX_PREFIX + "custom-" + indexName));

        client().performRequest("post", "_refresh");

        response = client().performRequest("get", AnomalyDetectorsIndex.RESULTS_INDEX_PREFIX + "custom-" + indexName + "/_count");
        assertEquals(200, response.getStatusLine().getStatusCode());
        responseAsString = responseEntityToString(response);
        assertThat(responseAsString, containsString("\"count\":0"));
    }

    public void testCreateJobInSharedIndexUpdatesMapping() throws Exception {
        String jobTemplate = "{\n" +
                "  \"analysis_config\" : {\n" +
                "        \"detectors\" :[{\"function\":\"metric\",\"field_name\":\"metric\", \"by_field_name\":\"%s\"}]\n" +
                "    },\n" +
                "  \"data_description\": {}\n" +
                "}";

        String jobId1 = "create-job-in-shared-index-updates-mapping-job-1";
        String byFieldName1 = "responsetime";
        String jobId2 = "create-job-in-shared-index-updates-mapping-job-2";
        String byFieldName2 = "cpu-usage";
        String jobConfig = String.format(Locale.ROOT, jobTemplate, byFieldName1);

        Response response = client().performRequest("put", MachineLearning.BASE_PATH
                + "anomaly_detectors/" + jobId1, Collections.emptyMap(), new StringEntity(jobConfig, ContentType.APPLICATION_JSON));
        assertEquals(200, response.getStatusLine().getStatusCode());

        // Check the index mapping contains the first by_field_name
        response = client().performRequest("get", AnomalyDetectorsIndex.RESULTS_INDEX_PREFIX
                + AnomalyDetectorsIndex.RESULTS_INDEX_DEFAULT + "/_mapping?pretty");
        assertEquals(200, response.getStatusLine().getStatusCode());
        String responseAsString = responseEntityToString(response);
        assertThat(responseAsString, containsString(byFieldName1));
        assertThat(responseAsString, not(containsString(byFieldName2)));

        jobConfig = String.format(Locale.ROOT, jobTemplate, byFieldName2);
        response = client().performRequest("put", MachineLearning.BASE_PATH
                + "anomaly_detectors/" + jobId2, Collections.emptyMap(), new StringEntity(jobConfig, ContentType.APPLICATION_JSON));
        assertEquals(200, response.getStatusLine().getStatusCode());

        // Check the index mapping now contains both fields
        response = client().performRequest("get", AnomalyDetectorsIndex.RESULTS_INDEX_PREFIX
                + AnomalyDetectorsIndex.RESULTS_INDEX_DEFAULT + "/_mapping?pretty");
        assertEquals(200, response.getStatusLine().getStatusCode());
        responseAsString = responseEntityToString(response);
        assertThat(responseAsString, containsString(byFieldName1));
        assertThat(responseAsString, containsString(byFieldName2));
    }

    public void testCreateJobInCustomSharedIndexUpdatesMapping() throws Exception {
        String jobTemplate = "{\n" +
                "  \"analysis_config\" : {\n" +
                "        \"detectors\" :[{\"function\":\"metric\",\"field_name\":\"metric\", \"by_field_name\":\"%s\"}]\n" +
                "  },\n" +
                "  \"data_description\": {},\n" +
                "  \"results_index_name\" : \"shared-index\"}";

        String jobId1 = "create-job-in-custom-shared-index-updates-mapping-job-1";
        String byFieldName1 = "responsetime";
        String jobId2 = "create-job-in-custom-shared-index-updates-mapping-job-2";
        String byFieldName2 = "cpu-usage";
        String jobConfig = String.format(Locale.ROOT, jobTemplate, byFieldName1);

        Response response = client().performRequest("put", MachineLearning.BASE_PATH
                + "anomaly_detectors/" + jobId1, Collections.emptyMap(), new StringEntity(jobConfig, ContentType.APPLICATION_JSON));
        assertEquals(200, response.getStatusLine().getStatusCode());

        // Check the index mapping contains the first by_field_name
        response = client().performRequest("get", AnomalyDetectorsIndex.RESULTS_INDEX_PREFIX + "custom-shared-index" + "/_mapping?pretty");
        assertEquals(200, response.getStatusLine().getStatusCode());
        String responseAsString = responseEntityToString(response);
        assertThat(responseAsString, containsString(byFieldName1));
        assertThat(responseAsString, not(containsString(byFieldName2)));

        jobConfig = String.format(Locale.ROOT, jobTemplate, byFieldName2);
        response = client().performRequest("put", MachineLearning.BASE_PATH
                + "anomaly_detectors/" + jobId2, Collections.emptyMap(), new StringEntity(jobConfig, ContentType.APPLICATION_JSON));
        assertEquals(200, response.getStatusLine().getStatusCode());

        // Check the index mapping now contains both fields
        response = client().performRequest("get", AnomalyDetectorsIndex.RESULTS_INDEX_PREFIX + "custom-shared-index" + "/_mapping?pretty");
        assertEquals(200, response.getStatusLine().getStatusCode());
        responseAsString = responseEntityToString(response);
        assertThat(responseAsString, containsString(byFieldName1));
        assertThat(responseAsString, containsString(byFieldName2));
    }

    public void testCreateJob_WithClashingFieldMappingsFails() throws Exception {
        String jobTemplate = "{\n" +
                "  \"analysis_config\" : {\n" +
                "        \"detectors\" :[{\"function\":\"metric\",\"field_name\":\"metric\", \"by_field_name\":\"%s\"}]\n" +
                "    },\n" +
                "  \"data_description\": {}\n" +
                "}";

        String jobId1 = "job-with-response-field";
        String byFieldName1 = "response";
        String jobId2 = "job-will-fail-with-mapping-error-on-response-field";
        String byFieldName2 = "response.time";
        String jobConfig = String.format(Locale.ROOT, jobTemplate, byFieldName1);

        Response response = client().performRequest("put", MachineLearning.BASE_PATH
                + "anomaly_detectors/" + jobId1, Collections.emptyMap(), new StringEntity(jobConfig, ContentType.APPLICATION_JSON));
        assertEquals(200, response.getStatusLine().getStatusCode());

        final String failingJobConfig = String.format(Locale.ROOT, jobTemplate, byFieldName2);
        ResponseException e = expectThrows(ResponseException.class,
                () -> client().performRequest("put", MachineLearning.BASE_PATH + "anomaly_detectors/" + jobId2,
                        Collections.emptyMap(), new StringEntity(failingJobConfig, ContentType.APPLICATION_JSON)));

        assertThat(e.getMessage(),
                containsString("A field has a different mapping type to an existing field with the same name. " +
                        "Use the 'results_index_name' setting to assign the job to another index"));
    }

    public void testDeleteJob() throws Exception {
        String jobId = "delete-job-job";
        String indexName = AnomalyDetectorsIndex.RESULTS_INDEX_PREFIX + AnomalyDetectorsIndex.RESULTS_INDEX_DEFAULT;
        createFarequoteJob(jobId);

        Response response = client().performRequest("get", "_cat/indices");
        assertEquals(200, response.getStatusLine().getStatusCode());
        String responseAsString = responseEntityToString(response);
        assertThat(responseAsString, containsString(indexName));

        response = client().performRequest("delete", MachineLearning.BASE_PATH + "anomaly_detectors/" + jobId);
        assertThat(response.getStatusLine().getStatusCode(), equalTo(200));

        // check that the index still exists (it's shared by default)
        response = client().performRequest("get", "_cat/indices");
        assertEquals(200, response.getStatusLine().getStatusCode());
        responseAsString = responseEntityToString(response);
        assertThat(responseAsString, containsString(indexName));

        assertBusy(() -> {
            try {
                Response r = client().performRequest("get", indexName + "/_count");
                assertEquals(200, r.getStatusLine().getStatusCode());
                String responseString = responseEntityToString(r);
                assertThat(responseString, containsString("\"count\":0"));
            } catch (Exception e) {
                fail(e.getMessage());
            }

        });

        // check that the job itself is gone
        expectThrows(ResponseException.class, () ->
                client().performRequest("get", MachineLearning.BASE_PATH + "anomaly_detectors/" + jobId + "/_stats"));
    }

    public void testDeleteJobAfterMissingIndex() throws Exception {
        String jobId = "delete-job-after-missing-index-job";
        String aliasName = AnomalyDetectorsIndex.jobResultsAliasedName(jobId);
        String indexName = AnomalyDetectorsIndex.RESULTS_INDEX_PREFIX + AnomalyDetectorsIndex.RESULTS_INDEX_DEFAULT;
        createFarequoteJob(jobId);

        Response response = client().performRequest("get", "_cat/indices");
        assertEquals(200, response.getStatusLine().getStatusCode());
        String responseAsString = responseEntityToString(response);
        assertThat(responseAsString, containsString(indexName));

        // Manually delete the index so that we can test that deletion proceeds
        // normally anyway
        response = client().performRequest("delete", indexName);
        assertEquals(200, response.getStatusLine().getStatusCode());

        response = client().performRequest("delete", MachineLearning.BASE_PATH + "anomaly_detectors/" + jobId);
        assertThat(response.getStatusLine().getStatusCode(), equalTo(200));

        // check index was deleted
        response = client().performRequest("get", "_cat/indices");
        assertEquals(200, response.getStatusLine().getStatusCode());
        responseAsString = responseEntityToString(response);
        assertThat(responseAsString, not(containsString(aliasName)));
        assertThat(responseAsString, not(containsString(indexName)));

        expectThrows(ResponseException.class, () ->
                client().performRequest("get", MachineLearning.BASE_PATH + "anomaly_detectors/" + jobId + "/_stats"));
    }

    public void testDeleteJobAfterMissingAlias() throws Exception {
        String jobId = "delete-job-after-missing-alias-job";
        String aliasName = AnomalyDetectorsIndex.jobResultsAliasedName(jobId);
        String indexName = AnomalyDetectorsIndex.RESULTS_INDEX_PREFIX + AnomalyDetectorsIndex.RESULTS_INDEX_DEFAULT;
        createFarequoteJob(jobId);

        Response response = client().performRequest("get", "_cat/aliases");
        assertEquals(200, response.getStatusLine().getStatusCode());
        String responseAsString = responseEntityToString(response);
        assertThat(responseAsString, containsString(aliasName));

        // Manually delete the alias so that we can test that deletion proceeds
        // normally anyway
        response = client().performRequest("delete", indexName + "/_alias/" + aliasName);
        assertEquals(200, response.getStatusLine().getStatusCode());

        // check alias was deleted
        expectThrows(ResponseException.class, () -> client().performRequest("get", "_cat/aliases"));

        response = client().performRequest("delete", MachineLearning.BASE_PATH + "anomaly_detectors/" + jobId);
        assertThat(response.getStatusLine().getStatusCode(), equalTo(200));
    }

    public void testMultiIndexDelete() throws Exception {
        String jobId = "multi-index-delete-job";
        String indexName = AnomalyDetectorsIndex.RESULTS_INDEX_PREFIX + AnomalyDetectorsIndex.RESULTS_INDEX_DEFAULT;
        createFarequoteJob(jobId);

        Response response = client().performRequest("put", indexName + "-001");
        assertEquals(200, response.getStatusLine().getStatusCode());

        response = client().performRequest("put", indexName + "-002");
        assertEquals(200, response.getStatusLine().getStatusCode());

        response = client().performRequest("get", "_cat/indices");
        assertEquals(200, response.getStatusLine().getStatusCode());
        String responseAsString = responseEntityToString(response);
        assertThat(responseAsString, containsString(indexName));
        assertThat(responseAsString, containsString(indexName + "-001"));
        assertThat(responseAsString, containsString(indexName + "-002"));

        // Add some documents to each index to make sure the DBQ clears them out
        String recordResult =
                String.format(Locale.ROOT,
                        "{\"job_id\":\"%s\", \"timestamp\": \"%s\", \"bucket_span\":%d, \"sequence_num\": %d, \"result_type\":\"record\"}",
                        jobId, 123, 1, 1);
        client().performRequest("put", indexName + "/result/" + 123,
                Collections.singletonMap("refresh", "true"), new StringEntity(recordResult, ContentType.APPLICATION_JSON));
        client().performRequest("put", indexName + "-001/result/" + 123,
                Collections.singletonMap("refresh", "true"), new StringEntity(recordResult, ContentType.APPLICATION_JSON));
        client().performRequest("put", indexName + "-002/result/" + 123,
                Collections.singletonMap("refresh", "true"), new StringEntity(recordResult, ContentType.APPLICATION_JSON));

        // Also index a few through the alias for the first job
        client().performRequest("put", indexName + "/result/" + 456,
                Collections.singletonMap("refresh", "true"), new StringEntity(recordResult, ContentType.APPLICATION_JSON));


        client().performRequest("post", "_refresh");

        // check for the documents
        response = client().performRequest("get", indexName+ "/_count");
        assertEquals(200, response.getStatusLine().getStatusCode());
        responseAsString = responseEntityToString(response);
        assertThat(responseAsString, containsString("\"count\":2"));

        response = client().performRequest("get", indexName + "-001/_count");
        assertEquals(200, response.getStatusLine().getStatusCode());
        responseAsString = responseEntityToString(response);
        assertThat(responseAsString, containsString("\"count\":1"));

        response = client().performRequest("get", indexName + "-002/_count");
        assertEquals(200, response.getStatusLine().getStatusCode());
        responseAsString = responseEntityToString(response);
        assertThat(responseAsString, containsString("\"count\":1"));

        // Delete
        response = client().performRequest("delete", MachineLearning.BASE_PATH + "anomaly_detectors/" + jobId);
        assertThat(response.getStatusLine().getStatusCode(), equalTo(200));

        client().performRequest("post", "_refresh");

        // check that the indices still exist but are empty
        response = client().performRequest("get", "_cat/indices");
        assertEquals(200, response.getStatusLine().getStatusCode());
        responseAsString = responseEntityToString(response);
        assertThat(responseAsString, containsString(indexName));
        assertThat(responseAsString, containsString(indexName + "-001"));
        assertThat(responseAsString, containsString(indexName + "-002"));

        response = client().performRequest("get", indexName + "/_count");
        assertEquals(200, response.getStatusLine().getStatusCode());
        responseAsString = responseEntityToString(response);
        assertThat(responseAsString, containsString("\"count\":0"));

        response = client().performRequest("get", indexName + "-001/_count");
        assertEquals(200, response.getStatusLine().getStatusCode());
        responseAsString = responseEntityToString(response);
        assertThat(responseAsString, containsString("\"count\":0"));

        response = client().performRequest("get", indexName + "-002/_count");
        assertEquals(200, response.getStatusLine().getStatusCode());
        responseAsString = responseEntityToString(response);
        assertThat(responseAsString, containsString("\"count\":0"));


        expectThrows(ResponseException.class, () ->
                client().performRequest("get", MachineLearning.BASE_PATH + "anomaly_detectors/" + jobId + "/_stats"));
    }

    private Response addBucketResult(String jobId, String timestamp, long bucketSpan) throws Exception {
        try {
            client().performRequest("put",
                    AnomalyDetectorsIndex.RESULTS_INDEX_PREFIX + AnomalyDetectorsIndex.RESULTS_INDEX_DEFAULT,
                    Collections.emptyMap(), new StringEntity(RESULT_MAPPING, ContentType.APPLICATION_JSON));
        } catch (ResponseException e) {
            // it is ok: the index already exists
            assertThat(e.getMessage(), containsString("resource_already_exists_exception"));
            assertThat(e.getResponse().getStatusLine().getStatusCode(), equalTo(400));
        }

        String bucketResult = String.format(Locale.ROOT,
                "{\"job_id\":\"%s\", \"timestamp\": \"%s\", \"result_type\":\"bucket\", \"bucket_span\": \"%s\"}",
                jobId, timestamp, bucketSpan);
        String id = String.format(Locale.ROOT,
                "%s_%s_%s", jobId, timestamp, bucketSpan);
        return client().performRequest("put", AnomalyDetectorsIndex.jobResultsAliasedName(jobId) + "/result/" + id,
                Collections.singletonMap("refresh", "true"), new StringEntity(bucketResult, ContentType.APPLICATION_JSON));
    }

    private Response addRecordResult(String jobId, String timestamp, long bucketSpan, int sequenceNum) throws Exception {
        try {
            client().performRequest("put",
                    AnomalyDetectorsIndex.RESULTS_INDEX_PREFIX + AnomalyDetectorsIndex.RESULTS_INDEX_DEFAULT,
                    Collections.emptyMap(), new StringEntity(RESULT_MAPPING, ContentType.APPLICATION_JSON));
        } catch (ResponseException e) {
            // it is ok: the index already exists
            assertThat(e.getMessage(), containsString("resource_already_exists_exception"));
            assertThat(e.getResponse().getStatusLine().getStatusCode(), equalTo(400));
        }

        String recordResult =
                String.format(Locale.ROOT,
                        "{\"job_id\":\"%s\", \"timestamp\": \"%s\", \"bucket_span\":%d, \"sequence_num\": %d, \"result_type\":\"record\"}",
                        jobId, timestamp, bucketSpan, sequenceNum);
        return client().performRequest("put", AnomalyDetectorsIndex.jobResultsAliasedName(jobId) + "/result/" + timestamp,
                Collections.singletonMap("refresh", "true"), new StringEntity(recordResult, ContentType.APPLICATION_JSON));
    }

    private static String responseEntityToString(Response response) throws Exception {
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(response.getEntity().getContent(), StandardCharsets.UTF_8))) {
            return reader.lines().collect(Collectors.joining("\n"));
        }
    }

    @After
    public void clearMlState() throws Exception {
        new MlRestTestStateCleaner(logger, adminClient(), this).clearMlMetadata();
    }
}
