/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.integration;

import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.common.util.concurrent.ConcurrentMapLong;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.SecuritySettingsSourceField;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xpack.core.ml.integration.MlRestTestStateCleaner;
import org.elasticsearch.xpack.ml.MachineLearning;
import org.elasticsearch.xpack.core.ml.job.persistence.AnomalyDetectorsIndex;
import org.elasticsearch.xpack.core.ml.job.persistence.AnomalyDetectorsIndexFields;
import org.elasticsearch.xpack.test.rest.XPackRestTestHelper;
import org.junit.After;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Locale;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken.basicAuthHeaderValue;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

public class MlJobIT extends ESRestTestCase {

    private static final String BASIC_AUTH_VALUE = basicAuthHeaderValue("x_pack_rest_user",
            SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING);

    @Override
    protected Settings restClientSettings() {
        return Settings.builder().put(super.restClientSettings()).put(ThreadContext.PREFIX + ".Authorization", BASIC_AUTH_VALUE).build();
    }

    @Override
    protected boolean preserveTemplatesUponCompletion() {
        return true;
    }

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

    private Response createFarequoteJob(String jobId) throws IOException {
        String job = "{\n" + "    \"description\":\"Analysis of response time by airline\",\n"
                + "    \"analysis_config\" : {\n" + "        \"bucket_span\": \"3600s\",\n"
                + "        \"detectors\" :[{\"function\":\"metric\",\"field_name\":\"responsetime\",\"by_field_name\":\"airline\"}]\n"
                + "    },\n" + "    \"data_description\" : {\n" + "        \"field_delimiter\":\",\",\n" + "        " +
                "\"time_field\":\"time\",\n"
                + "        \"time_format\":\"yyyy-MM-dd HH:mm:ssX\"\n" + "    }\n" + "}";

        return client().performRequest("put", MachineLearning.BASE_PATH + "anomaly_detectors/" + jobId,
                Collections.emptyMap(), new StringEntity(job, ContentType.APPLICATION_JSON));
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

        // With security enabled GET _aliases throws an index_not_found_exception
        // if no aliases have been created. In multi-node tests the alias may not
        // appear immediately so wait here.
        assertBusy(() -> {
            try {
                Response aliasesResponse = client().performRequest("get", "_aliases");
                assertEquals(200, aliasesResponse.getStatusLine().getStatusCode());
                String responseAsString = responseEntityToString(aliasesResponse);
                assertThat(responseAsString,
                        containsString("\"" + AnomalyDetectorsIndex.jobResultsAliasedName("custom-" + indexName) + "\":{\"aliases\":{"));
                assertThat(responseAsString, containsString("\"" + AnomalyDetectorsIndex.jobResultsAliasedName(jobId1)
                        + "\":{\"filter\":{\"term\":{\"job_id\":{\"value\":\"" + jobId1 + "\",\"boost\":1.0}}}}"));
                assertThat(responseAsString, containsString("\"" + AnomalyDetectorsIndex.resultsWriteAlias(jobId1) + "\":{}"));
                assertThat(responseAsString, containsString("\"" + AnomalyDetectorsIndex.jobResultsAliasedName(jobId2)
                        + "\":{\"filter\":{\"term\":{\"job_id\":{\"value\":\"" + jobId2 + "\",\"boost\":1.0}}}}"));
                assertThat(responseAsString, containsString("\"" + AnomalyDetectorsIndex.resultsWriteAlias(jobId2) + "\":{}"));
            } catch (ResponseException e) {
                throw new AssertionError(e);
            }
        });

        Response indicesResponse = client().performRequest("get", "_cat/indices");
        assertEquals(200, indicesResponse.getStatusLine().getStatusCode());
        String responseAsString = responseEntityToString(indicesResponse);
        assertThat(responseAsString,
                containsString(AnomalyDetectorsIndexFields.RESULTS_INDEX_PREFIX + "custom-" + indexName));
        assertThat(responseAsString, not(containsString(AnomalyDetectorsIndex.jobResultsAliasedName(jobId1))));
        assertThat(responseAsString, not(containsString(AnomalyDetectorsIndex.jobResultsAliasedName(jobId2))));

        String bucketResult = String.format(Locale.ROOT,
                "{\"job_id\":\"%s\", \"timestamp\": \"%s\", \"result_type\":\"bucket\", \"bucket_span\": \"%s\"}",
                jobId1, "1234", 1);
        String id = String.format(Locale.ROOT, "%s_bucket_%s_%s", jobId1, "1234", 300);
        response = client().performRequest("put", AnomalyDetectorsIndex.jobResultsAliasedName(jobId1) + "/doc/" + id,
                Collections.emptyMap(), new StringEntity(bucketResult, ContentType.APPLICATION_JSON));
        assertEquals(201, response.getStatusLine().getStatusCode());

        bucketResult = String.format(Locale.ROOT,
                "{\"job_id\":\"%s\", \"timestamp\": \"%s\", \"result_type\":\"bucket\", \"bucket_span\": \"%s\"}",
                jobId1, "1236", 1);
        id = String.format(Locale.ROOT, "%s_bucket_%s_%s", jobId1, "1236", 300);
        response = client().performRequest("put", AnomalyDetectorsIndex.jobResultsAliasedName(jobId1) + "/doc/" + id,
                Collections.emptyMap(), new StringEntity(bucketResult, ContentType.APPLICATION_JSON));
        assertEquals(201, response.getStatusLine().getStatusCode());

        client().performRequest("post", "_refresh");

        response = client().performRequest("get", MachineLearning.BASE_PATH + "anomaly_detectors/" + jobId1 + "/results/buckets");
        assertThat(response.getStatusLine().getStatusCode(), equalTo(200));
        responseAsString = responseEntityToString(response);
        assertThat(responseAsString, containsString("\"count\":2"));

        response = client().performRequest("get", AnomalyDetectorsIndex.jobResultsAliasedName(jobId1) + "/_search");
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
        assertThat(responseAsString, containsString(AnomalyDetectorsIndexFields.RESULTS_INDEX_PREFIX + "custom-" + indexName));

        client().performRequest("post", "_refresh");

        response = client().performRequest("get", AnomalyDetectorsIndexFields.RESULTS_INDEX_PREFIX + "custom-" + indexName + "/_count");
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
        response = client().performRequest("get", AnomalyDetectorsIndexFields.RESULTS_INDEX_PREFIX
                + AnomalyDetectorsIndexFields.RESULTS_INDEX_DEFAULT + "/_mapping?pretty");
        assertEquals(200, response.getStatusLine().getStatusCode());
        String responseAsString = responseEntityToString(response);
        assertThat(responseAsString, containsString(byFieldName1));
        assertThat(responseAsString, not(containsString(byFieldName2)));

        jobConfig = String.format(Locale.ROOT, jobTemplate, byFieldName2);
        response = client().performRequest("put", MachineLearning.BASE_PATH
                + "anomaly_detectors/" + jobId2, Collections.emptyMap(), new StringEntity(jobConfig, ContentType.APPLICATION_JSON));
        assertEquals(200, response.getStatusLine().getStatusCode());

        // Check the index mapping now contains both fields
        response = client().performRequest("get", AnomalyDetectorsIndexFields.RESULTS_INDEX_PREFIX
                + AnomalyDetectorsIndexFields.RESULTS_INDEX_DEFAULT + "/_mapping?pretty");
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
        response = client().performRequest("get",
                AnomalyDetectorsIndexFields.RESULTS_INDEX_PREFIX + "custom-shared-index" + "/_mapping?pretty");
        assertEquals(200, response.getStatusLine().getStatusCode());
        String responseAsString = responseEntityToString(response);
        assertThat(responseAsString, containsString(byFieldName1));
        assertThat(responseAsString, not(containsString(byFieldName2)));

        jobConfig = String.format(Locale.ROOT, jobTemplate, byFieldName2);
        response = client().performRequest("put", MachineLearning.BASE_PATH
                + "anomaly_detectors/" + jobId2, Collections.emptyMap(), new StringEntity(jobConfig, ContentType.APPLICATION_JSON));
        assertEquals(200, response.getStatusLine().getStatusCode());

        // Check the index mapping now contains both fields
        response = client().performRequest("get",
                AnomalyDetectorsIndexFields.RESULTS_INDEX_PREFIX + "custom-shared-index" + "/_mapping?pretty");
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
        String byFieldName1;
        String jobId2 = "job-will-fail-with-mapping-error-on-response-field";
        String byFieldName2;
        // we should get the friendly advice nomatter which way around the clashing fields are seen
        if (randomBoolean()) {
            byFieldName1 = "response";
            byFieldName2 = "response.time";
        } else {
            byFieldName1 = "response.time";
            byFieldName2 = "response";
        }
        String jobConfig = String.format(Locale.ROOT, jobTemplate, byFieldName1);

        Response response = client().performRequest("put", MachineLearning.BASE_PATH
                + "anomaly_detectors/" + jobId1, Collections.emptyMap(), new StringEntity(jobConfig, ContentType.APPLICATION_JSON));
        assertEquals(200, response.getStatusLine().getStatusCode());

        final String failingJobConfig = String.format(Locale.ROOT, jobTemplate, byFieldName2);
        ResponseException e = expectThrows(ResponseException.class,
                () -> client().performRequest("put", MachineLearning.BASE_PATH + "anomaly_detectors/" + jobId2,
                        Collections.emptyMap(), new StringEntity(failingJobConfig, ContentType.APPLICATION_JSON)));

        assertThat(e.getMessage(),
                containsString("This job would cause a mapping clash with existing field [response] - " +
                        "avoid the clash by assigning a dedicated results index"));
    }

    public void testDeleteJob() throws Exception {
        String jobId = "delete-job-job";
        String indexName = AnomalyDetectorsIndexFields.RESULTS_INDEX_PREFIX + AnomalyDetectorsIndexFields.RESULTS_INDEX_DEFAULT;
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
        String indexName = AnomalyDetectorsIndexFields.RESULTS_INDEX_PREFIX + AnomalyDetectorsIndexFields.RESULTS_INDEX_DEFAULT;
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

    public void testDeleteJobAfterMissingAliases() throws Exception {
        String jobId = "delete-job-after-missing-alias-job";
        String readAliasName = AnomalyDetectorsIndex.jobResultsAliasedName(jobId);
        String writeAliasName = AnomalyDetectorsIndex.resultsWriteAlias(jobId);
        String indexName = AnomalyDetectorsIndexFields.RESULTS_INDEX_PREFIX + AnomalyDetectorsIndexFields.RESULTS_INDEX_DEFAULT;
        createFarequoteJob(jobId);

        // With security enabled cat aliases throws an index_not_found_exception
        // if no aliases have been created. In multi-node tests the alias may not
        // appear immediately so wait here.
        assertBusy(() -> {
            try {
                Response aliasesResponse = client().performRequest(new Request("get", "_cat/aliases"));
                assertEquals(200, aliasesResponse.getStatusLine().getStatusCode());
                String responseAsString = responseEntityToString(aliasesResponse);
                assertThat(responseAsString, containsString(readAliasName));
                assertThat(responseAsString, containsString(writeAliasName));
            } catch (ResponseException e) {
                throw new AssertionError(e);
            }
        });

        // Manually delete the aliases so that we can test that deletion proceeds
        // normally anyway
        Response response = client().performRequest("delete", indexName + "/_alias/" + readAliasName);
        assertEquals(200, response.getStatusLine().getStatusCode());
        response = client().performRequest("delete", indexName + "/_alias/" + writeAliasName);
        assertEquals(200, response.getStatusLine().getStatusCode());

        // check aliases were deleted
        expectThrows(ResponseException.class, () -> client().performRequest("get", indexName + "/_alias/" + readAliasName));
        expectThrows(ResponseException.class, () -> client().performRequest("get", indexName + "/_alias/" + writeAliasName));

        response = client().performRequest("delete", MachineLearning.BASE_PATH + "anomaly_detectors/" + jobId);
        assertThat(response.getStatusLine().getStatusCode(), equalTo(200));
    }

    public void testMultiIndexDelete() throws Exception {
        String jobId = "multi-index-delete-job";
        String indexName = AnomalyDetectorsIndexFields.RESULTS_INDEX_PREFIX + AnomalyDetectorsIndexFields.RESULTS_INDEX_DEFAULT;
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
                        "{\"job_id\":\"%s\", \"timestamp\": \"%s\", \"bucket_span\":%d, \"result_type\":\"record\"}",
                        jobId, 123, 1);
        client().performRequest("put", indexName + "/doc/" + 123,
                Collections.singletonMap("refresh", "true"), new StringEntity(recordResult, ContentType.APPLICATION_JSON));
        client().performRequest("put", indexName + "-001/doc/" + 123,
                Collections.singletonMap("refresh", "true"), new StringEntity(recordResult, ContentType.APPLICATION_JSON));
        client().performRequest("put", indexName + "-002/doc/" + 123,
                Collections.singletonMap("refresh", "true"), new StringEntity(recordResult, ContentType.APPLICATION_JSON));

        // Also index a few through the alias for the first job
        client().performRequest("put", indexName + "/doc/" + 456,
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

    public void testDelete_multipleRequest() throws Exception {
        String jobId = "delete-job-mulitple-times";
        createFarequoteJob(jobId);

        ConcurrentMapLong<Response> responses = ConcurrentCollections.newConcurrentMapLong();
        ConcurrentMapLong<ResponseException> responseExceptions = ConcurrentCollections.newConcurrentMapLong();
        AtomicReference<IOException> ioe = new AtomicReference<>();
        AtomicInteger recreationGuard = new AtomicInteger(0);
        AtomicReference<Response> recreationResponse = new AtomicReference<>();
        AtomicReference<ResponseException> recreationException = new AtomicReference<>();

        Runnable deleteJob = () -> {
            try {
                boolean forceDelete = randomBoolean();
                String url = MachineLearning.BASE_PATH + "anomaly_detectors/" + jobId;
                if (forceDelete) {
                    url += "?force=true";
                }
                Response response = client().performRequest("delete", url);
                responses.put(Thread.currentThread().getId(), response);
            } catch (ResponseException re) {
                responseExceptions.put(Thread.currentThread().getId(), re);
            } catch (IOException e) {
                ioe.set(e);
            }

            // Immediately after the first deletion finishes, recreate the job.  This should pick up
            // race conditions where another delete request deletes part of the newly created job.
            if (recreationGuard.getAndIncrement() == 0) {
                try {
                    recreationResponse.set(createFarequoteJob(jobId));
                } catch (ResponseException re) {
                    recreationException.set(re);
                } catch (IOException e) {
                    ioe.set(e);
                }
            }
        };

        // The idea is to hit the situation where one request waits for
        // the other to complete. This is difficult to schedule but
        // hopefully it will happen in CI
        int numThreads = 5;
        Thread [] threads = new Thread[numThreads];
        for (int i=0; i<numThreads; i++) {
            threads[i] = new Thread(deleteJob);
        }
        for (int i=0; i<numThreads; i++) {
            threads[i].start();
        }
        for (int i=0; i<numThreads; i++) {
            threads[i].join();
        }

        if (ioe.get() != null) {
            // This looks redundant but the check is done so we can
            // print the exception's error message
            assertNull(ioe.get().getMessage(), ioe.get());
        }

        assertEquals(numThreads, responses.size() + responseExceptions.size());

        // 404s are ok as it means the job had already been deleted.
        for (ResponseException re : responseExceptions.values()) {
            assertEquals(re.getMessage(), 404, re.getResponse().getStatusLine().getStatusCode());
        }

        for (Response response : responses.values()) {
            assertEquals(responseEntityToString(response), 200, response.getStatusLine().getStatusCode());
        }

        assertNotNull(recreationResponse.get());
        assertEquals(responseEntityToString(recreationResponse.get()), 200, recreationResponse.get().getStatusLine().getStatusCode());

        if (recreationException.get() != null) {
            assertNull(recreationException.get().getMessage(), recreationException.get());
        }

        try {
            // The idea of the code above is that the deletion is sufficiently time-consuming that
            // all threads enter the deletion call before the first one exits it.  Usually this happens,
            // but in the case that it does not the job that is recreated may get deleted.
            // It is not a error if the job does not exist but the following assertions
            // will fail in that case.
            client().performRequest("get", MachineLearning.BASE_PATH + "anomaly_detectors/" + jobId);

            // Check that the job aliases exist.  These are the last thing to be deleted when a job is deleted, so
            // if there's been a race between deletion and recreation these are what will be missing.
            String aliases = getAliases();

            assertThat(aliases, containsString("\"" + AnomalyDetectorsIndex.jobResultsAliasedName(jobId)
                    + "\":{\"filter\":{\"term\":{\"job_id\":{\"value\":\"" + jobId + "\",\"boost\":1.0}}}}"));
            assertThat(aliases, containsString("\"" + AnomalyDetectorsIndex.resultsWriteAlias(jobId) + "\":{}"));


        } catch (ResponseException missingJobException) {
            // The job does not exist
            assertThat(missingJobException.getResponse().getStatusLine().getStatusCode(), equalTo(404));

            // The job aliases should be deleted
            String aliases = getAliases();
            assertThat(aliases, not(containsString("\"" + AnomalyDetectorsIndex.jobResultsAliasedName(jobId)
                    + "\":{\"filter\":{\"term\":{\"job_id\":{\"value\":\"" + jobId + "\",\"boost\":1.0}}}}")));
            assertThat(aliases, not(containsString("\"" + AnomalyDetectorsIndex.resultsWriteAlias(jobId) + "\":{}")));
        }

        assertEquals(numThreads, recreationGuard.get());
    }

    private String getAliases() throws IOException {
        Response response = client().performRequest("get", "_aliases");
        assertEquals(200, response.getStatusLine().getStatusCode());
        return responseEntityToString(response);
    }

    private static String responseEntityToString(Response response) throws IOException {
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(response.getEntity().getContent(), StandardCharsets.UTF_8))) {
            return reader.lines().collect(Collectors.joining("\n"));
        }
    }

    @After
    public void clearMlState() throws Exception {
        new MlRestTestStateCleaner(logger, adminClient()).clearMlMetadata();
        XPackRestTestHelper.waitForPendingTasks(adminClient());
    }
}
