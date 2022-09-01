/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.integration;

import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.SecuritySettingsSourceField;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xpack.core.ml.MlTasks;
import org.elasticsearch.xpack.core.ml.integration.MlRestTestStateCleaner;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.core.ml.job.persistence.AnomalyDetectorsIndex;
import org.elasticsearch.xpack.core.ml.job.persistence.AnomalyDetectorsIndexFields;
import org.elasticsearch.xpack.core.ml.job.process.autodetect.state.TimingStats;
import org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken;
import org.elasticsearch.xpack.ml.MachineLearning;
import org.junit.After;

import java.io.IOException;
import java.util.Collections;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.not;

public class MlJobIT extends ESRestTestCase {

    private static final String BASIC_AUTH_VALUE = UsernamePasswordToken.basicAuthHeaderValue(
        "x_pack_rest_user",
        SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING
    );
    private static final RequestOptions POST_DATA = RequestOptions.DEFAULT.toBuilder()
        .setWarningsHandler(
            warnings -> Collections.singletonList(
                "Posting data directly to anomaly detection jobs is deprecated, "
                    + "in a future major version it will be compulsory to use a datafeed"
            ).equals(warnings) == false
        )
        .build();

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
        String responseAsString = EntityUtils.toString(response.getEntity());
        assertThat(responseAsString, containsString("\"job_id\":\"given-farequote-config-job\""));
    }

    public void testGetJob_GivenNoSuchJob() {
        ResponseException e = expectThrows(
            ResponseException.class,
            () -> client().performRequest(new Request("GET", MachineLearning.BASE_PATH + "anomaly_detectors/non-existing-job/_stats"))
        );

        assertThat(e.getResponse().getStatusLine().getStatusCode(), equalTo(404));
        assertThat(e.getMessage(), containsString("No known job with id 'non-existing-job'"));
    }

    public void testGetJob_GivenJobExists() throws Exception {
        createFarequoteJob("get-job_given-job-exists-job");

        Response response = client().performRequest(
            new Request("GET", MachineLearning.BASE_PATH + "anomaly_detectors/get-job_given-job-exists-job/_stats")
        );
        String responseAsString = EntityUtils.toString(response.getEntity());
        assertThat(responseAsString, containsString("\"count\":1"));
        assertThat(responseAsString, containsString("\"job_id\":\"get-job_given-job-exists-job\""));
    }

    public void testGetJobs_GivenSingleJob() throws Exception {
        String jobId = "get-jobs_given-single-job-job";
        createFarequoteJob(jobId);

        // Explicit _all
        String explictAll = EntityUtils.toString(
            client().performRequest(new Request("GET", MachineLearning.BASE_PATH + "anomaly_detectors/_all")).getEntity()
        );
        assertThat(explictAll, containsString("\"count\":1"));
        assertThat(explictAll, containsString("\"job_id\":\"" + jobId + "\""));

        // Implicit _all
        String implicitAll = EntityUtils.toString(
            client().performRequest(new Request("GET", MachineLearning.BASE_PATH + "anomaly_detectors")).getEntity()
        );
        assertThat(implicitAll, containsString("\"count\":1"));
        assertThat(implicitAll, containsString("\"job_id\":\"" + jobId + "\""));
    }

    public void testGetJobs_GivenMultipleJobs() throws Exception {
        createFarequoteJob("given-multiple-jobs-job-1");
        createFarequoteJob("given-multiple-jobs-job-2");
        createFarequoteJob("given-multiple-jobs-job-3");

        // Explicit _all
        String explicitAll = EntityUtils.toString(
            client().performRequest(new Request("GET", MachineLearning.BASE_PATH + "anomaly_detectors/_all")).getEntity()
        );
        assertThat(explicitAll, containsString("\"count\":3"));
        assertThat(explicitAll, containsString("\"job_id\":\"given-multiple-jobs-job-1\""));
        assertThat(explicitAll, containsString("\"job_id\":\"given-multiple-jobs-job-2\""));
        assertThat(explicitAll, containsString("\"job_id\":\"given-multiple-jobs-job-3\""));

        // Implicit _all
        String implicitAll = EntityUtils.toString(
            client().performRequest(new Request("GET", MachineLearning.BASE_PATH + "anomaly_detectors")).getEntity()
        );
        assertThat(implicitAll, containsString("\"count\":3"));
        assertThat(implicitAll, containsString("\"job_id\":\"given-multiple-jobs-job-1\""));
        assertThat(implicitAll, containsString("\"job_id\":\"given-multiple-jobs-job-2\""));
        assertThat(implicitAll, containsString("\"job_id\":\"given-multiple-jobs-job-3\""));
    }

    // tests the _xpack/usage endpoint
    public void testUsage() throws IOException {
        createFarequoteJob("job-1");
        createFarequoteJob("job-2");
        Map<String, Object> usage = entityAsMap(client().performRequest(new Request("GET", "_xpack/usage")));
        assertEquals(2, XContentMapValues.extractValue("ml.jobs._all.count", usage));
        assertEquals(2, XContentMapValues.extractValue("ml.jobs.closed.count", usage));
        openJob("job-1");
        usage = entityAsMap(client().performRequest(new Request("GET", "_xpack/usage")));
        assertEquals(2, XContentMapValues.extractValue("ml.jobs._all.count", usage));
        assertEquals(1, XContentMapValues.extractValue("ml.jobs.closed.count", usage));
        assertEquals(1, XContentMapValues.extractValue("ml.jobs.opened.count", usage));
    }

    public void testOpenJob_GivenTimeout_Returns408() throws IOException {
        String jobId = "test-timeout-returns-408";
        createFarequoteJob(jobId);

        ResponseException e = expectThrows(ResponseException.class, () -> openJob(jobId, Optional.of(TimeValue.timeValueNanos(1L))));

        assertThat(e.getResponse().getStatusLine().getStatusCode(), equalTo(RestStatus.REQUEST_TIMEOUT.getStatus()));
    }

    private Response createFarequoteJob(String jobId) throws IOException {
        return putJob(jobId, """
            {
                "description":"Analysis of response time by airline",
                "analysis_config" : {
                    "bucket_span": "3600s",
                    "detectors" :[{"function":"metric","field_name":"responsetime","by_field_name":"airline"}]
                },
                "data_description" : {
                    "time_field":"time",
                    "time_format":"yyyy-MM-dd HH:mm:ssX"
                }
            }""");
    }

    public void testCantCreateJobWithSameID() throws Exception {
        String jobTemplate = """
            {
              "analysis_config" : {
                    "detectors" :[{"function":"metric","field_name":"responsetime"}]
                },
              "data_description": {},
              "results_index_name" : "%s"}""";

        String jobId = "cant-create-job-with-same-id-job";
        putJob(jobId, String.format(Locale.ROOT, jobTemplate, "index-1"));
        ResponseException e = expectThrows(
            ResponseException.class,
            () -> putJob(jobId, String.format(Locale.ROOT, jobTemplate, "index-2"))
        );

        assertThat(e.getResponse().getStatusLine().getStatusCode(), equalTo(400));
        assertThat(e.getMessage(), containsString("The job cannot be created with the Id '" + jobId + "'. The Id is already used."));
    }

    public void testCreateJobsWithIndexNameOption() throws Exception {
        String jobTemplate = """
            {
              "analysis_config" : {
                    "detectors" :[{"function":"metric","field_name":"responsetime"}]
                },
              "data_description": {},
              "results_index_name" : "%s"}""";

        String jobId1 = "create-jobs-with-index-name-option-job-1";
        String indexName = "non-default-index";
        putJob(jobId1, String.format(Locale.ROOT, jobTemplate, indexName));

        String jobId2 = "create-jobs-with-index-name-option-job-2";
        putJob(jobId2, String.format(Locale.ROOT, jobTemplate, indexName));

        // With security enabled GET _aliases throws an index_not_found_exception
        // if no aliases have been created. In multi-node tests the alias may not
        // appear immediately so wait here.
        assertBusy(() -> {
            try {
                String aliasesResponse = getAliases();
                assertThat(aliasesResponse, containsString("""
                    "%s":{"aliases":{""".formatted(AnomalyDetectorsIndex.jobResultsAliasedName("custom-" + indexName))));
                assertThat(
                    aliasesResponse,
                    containsString(
                        """
                            "%s":{"filter":{"term":{"job_id":{"value":"%s"}}},"is_hidden":true}""".formatted(
                            AnomalyDetectorsIndex.jobResultsAliasedName(jobId1),
                            jobId1
                        )
                    )
                );
                assertThat(aliasesResponse, containsString("""
                    "%s":{"is_hidden":true}""".formatted(AnomalyDetectorsIndex.resultsWriteAlias(jobId1))));
                assertThat(
                    aliasesResponse,
                    containsString(
                        """
                            "%s":{"filter":{"term":{"job_id":{"value":"%s"}}},"is_hidden":true}""".formatted(
                            AnomalyDetectorsIndex.jobResultsAliasedName(jobId2),
                            jobId2
                        )
                    )
                );
                assertThat(aliasesResponse, containsString("""
                    "%s":{"is_hidden":true}""".formatted(AnomalyDetectorsIndex.resultsWriteAlias(jobId2))));
            } catch (ResponseException e) {
                throw new AssertionError(e);
            }
        });

        // Use _cat/indices/.ml-anomalies-* instead of _cat/indices/_all to workaround https://github.com/elastic/elasticsearch/issues/45652
        String responseAsString = EntityUtils.toString(
            client().performRequest(new Request("GET", "/_cat/indices/" + AnomalyDetectorsIndexFields.RESULTS_INDEX_PREFIX + "*"))
                .getEntity()
        );
        assertThat(responseAsString, containsString(AnomalyDetectorsIndexFields.RESULTS_INDEX_PREFIX + "custom-" + indexName));
        assertThat(responseAsString, not(containsString(AnomalyDetectorsIndex.jobResultsAliasedName(jobId1))));
        assertThat(responseAsString, not(containsString(AnomalyDetectorsIndex.jobResultsAliasedName(jobId2))));

        { // create jobId1 docs
            String id = String.format(Locale.ROOT, "%s_bucket_%s_%s", jobId1, "1234", 300);
            Request createResultRequest = new Request("PUT", AnomalyDetectorsIndex.jobResultsAliasedName(jobId1) + "/_doc/" + id);
            createResultRequest.setJsonEntity(String.format(Locale.ROOT, """
                {"job_id":"%s", "timestamp": "%s", "result_type":"bucket", "bucket_span": "%s"}""", jobId1, "1234", 1));
            client().performRequest(createResultRequest);

            id = String.format(Locale.ROOT, "%s_bucket_%s_%s", jobId1, "1236", 300);
            createResultRequest = new Request("PUT", AnomalyDetectorsIndex.jobResultsAliasedName(jobId1) + "/_doc/" + id);
            createResultRequest.setJsonEntity(String.format(Locale.ROOT, """
                {"job_id":"%s", "timestamp": "%s", "result_type":"bucket", "bucket_span": "%s"}""", jobId1, "1236", 1));
            client().performRequest(createResultRequest);

            refreshAllIndices();

            responseAsString = EntityUtils.toString(
                client().performRequest(new Request("GET", MachineLearning.BASE_PATH + "anomaly_detectors/" + jobId1 + "/results/buckets"))
                    .getEntity()
            );
            assertThat(responseAsString, containsString("\"count\":2"));

            responseAsString = EntityUtils.toString(
                client().performRequest(new Request("GET", AnomalyDetectorsIndex.jobResultsAliasedName(jobId1) + "/_search")).getEntity()
            );
            assertThat(responseAsString, containsString("\"value\":2"));
        }
        { // create jobId2 docs
            String id = String.format(Locale.ROOT, "%s_bucket_%s_%s", jobId2, "1234", 300);
            Request createResultRequest = new Request("PUT", AnomalyDetectorsIndex.jobResultsAliasedName(jobId2) + "/_doc/" + id);
            createResultRequest.setJsonEntity(String.format(Locale.ROOT, """
                {"job_id":"%s", "timestamp": "%s", "result_type":"bucket", "bucket_span": "%s"}""", jobId2, "1234", 1));
            client().performRequest(createResultRequest);

            id = String.format(Locale.ROOT, "%s_bucket_%s_%s", jobId2, "1236", 300);
            createResultRequest = new Request("PUT", AnomalyDetectorsIndex.jobResultsAliasedName(jobId2) + "/_doc/" + id);
            createResultRequest.setJsonEntity(String.format(Locale.ROOT, """
                {"job_id":"%s", "timestamp": "%s", "result_type":"bucket", "bucket_span": "%s"}""", jobId2, "1236", 1));
            client().performRequest(createResultRequest);

            refreshAllIndices();

            responseAsString = EntityUtils.toString(
                client().performRequest(new Request("GET", MachineLearning.BASE_PATH + "anomaly_detectors/" + jobId2 + "/results/buckets"))
                    .getEntity()
            );
            assertThat(responseAsString, containsString("\"count\":2"));

            responseAsString = EntityUtils.toString(
                client().performRequest(new Request("GET", AnomalyDetectorsIndex.jobResultsAliasedName(jobId2) + "/_search")).getEntity()
            );
            assertThat(responseAsString, containsString("\"value\":2"));
        }

        client().performRequest(new Request("DELETE", MachineLearning.BASE_PATH + "anomaly_detectors/" + jobId1));

        // check that indices still exist, but no longer have job1 entries and aliases are gone
        responseAsString = getAliases();
        assertThat(responseAsString, not(containsString(AnomalyDetectorsIndex.jobResultsAliasedName(jobId1))));
        assertThat(responseAsString, containsString(AnomalyDetectorsIndex.jobResultsAliasedName(jobId2))); // job2 still exists

        responseAsString = EntityUtils.toString(
            client().performRequest(new Request("GET", "/_cat/indices/" + AnomalyDetectorsIndexFields.RESULTS_INDEX_PREFIX + "*"))
                .getEntity()
        );
        assertThat(responseAsString, containsString(AnomalyDetectorsIndexFields.RESULTS_INDEX_PREFIX + "custom-" + indexName));

        refreshAllIndices();

        responseAsString = EntityUtils.toString(
            client().performRequest(
                new Request("GET", AnomalyDetectorsIndexFields.RESULTS_INDEX_PREFIX + "custom-" + indexName + "/_count")
            ).getEntity()
        );
        assertThat(responseAsString, containsString("\"count\":2"));

        // Delete the second job and verify aliases are gone, and original concrete/custom index is gone
        client().performRequest(new Request("DELETE", MachineLearning.BASE_PATH + "anomaly_detectors/" + jobId2));
        responseAsString = getAliases();
        assertThat(responseAsString, not(containsString(AnomalyDetectorsIndex.jobResultsAliasedName(jobId2))));

        refreshAllIndices();
        responseAsString = EntityUtils.toString(
            client().performRequest(new Request("GET", "/_cat/indices/" + AnomalyDetectorsIndexFields.RESULTS_INDEX_PREFIX + "*"))
                .getEntity()
        );
        assertThat(responseAsString, not(containsString(AnomalyDetectorsIndexFields.RESULTS_INDEX_PREFIX + "custom-" + indexName)));
    }

    public void testCreateJobInSharedIndexUpdatesMapping() throws Exception {
        String jobTemplate = """
            {
              "analysis_config" : {
                    "detectors" :[{"function":"metric","field_name":"metric", "by_field_name":"%s"}]
                },
              "data_description": {}
            }""";

        String jobId1 = "create-job-in-shared-index-updates-mapping-job-1";
        String byFieldName1 = "responsetime";
        String jobId2 = "create-job-in-shared-index-updates-mapping-job-2";
        String byFieldName2 = "cpu-usage";

        putJob(jobId1, String.format(Locale.ROOT, jobTemplate, byFieldName1));

        // Check the index mapping contains the first by_field_name
        Request getResultsMappingRequest = new Request(
            "GET",
            AnomalyDetectorsIndexFields.RESULTS_INDEX_PREFIX + AnomalyDetectorsIndexFields.RESULTS_INDEX_DEFAULT + "/_mapping"
        );
        getResultsMappingRequest.addParameter("pretty", null);
        String resultsMappingAfterJob1 = EntityUtils.toString(client().performRequest(getResultsMappingRequest).getEntity());
        assertThat(resultsMappingAfterJob1, containsString(byFieldName1));
        assertThat(resultsMappingAfterJob1, not(containsString(byFieldName2)));

        putJob(jobId2, String.format(Locale.ROOT, jobTemplate, byFieldName2));
        // Check the index mapping now contains both fields
        String resultsMappingAfterJob2 = EntityUtils.toString(client().performRequest(getResultsMappingRequest).getEntity());
        assertThat(resultsMappingAfterJob2, containsString(byFieldName1));
        assertThat(resultsMappingAfterJob2, containsString(byFieldName2));
    }

    public void testCreateJobInCustomSharedIndexUpdatesMapping() throws Exception {
        String jobTemplate = """
            {
              "analysis_config" : {
                    "detectors" :[{"function":"metric","field_name":"metric", "by_field_name":"%s"}]
              },
              "data_description": {},
              "results_index_name" : "shared-index"}""";

        String jobId1 = "create-job-in-custom-shared-index-updates-mapping-job-1";
        String byFieldName1 = "responsetime";
        String jobId2 = "create-job-in-custom-shared-index-updates-mapping-job-2";
        String byFieldName2 = "cpu-usage";

        putJob(jobId1, String.format(Locale.ROOT, jobTemplate, byFieldName1));

        // Check the index mapping contains the first by_field_name
        Request getResultsMappingRequest = new Request(
            "GET",
            AnomalyDetectorsIndexFields.RESULTS_INDEX_PREFIX + "custom-shared-index/_mapping"
        );
        getResultsMappingRequest.addParameter("pretty", null);
        String resultsMappingAfterJob1 = EntityUtils.toString(client().performRequest(getResultsMappingRequest).getEntity());
        assertThat(resultsMappingAfterJob1, containsString(byFieldName1));
        assertThat(resultsMappingAfterJob1, not(containsString(byFieldName2)));

        putJob(jobId2, String.format(Locale.ROOT, jobTemplate, byFieldName2));

        // Check the index mapping now contains both fields
        String resultsMappingAfterJob2 = EntityUtils.toString(client().performRequest(getResultsMappingRequest).getEntity());
        assertThat(resultsMappingAfterJob2, containsString(byFieldName1));
        assertThat(resultsMappingAfterJob2, containsString(byFieldName2));
    }

    public void testCreateJob_WithClashingFieldMappingsFails() throws Exception {
        String jobTemplate = """
            {
              "analysis_config" : {
                    "detectors" :[{"function":"metric","field_name":"metric", "by_field_name":"%s"}]
                },
              "data_description": {}
            }""";

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

        putJob(jobId1, String.format(Locale.ROOT, jobTemplate, byFieldName1));

        ResponseException e = expectThrows(
            ResponseException.class,
            () -> putJob(jobId2, String.format(Locale.ROOT, jobTemplate, byFieldName2))
        );
        assertThat(
            e.getMessage(),
            containsString(
                "This job would cause a mapping clash with existing field [response] - "
                    + "avoid the clash by assigning a dedicated results index"
            )
        );
    }

    public void testOpenJobFailsWhenPersistentTaskAssignmentDisabled() throws Exception {
        String jobId = "open-job-with-persistent-task-assignment-disabled";
        createFarequoteJob(jobId);

        Request disablePersistentTaskAssignmentRequest = new Request("PUT", "_cluster/settings");
        disablePersistentTaskAssignmentRequest.setJsonEntity("""
            {
              "persistent": {
                "cluster.persistent_tasks.allocation.enable": "none"
              }
            }""");
        Response disablePersistentTaskAssignmentResponse = client().performRequest(disablePersistentTaskAssignmentRequest);
        assertThat(entityAsMap(disablePersistentTaskAssignmentResponse), hasEntry("acknowledged", true));

        try {
            ResponseException exception = expectThrows(ResponseException.class, () -> openJob(jobId));
            assertThat(exception.getResponse().getStatusLine().getStatusCode(), equalTo(429));
            assertThat(
                EntityUtils.toString(exception.getResponse().getEntity()),
                containsString(
                    "Cannot open jobs because persistent task assignment is disabled by the "
                        + "[cluster.persistent_tasks.allocation.enable] setting"
                )
            );
        } finally {
            // Try to revert the cluster setting change even if the test fails,
            // because otherwise this setting will cause many other tests to fail
            Request enablePersistentTaskAssignmentRequest = new Request("PUT", "_cluster/settings");
            enablePersistentTaskAssignmentRequest.setJsonEntity("""
                {
                  "persistent": {
                    "cluster.persistent_tasks.allocation.enable": "all"
                  }
                }""");
            Response enablePersistentTaskAssignmentResponse = client().performRequest(disablePersistentTaskAssignmentRequest);
            assertThat(entityAsMap(enablePersistentTaskAssignmentResponse), hasEntry("acknowledged", true));
        }
    }

    public void testDeleteJob() throws Exception {
        String jobId = "delete-job-job";
        String indexName = AnomalyDetectorsIndexFields.RESULTS_INDEX_PREFIX + AnomalyDetectorsIndexFields.RESULTS_INDEX_DEFAULT;
        createFarequoteJob(jobId);

        // Use _cat/indices/.ml-anomalies-* instead of _cat/indices/_all to workaround https://github.com/elastic/elasticsearch/issues/45652
        String indicesBeforeDelete = EntityUtils.toString(
            client().performRequest(new Request("GET", "/_cat/indices/" + AnomalyDetectorsIndexFields.RESULTS_INDEX_PREFIX + "*"))
                .getEntity()
        );
        assertThat(indicesBeforeDelete, containsString(indexName));

        client().performRequest(new Request("DELETE", MachineLearning.BASE_PATH + "anomaly_detectors/" + jobId));

        // check that the index still exists (it's shared by default)
        String indicesAfterDelete = EntityUtils.toString(
            client().performRequest(new Request("GET", "/_cat/indices/" + AnomalyDetectorsIndexFields.RESULTS_INDEX_PREFIX + "*"))
                .getEntity()
        );
        assertThat(indicesAfterDelete, containsString(indexName));

        waitUntilIndexIsEmpty(indexName);

        // check that the job itself is gone
        expectThrows(
            ResponseException.class,
            () -> client().performRequest(new Request("GET", MachineLearning.BASE_PATH + "anomaly_detectors/" + jobId + "/_stats"))
        );
    }

    public void testOutOfOrderData() throws Exception {
        String jobId = "job-with-out-of-order-docs";
        createFarequoteJob(jobId);

        openJob(jobId);

        Request postDataRequest = new Request("POST", MachineLearning.BASE_PATH + "anomaly_detectors/" + jobId + "/_data");
        // Post data is deprecated, so expect a deprecation warning
        postDataRequest.setOptions(POST_DATA);
        // Bucket span is 1h (3600s). So, posting data within the same hour should not result in out of order data
        postDataRequest.setJsonEntity("{ \"airline\":\"LOT\", \"responsetime\":100, \"time\":\"2019-07-01 00:00:00Z\" }");
        client().performRequest(postDataRequest);
        postDataRequest.setJsonEntity("{ \"airline\":\"LOT\", \"responsetime\":100, \"time\":\"2019-07-01 00:30:00Z\" }");
        client().performRequest(postDataRequest);
        // out of order, but in the same time bucket
        postDataRequest.setJsonEntity("{ \"airline\":\"LOT\", \"responsetime\":100, \"time\":\"2019-07-01 00:10:00Z\" }");
        client().performRequest(postDataRequest);

        Response flushResponse = client().performRequest(
            new Request("POST", MachineLearning.BASE_PATH + "anomaly_detectors/" + jobId + "/_flush")
        );
        assertThat(entityAsMap(flushResponse), hasEntry("flushed", true));

        closeJob(jobId);

        String stats = EntityUtils.toString(
            client().performRequest(new Request("GET", "_ml/anomaly_detectors/" + jobId + "/_stats")).getEntity()
        );
        // assert 2019-07-01 00:30:00Z
        assertThat(stats, containsString("\"latest_record_timestamp\":1561941000000"));
        assertThat(stats, containsString("\"out_of_order_timestamp_count\":0"));
        assertThat(stats, containsString("\"processed_record_count\":3"));

        client().performRequest(new Request("DELETE", MachineLearning.BASE_PATH + "anomaly_detectors/" + jobId));
    }

    public void testDeleteJob_TimingStatsDocumentIsDeleted() throws Exception {
        String jobId = "delete-job-with-timing-stats-document-job";
        String indexName = AnomalyDetectorsIndexFields.RESULTS_INDEX_PREFIX + AnomalyDetectorsIndexFields.RESULTS_INDEX_DEFAULT;
        createFarequoteJob(jobId);

        assertThat(
            EntityUtils.toString(client().performRequest(new Request("GET", indexName + "/_count")).getEntity()),
            containsString("\"count\":0")
        );  // documents related to the job do not exist yet

        openJob(jobId);

        Request postDataRequest = new Request("POST", MachineLearning.BASE_PATH + "anomaly_detectors/" + jobId + "/_data");
        // Post data is deprecated, so expect a deprecation warning
        postDataRequest.setOptions(POST_DATA);
        postDataRequest.setJsonEntity("""
            { "airline":"LOT", "response_time":100, "time":"2019-07-01 00:00:00Z" }""");
        client().performRequest(postDataRequest);
        postDataRequest.setJsonEntity("""
            { "airline":"LOT", "response_time":100, "time":"2019-07-01 02:00:00Z" }""");
        client().performRequest(postDataRequest);

        Response flushResponse = client().performRequest(
            new Request("POST", MachineLearning.BASE_PATH + "anomaly_detectors/" + jobId + "/_flush")
        );
        assertThat(entityAsMap(flushResponse), hasEntry("flushed", true));

        closeJob(jobId);

        String timingStatsDoc = EntityUtils.toString(
            client().performRequest(new Request("GET", indexName + "/_doc/" + TimingStats.documentId(jobId))).getEntity()
        );
        assertThat(timingStatsDoc, containsString("\"bucket_count\":2"));  // TimingStats doc exists, 2 buckets have been processed

        client().performRequest(new Request("DELETE", MachineLearning.BASE_PATH + "anomaly_detectors/" + jobId));

        waitUntilIndexIsEmpty(indexName);  // when job is being deleted, it also deletes all related documents from the shared index

        // check that the TimingStats documents got deleted
        ResponseException exception = expectThrows(
            ResponseException.class,
            () -> client().performRequest(new Request("GET", indexName + "/_doc/" + TimingStats.documentId(jobId)))
        );
        assertThat(exception.getResponse().getStatusLine().getStatusCode(), equalTo(404));

        // check that the job itself is gone
        exception = expectThrows(
            ResponseException.class,
            () -> client().performRequest(new Request("GET", MachineLearning.BASE_PATH + "anomaly_detectors/" + jobId + "/_stats"))
        );
        assertThat(exception.getResponse().getStatusLine().getStatusCode(), equalTo(404));
    }

    public void testDeleteJobAsync() throws Exception {
        String jobId = "delete-job-async-job";
        String indexName = AnomalyDetectorsIndexFields.RESULTS_INDEX_PREFIX + AnomalyDetectorsIndexFields.RESULTS_INDEX_DEFAULT;
        createFarequoteJob(jobId);

        // Use _cat/indices/.ml-anomalies-* instead of _cat/indices/_all to workaround https://github.com/elastic/elasticsearch/issues/45652
        String indicesBeforeDelete = EntityUtils.toString(
            client().performRequest(new Request("GET", "/_cat/indices/" + AnomalyDetectorsIndexFields.RESULTS_INDEX_PREFIX + "*"))
                .getEntity()
        );
        assertThat(indicesBeforeDelete, containsString(indexName));

        Response response = client().performRequest(
            new Request("DELETE", MachineLearning.BASE_PATH + "anomaly_detectors/" + jobId + "?wait_for_completion=false")
        );

        // Wait for task to complete
        String taskId = extractTaskId(response);
        Response taskResponse = client().performRequest(new Request("GET", "_tasks/" + taskId + "?wait_for_completion=true"));
        assertThat(EntityUtils.toString(taskResponse.getEntity()), containsString("\"acknowledged\":true"));

        // check that the index still exists (it's shared by default)
        String indicesAfterDelete = EntityUtils.toString(
            client().performRequest(new Request("GET", "/_cat/indices/" + AnomalyDetectorsIndexFields.RESULTS_INDEX_PREFIX + "*"))
                .getEntity()
        );
        assertThat(indicesAfterDelete, containsString(indexName));

        waitUntilIndexIsEmpty(indexName);

        // check that the job itself is gone
        expectThrows(
            ResponseException.class,
            () -> client().performRequest(new Request("GET", MachineLearning.BASE_PATH + "anomaly_detectors/" + jobId + "/_stats"))
        );
    }

    private void waitUntilIndexIsEmpty(String indexName) throws Exception {
        assertBusy(() -> {
            try {
                String count = EntityUtils.toString(client().performRequest(new Request("GET", indexName + "/_count")).getEntity());
                assertThat(count, containsString("\"count\":0"));
            } catch (Exception e) {
                fail(e.getMessage());
            }
        });
    }

    private static String extractTaskId(Response response) throws IOException {
        String responseAsString = EntityUtils.toString(response.getEntity());
        Pattern matchTaskId = Pattern.compile(".*\"task\":.*\"(.*)\".*");
        Matcher taskIdMatcher = matchTaskId.matcher(responseAsString);
        assertTrue(taskIdMatcher.matches());
        return taskIdMatcher.group(1);
    }

    public void testDeleteJobAfterMissingIndex() throws Exception {
        String jobId = "delete-job-after-missing-index-job";
        String aliasName = AnomalyDetectorsIndex.jobResultsAliasedName(jobId);
        String indexName = AnomalyDetectorsIndexFields.RESULTS_INDEX_PREFIX + AnomalyDetectorsIndexFields.RESULTS_INDEX_DEFAULT;
        createFarequoteJob(jobId);

        // Use _cat/indices/.ml-anomalies-* instead of _cat/indices/_all to workaround https://github.com/elastic/elasticsearch/issues/45652
        String indicesBeforeDelete = EntityUtils.toString(
            client().performRequest(new Request("GET", "/_cat/indices/" + AnomalyDetectorsIndexFields.RESULTS_INDEX_PREFIX + "*"))
                .getEntity()
        );
        assertThat(indicesBeforeDelete, containsString(indexName));

        // Manually delete the index so that we can test that deletion proceeds
        // normally anyway
        client().performRequest(new Request("DELETE", indexName));

        client().performRequest(new Request("DELETE", MachineLearning.BASE_PATH + "anomaly_detectors/" + jobId));

        // check index was deleted
        String indicesAfterDelete = EntityUtils.toString(
            client().performRequest(new Request("GET", "/_cat/indices/" + AnomalyDetectorsIndexFields.RESULTS_INDEX_PREFIX + "*"))
                .getEntity()
        );
        assertThat(indicesAfterDelete, not(containsString(aliasName)));
        assertThat(indicesAfterDelete, not(containsString(indexName)));

        expectThrows(
            ResponseException.class,
            () -> client().performRequest(new Request("GET", MachineLearning.BASE_PATH + "anomaly_detectors/" + jobId + "/_stats"))
        );
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
                String aliases = EntityUtils.toString(client().performRequest(new Request("GET", "/_cat/aliases")).getEntity());
                assertThat(aliases, containsString(readAliasName));
                assertThat(aliases, containsString(writeAliasName));
            } catch (ResponseException e) {
                throw new AssertionError(e);
            }
        });

        // Manually delete the aliases so that we can test that deletion proceeds
        // normally anyway
        client().performRequest(new Request("DELETE", indexName + "/_alias/" + readAliasName));
        client().performRequest(new Request("DELETE", indexName + "/_alias/" + writeAliasName));

        // check aliases were deleted
        expectThrows(ResponseException.class, () -> client().performRequest(new Request("GET", indexName + "/_alias/" + readAliasName)));
        expectThrows(ResponseException.class, () -> client().performRequest(new Request("GET", indexName + "/_alias/" + writeAliasName)));

        client().performRequest(new Request("DELETE", MachineLearning.BASE_PATH + "anomaly_detectors/" + jobId));
    }

    public void testMultiIndexDelete() throws Exception {
        String jobId = "multi-index-delete-job";
        String indexName = AnomalyDetectorsIndexFields.RESULTS_INDEX_PREFIX + AnomalyDetectorsIndexFields.RESULTS_INDEX_DEFAULT;
        createFarequoteJob(jobId);

        // Make the job's results span an extra two indices, i.e. three in total.
        // To do this the job's results alias needs to encompass all three indices.
        Request extraIndex1 = new Request("PUT", indexName + "-001");
        extraIndex1.setJsonEntity("""
            {
              "aliases": {
                "%s": {
                  "is_hidden": true,
                  "filter": {
                    "term": {
                      "%s": "%s"
                    }
                  }
                }
              }
            }""".formatted(AnomalyDetectorsIndex.jobResultsAliasedName(jobId), Job.ID, jobId));
        client().performRequest(extraIndex1);
        Request extraIndex2 = new Request("PUT", indexName + "-002");
        extraIndex2.setJsonEntity("""
            {
              "aliases": {
                "%s": {
                  "is_hidden": true,
                  "filter": {
                    "term": {
                      "%s": "%s"
                    }
                  }
                }
              }
            }""".formatted(AnomalyDetectorsIndex.jobResultsAliasedName(jobId), Job.ID, jobId));
        client().performRequest(extraIndex2);

        // Use _cat/indices/.ml-anomalies-* instead of _cat/indices/_all to workaround https://github.com/elastic/elasticsearch/issues/45652
        String indicesBeforeDelete = EntityUtils.toString(
            client().performRequest(new Request("GET", "/_cat/indices/" + AnomalyDetectorsIndexFields.RESULTS_INDEX_PREFIX + "*"))
                .getEntity()
        );
        assertThat(indicesBeforeDelete, containsString(indexName));
        assertThat(indicesBeforeDelete, containsString(indexName + "-001"));
        assertThat(indicesBeforeDelete, containsString(indexName + "-002"));

        // Add some documents to each index to make sure the DBQ clears them out
        Request createDoc0 = new Request("PUT", indexName + "/_doc/" + 123);
        createDoc0.setJsonEntity(String.format(Locale.ROOT, """
            {"job_id":"%s", "timestamp": "%s", "bucket_span":%d, "result_type":"record"}""", jobId, 123, 1));
        client().performRequest(createDoc0);
        Request createDoc1 = new Request("PUT", indexName + "-001/_doc/" + 123);
        createDoc1.setEntity(createDoc0.getEntity());
        client().performRequest(createDoc1);
        Request createDoc2 = new Request("PUT", indexName + "-002/_doc/" + 123);
        createDoc2.setEntity(createDoc0.getEntity());
        client().performRequest(createDoc2);

        // Also index a few through the alias for the first job
        Request createDoc3 = new Request("PUT", indexName + "/_doc/" + 456);
        createDoc3.setEntity(createDoc0.getEntity());
        client().performRequest(createDoc3);

        refreshAllIndices();

        // check for the documents
        assertThat(
            EntityUtils.toString(client().performRequest(new Request("GET", indexName + "/_count")).getEntity()),
            containsString("\"count\":2")
        );
        assertThat(
            EntityUtils.toString(client().performRequest(new Request("GET", indexName + "-001/_count")).getEntity()),
            containsString("\"count\":1")
        );
        assertThat(
            EntityUtils.toString(client().performRequest(new Request("GET", indexName + "-002/_count")).getEntity()),
            containsString("\"count\":1")
        );

        // Delete
        client().performRequest(new Request("DELETE", MachineLearning.BASE_PATH + "anomaly_detectors/" + jobId));

        refreshAllIndices();

        // check that the default shared index still exists but is empty
        String indicesAfterDelete = EntityUtils.toString(
            client().performRequest(new Request("GET", "/_cat/indices/" + AnomalyDetectorsIndexFields.RESULTS_INDEX_PREFIX + "*"))
                .getEntity()
        );
        assertThat(indicesAfterDelete, containsString(indexName));

        // other results indices should be deleted as this test job ID is the only job in those indices
        assertThat(indicesAfterDelete, not(containsString(indexName + "-001")));
        assertThat(indicesAfterDelete, not(containsString(indexName + "-002")));

        assertThat(
            EntityUtils.toString(client().performRequest(new Request("GET", indexName + "/_count")).getEntity()),
            containsString("\"count\":0")
        );
        expectThrows(
            ResponseException.class,
            () -> client().performRequest(new Request("GET", MachineLearning.BASE_PATH + "anomaly_detectors/" + jobId + "/_stats"))
        );
    }

    public void testDelete_multipleRequest() throws Exception {
        String jobId = "delete-job-multiple-times";
        createFarequoteJob(jobId);

        Map<Long, Response> responses = ConcurrentCollections.newConcurrentMap();
        Map<Long, ResponseException> responseExceptions = ConcurrentCollections.newConcurrentMap();
        AtomicReference<IOException> ioe = new AtomicReference<>();
        AtomicInteger recreationGuard = new AtomicInteger(0);
        AtomicReference<Response> recreationResponse = new AtomicReference<>();
        AtomicReference<ResponseException> recreationException = new AtomicReference<>();

        Runnable deleteJob = () -> {
            boolean forceDelete = randomBoolean();
            try {
                String url = MachineLearning.BASE_PATH + "anomaly_detectors/" + jobId;
                if (forceDelete) {
                    url += "?force=true";
                }
                Response response = client().performRequest(new Request("DELETE", url));
                responses.put(Thread.currentThread().getId(), response);
            } catch (ResponseException re) {
                responseExceptions.put(Thread.currentThread().getId(), re);
            } catch (IOException e) {
                ioe.set(e);
            }

            // Immediately after the first deletion finishes, recreate the job. This should pick up
            // race conditions where another delete request deletes part of the newly created job.
            if (recreationGuard.getAndIncrement() == 0) {
                try {
                    recreationResponse.set(createFarequoteJob(jobId));
                } catch (ResponseException re) {
                    recreationException.set(re);
                } catch (IOException e) {
                    logger.error("Error trying to recreate the job", e);
                    ioe.set(e);
                }
            }
        };

        // The idea is to hit the situation where one request waits for
        // the other to complete. This is difficult to schedule but
        // hopefully it will happen in CI
        int numThreads = 5;
        Thread[] threads = new Thread[numThreads];
        for (int i = 0; i < numThreads; i++) {
            threads[i] = new Thread(deleteJob);
        }
        for (int i = 0; i < numThreads; i++) {
            threads[i].start();
        }
        for (int i = 0; i < numThreads; i++) {
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
            assertEquals(EntityUtils.toString(response.getEntity()), 200, response.getStatusLine().getStatusCode());
        }

        assertNotNull(recreationResponse.get());
        assertEquals(
            EntityUtils.toString(recreationResponse.get().getEntity()),
            200,
            recreationResponse.get().getStatusLine().getStatusCode()
        );

        if (recreationException.get() != null) {
            assertNull(recreationException.get().getMessage(), recreationException.get());
        }

        String expectedReadAliasString = """
            "%s":{"filter":{"term":{"job_id":{"value":"%s"}}},"is_hidden":true}""".formatted(
            AnomalyDetectorsIndex.jobResultsAliasedName(jobId),
            jobId
        );
        String expectedWriteAliasString = """
            "%s":{"is_hidden":true}""".formatted(AnomalyDetectorsIndex.resultsWriteAlias(jobId));
        try {
            // The idea of the code above is that the deletion is sufficiently time-consuming that
            // all threads enter the deletion call before the first one exits it. Usually this happens,
            // but in the case that it does not the job that is recreated may get deleted.
            // It is not a error if the job does not exist but the following assertions
            // will fail in that case.
            client().performRequest(new Request("GET", MachineLearning.BASE_PATH + "anomaly_detectors/" + jobId));

            // Check that the job aliases exist. These are the last thing to be deleted when a job is deleted, so
            // if there's been a race between deletion and recreation these are what will be missing.
            String aliases = getAliases();

            assertThat(aliases, containsString(expectedReadAliasString));
            assertThat(aliases, containsString(expectedWriteAliasString));

        } catch (ResponseException missingJobException) {
            // The job does not exist
            assertThat(missingJobException.getResponse().getStatusLine().getStatusCode(), equalTo(404));

            // The job aliases should be deleted
            String aliases = getAliases();
            assertThat(aliases, not(containsString(expectedReadAliasString)));
            assertThat(aliases, not(containsString(expectedWriteAliasString)));
        }

        assertEquals(numThreads, recreationGuard.get());
    }

    private String getAliases() throws IOException {
        final Request aliasesRequest = new Request("GET", "/_aliases");
        // Allow system index deprecation warnings - this can be removed once system indices are omitted from responses rather than
        // triggering a deprecation warning.
        aliasesRequest.setOptions(RequestOptions.DEFAULT.toBuilder().setWarningsHandler(warnings -> {
            if (warnings.isEmpty()) {
                return false;
            } else if (warnings.size() > 1) {
                return true;
            } else {
                return warnings.get(0).startsWith("this request accesses system indices:") == false;
            }
        }).build());
        Response response = client().performRequest(aliasesRequest);
        return EntityUtils.toString(response.getEntity());
    }

    private void openJob(String jobId) throws IOException {
        Response response = openJob(jobId, Optional.empty());
        assertThat(entityAsMap(response), hasEntry("opened", true));
    }

    private Response openJob(String jobId, Optional<TimeValue> timeout) throws IOException {
        StringBuilder path = new StringBuilder(MachineLearning.BASE_PATH + "anomaly_detectors/" + jobId + "/_open");
        if (timeout.isPresent()) {
            path.append("?timeout=" + timeout.get().getStringRep());
        }
        Response openResponse = client().performRequest(new Request("POST", path.toString()));
        return openResponse;
    }

    private void closeJob(String jobId) throws IOException {
        Response openResponse = client().performRequest(
            new Request("POST", MachineLearning.BASE_PATH + "anomaly_detectors/" + jobId + "/_close")
        );
        assertThat(entityAsMap(openResponse), hasEntry("closed", true));
    }

    private Response putJob(String jobId, String jsonBody) throws IOException {
        Request request = new Request("PUT", MachineLearning.BASE_PATH + "anomaly_detectors/" + jobId);
        request.setJsonEntity(jsonBody);
        return client().performRequest(request);
    }

    @After
    public void clearMlState() throws Exception {
        new MlRestTestStateCleaner(logger, adminClient()).resetFeatures();
        // Don't check analytics jobs as they are independent of anomaly detection jobs and should not be created by this test.
        waitForPendingTasks(adminClient(), taskName -> taskName.contains(MlTasks.DATA_FRAME_ANALYTICS_TASK_NAME));
    }
}
