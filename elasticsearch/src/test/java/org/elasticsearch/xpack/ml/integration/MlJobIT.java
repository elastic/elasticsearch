/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.integration;

import org.apache.http.entity.StringEntity;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xpack.ml.MlPlugin;
import org.elasticsearch.xpack.ml.job.persistence.AnomalyDetectorsIndex;
import org.junit.After;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.isEmptyString;
import static org.hamcrest.Matchers.not;

public class MlJobIT extends ESRestTestCase {

    private static final String RESULT_MAPPING = "{ \"mappings\": {\"result\": { \"properties\": { " +
            "\"result_type\": { \"type\" : \"keyword\" }," +
            "\"timestamp\": { \"type\" : \"date\" }, " +
            "\"anomaly_score\": { \"type\" : \"double\" }, " +
            "\"normalized_probability\": { \"type\" : \"double\" }, " +
            "\"over_field_value\": { \"type\" : \"keyword\" }, " +
            "\"partition_field_value\": { \"type\" : \"keyword\" }, " +
            "\"by_field_value\": { \"type\" : \"keyword\" }, " +
            "\"field_name\": { \"type\" : \"keyword\" }, " +
            "\"function\": { \"type\" : \"keyword\" } " +
            "} } } }";

    public void testPutJob_GivenFarequoteConfig() throws Exception {
        Response response = createFarequoteJob();

        assertThat(response.getStatusLine().getStatusCode(), equalTo(200));
        String responseAsString = responseEntityToString(response);
        assertThat(responseAsString, containsString("\"job_id\":\"farequote\""));
    }

    public void testGetJob_GivenNoSuchJob() throws Exception {
        ResponseException e = expectThrows(ResponseException.class,
                () -> client().performRequest("get", MlPlugin.BASE_PATH + "anomaly_detectors/non-existing-job/_stats"));

        assertThat(e.getResponse().getStatusLine().getStatusCode(), equalTo(404));
        assertThat(e.getMessage(), containsString("No known job with id 'non-existing-job'"));
    }

    public void testGetJob_GivenJobExists() throws Exception {
        createFarequoteJob();

        Response response = client().performRequest("get", MlPlugin.BASE_PATH + "anomaly_detectors/farequote/_stats");

        assertThat(response.getStatusLine().getStatusCode(), equalTo(200));
        String responseAsString = responseEntityToString(response);
        assertThat(responseAsString, containsString("\"count\":1"));
        assertThat(responseAsString, containsString("\"job_id\":\"farequote\""));
    }

    public void testGetJobs_GivenSingleJob() throws Exception {
        createFarequoteJob();

        Response response = client().performRequest("get", MlPlugin.BASE_PATH + "anomaly_detectors/_all");

        assertThat(response.getStatusLine().getStatusCode(), equalTo(200));
        String responseAsString = responseEntityToString(response);
        assertThat(responseAsString, containsString("\"count\":1"));
        assertThat(responseAsString, containsString("\"job_id\":\"farequote\""));
    }

    public void testGetJobs_GivenMultipleJobs() throws Exception {
        createFarequoteJob("farequote_1");
        createFarequoteJob("farequote_2");
        createFarequoteJob("farequote_3");

        Response response = client().performRequest("get", MlPlugin.BASE_PATH + "anomaly_detectors/_all");

        assertThat(response.getStatusLine().getStatusCode(), equalTo(200));
        String responseAsString = responseEntityToString(response);
        assertThat(responseAsString, containsString("\"count\":3"));
        assertThat(responseAsString, containsString("\"job_id\":\"farequote_1\""));
        assertThat(responseAsString, containsString("\"job_id\":\"farequote_2\""));
        assertThat(responseAsString, containsString("\"job_id\":\"farequote_3\""));
    }

    private Response createFarequoteJob() throws Exception {
        return createFarequoteJob("farequote");
    }

    private Response createFarequoteJob(String jobId) throws Exception {
        String job = "{\n" + "    \"description\":\"Analysis of response time by airline\",\n"
                + "    \"analysis_config\" : {\n" + "        \"bucket_span\":3600,\n"
                + "        \"detectors\" :[{\"function\":\"metric\",\"field_name\":\"responsetime\",\"by_field_name\":\"airline\"}]\n"
                + "    },\n" + "    \"data_description\" : {\n" + "        \"field_delimiter\":\",\",\n" + "        " +
                "\"time_field\":\"time\",\n"
                + "        \"time_format\":\"yyyy-MM-dd HH:mm:ssX\"\n" + "    }\n" + "}";

        return client().performRequest("put", MlPlugin.BASE_PATH + "anomaly_detectors/" + jobId,
                Collections.emptyMap(), new StringEntity(job));
    }

    public void testGetBucketResults() throws Exception {
        Map<String, String> params = new HashMap<>();
        params.put("start", "1200"); // inclusive
        params.put("end", "1400"); // exclusive

        ResponseException e = expectThrows(ResponseException.class,
                () -> client().performRequest("get", MlPlugin.BASE_PATH + "anomaly_detectors/1/results/buckets", params));
        assertThat(e.getResponse().getStatusLine().getStatusCode(), equalTo(404));
        assertThat(e.getMessage(), containsString("No known job with id '1'"));

        addBucketResult("1", "1234", 1);
        addBucketResult("1", "1235", 1);
        addBucketResult("1", "1236", 1);
        Response response = client().performRequest("get", MlPlugin.BASE_PATH + "anomaly_detectors/1/results/buckets", params);
        assertThat(response.getStatusLine().getStatusCode(), equalTo(200));
        String responseAsString = responseEntityToString(response);
        assertThat(responseAsString, containsString("\"count\":3"));

        params.put("end", "1235");
        response = client().performRequest("get", MlPlugin.BASE_PATH + "anomaly_detectors/1/results/buckets", params);
        assertThat(response.getStatusLine().getStatusCode(), equalTo(200));
        responseAsString = responseEntityToString(response);
        assertThat(responseAsString, containsString("\"count\":1"));

        e = expectThrows(ResponseException.class, () -> client().performRequest("get", MlPlugin.BASE_PATH
                + "anomaly_detectors/2/results/buckets/1234"));
        assertThat(e.getResponse().getStatusLine().getStatusCode(), equalTo(404));
        assertThat(e.getMessage(), containsString("No known job with id '2'"));

        e = expectThrows(ResponseException.class, () -> client().performRequest("get",
                MlPlugin.BASE_PATH + "anomaly_detectors/1/results/buckets/1"));
        assertThat(e.getResponse().getStatusLine().getStatusCode(), equalTo(404));

        response = client().performRequest("get", MlPlugin.BASE_PATH + "anomaly_detectors/1/results/buckets/1234");
        assertThat(response.getStatusLine().getStatusCode(), equalTo(200));
        responseAsString = responseEntityToString(response);
        assertThat(responseAsString, not(isEmptyString()));
    }

    public void testGetRecordResults() throws Exception {
        Map<String, String> params = new HashMap<>();
        params.put("start", "1200"); // inclusive
        params.put("end", "1400"); // exclusive

        ResponseException e = expectThrows(ResponseException.class,
                () -> client().performRequest("get", MlPlugin.BASE_PATH + "anomaly_detectors/1/results/records", params));
        assertThat(e.getResponse().getStatusLine().getStatusCode(), equalTo(404));
        assertThat(e.getMessage(), containsString("No known job with id '1'"));

        addRecordResult("1", "1234", 1, 1);
        addRecordResult("1", "1235", 1, 2);
        addRecordResult("1", "1236", 1, 3);
        Response response = client().performRequest("get", MlPlugin.BASE_PATH + "anomaly_detectors/1/results/records", params);
        assertThat(response.getStatusLine().getStatusCode(), equalTo(200));
        String responseAsString = responseEntityToString(response);
        assertThat(responseAsString, containsString("\"count\":3"));

        params.put("end", "1235");
        response = client().performRequest("get", MlPlugin.BASE_PATH + "anomaly_detectors/1/results/records", params);
        assertThat(response.getStatusLine().getStatusCode(), equalTo(200));
        responseAsString = responseEntityToString(response);
        assertThat(responseAsString, containsString("\"count\":1"));
    }

    public void testCreateJobWithIndexNameOption() throws Exception {
        String jobTemplate = "{\n" +
                "  \"analysis_config\" : {\n" +
                "        \"detectors\" :[{\"function\":\"metric\",\"field_name\":\"responsetime\"}]\n" +
                "    },\n" +
                "  \"index_name\" : \"%s\"}";

        String jobId = "aliased-job";
        String indexName = "non-default-index";
        String jobConfig = String.format(Locale.ROOT, jobTemplate, indexName);

        Response response = client().performRequest("put", MlPlugin.BASE_PATH + "anomaly_detectors/" + jobId, Collections.emptyMap(),
                new StringEntity(jobConfig));
        assertEquals(200, response.getStatusLine().getStatusCode());

        response = client().performRequest("get", "_aliases");
        assertEquals(200, response.getStatusLine().getStatusCode());
        String responseAsString = responseEntityToString(response);
        assertThat(responseAsString, containsString("\"" + AnomalyDetectorsIndex.jobResultsIndexName(indexName)
                + "\":{\"aliases\":{\"" + AnomalyDetectorsIndex.jobResultsIndexName(jobId) + "\""));

        response = client().performRequest("get", "_cat/indices");
        assertEquals(200, response.getStatusLine().getStatusCode());
        responseAsString = responseEntityToString(response);
        assertThat(responseAsString, containsString(indexName));

        addBucketResult(indexName, "1234", 1);
        addBucketResult(indexName, "1236", 1);
        response = client().performRequest("get", MlPlugin.BASE_PATH + "anomaly_detectors/" + jobId + "/results/buckets");
        assertThat(response.getStatusLine().getStatusCode(), equalTo(200));
        responseAsString = responseEntityToString(response);
        assertThat(responseAsString, containsString("\"count\":2"));

        response = client().performRequest("get", AnomalyDetectorsIndex.jobResultsIndexName(indexName) + "/result/_search");
        assertThat(response.getStatusLine().getStatusCode(), equalTo(200));
        responseAsString = responseEntityToString(response);
        assertThat(responseAsString, containsString("\"total\":2"));

        // test that we can't create another job with the same index_name
        String jobConfigSameIndexName = String.format(Locale.ROOT, jobTemplate, "new-job-id", indexName);
        expectThrows(ResponseException.class, () -> client().performRequest("put",
                MlPlugin.BASE_PATH + "anomaly_detectors", Collections.emptyMap(), new StringEntity(jobConfigSameIndexName)));

        response = client().performRequest("delete", MlPlugin.BASE_PATH + "anomaly_detectors/" + jobId);
        assertThat(response.getStatusLine().getStatusCode(), equalTo(200));

        // check index and alias were deleted
        response = client().performRequest("get", "_aliases");
        assertEquals(200, response.getStatusLine().getStatusCode());
        responseAsString = responseEntityToString(response);
        assertThat(responseAsString, not(containsString(AnomalyDetectorsIndex.jobResultsIndexName(jobId))));

        response = client().performRequest("get", "_cat/indices");
        assertEquals(200, response.getStatusLine().getStatusCode());
        responseAsString = responseEntityToString(response);
        assertThat(responseAsString, not(containsString(indexName)));
    }

    public void testDeleteJob() throws Exception {
        String jobId = "foo";
        String indexName = AnomalyDetectorsIndex.jobResultsIndexName(jobId);
        createFarequoteJob(jobId);

        Response response = client().performRequest("get", "_cat/indices");
        assertEquals(200, response.getStatusLine().getStatusCode());
        String responseAsString = responseEntityToString(response);
        assertThat(responseAsString, containsString(indexName));

        response = client().performRequest("delete", MlPlugin.BASE_PATH + "anomaly_detectors/" + jobId);
        assertThat(response.getStatusLine().getStatusCode(), equalTo(200));

        // check index was deleted
        response = client().performRequest("get", "_cat/indices");
        assertEquals(200, response.getStatusLine().getStatusCode());
        responseAsString = responseEntityToString(response);
        assertThat(responseAsString, not(containsString(indexName)));

        // check that the job itself is gone
        expectThrows(ResponseException.class, () ->
                client().performRequest("get", MlPlugin.BASE_PATH + "anomaly_detectors/" + jobId + "/_stats"));
    }

    public void testDeleteJobAfterMissingIndex() throws Exception {
        String jobId = "foo";
        String indexName = AnomalyDetectorsIndex.jobResultsIndexName(jobId);
        createFarequoteJob(jobId);

        Response response = client().performRequest("get", "_cat/indices");
        assertEquals(200, response.getStatusLine().getStatusCode());
        String responseAsString = responseEntityToString(response);
        assertThat(responseAsString, containsString(indexName));

        // Manually delete the index so that we can test that deletion proceeds
        // normally anyway
        response = client().performRequest("delete", indexName);
        assertEquals(200, response.getStatusLine().getStatusCode());

        response = client().performRequest("delete", MlPlugin.BASE_PATH + "anomaly_detectors/" + jobId);
        assertThat(response.getStatusLine().getStatusCode(), equalTo(200));

        // check index was deleted
        response = client().performRequest("get", "_cat/indices");
        assertEquals(200, response.getStatusLine().getStatusCode());
        responseAsString = responseEntityToString(response);
        assertThat(responseAsString, not(containsString(indexName)));

        expectThrows(ResponseException.class, () ->
                client().performRequest("get", MlPlugin.BASE_PATH + "anomaly_detectors/" + jobId + "/_stats"));
    }

    private Response addBucketResult(String jobId, String timestamp, long bucketSpan) throws Exception {
        try {
            client().performRequest("put", AnomalyDetectorsIndex.jobResultsIndexName(jobId),
                    Collections.emptyMap(), new StringEntity(RESULT_MAPPING));
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
        return client().performRequest("put", AnomalyDetectorsIndex.jobResultsIndexName(jobId) + "/result/" + id,
                Collections.singletonMap("refresh", "true"), new StringEntity(bucketResult));
    }

    private Response addRecordResult(String jobId, String timestamp, long bucketSpan, int sequenceNum) throws Exception {
        try {
            client().performRequest("put", AnomalyDetectorsIndex.jobResultsIndexName(jobId), Collections.emptyMap(),
                    new StringEntity(RESULT_MAPPING));
        } catch (ResponseException e) {
            // it is ok: the index already exists
            assertThat(e.getMessage(), containsString("resource_already_exists_exception"));
            assertThat(e.getResponse().getStatusLine().getStatusCode(), equalTo(400));
        }

        String recordResult =
                String.format(Locale.ROOT,
                        "{\"job_id\":\"%s\", \"timestamp\": \"%s\", \"bucket_span\":%d, \"sequence_num\": %d, \"result_type\":\"record\"}",
                        jobId, timestamp, bucketSpan, sequenceNum);
        return client().performRequest("put", AnomalyDetectorsIndex.jobResultsIndexName(jobId) + "/result/" + timestamp,
                Collections.singletonMap("refresh", "true"), new StringEntity(recordResult));
    }

    private static String responseEntityToString(Response response) throws Exception {
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(response.getEntity().getContent(), StandardCharsets.UTF_8))) {
            return reader.lines().collect(Collectors.joining("\n"));
        }
    }

    @After
    public void clearMlState() throws IOException {
        new MlRestTestStateCleaner(client(), this).clearMlMetadata();
    }
}
