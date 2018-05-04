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
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xpack.ml.MachineLearning;

import java.io.IOException;
import java.net.URLEncoder;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.common.xcontent.XContentType.JSON;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class MlBasicMultiNodeIT extends ESRestTestCase {

    @SuppressWarnings("unchecked")
    public void testMachineLearningInstalled() throws Exception {
        Response response = client().performRequest("get", "/_xpack");
        assertEquals(200, response.getStatusLine().getStatusCode());
        Map<String, Object> features = (Map<String, Object>) responseEntityToMap(response).get("features");
        Map<String, Object> ml = (Map<String, Object>) features.get("ml");
        assertNotNull(ml);
        assertTrue((Boolean) ml.get("available"));
        assertTrue((Boolean) ml.get("enabled"));
    }

    public void testInvalidJob() throws Exception {
        // The job name is invalid because it contains a space
        String jobId = "invalid job";
        ResponseException e = expectThrows(ResponseException.class, () -> createFarequoteJob(jobId));
        assertTrue(e.getMessage(), e.getMessage().contains("can contain lowercase alphanumeric (a-z and 0-9), hyphens or underscores"));
        // If validation of the invalid job is not done until after transportation to the master node then the
        // root cause gets reported as a remote_transport_exception.  The code in PubJobAction is supposed to
        // validate before transportation to avoid this.  This test must be done in a multi-node cluster to have
        // a chance of catching a problem, hence it is here rather than in the single node integration tests.
        assertFalse(e.getMessage(), e.getMessage().contains("remote_transport_exception"));
    }

    public void testMiniFarequote() throws Exception {
        String jobId = "mini-farequote-job";
        createFarequoteJob(jobId);

        Response response = client().performRequest("post", MachineLearning.BASE_PATH + "anomaly_detectors/" + jobId + "/_open");
        assertEquals(200, response.getStatusLine().getStatusCode());
        assertEquals(Collections.singletonMap("opened", true), responseEntityToMap(response));

        String postData =
                "{\"airline\":\"AAL\",\"responsetime\":\"132.2046\",\"sourcetype\":\"farequote\",\"time\":\"1403481600\"}\n" +
                "{\"airline\":\"JZA\",\"responsetime\":\"990.4628\",\"sourcetype\":\"farequote\",\"time\":\"1403481700\"}";
        response = client().performRequest("post", MachineLearning.BASE_PATH + "anomaly_detectors/" + jobId + "/_data",
                Collections.emptyMap(),
                new StringEntity(postData, randomFrom(ContentType.APPLICATION_JSON, ContentType.create("application/x-ndjson"))));
        assertEquals(202, response.getStatusLine().getStatusCode());
        Map<String, Object> responseBody = responseEntityToMap(response);
        assertEquals(2, responseBody.get("processed_record_count"));
        assertEquals(4, responseBody.get("processed_field_count"));
        assertEquals(177, responseBody.get("input_bytes"));
        assertEquals(6, responseBody.get("input_field_count"));
        assertEquals(0, responseBody.get("invalid_date_count"));
        assertEquals(0, responseBody.get("missing_field_count"));
        assertEquals(0, responseBody.get("out_of_order_timestamp_count"));
        assertEquals(0, responseBody.get("bucket_count"));
        assertEquals(1403481600000L, responseBody.get("earliest_record_timestamp"));
        assertEquals(1403481700000L, responseBody.get("latest_record_timestamp"));

        response = client().performRequest("post", MachineLearning.BASE_PATH + "anomaly_detectors/" + jobId + "/_flush");
        assertEquals(200, response.getStatusLine().getStatusCode());
        assertFlushResponse(response, true, 1403481600000L);

        response = client().performRequest("post", MachineLearning.BASE_PATH + "anomaly_detectors/" + jobId + "/_close",
                Collections.singletonMap("timeout", "20s"));
        assertEquals(200, response.getStatusLine().getStatusCode());
        assertEquals(Collections.singletonMap("closed", true), responseEntityToMap(response));

        response = client().performRequest("get", MachineLearning.BASE_PATH + "anomaly_detectors/" + jobId + "/_stats");
        assertEquals(200, response.getStatusLine().getStatusCode());
        @SuppressWarnings("unchecked")
        Map<String, Object> dataCountsDoc = (Map<String, Object>)
                ((Map)((List) responseEntityToMap(response).get("jobs")).get(0)).get("data_counts");
        assertEquals(2, dataCountsDoc.get("processed_record_count"));
        assertEquals(4, dataCountsDoc.get("processed_field_count"));
        assertEquals(177, dataCountsDoc.get("input_bytes"));
        assertEquals(6, dataCountsDoc.get("input_field_count"));
        assertEquals(0, dataCountsDoc.get("invalid_date_count"));
        assertEquals(0, dataCountsDoc.get("missing_field_count"));
        assertEquals(0, dataCountsDoc.get("out_of_order_timestamp_count"));
        assertEquals(0, dataCountsDoc.get("bucket_count"));
        assertEquals(1403481600000L, dataCountsDoc.get("earliest_record_timestamp"));
        assertEquals(1403481700000L, dataCountsDoc.get("latest_record_timestamp"));

        response = client().performRequest("delete", MachineLearning.BASE_PATH + "anomaly_detectors/" + jobId);
        assertEquals(200, response.getStatusLine().getStatusCode());
    }

    public void testMiniFarequoteWithDatafeeder() throws Exception {
        String mappings = "{"
                + "  \"mappings\": {"
                + "    \"response\": {"
                + "      \"properties\": {"
                + "        \"time\": { \"type\":\"date\"},"
                + "        \"airline\": { \"type\":\"keyword\"},"
                + "        \"responsetime\": { \"type\":\"float\"}"
                + "      }"
                + "    }"
                + "  }"
                + "}";
        client().performRequest("put", "airline-data", Collections.emptyMap(), new StringEntity(mappings, ContentType.APPLICATION_JSON));
        client().performRequest("put", "airline-data/response/1", Collections.emptyMap(),
                new StringEntity("{\"time\":\"2016-06-01T00:00:00Z\",\"airline\":\"AAA\",\"responsetime\":135.22}",
                        ContentType.APPLICATION_JSON));
        client().performRequest("put", "airline-data/response/2", Collections.emptyMap(),
                new StringEntity("{\"time\":\"2016-06-01T01:59:00Z\",\"airline\":\"AAA\",\"responsetime\":541.76}",
                        ContentType.APPLICATION_JSON));

        // Ensure all data is searchable
        client().performRequest("post", "_refresh");

        String jobId = "mini-farequote-with-data-feeder-job";
        createFarequoteJob(jobId);
        String datafeedId = "bar";
        createDatafeed(datafeedId, jobId);

        Response response = client().performRequest("post", MachineLearning.BASE_PATH + "anomaly_detectors/" + jobId + "/_open");
        assertEquals(200, response.getStatusLine().getStatusCode());
        assertEquals(Collections.singletonMap("opened", true), responseEntityToMap(response));

        response = client().performRequest("post", MachineLearning.BASE_PATH + "datafeeds/" + datafeedId + "/_start",
                Collections.singletonMap("start", "0"));
        assertEquals(200, response.getStatusLine().getStatusCode());
        assertEquals(Collections.singletonMap("started", true), responseEntityToMap(response));

        assertBusy(() -> {
            try {
                Response statsResponse =
                        client().performRequest("get", MachineLearning.BASE_PATH + "anomaly_detectors/" + jobId + "/_stats");
                assertEquals(200, statsResponse.getStatusLine().getStatusCode());
                @SuppressWarnings("unchecked")
                Map<String, Object> dataCountsDoc = (Map<String, Object>)
                        ((Map)((List) responseEntityToMap(statsResponse).get("jobs")).get(0)).get("data_counts");
                assertEquals(2, dataCountsDoc.get("input_record_count"));
                assertEquals(2, dataCountsDoc.get("processed_record_count"));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });

        response = client().performRequest("post", MachineLearning.BASE_PATH + "datafeeds/" + datafeedId + "/_stop");
        assertEquals(200, response.getStatusLine().getStatusCode());
        assertEquals(Collections.singletonMap("stopped", true), responseEntityToMap(response));

        response = client().performRequest("post", MachineLearning.BASE_PATH + "anomaly_detectors/" + jobId + "/_close",
                Collections.singletonMap("timeout", "20s"));
        assertEquals(200, response.getStatusLine().getStatusCode());
        assertEquals(Collections.singletonMap("closed", true), responseEntityToMap(response));

        response = client().performRequest("delete", MachineLearning.BASE_PATH + "datafeeds/" + datafeedId);
        assertEquals(200, response.getStatusLine().getStatusCode());

        response = client().performRequest("delete", MachineLearning.BASE_PATH + "anomaly_detectors/" + jobId);
        assertEquals(200, response.getStatusLine().getStatusCode());
    }

    public void testMiniFarequoteReopen() throws Exception {
        String jobId = "mini-farequote-reopen";
        createFarequoteJob(jobId);

        Response response = client().performRequest("post", MachineLearning.BASE_PATH + "anomaly_detectors/" + jobId + "/_open");
        assertEquals(200, response.getStatusLine().getStatusCode());
        assertEquals(Collections.singletonMap("opened", true), responseEntityToMap(response));

        String postData =
                "{\"airline\":\"AAL\",\"responsetime\":\"132.2046\",\"sourcetype\":\"farequote\",\"time\":\"1403481600\"}\n" +
                "{\"airline\":\"JZA\",\"responsetime\":\"990.4628\",\"sourcetype\":\"farequote\",\"time\":\"1403481700\"}\n" +
                "{\"airline\":\"JBU\",\"responsetime\":\"877.5927\",\"sourcetype\":\"farequote\",\"time\":\"1403481800\"}\n" +
                "{\"airline\":\"KLM\",\"responsetime\":\"1355.4812\",\"sourcetype\":\"farequote\",\"time\":\"1403481900\"}\n" +
                "{\"airline\":\"NKS\",\"responsetime\":\"9991.3981\",\"sourcetype\":\"farequote\",\"time\":\"1403482000\"}";
        response = client().performRequest("post", MachineLearning.BASE_PATH + "anomaly_detectors/" + jobId + "/_data",
                Collections.emptyMap(),
                new StringEntity(postData, randomFrom(ContentType.APPLICATION_JSON, ContentType.create("application/x-ndjson"))));
        assertEquals(202, response.getStatusLine().getStatusCode());
        Map<String, Object> responseBody = responseEntityToMap(response);
        assertEquals(5, responseBody.get("processed_record_count"));
        assertEquals(10, responseBody.get("processed_field_count"));
        assertEquals(446, responseBody.get("input_bytes"));
        assertEquals(15, responseBody.get("input_field_count"));
        assertEquals(0, responseBody.get("invalid_date_count"));
        assertEquals(0, responseBody.get("missing_field_count"));
        assertEquals(0, responseBody.get("out_of_order_timestamp_count"));
        assertEquals(0, responseBody.get("bucket_count"));
        assertEquals(1403481600000L, responseBody.get("earliest_record_timestamp"));
        assertEquals(1403482000000L, responseBody.get("latest_record_timestamp"));

        response = client().performRequest("post", MachineLearning.BASE_PATH + "anomaly_detectors/" + jobId + "/_flush");
        assertEquals(200, response.getStatusLine().getStatusCode());
        assertFlushResponse(response, true, 1403481600000L);

        response = client().performRequest("post", MachineLearning.BASE_PATH + "anomaly_detectors/" + jobId + "/_close",
                Collections.singletonMap("timeout", "20s"));
        assertEquals(200, response.getStatusLine().getStatusCode());
        assertEquals(Collections.singletonMap("closed", true), responseEntityToMap(response));

        response = client().performRequest("get", MachineLearning.BASE_PATH + "anomaly_detectors/" + jobId + "/_stats");
        assertEquals(200, response.getStatusLine().getStatusCode());
        
        response = client().performRequest("post", MachineLearning.BASE_PATH + "anomaly_detectors/" + jobId + "/_open",
                Collections.singletonMap("timeout", "20s"));
        assertEquals(200, response.getStatusLine().getStatusCode());
        assertEquals(Collections.singletonMap("opened", true), responseEntityToMap(response));

        // feed some more data points
        postData =
                "{\"airline\":\"AAL\",\"responsetime\":\"136.2361\",\"sourcetype\":\"farequote\",\"time\":\"1407081600\"}\n" +
                "{\"airline\":\"VRD\",\"responsetime\":\"282.9847\",\"sourcetype\":\"farequote\",\"time\":\"1407081700\"}\n" +
                "{\"airline\":\"JAL\",\"responsetime\":\"493.0338\",\"sourcetype\":\"farequote\",\"time\":\"1407081800\"}\n" +
                "{\"airline\":\"UAL\",\"responsetime\":\"8.4275\",\"sourcetype\":\"farequote\",\"time\":\"1407081900\"}\n" +
                "{\"airline\":\"FFT\",\"responsetime\":\"221.8693\",\"sourcetype\":\"farequote\",\"time\":\"1407082000\"}";
        response = client().performRequest("post", MachineLearning.BASE_PATH + "anomaly_detectors/" + jobId + "/_data",
                Collections.emptyMap(),
                new StringEntity(postData, randomFrom(ContentType.APPLICATION_JSON, ContentType.create("application/x-ndjson"))));
        assertEquals(202, response.getStatusLine().getStatusCode());
        responseBody = responseEntityToMap(response);
        assertEquals(5, responseBody.get("processed_record_count"));
        assertEquals(10, responseBody.get("processed_field_count"));
        assertEquals(442, responseBody.get("input_bytes"));
        assertEquals(15, responseBody.get("input_field_count"));
        assertEquals(0, responseBody.get("invalid_date_count"));
        assertEquals(0, responseBody.get("missing_field_count"));
        assertEquals(0, responseBody.get("out_of_order_timestamp_count"));
        assertEquals(1000, responseBody.get("bucket_count"));
        
        // unintuitive: should return the earliest record timestamp of this feed???
        assertEquals(null, responseBody.get("earliest_record_timestamp"));
        assertEquals(1407082000000L, responseBody.get("latest_record_timestamp"));
        
        response = client().performRequest("post", MachineLearning.BASE_PATH + "anomaly_detectors/" + jobId + "/_close",
                Collections.singletonMap("timeout", "20s"));
        assertEquals(200, response.getStatusLine().getStatusCode());
        assertEquals(Collections.singletonMap("closed", true), responseEntityToMap(response));

        // counts should be summed up
        response = client().performRequest("get", MachineLearning.BASE_PATH + "anomaly_detectors/" + jobId + "/_stats");
        assertEquals(200, response.getStatusLine().getStatusCode());
        
        @SuppressWarnings("unchecked")
        Map<String, Object> dataCountsDoc = (Map<String, Object>)
                ((Map)((List) responseEntityToMap(response).get("jobs")).get(0)).get("data_counts");
        assertEquals(10, dataCountsDoc.get("processed_record_count"));
        assertEquals(20, dataCountsDoc.get("processed_field_count"));
        assertEquals(888, dataCountsDoc.get("input_bytes"));
        assertEquals(30, dataCountsDoc.get("input_field_count"));
        assertEquals(0, dataCountsDoc.get("invalid_date_count"));
        assertEquals(0, dataCountsDoc.get("missing_field_count"));
        assertEquals(0, dataCountsDoc.get("out_of_order_timestamp_count"));
        assertEquals(1000, dataCountsDoc.get("bucket_count"));
        assertEquals(1403481600000L, dataCountsDoc.get("earliest_record_timestamp"));
        assertEquals(1407082000000L, dataCountsDoc.get("latest_record_timestamp"));

        response = client().performRequest("delete", MachineLearning.BASE_PATH + "anomaly_detectors/" + jobId);
        assertEquals(200, response.getStatusLine().getStatusCode());
    }

    private Response createDatafeed(String datafeedId, String jobId) throws Exception {
        XContentBuilder xContentBuilder = jsonBuilder();
        xContentBuilder.startObject();
        xContentBuilder.field("job_id", jobId);
        xContentBuilder.array("indexes", "airline-data");
        xContentBuilder.array("types", "response");
        xContentBuilder.field("_source", true);
        xContentBuilder.endObject();
        return client().performRequest("put", MachineLearning.BASE_PATH + "datafeeds/" + datafeedId,
                Collections.emptyMap(), new StringEntity(Strings.toString(xContentBuilder), ContentType.APPLICATION_JSON));
    }

    private Response createFarequoteJob(String jobId) throws Exception {
        XContentBuilder xContentBuilder = jsonBuilder();
        xContentBuilder.startObject();
        xContentBuilder.field("job_id", jobId);
        xContentBuilder.field("description", "Analysis of response time by airline");

        xContentBuilder.startObject("analysis_config");
        xContentBuilder.field("bucket_span", "3600s");
        xContentBuilder.startArray("detectors");
        xContentBuilder.startObject();
        xContentBuilder.field("function", "metric");
        xContentBuilder.field("field_name", "responsetime");
        xContentBuilder.field("by_field_name", "airline");
        xContentBuilder.endObject();
        xContentBuilder.endArray();
        xContentBuilder.endObject();

        xContentBuilder.startObject("data_description");
        xContentBuilder.field("format", "xcontent");
        xContentBuilder.field("time_field", "time");
        xContentBuilder.field("time_format", "epoch");
        xContentBuilder.endObject();
        xContentBuilder.endObject();

        return client().performRequest("put", MachineLearning.BASE_PATH + "anomaly_detectors/" + URLEncoder.encode(jobId, "UTF-8"),
                Collections.emptyMap(), new StringEntity(Strings.toString(xContentBuilder), ContentType.APPLICATION_JSON));
    }

    private static Map<String, Object> responseEntityToMap(Response response) throws IOException {
        return XContentHelper.convertToMap(JSON.xContent(), response.getEntity().getContent(), false);
    }

    private static void assertFlushResponse(Response response, boolean expectedFlushed, long expectedLastFinalizedBucketEnd)
            throws IOException {
        Map<String, Object> asMap = responseEntityToMap(response);
        assertThat(asMap.size(), equalTo(2));
        assertThat(asMap.get("flushed"), is(true));
        assertThat(asMap.get("last_finalized_bucket_end"), equalTo(expectedLastFinalizedBucketEnd));
    }
}
