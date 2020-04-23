/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.integration;

import org.apache.http.entity.ContentType;
import org.apache.http.nio.entity.NStringEntity;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xpack.ml.MachineLearning;
import org.yaml.snakeyaml.util.UriEncoder;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.is;

public class MlBasicMultiNodeIT extends ESRestTestCase {

    public void testMachineLearningInstalled() throws Exception {
        Response response = client().performRequest(new Request("GET", "/_xpack"));
        Map<?, ?> features = (Map<?, ?>) entityAsMap(response).get("features");
        Map<?, ?> ml = (Map<?, ?>) features.get("ml");
        assertNotNull(ml);
        assertTrue((Boolean) ml.get("available"));
        assertTrue((Boolean) ml.get("enabled"));
    }

    public void testInvalidJob() {
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

        Response openResponse = client().performRequest(
                new Request("POST", MachineLearning.BASE_PATH + "anomaly_detectors/" + jobId + "/_open"));
        assertThat(entityAsMap(openResponse), hasEntry("opened", true));

        Request addData = new Request("POST", MachineLearning.BASE_PATH + "anomaly_detectors/" + jobId + "/_data");
        addData.setEntity(new NStringEntity(
                "{\"airline\":\"AAL\",\"responsetime\":\"132.2046\",\"sourcetype\":\"farequote\",\"time\":\"1403481600\"}\n" +
                "{\"airline\":\"JZA\",\"responsetime\":\"990.4628\",\"sourcetype\":\"farequote\",\"time\":\"1403481700\"}",
                randomFrom(ContentType.APPLICATION_JSON, ContentType.create("application/x-ndjson"))));
        Response addDataResponse = client().performRequest(addData);
        assertEquals(202, addDataResponse.getStatusLine().getStatusCode());
        Map<String, Object> responseBody = entityAsMap(addDataResponse);
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

        Response flushResponse = client().performRequest(
                new Request("POST", MachineLearning.BASE_PATH + "anomaly_detectors/" + jobId + "/_flush"));
        assertFlushResponse(flushResponse, true, 1403481600000L);

        Request closeRequest = new Request("POST", MachineLearning.BASE_PATH + "anomaly_detectors/" + jobId + "/_close");
        closeRequest.addParameter("timeout", "20s");
        Response closeResponse = client().performRequest(closeRequest);
        assertEquals(Collections.singletonMap("closed", true), entityAsMap(closeResponse));

        Response statsResponse = client().performRequest(
                new Request("GET", MachineLearning.BASE_PATH + "anomaly_detectors/" + jobId + "/_stats"));
        Map<?, ?> dataCountsDoc = (Map<?, ?>)
                ((Map<?, ?>)((List<?>) entityAsMap(statsResponse).get("jobs")).get(0)).get("data_counts");
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

        client().performRequest(new Request("DELETE", MachineLearning.BASE_PATH + "anomaly_detectors/" + jobId));
    }

    public void testMiniFarequoteWithDatafeeder() throws Exception {
        boolean datesHaveNanoSecondResolution = randomBoolean();
        String dateMappingType = datesHaveNanoSecondResolution ? "date_nanos" : "date";
        String dateFormat = datesHaveNanoSecondResolution ? "strict_date_optional_time_nanos" : "strict_date_optional_time";
        String randomNanos = datesHaveNanoSecondResolution ? "," + randomIntBetween(100000000, 999999999) : "";
        Request createAirlineDataRequest = new Request("PUT", "/airline-data");
        createAirlineDataRequest.setJsonEntity("{"
                + "  \"mappings\": {"
                + "    \"properties\": {"
                + "      \"time\": { \"type\":\"" + dateMappingType + "\", \"format\":\"" + dateFormat + "\"},"
                + "      \"airline\": { \"type\":\"keyword\"},"
                + "      \"responsetime\": { \"type\":\"float\"}"
                + "    }"
                + "  }"
                + "}");
        client().performRequest(createAirlineDataRequest);
        Request airlineData1 = new Request("PUT", "/airline-data/_doc/1");
        airlineData1.setJsonEntity("{\"time\":\"2016-06-01T00:00:00" + randomNanos + "Z\",\"airline\":\"AAA\",\"responsetime\":135.22}");
        client().performRequest(airlineData1);
        Request airlineData2 = new Request("PUT", "/airline-data/_doc/2");
        airlineData2.setJsonEntity("{\"time\":\"2016-06-01T01:59:00" + randomNanos + "Z\",\"airline\":\"AAA\",\"responsetime\":541.76}");
        client().performRequest(airlineData2);

        // Ensure all data is searchable
        refreshAllIndices();

        String jobId = "mini-farequote-with-data-feeder-job";
        createFarequoteJob(jobId);
        String datafeedId = "bar";
        createDatafeed(datafeedId, jobId);

        Response openResponse = client().performRequest(
                new Request("POST", MachineLearning.BASE_PATH + "anomaly_detectors/" + jobId + "/_open"));
        assertThat(entityAsMap(openResponse), hasEntry("opened", true));

        Request startRequest = new Request("POST", MachineLearning.BASE_PATH + "datafeeds/" + datafeedId + "/_start");
        startRequest.addParameter("start", "0");
        Response startResponse = client().performRequest(startRequest);
        assertThat(entityAsMap(startResponse), hasEntry("started", true));

        assertBusy(() -> {
            try {
                Response statsResponse = client().performRequest(
                        new Request("GET", MachineLearning.BASE_PATH + "anomaly_detectors/" + jobId + "/_stats"));
                Map<?, ?> dataCountsDoc = (Map<?, ?>)
                        ((Map<?, ?>)((List<?>) entityAsMap(statsResponse).get("jobs")).get(0)).get("data_counts");
                assertEquals(2, dataCountsDoc.get("input_record_count"));
                assertEquals(2, dataCountsDoc.get("processed_record_count"));
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        });

        Response stopResponse = client().performRequest(
                new Request("POST", MachineLearning.BASE_PATH + "datafeeds/" + datafeedId + "/_stop"));
        assertEquals(Collections.singletonMap("stopped", true), entityAsMap(stopResponse));

        Request closeRequest = new Request("POST", MachineLearning.BASE_PATH + "anomaly_detectors/" + jobId + "/_close");
        closeRequest.addParameter("timeout", "20s");
        assertEquals(Collections.singletonMap("closed", true),
                entityAsMap(client().performRequest(closeRequest)));

        client().performRequest(new Request("DELETE", MachineLearning.BASE_PATH + "datafeeds/" + datafeedId));
        client().performRequest(new Request("DELETE", MachineLearning.BASE_PATH + "anomaly_detectors/" + jobId));
    }

    public void testMiniFarequoteReopen() throws Exception {
        String jobId = "mini-farequote-reopen";
        createFarequoteJob(jobId);

        Response openResponse = client().performRequest(
                new Request("POST", MachineLearning.BASE_PATH + "anomaly_detectors/" + jobId + "/_open"));
        assertThat(entityAsMap(openResponse), hasEntry("opened", true));

        Request addDataRequest = new Request("POST", MachineLearning.BASE_PATH + "anomaly_detectors/" + jobId + "/_data");
        addDataRequest.setEntity(new NStringEntity(
                "{\"airline\":\"AAL\",\"responsetime\":\"132.2046\",\"sourcetype\":\"farequote\",\"time\":\"1403481600\"}\n" +
                "{\"airline\":\"JZA\",\"responsetime\":\"990.4628\",\"sourcetype\":\"farequote\",\"time\":\"1403481700\"}\n" +
                "{\"airline\":\"JBU\",\"responsetime\":\"877.5927\",\"sourcetype\":\"farequote\",\"time\":\"1403481800\"}\n" +
                "{\"airline\":\"KLM\",\"responsetime\":\"1355.4812\",\"sourcetype\":\"farequote\",\"time\":\"1403481900\"}\n" +
                "{\"airline\":\"NKS\",\"responsetime\":\"9991.3981\",\"sourcetype\":\"farequote\",\"time\":\"1403482000\"}",
                randomFrom(ContentType.APPLICATION_JSON, ContentType.create("application/x-ndjson"))));
        Response addDataResponse = client().performRequest(addDataRequest);
        assertEquals(202, addDataResponse.getStatusLine().getStatusCode());
        Map<String, Object> responseBody = entityAsMap(addDataResponse);
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

        Response flushResponse = client().performRequest(
                new Request("POST", MachineLearning.BASE_PATH + "anomaly_detectors/" + jobId + "/_flush"));
        assertFlushResponse(flushResponse, true, 1403481600000L);

        Request closeRequest = new Request("POST", MachineLearning.BASE_PATH + "anomaly_detectors/" + jobId + "/_close");
        closeRequest.addParameter("timeout", "20s");
        assertEquals(Collections.singletonMap("closed", true),
                entityAsMap(client().performRequest(closeRequest)));

        Request statsRequest = new Request("GET", MachineLearning.BASE_PATH + "anomaly_detectors/" + jobId + "/_stats");
        client().performRequest(statsRequest);

        Request openRequest = new Request("POST", MachineLearning.BASE_PATH + "anomaly_detectors/" + jobId + "/_open");
        openRequest.addParameter("timeout", "20s");
        Response openResponse2 = client().performRequest(openRequest);
        assertThat(entityAsMap(openResponse2), hasEntry("opened", true));

        // feed some more data points
        Request addDataRequest2 = new Request("POST", MachineLearning.BASE_PATH + "anomaly_detectors/" + jobId + "/_data");
        addDataRequest2.setEntity(new NStringEntity(
                "{\"airline\":\"AAL\",\"responsetime\":\"136.2361\",\"sourcetype\":\"farequote\",\"time\":\"1407081600\"}\n" +
                "{\"airline\":\"VRD\",\"responsetime\":\"282.9847\",\"sourcetype\":\"farequote\",\"time\":\"1407081700\"}\n" +
                "{\"airline\":\"JAL\",\"responsetime\":\"493.0338\",\"sourcetype\":\"farequote\",\"time\":\"1407081800\"}\n" +
                "{\"airline\":\"UAL\",\"responsetime\":\"8.4275\",\"sourcetype\":\"farequote\",\"time\":\"1407081900\"}\n" +
                "{\"airline\":\"FFT\",\"responsetime\":\"221.8693\",\"sourcetype\":\"farequote\",\"time\":\"1407082000\"}",
                randomFrom(ContentType.APPLICATION_JSON, ContentType.create("application/x-ndjson"))));
        Response addDataResponse2 = client().performRequest(addDataRequest2);
        assertEquals(202, addDataResponse2.getStatusLine().getStatusCode());
        Map<String, Object> responseBody2 = entityAsMap(addDataResponse2);
        assertEquals(5, responseBody2.get("processed_record_count"));
        assertEquals(10, responseBody2.get("processed_field_count"));
        assertEquals(442, responseBody2.get("input_bytes"));
        assertEquals(15, responseBody2.get("input_field_count"));
        assertEquals(0, responseBody2.get("invalid_date_count"));
        assertEquals(0, responseBody2.get("missing_field_count"));
        assertEquals(0, responseBody2.get("out_of_order_timestamp_count"));
        assertEquals(1000, responseBody2.get("bucket_count"));

        // unintuitive: should return the earliest record timestamp of this feed???
        assertNull(responseBody2.get("earliest_record_timestamp"));
        assertEquals(1407082000000L, responseBody2.get("latest_record_timestamp"));

        assertEquals(Collections.singletonMap("closed", true),
                entityAsMap(client().performRequest(closeRequest)));

        // counts should be summed up
        Response statsResponse = client().performRequest(statsRequest);

        Map<?, ?> dataCountsDoc = (Map<?, ?>)
                ((Map<?, ?>)((List<?>) entityAsMap(statsResponse).get("jobs")).get(0)).get("data_counts");
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

        client().performRequest(new Request("DELETE", MachineLearning.BASE_PATH + "anomaly_detectors/" + jobId));
    }

    private Response createDatafeed(String datafeedId, String jobId) throws Exception {
        XContentBuilder xContentBuilder = jsonBuilder();
        xContentBuilder.startObject();
        xContentBuilder.field("job_id", jobId);
        xContentBuilder.array("indexes", "airline-data");
        xContentBuilder.endObject();
        Request request = new Request("PUT", MachineLearning.BASE_PATH + "datafeeds/" + datafeedId);
        request.setJsonEntity(Strings.toString(xContentBuilder));
        return client().performRequest(request);
    }

    private Response createFarequoteJob(String jobId) throws Exception {
        XContentBuilder xContentBuilder = jsonBuilder();
        xContentBuilder.startObject();
        {
            xContentBuilder.field("job_id", jobId);
            xContentBuilder.field("description", "Analysis of response time by airline");

            xContentBuilder.startObject("analysis_config");
            {
                xContentBuilder.field("bucket_span", "3600s");
                xContentBuilder.startArray("detectors");
                {
                    xContentBuilder.startObject();
                    {
                        xContentBuilder.field("function", "metric");
                        xContentBuilder.field("field_name", "responsetime");
                        xContentBuilder.field("by_field_name", "airline");
                    }
                    xContentBuilder.endObject();
                }
                xContentBuilder.endArray();
            }
            xContentBuilder.endObject();

            xContentBuilder.startObject("data_description");
            {
                xContentBuilder.field("format", "xcontent");
                xContentBuilder.field("time_field", "time");
                xContentBuilder.field("time_format", "epoch");
            }
            xContentBuilder.endObject();
        }
        xContentBuilder.endObject();

        Request request = new Request("PUT", MachineLearning.BASE_PATH + "anomaly_detectors/" + UriEncoder.encode(jobId));
        request.setJsonEntity(Strings.toString(xContentBuilder));
        return client().performRequest(request);
    }

    private static void assertFlushResponse(Response response, boolean expectedFlushed, long expectedLastFinalizedBucketEnd)
            throws IOException {
        Map<String, Object> asMap = entityAsMap(response);
        assertThat(asMap.size(), equalTo(2));
        assertThat(asMap.get("flushed"), is(true));
        assertThat(asMap.get("last_finalized_bucket_end"), equalTo(expectedLastFinalizedBucketEnd));
    }
}
