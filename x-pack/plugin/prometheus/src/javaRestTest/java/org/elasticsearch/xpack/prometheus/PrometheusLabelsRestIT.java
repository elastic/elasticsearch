/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.prometheus;

import org.apache.http.client.utils.URIBuilder;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.notNullValue;

/**
 * Integration tests for the Prometheus {@code GET /api/v1/labels} endpoint.
 *
 * <p>Tests focus on high-level HTTP concerns: routing, request/response format, status codes.
 * Detailed plan-building and response-parsing logic is covered by unit tests.
 */
public class PrometheusLabelsRestIT extends AbstractPrometheusRestIT {

    public void testInvalidSelectorSyntaxReturnsBadRequest() throws Exception {
        // {not valid!!!} is not valid PromQL
        Request request = labelsRequest("{not valid!!!}");
        addReadAuth(request);
        ResponseException e = expectThrows(ResponseException.class, () -> client().performRequest(request));
        assertThat(e.getResponse().getStatusLine().getStatusCode(), equalTo(400));
    }

    public void testRangeSelectorReturnsBadRequest() throws Exception {
        // up[5m] is a range vector, not an instant vector
        Request request = labelsRequest("up[5m]");
        addReadAuth(request);
        ResponseException e = expectThrows(ResponseException.class, () -> client().performRequest(request));
        assertThat(e.getResponse().getStatusLine().getStatusCode(), equalTo(400));
    }

    public void testGetWithNoMatchSelectorReturnsSuccess() throws Exception {
        // match[] is optional for the labels endpoint (unlike series)
        writeMetric("labels_no_selector_gauge", Map.of());
        Request request = new Request("GET", "/_prometheus/api/v1/labels");
        addReadAuth(request);
        Response response = client().performRequest(request);

        assertThat(response.getStatusLine().getStatusCode(), equalTo(200));
        Map<String, Object> body = entityAsMap(response);
        assertThat(body.get("status"), equalTo("success"));
        assertThat(body.get("data"), notNullValue());
    }

    public void testGetResponseIsJsonWithSuccessEnvelope() throws Exception {
        writeMetric("labels_format_gauge", Map.of());

        Response response = queryLabels("labels_format_gauge");

        assertThat(response.getStatusLine().getStatusCode(), equalTo(200));
        assertThat(response.getEntity().getContentType().getValue(), containsString("application/json"));

        Map<String, Object> body = entityAsMap(response);
        assertThat(body.get("status"), equalTo("success"));
        assertThat(body.get("data"), notNullValue());
    }

    public void testGetAlwaysReturnsNameLabel() throws Exception {
        writeMetric("labels_name_gauge", Map.of("job", "labels_test"));

        List<String> data = queryLabelsData("labels_name_gauge");

        assertThat(data, containsInAnyOrder("__name__", "job"));
    }

    public void testGetReturnsIndexedLabels() throws Exception {
        writeMetric("labels_round_trip_gauge", Map.of("job", "labels_test", "instance", "localhost:9090"));

        List<String> data = queryLabelsData("labels_round_trip_gauge");

        assertThat(data, containsInAnyOrder("__name__", "instance", "job"));
    }

    public void testGetWithMatchSelectorFiltersToMatchingLabels() throws Exception {
        writeMetric("labels_filtered_gauge", Map.of("unique_label", "only_here"));
        writeMetric("labels_other_gauge", Map.of("other_label", "other_value"));

        // Query by exact metric name — should return labels only from the matched series
        List<String> data = queryLabelsData("labels_filtered_gauge");

        assertThat(data, hasItem("unique_label"));
    }

    @SuppressWarnings("unchecked")
    public void testGetWithMultipleMatchSelectorsReturnsCombinedLabels() throws Exception {
        writeMetric("multi_labels_metric_a", Map.of("label_only_in_a", "value_a"));
        writeMetric("multi_labels_metric_b", Map.of("label_only_in_b", "value_b"));
        writeMetric("multi_labels_metric_c", Map.of("label_only_in_c", "value_c")); // must not appear in results

        // Use URIBuilder to send two match[] selectors in a single request, working around the
        // test client's single-value-per-key restriction on Request.addParameter.
        Request request = new Request(
            "GET",
            new URIBuilder("/_prometheus/api/v1/labels").addParameter("match[]", "multi_labels_metric_a")
                .addParameter("match[]", "multi_labels_metric_b")
                .build()
                .toString()
        );
        addReadAuth(request);
        List<String> data = (List<String>) entityAsMap(client().performRequest(request)).get("data");

        assertThat(data, containsInAnyOrder("__name__", "label_only_in_a", "label_only_in_b"));
    }

    /** Builds a labels request with optional {@code match[]} parameters. */
    private static Request labelsRequest(String... matchers) {
        Request request = new Request("GET", "/_prometheus/api/v1/labels");
        for (String matcher : matchers) {
            request.addParameter("match[]", matcher);
        }
        return request;
    }

    private Response queryLabels(String... matchers) throws IOException {
        Request request = labelsRequest(matchers);
        addReadAuth(request);
        return client().performRequest(request);
    }

    @SuppressWarnings("unchecked")
    private List<String> queryLabelsData(String... matchers) throws IOException {
        Map<String, Object> body = entityAsMap(queryLabels(matchers));
        return (List<String>) body.get("data");
    }

}
