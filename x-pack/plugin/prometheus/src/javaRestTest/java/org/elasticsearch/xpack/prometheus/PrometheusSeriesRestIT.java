/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.prometheus;

import org.apache.http.client.utils.URIBuilder;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.notNullValue;

/**
 * Integration tests for the Prometheus {@code GET /api/v1/series} endpoint.
 *
 * <p>Tests focus on high-level HTTP concerns: routing, request/response format, status codes.
 * Detailed plan-building and response-parsing logic is covered by unit tests.
 */
public class PrometheusSeriesRestIT extends AbstractPrometheusRestIT {

    // Validation — 400 responses

    public void testMissingMatchSelectorReturnsBadRequest() throws Exception {
        Request request = new Request("GET", "/_prometheus/api/v1/series");
        addReadAuth(request);
        ResponseException e = expectThrows(ResponseException.class, () -> client().performRequest(request));
        assertThat(e.getResponse().getStatusLine().getStatusCode(), equalTo(400));
        assertThat(EntityUtils.toString(e.getResponse().getEntity()), containsString("match[]"));
    }

    public void testInvalidSelectorSyntaxReturnsBadRequest() throws Exception {
        // {not valid!!!} is not valid PromQL
        Request request = seriesRequest("{not valid!!!}");
        addReadAuth(request);
        ResponseException e = expectThrows(ResponseException.class, () -> client().performRequest(request));
        assertThat(e.getResponse().getStatusLine().getStatusCode(), equalTo(400));
    }

    public void testRangeSelectorReturnsBadRequest() throws Exception {
        // up[5m] is a range vector, not an instant vector
        Request request = seriesRequest("up[5m]");
        addReadAuth(request);
        ResponseException e = expectThrows(ResponseException.class, () -> client().performRequest(request));
        assertThat(e.getResponse().getStatusLine().getStatusCode(), equalTo(400));
    }

    // Response format — 200 with JSON envelope

    public void testGetResponseIsJsonWithSuccessEnvelope() throws Exception {
        writeMetric("test_gauge", Map.of());

        Response response = querySeries("test_gauge");

        assertThat(response.getStatusLine().getStatusCode(), equalTo(200));
        assertThat(response.getEntity().getContentType().getValue(), containsString("application/json"));

        Map<String, Object> body = entityAsMap(response);
        assertThat(body.get("status"), equalTo("success"));
        assertThat(body.get("data"), notNullValue());
    }

    // Data round-trip — write via remote write, read back via series

    public void testGetReturnsIndexedSeries() throws Exception {
        writeMetric("test_gauge", Map.of("job", "series_test", "instance", "localhost:9090"), 42.0);

        List<Map<String, Object>> data = querySeriesData("test_gauge");

        assertThat(data, hasSize(1));
        Map<String, Object> series = data.getFirst();
        assertThat(series.get("__name__"), equalTo("test_gauge"));
        assertThat(series.get("job"), equalTo("series_test"));
        assertThat(series.get("instance"), equalTo("localhost:9090"));
    }

    public void testGetWithLabelMatcherFiltersCorrectly() throws Exception {
        writeMetric("matched_metric", Map.of("job", "target_job"));
        writeMetric("other_metric", Map.of("job", "other_job"));

        // Query by exact metric name — should only return the matched metric
        List<Map<String, Object>> data = querySeriesData("matched_metric");

        assertThat(data, hasSize(1));
        assertThat(data.getFirst().get("__name__"), equalTo("matched_metric"));
        assertThat(data.getFirst().get("job"), equalTo("target_job"));
    }

    public void testSeriesWithIndexPattern() throws Exception {
        writeMetric("test_gauge_idx", Map.of("job", "index_test"));

        List<Map<String, Object>> data = querySeriesData("metrics-generic.prometheus-*", "test_gauge_idx");

        assertThat(data, hasSize(1));
        assertThat(data.getFirst().get("__name__"), equalTo("test_gauge_idx"));
    }

    public void testGetWithMultipleMatchSelectorsReturnsCombinedSeries() throws Exception {
        writeMetric("multi_series_selector_a", Map.of("job", "job_a"));
        writeMetric("multi_series_selector_b", Map.of("job", "job_b"));
        writeMetric("multi_series_selector_c", Map.of("job", "job_c")); // must not appear in results

        // Use URIBuilder to send two match[] selectors in a single request, working around the
        // test client's single-value-per-key restriction on Request.addParameter.
        Request request = new Request(
            "GET",
            new URIBuilder("/_prometheus/api/v1/series").addParameter("match[]", "multi_series_selector_a")
                .addParameter("match[]", "multi_series_selector_b")
                .build()
                .toString()
        );
        addReadAuth(request);
        List<Map<String, Object>> data = seriesData(client().performRequest(request));

        List<String> names = data.stream().map(s -> (String) s.get("__name__")).toList();
        assertThat(names, containsInAnyOrder("multi_series_selector_a", "multi_series_selector_b"));
    }

    // Helpers

    private static Request seriesRequest(String matcher) {
        return seriesRequest(null, matcher);
    }

    private static Request seriesRequest(String index, String matcher) {
        String path = index == null ? "/_prometheus/api/v1/series" : "/_prometheus/" + index + "/api/v1/series";
        Request request = new Request("GET", path);
        request.addParameter("match[]", matcher);
        return request;
    }

    private Response querySeries(String matcher) throws IOException {
        return querySeries(null, matcher);
    }

    private Response querySeries(String index, String matcher) throws IOException {
        Request request = seriesRequest(index, matcher);
        addReadAuth(request);
        return client().performRequest(request);
    }

    @SuppressWarnings("unchecked")
    private List<Map<String, Object>> querySeriesData(String matcher) throws IOException {
        return querySeriesData(null, matcher);
    }

    @SuppressWarnings("unchecked")
    private List<Map<String, Object>> querySeriesData(String index, String matcher) throws IOException {
        return seriesData(querySeries(index, matcher));
    }

    @SuppressWarnings("unchecked")
    private List<Map<String, Object>> seriesData(Response response) throws IOException {
        Map<String, Object> body = entityAsMap(response);
        return (List<Map<String, Object>>) body.get("data");
    }

}
