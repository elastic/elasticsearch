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
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;

/**
 * Integration tests for the Prometheus {@code GET /_prometheus/api/v1/label/{name}/values} endpoint.
 *
 * <p>Tests focus on high-level HTTP concerns: routing, request/response format, status codes.
 * Detailed plan-building and response-parsing logic is covered by unit tests.
 */
public class PrometheusLabelValuesRestIT extends AbstractPrometheusRestIT {

    public void testInvalidSelectorSyntaxReturnsBadRequest() throws Exception {
        Request request = labelValuesRequest("job", "{not valid!!!}");
        addReadAuth(request);
        ResponseException e = expectThrows(ResponseException.class, () -> client().performRequest(request));
        assertThat(e.getResponse().getStatusLine().getStatusCode(), equalTo(400));
    }

    public void testRangeSelectorReturnsBadRequest() throws Exception {
        // up[5m] is a range vector, not an instant vector
        Request request = labelValuesRequest("job", "up[5m]");
        addReadAuth(request);
        ResponseException e = expectThrows(ResponseException.class, () -> client().performRequest(request));
        assertThat(e.getResponse().getStatusLine().getStatusCode(), equalTo(400));
    }

    public void testGetResponseIsJsonWithSuccessEnvelope() throws Exception {
        writeMetric("test_gauge", Map.of("job", "prometheus"));

        Request getRequest = labelValuesRequest("job");
        addReadAuth(getRequest);
        Response response = client().performRequest(getRequest);

        assertThat(response.getStatusLine().getStatusCode(), equalTo(200));
        assertThat(response.getEntity().getContentType().getValue(), containsString("application/json"));

        Map<String, Object> body = entityAsMap(response);
        assertThat(body.get("status"), equalTo("success"));
        assertThat(body.get("data"), notNullValue());
    }

    public void testUnknownLabelOnEmptyClusterReturnsKnownError() throws Exception {
        // Known gap: before any Prometheus TS source exists, ESQL fails while resolving the
        // generated @timestamp time filter. Once a TS source exists, unmapped labels are handled by
        // SET unmapped_fields = "NULLIFY" (see testUnmappedLabelOnSpecificPrometheusDataStreamReturnsEmptyNotError).
        Request request = labelValuesRequest("label_that_does_not_exist_anywhere");
        addReadAuth(request);
        ResponseException e = expectThrows(ResponseException.class, () -> client().performRequest(request));

        assertThat(e.getResponse().getStatusLine().getStatusCode(), equalTo(500));
        String body = EntityUtils.toString(e.getResponse().getEntity());
        assertThat(body, containsString("\"status\":\"error\""));
        assertThat(body, containsString("\"errorType\":\"execution\""));
        assertThat(body, containsString("Invalid call to dataType on an unresolved object ?@timestamp"));
    }

    public void testGetReturnsValuesForRegularLabel() throws Exception {
        writeMetric("roundtrip_gauge", Map.of("job", "node_exporter", "instance", "host1:9100"));
        writeMetric("roundtrip_gauge", Map.of("job", "prometheus", "instance", "host2:9090"));

        Request jobRequest = labelValuesRequest("job");
        addReadAuth(jobRequest);
        List<String> values = labelValuesData(client().performRequest(jobRequest));

        assertThat(values, hasItem("node_exporter"));
        assertThat(values, hasItem("prometheus"));
    }

    public void testGetReturnsValuesForNameLabel() throws Exception {
        writeMetric("name_label_metric_a", Map.of("job", "test"));
        writeMetric("name_label_metric_b", Map.of("job", "test"));

        Request nameRequest = labelValuesRequest("__name__");
        addReadAuth(nameRequest);
        List<String> values = labelValuesData(client().performRequest(nameRequest));

        assertThat(values, hasItem("name_label_metric_a"));
        assertThat(values, hasItem("name_label_metric_b"));
    }

    public void testGetWithMatchSelectorFiltersValues() throws Exception {
        writeMetric("selector_metric", Map.of("job", "filtered_job", "env", "prod"));
        writeMetric("other_metric", Map.of("job", "other_job", "env", "staging"));

        // Only request values for "job" where the metric is selector_metric
        Request filteredRequest = labelValuesRequest("job", "selector_metric");
        addReadAuth(filteredRequest);
        List<String> values = labelValuesData(client().performRequest(filteredRequest));

        assertThat(values, hasItem("filtered_job"));
        assertThat(values, not(hasItem("other_job")));
    }

    public void testGetWithRegexNameMatchSelectorForRegularLabelFiltersCorrectly() throws Exception {
        // Verifies that regex __name__ matchers are NOT dropped for regular-label (non-__name__) queries.
        // Before the fix, buildRegularLabelPlan silently ignored such matchers and returned values
        // from all metrics. Now it falls back to filtering on labels.__name__.
        writeMetric("http_requests_regex_lv_test", Map.of("job", "http_job_unique"));
        writeMetric("node_cpu_regex_lv_test", Map.of("job", "other_job_unique"));

        Request request = labelValuesRequest("job", "{__name__=~\"http_requests_regex.*\"}");
        addReadAuth(request);
        List<String> values = labelValuesData(client().performRequest(request));

        assertThat(values, hasItem("http_job_unique"));
        assertThat(values, not(hasItem("other_job_unique")));
    }

    public void testGetWithMultipleMatchSelectorsReturnsCombinedValues() throws Exception {
        writeMetric("multi_selector_metric_a", Map.of("job", "multi_job_a"));
        writeMetric("multi_selector_metric_b", Map.of("job", "multi_job_b"));
        writeMetric("multi_selector_metric_c", Map.of("job", "multi_job_c")); // must not appear in results

        // Use URIBuilder to send two match[] selectors in a single request, working around the
        // test client's single-value-per-key restriction on Request.addParameter.
        Request request = new Request(
            "GET",
            new URIBuilder("/_prometheus/api/v1/label/job/values").addParameter("match[]", "multi_selector_metric_a")
                .addParameter("match[]", "multi_selector_metric_b")
                .build()
                .toString()
        );
        addReadAuth(request);
        List<String> values = labelValuesData(client().performRequest(request));

        assertThat(values, containsInAnyOrder("multi_job_a", "multi_job_b"));
    }

    public void testGetValuesAreSorted() throws Exception {
        writeMetric("sorted_gauge", Map.of("job", "zebra"));
        writeMetric("sorted_gauge", Map.of("job", "alpha"));
        writeMetric("sorted_gauge", Map.of("job", "middle"));

        Request sortedRequest = labelValuesRequest("job");
        addReadAuth(sortedRequest);
        List<String> values = labelValuesData(client().performRequest(sortedRequest));

        // Extract just the values that we wrote (there may be others from earlier tests)
        List<String> ours = values.stream().filter(v -> List.of("zebra", "alpha", "middle").contains(v)).toList();
        assertThat(ours, equalTo(List.of("alpha", "middle", "zebra")));
    }

    public void testNameLabelValuesWithDefaultIndexScopeAndMixedMetricsStreams() throws Exception {
        writeMetric(MIXED_METRICS_PROMETHEUS_METRIC, Map.of("job", "prometheus"));
        writeNonPrometheusMetricsDataStream();

        String apiKey = createPrometheusReadApiKey("prometheus-read-view-index-metadata-key", "metrics-*");

        List<String> defaultScopeValues = labelValuesData(
            client().performRequest(labelValuesRequestWithApiKey("/_prometheus/api/v1/label/__name__/values", apiKey))
        );
        List<String> prometheusScopeValues = labelValuesData(
            client().performRequest(
                labelValuesRequestWithApiKey("/_prometheus/metrics-*.prometheus-*/api/v1/label/__name__/values", apiKey)
            )
        );

        assertThat(defaultScopeValues, hasItem(MIXED_METRICS_PROMETHEUS_METRIC));
        assertThat(prometheusScopeValues, hasItem(MIXED_METRICS_PROMETHEUS_METRIC));
    }

    public void testUnmappedLabelOnSpecificPrometheusDataStreamReturnsEmptyNotError() throws Exception {
        // Write a metric with only "job" label, then query the default data stream by name
        // for "foo" — a label never written by any test and therefore absent from the mapping.
        // SET unmapped_fields = "NULLIFY" makes the field evaluate to null, so ESQL returns no rows.
        writeMetric("prom_nullify_test_metric", Map.of("job", "myjob"));
        Request request = new Request("GET", "/_prometheus/" + DEFAULT_DATA_STREAM + "/api/v1/label/foo/values");
        addReadAuth(request);

        Response response = client().performRequest(request);
        assertThat(response.getStatusLine().getStatusCode(), equalTo(200));
        List<String> values = labelValuesData(response);
        assertThat(values.isEmpty(), equalTo(true));
    }

    public void testUEncodedLabelNameIsDecoded() throws Exception {
        // U__http_2e_requests decodes to http.requests — which doesn't exist, so we just
        // verify the endpoint decodes the label and reaches the known empty-cluster ESQL gap.
        Request request = new Request("GET", "/_prometheus/api/v1/label/U__http_2e_requests/values");
        addReadAuth(request);
        ResponseException e = expectThrows(ResponseException.class, () -> client().performRequest(request));
        assertThat(e.getResponse().getStatusLine().getStatusCode(), equalTo(500));
        String body = EntityUtils.toString(e.getResponse().getEntity());
        assertThat(body, containsString("\"status\":\"error\""));
        assertThat(body, containsString("Invalid call to dataType on an unresolved object ?@timestamp"));
    }

    private static Request labelValuesRequest(String labelName, String... matchers) {
        Request request = new Request("GET", "/_prometheus/api/v1/label/" + labelName + "/values");
        for (String matcher : matchers) {
            request.addParameter("match[]", matcher);
        }
        return request;
    }

    private Request labelValuesRequestWithApiKey(String path, String apiKey) {
        return prometheusGetRequest(path, apiKey);
    }

    @SuppressWarnings("unchecked")
    private List<String> labelValuesData(Response response) throws IOException {
        Map<String, Object> body = entityAsMap(response);
        return (List<String>) body.get("data");
    }

}
