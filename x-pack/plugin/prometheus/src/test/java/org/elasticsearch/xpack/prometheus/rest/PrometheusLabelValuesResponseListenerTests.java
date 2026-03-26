/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.prometheus.rest;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.rest.FakeRestChannel;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.elasticsearch.xpack.esql.action.EsqlQueryResponse;

import java.util.Arrays;
import java.util.List;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

public class PrometheusLabelValuesResponseListenerTests extends ESTestCase {

    public void testCollectValuesStripsLabelsPrefix() {
        List<String> values = PrometheusLabelValuesResponseListener.collectValues(
            List.of(List.of("labels.prometheus"), List.of("labels.node_exporter"))
        );
        assertThat(values, equalTo(List.of("prometheus", "node_exporter")));
    }

    public void testCollectValuesSingleEntry() {
        List<String> values = PrometheusLabelValuesResponseListener.collectValues(List.of(List.of("labels.production")));
        assertThat(values, equalTo(List.of("production")));
    }

    public void testCollectValuesSkipsNullValues() {
        List<List<Object>> rows = new java.util.ArrayList<>();
        rows.add(Arrays.asList((Object) null));
        rows.add(List.of("labels.staging"));
        assertThat(PrometheusLabelValuesResponseListener.collectValues(rows), equalTo(List.of("staging")));
    }

    public void testCollectValuesEmptyRowsReturnsEmptyList() {
        assertThat(PrometheusLabelValuesResponseListener.collectValues(List.of()).isEmpty(), is(true));
    }

    public void testCollectValuesStripsMetricsPrefix() {
        List<String> values = PrometheusLabelValuesResponseListener.collectValues(
            List.of(List.of("metrics.up"), List.of("metrics.http_requests_total"))
        );
        assertThat(values, equalTo(List.of("up", "http_requests_total")));
    }

    public void testCollectValuesOtelValuesReturnedAsIs() {
        List<String> values = PrometheusLabelValuesResponseListener.collectValues(List.of(List.of("http_server"), List.of("grpc_server")));
        assertThat(values, equalTo(List.of("http_server", "grpc_server")));
    }

    public void testBuildSuccessResponseBodyShape() throws Exception {
        String body = PrometheusLabelValuesResponseListener.buildSuccessResponse(List.of("prometheus", "node_exporter"), 0)
            .content()
            .utf8ToString();
        assertThat(body, containsString("\"status\":\"success\""));
        assertThat(body, containsString("\"prometheus\""));
        assertThat(body, containsString("\"node_exporter\""));
    }

    public void testBuildSuccessResponseEmptyData() throws Exception {
        String body = PrometheusLabelValuesResponseListener.buildSuccessResponse(List.of(), 0).content().utf8ToString();
        assertThat(body, containsString("\"status\":\"success\""));
        assertThat(body, containsString("\"data\":[]"));
    }

    public void testNoTruncationWarningWhenRowsBelowLimitPlusOne() throws Exception {
        // 2 rows, limit=5 → limit+1=6, no truncation
        String body = PrometheusLabelValuesResponseListener.buildSuccessResponse(List.of("a", "b"), 5).content().utf8ToString();
        assertThat(body, not(containsString("warnings")));
    }

    public void testTruncationWarningWhenRowsEqualLimitPlusOne() throws Exception {
        // 6 rows returned (limit+1 sentinel), limit=5 → truncated
        List<String> values = List.of("a", "b", "c", "d", "e", "f");
        String body = PrometheusLabelValuesResponseListener.buildSuccessResponse(values, 5).content().utf8ToString();
        assertThat(body, containsString("warnings"));
        assertThat(body, containsString("results truncated due to limit"));
        // last value should be dropped
        assertThat(body, not(containsString("\"f\"")));
        assertThat(body, containsString("\"e\""));
    }

    public void testTruncationOutputContainsExactlyLimitValues() throws Exception {
        List<String> values = List.of("a", "b", "c", "d", "e", "f"); // 6 = limit+1
        String body = PrometheusLabelValuesResponseListener.buildSuccessResponse(values, 5).content().utf8ToString();
        // "a" through "e" should be present, "f" absent
        assertThat(body, containsString("\"a\""));
        assertThat(body, containsString("\"e\""));
        assertThat(body, not(containsString("\"f\"")));
    }

    public void testNoTruncationWhenLimitIsZero() throws Exception {
        List<String> values = List.of("a", "b", "c");
        String body = PrometheusLabelValuesResponseListener.buildSuccessResponse(values, 0).content().utf8ToString();
        assertThat(body, not(containsString("warnings")));
        assertThat(body, containsString("\"c\""));
    }

    public void testOnFailureBadRequest() {
        FakeRestRequest fakeRequest = new FakeRestRequest();
        FakeRestChannel channel = new FakeRestChannel(fakeRequest, true);
        ActionListener<EsqlQueryResponse> listener = PrometheusLabelValuesResponseListener.create(channel, 0);

        ElasticsearchStatusException ex = new ElasticsearchStatusException("bad selector syntax", RestStatus.BAD_REQUEST);
        listener.onFailure(ex);

        assertThat(channel.errors().get(), is(1));
        assertThat(channel.capturedResponse().status(), equalTo(RestStatus.BAD_REQUEST));
    }

    public void testOnFailureInternalError() {
        FakeRestRequest fakeRequest = new FakeRestRequest();
        FakeRestChannel channel = new FakeRestChannel(fakeRequest, true);
        ActionListener<EsqlQueryResponse> listener = PrometheusLabelValuesResponseListener.create(channel, 0);

        listener.onFailure(new RuntimeException("something went wrong"));

        assertThat(channel.errors().get(), is(1));
        assertThat(channel.capturedResponse().status(), equalTo(RestStatus.INTERNAL_SERVER_ERROR));
    }

    public void testOnFailureResponseBodyContainsBadDataErrorType() {
        FakeRestRequest fakeRequest = new FakeRestRequest();
        FakeRestChannel channel = new FakeRestChannel(fakeRequest, true);
        ActionListener<EsqlQueryResponse> listener = PrometheusLabelValuesResponseListener.create(channel, 0);

        ElasticsearchStatusException ex = new ElasticsearchStatusException("invalid parameter", RestStatus.BAD_REQUEST);
        listener.onFailure(ex);

        String body = channel.capturedResponse().content().utf8ToString();
        assertThat(body, containsString("\"status\":\"error\""));
        assertThat(body, containsString("\"errorType\":\"bad_data\""));
        assertThat(body, containsString("invalid parameter"));
    }

    public void testOnFailureTimeoutReturnsExecutionErrorType() {
        FakeRestRequest fakeRequest = new FakeRestRequest();
        FakeRestChannel channel = new FakeRestChannel(fakeRequest, true);
        ActionListener<EsqlQueryResponse> listener = PrometheusLabelValuesResponseListener.create(channel, 0);

        // Generic RuntimeException does not trigger timeout — errorType is "execution"
        listener.onFailure(new RuntimeException("query failed"));

        String body = channel.capturedResponse().content().utf8ToString();
        assertThat(body, containsString("\"errorType\":\"execution\""));
    }

    public void testIsUnknownColumnReturnsTrueForBadRequest() {
        ElasticsearchStatusException ex = new ElasticsearchStatusException("Unknown column [my_label]", RestStatus.BAD_REQUEST);
        assertThat(PrometheusLabelValuesResponseListener.isUnknownColumn(ex), is(true));
    }

    public void testIsUnknownColumnReturnsTrueForInternalError() {
        // When no TS indices exist at all, @timestamp is also unresolved — the combined error
        // arrives as a 500 but still contains "Unknown column [...]"
        ElasticsearchStatusException ex = new ElasticsearchStatusException(
            "Found 2 problems\nline -1:-1: [] requires the [@timestamp] field...\nline -1:-1: Unknown column [my_label]",
            RestStatus.INTERNAL_SERVER_ERROR
        );
        assertThat(PrometheusLabelValuesResponseListener.isUnknownColumn(ex), is(true));
    }

    public void testIsUnknownColumnReturnsFalseForUnrelatedError() {
        ElasticsearchStatusException ex = new ElasticsearchStatusException(
            "match[] selector must be an instant vector selector",
            RestStatus.BAD_REQUEST
        );
        assertThat(PrometheusLabelValuesResponseListener.isUnknownColumn(ex), is(false));
    }

    public void testIsUnknownColumnReturnsFalseForNull() {
        assertThat(PrometheusLabelValuesResponseListener.isUnknownColumn(null), is(false));
    }

    public void testOnFailureUnknownColumnReturnsEmptyData() throws Exception {
        FakeRestRequest fakeRequest = new FakeRestRequest();
        FakeRestChannel channel = new FakeRestChannel(fakeRequest, true);
        ActionListener<EsqlQueryResponse> listener = PrometheusLabelValuesResponseListener.create(channel, 0);

        ElasticsearchStatusException ex = new ElasticsearchStatusException("Unknown column [nonexistent_label]", RestStatus.BAD_REQUEST);
        listener.onFailure(ex);

        assertThat(channel.responses().get(), is(1));
        assertThat(channel.capturedResponse().status(), equalTo(RestStatus.OK));
        String body = channel.capturedResponse().content().utf8ToString();
        assertThat(body, containsString("\"status\":\"success\""));
        assertThat(body, containsString("\"data\":[]"));
    }
}
