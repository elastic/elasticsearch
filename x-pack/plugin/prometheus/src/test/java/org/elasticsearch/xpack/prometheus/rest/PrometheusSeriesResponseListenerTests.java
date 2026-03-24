/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.prometheus.rest;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.rest.FakeRestChannel;
import org.elasticsearch.test.rest.FakeRestRequest;

import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class PrometheusSeriesResponseListenerTests extends ESTestCase {

    // -------------------------------------------------------------------------
    // parseDimensions tests
    // -------------------------------------------------------------------------

    public void testParseDimensionsStripsLabelsPrefix() {
        String json = "{\"labels.__name__\":\"up\",\"labels.job\":\"prometheus\",\"labels.instance\":\"localhost:9090\"}";
        Map<String, String> labels = PrometheusSeriesResponseListener.parseDimensions(json);
        assertThat(labels.get("__name__"), equalTo("up"));
        assertThat(labels.get("job"), equalTo("prometheus"));
        assertThat(labels.get("instance"), equalTo("localhost:9090"));
        assertThat(labels.size(), is(3));
    }

    public void testParseDimensionsKeepsNonLabelsPrefixKeys() {
        String json = "{\"metric_name\":\"cpu_usage\",\"labels.env\":\"prod\"}";
        Map<String, String> labels = PrometheusSeriesResponseListener.parseDimensions(json);
        assertThat(labels.get("metric_name"), equalTo("cpu_usage"));
        assertThat(labels.get("env"), equalTo("prod"));
        assertThat(labels.size(), is(2));
    }

    public void testParseDimensionsEmptyObject() {
        assertThat(PrometheusSeriesResponseListener.parseDimensions("{}").isEmpty(), is(true));
    }

    public void testParseDimensionsNullOrBlank() {
        assertThat(PrometheusSeriesResponseListener.parseDimensions(null).isEmpty(), is(true));
        assertThat(PrometheusSeriesResponseListener.parseDimensions("").isEmpty(), is(true));
        assertThat(PrometheusSeriesResponseListener.parseDimensions("   ").isEmpty(), is(true));
    }

    public void testParseDimensionsSingleLabel() {
        String json = "{\"labels.__name__\":\"http_requests_total\"}";
        Map<String, String> labels = PrometheusSeriesResponseListener.parseDimensions(json);
        assertThat(labels.get("__name__"), equalTo("http_requests_total"));
        assertThat(labels.size(), is(1));
    }

    // -------------------------------------------------------------------------
    // Error response tests
    // -------------------------------------------------------------------------

    public void testOnFailureBadRequest() throws Exception {
        FakeRestRequest fakeRequest = new FakeRestRequest();
        FakeRestChannel channel = new FakeRestChannel(fakeRequest, true, 1);
        PrometheusSeriesResponseListener listener = new PrometheusSeriesResponseListener(channel);

        ElasticsearchStatusException ex = new ElasticsearchStatusException("bad selector syntax", RestStatus.BAD_REQUEST);
        listener.onFailure(ex);

        assertThat(channel.errors().get(), is(1));
        assertThat(channel.capturedResponse().status(), equalTo(RestStatus.BAD_REQUEST));
    }

    public void testOnFailureInternalError() throws Exception {
        FakeRestRequest fakeRequest = new FakeRestRequest();
        FakeRestChannel channel = new FakeRestChannel(fakeRequest, true, 1);
        PrometheusSeriesResponseListener listener = new PrometheusSeriesResponseListener(channel);

        listener.onFailure(new RuntimeException("something went wrong"));

        assertThat(channel.errors().get(), is(1));
        assertThat(channel.capturedResponse().status(), equalTo(RestStatus.INTERNAL_SERVER_ERROR));
    }

    public void testOnFailureResponseBodyContainsErrorType() throws Exception {
        FakeRestRequest fakeRequest = new FakeRestRequest();
        FakeRestChannel channel = new FakeRestChannel(fakeRequest, true, 1);
        PrometheusSeriesResponseListener listener = new PrometheusSeriesResponseListener(channel);

        ElasticsearchStatusException ex = new ElasticsearchStatusException("invalid parameter", RestStatus.BAD_REQUEST);
        listener.onFailure(ex);

        String body = channel.capturedResponse().content().utf8ToString();
        assertThat(body, containsString("\"status\":\"error\""));
        assertThat(body, containsString("\"errorType\":\"bad_data\""));
        assertThat(body, containsString("invalid parameter"));
    }

    // -------------------------------------------------------------------------
    // buildLabelMap / metric_name fallback tests (Change 3)
    // -------------------------------------------------------------------------

    public void testBuildLabelMapUsesMetricNameAsFallback() {
        Map<String, String> labels = PrometheusSeriesResponseListener.buildLabelMap("cpu_usage", "{}");
        assertThat(labels.get("__name__"), equalTo("cpu_usage"));
    }

    public void testBuildLabelMapDoesNotOverrideExistingName() {
        Map<String, String> labels = PrometheusSeriesResponseListener.buildLabelMap("something_else", "{\"labels.__name__\":\"up\"}");
        assertThat(labels.get("__name__"), equalTo("up"));
    }

    public void testBuildLabelMapNullMetricNameAndEmptyDimensionsTripsAssertion() {
        expectThrows(AssertionError.class, () -> PrometheusSeriesResponseListener.buildLabelMap(null, "{}"));
    }

    public void testBuildLabelMapWithDimensionsAndFallback() {
        // dimensions has other labels but no __name__; metric_name must fill in __name__
        Map<String, String> labels = PrometheusSeriesResponseListener.buildLabelMap(
            "otel_metric",
            "{\"labels.job\":\"myservice\",\"labels.env\":\"prod\"}"
        );
        assertThat(labels.get("__name__"), equalTo("otel_metric"));
        assertThat(labels.get("job"), equalTo("myservice"));
        assertThat(labels.get("env"), equalTo("prod"));
    }

}
