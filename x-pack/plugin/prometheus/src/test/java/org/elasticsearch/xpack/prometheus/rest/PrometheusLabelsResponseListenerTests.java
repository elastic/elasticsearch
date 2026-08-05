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

import java.util.List;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

public class PrometheusLabelsResponseListenerTests extends ESTestCase {

    public void testCollectLabelNamesStripsLabelsPrefix() {
        List<String> names = PrometheusLabelsResponseListener.collectLabelNames(
            List.of(List.of("labels.__name__"), List.of("labels.job"), List.of("labels.instance"))
        );
        assertThat(names, equalTo(List.of("__name__", "job", "instance")));
    }

    public void testCollectLabelNamesSingleEntry() {
        List<String> names = PrometheusLabelsResponseListener.collectLabelNames(List.of(List.of("labels.__name__")));
        assertThat(names, equalTo(List.of("__name__")));
    }

    public void testCollectLabelNamesSkipsNullValues() {
        List<List<Object>> rows = new java.util.ArrayList<>();
        rows.add(java.util.Arrays.asList((Object) null));
        rows.add(List.of("labels.job"));
        assertThat(PrometheusLabelsResponseListener.collectLabelNames(rows), equalTo(List.of("job")));
    }

    public void testCollectLabelNamesEmptyRowsReturnsEmptyList() {
        assertThat(PrometheusLabelsResponseListener.collectLabelNames(List.of()).isEmpty(), is(true));
    }

    public void testCollectLabelNamesOtelFieldsReturnedAsIs() {
        // OTel dimension fields carry no "labels." prefix
        List<String> names = PrometheusLabelsResponseListener.collectLabelNames(
            List.of(List.of("attributes.service.name"), List.of("attributes.host.name"))
        );
        assertThat(names, equalTo(List.of("attributes.service.name", "attributes.host.name")));
    }

    public void testCollectLabelNamesMixedPrefixAndOtel() {
        // Prometheus and OTel dimension field names in the same result set
        List<String> names = PrometheusLabelsResponseListener.collectLabelNames(
            List.of(List.of("labels.__name__"), List.of("labels.job"), List.of("attributes.service.name"))
        );
        assertThat(names, equalTo(List.of("__name__", "job", "attributes.service.name")));
    }

    public void testBuildSuccessResponseAlwaysEmitsNameLabelFirst() throws Exception {
        // OTel case: __name__ is absent from the ESQL result — injected by serialisation
        String body = PrometheusLabelsResponseListener.buildSuccessResponse(List.of("job", "env"), 0).content().utf8ToString();
        assertThat(body, containsString("\"__name__\""));
        // __name__ must appear before other labels in the JSON array
        assertThat(body.indexOf("\"__name__\"") < body.indexOf("\"env\""), is(true));
        assertThat(body.indexOf("\"__name__\"") < body.indexOf("\"job\""), is(true));
    }

    public void testBuildSuccessResponseDeduplicatesNameLabelForPrometheus() throws Exception {
        // Prometheus case: __name__ comes through from the ESQL plan — must not appear twice
        String body = PrometheusLabelsResponseListener.buildSuccessResponse(List.of("__name__", "job"), 0).content().utf8ToString();
        int first = body.indexOf("\"__name__\"");
        int second = body.indexOf("\"__name__\"", first + 1);
        assertThat("__name__ should appear exactly once", second, is(-1));
    }

    public void testBuildSuccessResponseBodyShape() throws Exception {
        String body = PrometheusLabelsResponseListener.buildSuccessResponse(List.of("job"), 0).content().utf8ToString();
        assertThat(body, containsString("\"status\":\"success\""));
        assertThat(body, containsString("\"__name__\""));
        assertThat(body, containsString("\"job\""));
    }

    public void testNoWarningWhenResultsBelowLimit() throws Exception {
        String body = PrometheusLabelsResponseListener.buildSuccessResponse(List.of("env", "job"), 5).content().utf8ToString();
        assertThat(body, not(containsString("warnings")));
    }

    public void testWarningWhenResultsEqualLimitPlusOne() throws Exception {
        // The plan builder emits LIMIT limit+1 as a sentinel. If ESQL returns limit+1 rows the
        // result was truncated. The sentinel row is excluded from the output.
        String body = PrometheusLabelsResponseListener.buildSuccessResponse(List.of("env", "job", "sentinel"), 2).content().utf8ToString();
        assertThat(body, containsString("warnings"));
        assertThat(body, containsString("results truncated due to limit"));
        // sentinel row must not be emitted
        assertThat(body, not(containsString("sentinel")));
    }

    public void testNoWarningWhenLimitIsZero() throws Exception {
        String body = PrometheusLabelsResponseListener.buildSuccessResponse(List.of("env", "job"), 0).content().utf8ToString();
        assertThat(body, not(containsString("warnings")));
    }

    public void testOnFailureBadRequest() throws Exception {
        FakeRestRequest fakeRequest = new FakeRestRequest();
        FakeRestChannel channel = new FakeRestChannel(fakeRequest, true);
        var listener = PrometheusLabelsResponseListener.create(channel, 0);

        ElasticsearchStatusException ex = new ElasticsearchStatusException("bad selector syntax", RestStatus.BAD_REQUEST);
        listener.onFailure(ex);

        assertThat(channel.errors().get(), is(1));
        assertThat(channel.capturedResponse().status(), equalTo(RestStatus.BAD_REQUEST));
    }

    public void testOnFailureInternalError() throws Exception {
        FakeRestRequest fakeRequest = new FakeRestRequest();
        FakeRestChannel channel = new FakeRestChannel(fakeRequest, true);
        var listener = PrometheusLabelsResponseListener.create(channel, 0);

        listener.onFailure(new RuntimeException("something went wrong"));

        assertThat(channel.errors().get(), is(1));
        assertThat(channel.capturedResponse().status(), equalTo(RestStatus.INTERNAL_SERVER_ERROR));
    }

    public void testOnFailureResponseBodyContainsErrorType() throws Exception {
        FakeRestRequest fakeRequest = new FakeRestRequest();
        FakeRestChannel channel = new FakeRestChannel(fakeRequest, true);
        var listener = PrometheusLabelsResponseListener.create(channel, 0);

        ElasticsearchStatusException ex = new ElasticsearchStatusException("invalid parameter", RestStatus.BAD_REQUEST);
        listener.onFailure(ex);

        String body = channel.capturedResponse().content().utf8ToString();
        assertThat(body, containsString("\"status\":\"error\""));
        assertThat(body, containsString("\"errorType\":\"bad_data\""));
        assertThat(body, containsString("invalid parameter"));
    }
}
