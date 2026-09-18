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
import org.elasticsearch.xpack.prometheus.rest.PrometheusMetadataResponseListener.MetadataEntry;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

public class PrometheusMetadataResponseListenerTests extends ESTestCase {

    // --- collectEntries: prefix stripping ---

    public void testCollectEntriesStripsMetricsPrefix() {
        LinkedHashMap<String, List<MetadataEntry>> entries = PrometheusMetadataResponseListener.collectEntries(
            List.of(List.of("metrics.up", "gauge", "bytes")),
            0,
            0
        );
        assertThat(entries.containsKey("up"), is(true));
        assertThat(entries.containsKey("metrics.up"), is(false));
    }

    public void testCollectEntriesNamesWithoutMetricsPrefixPassThrough() {
        LinkedHashMap<String, List<MetadataEntry>> entries = PrometheusMetadataResponseListener.collectEntries(
            List.of(List.of("http_server_requests", "counter", "")),
            0,
            0
        );
        assertThat(entries.containsKey("http_server_requests"), is(true));
    }

    // --- collectEntries: null handling ---

    public void testCollectEntriesSkipsNullName() {
        List<List<Object>> rows = new java.util.ArrayList<>();
        rows.add(Arrays.asList((Object) null, "gauge", "bytes"));
        rows.add(List.of("metrics.up", "gauge", ""));
        LinkedHashMap<String, List<MetadataEntry>> entries = PrometheusMetadataResponseListener.collectEntries(rows, 0, 0);
        assertThat(entries.size(), is(1));
        assertThat(entries.containsKey("up"), is(true));
    }

    public void testCollectEntriesNullUnitBecomesEmptyString() {
        List<List<Object>> rows = new java.util.ArrayList<>();
        rows.add(Arrays.asList((Object) "metrics.up", "gauge", null));
        LinkedHashMap<String, List<MetadataEntry>> entries = PrometheusMetadataResponseListener.collectEntries(rows, 0, 0);
        assertThat(entries.get("up").get(0).unit(), equalTo(""));
    }

    public void testCollectEntriesNullTypeBecomesEmptyString() {
        List<List<Object>> rows = new java.util.ArrayList<>();
        rows.add(Arrays.asList((Object) "metrics.up", null, "bytes"));
        LinkedHashMap<String, List<MetadataEntry>> entries = PrometheusMetadataResponseListener.collectEntries(rows, 0, 0);
        assertThat(entries.get("up").get(0).type(), equalTo(""));
    }

    // --- collectEntries: multi-metric ---

    public void testCollectEntriesMultipleMetrics() {
        LinkedHashMap<String, List<MetadataEntry>> entries = PrometheusMetadataResponseListener.collectEntries(
            List.of(List.of("metrics.up", "gauge", ""), List.of("metrics.http_requests_total", "counter", "requests")),
            0,
            0
        );
        assertThat(entries.size(), is(2));
        assertThat(entries.containsKey("up"), is(true));
        assertThat(entries.containsKey("http_requests_total"), is(true));
    }

    public void testCollectEntriesSingleMetricMultipleEntries() {
        LinkedHashMap<String, List<MetadataEntry>> entries = PrometheusMetadataResponseListener.collectEntries(
            List.of(List.of("metrics.up", "gauge", ""), List.of("metrics.up", "counter", "requests")),
            0,
            0
        );
        assertThat(entries.get("up").size(), is(2));
    }

    // --- collectEntries: limit on distinct metric names ---

    public void testCollectEntriesLimitZeroAllowsUnlimited() {
        LinkedHashMap<String, List<MetadataEntry>> entries = PrometheusMetadataResponseListener.collectEntries(
            List.of(List.of("a", "gauge", ""), List.of("b", "counter", ""), List.of("c", "gauge", "")),
            0,
            0
        );
        assertThat(entries.size(), is(3));
    }

    public void testCollectEntriesLimitCollectsSentinelPlusOne() {
        // limit=2: collect at most 3 distinct metric names as sentinel
        LinkedHashMap<String, List<MetadataEntry>> entries = PrometheusMetadataResponseListener.collectEntries(
            List.of(List.of("a", "gauge", ""), List.of("b", "gauge", ""), List.of("c", "gauge", ""), List.of("d", "gauge", "")),
            2,
            0
        );
        // Should collect 3 (limit+1 sentinel), not 4
        assertThat(entries.size(), is(3));
    }

    // --- collectEntries: limitPerMetric ---

    public void testCollectEntriesLimitPerMetricCapsEntries() {
        LinkedHashMap<String, List<MetadataEntry>> entries = PrometheusMetadataResponseListener.collectEntries(
            List.of(List.of("metrics.up", "gauge", ""), List.of("metrics.up", "counter", ""), List.of("metrics.up", "histogram", "")),
            0,
            2
        );
        assertThat(entries.get("up").size(), is(2));
    }

    public void testCollectEntriesLimitPerMetricZeroAllowsUnlimited() {
        LinkedHashMap<String, List<MetadataEntry>> entries = PrometheusMetadataResponseListener.collectEntries(
            List.of(List.of("metrics.up", "gauge", ""), List.of("metrics.up", "counter", ""), List.of("metrics.up", "histogram", "")),
            0,
            0
        );
        assertThat(entries.get("up").size(), is(3));
    }

    // --- buildSuccessResponse: JSON shape ---

    public void testBuildSuccessResponseStatusIsSuccess() throws Exception {
        LinkedHashMap<String, List<MetadataEntry>> entries = new LinkedHashMap<>();
        entries.put("up", List.of(new MetadataEntry("gauge", "")));
        String body = PrometheusMetadataResponseListener.buildSuccessResponse(entries, 0).content().utf8ToString();
        assertThat(body, containsString("\"status\":\"success\""));
    }

    public void testBuildSuccessResponseDataIsObject() throws Exception {
        LinkedHashMap<String, List<MetadataEntry>> entries = new LinkedHashMap<>();
        entries.put("up", List.of(new MetadataEntry("gauge", "")));
        String body = PrometheusMetadataResponseListener.buildSuccessResponse(entries, 0).content().utf8ToString();
        assertThat(body, containsString("\"data\":{"));
    }

    public void testBuildSuccessResponseEntryHasTypeHelpUnit() throws Exception {
        LinkedHashMap<String, List<MetadataEntry>> entries = new LinkedHashMap<>();
        entries.put("up", List.of(new MetadataEntry("gauge", "bytes")));
        String body = PrometheusMetadataResponseListener.buildSuccessResponse(entries, 0).content().utf8ToString();
        assertThat(body, containsString("\"type\":\"gauge\""));
        assertThat(body, containsString("\"help\":\"\""));
        assertThat(body, containsString("\"unit\":\"bytes\""));
    }

    public void testBuildSuccessResponseHelpIsAlwaysEmpty() throws Exception {
        LinkedHashMap<String, List<MetadataEntry>> entries = new LinkedHashMap<>();
        entries.put("up", List.of(new MetadataEntry("gauge", "")));
        String body = PrometheusMetadataResponseListener.buildSuccessResponse(entries, 0).content().utf8ToString();
        assertThat(body, containsString("\"help\":\"\""));
    }

    public void testBuildSuccessResponseEmptyData() throws Exception {
        String body = PrometheusMetadataResponseListener.buildSuccessResponse(new LinkedHashMap<>(), 0).content().utf8ToString();
        assertThat(body, containsString("\"status\":\"success\""));
        assertThat(body, containsString("\"data\":{}"));
    }

    // --- buildSuccessResponse: limit truncation ---

    public void testNoTruncationWarningWhenBelowSentinel() throws Exception {
        LinkedHashMap<String, List<MetadataEntry>> entries = new LinkedHashMap<>();
        entries.put("a", List.of(new MetadataEntry("gauge", "")));
        entries.put("b", List.of(new MetadataEntry("counter", "")));
        // limit=5, only 2 distinct metrics → no truncation
        String body = PrometheusMetadataResponseListener.buildSuccessResponse(entries, 5).content().utf8ToString();
        assertThat(body, not(containsString("warnings")));
    }

    public void testTruncationWarningWhenSentinelReached() throws Exception {
        // limit=2: sentinel is 3 distinct metrics → truncate to 2
        LinkedHashMap<String, List<MetadataEntry>> entries = new LinkedHashMap<>();
        entries.put("a", List.of(new MetadataEntry("gauge", "")));
        entries.put("b", List.of(new MetadataEntry("counter", "")));
        entries.put("c", List.of(new MetadataEntry("gauge", "")));
        String body = PrometheusMetadataResponseListener.buildSuccessResponse(entries, 2).content().utf8ToString();
        assertThat(body, containsString("warnings"));
        assertThat(body, containsString("results truncated due to limit"));
        assertThat(body, not(containsString("\"c\"")));
        assertThat(body, containsString("\"b\""));
    }

    public void testNoTruncationWhenLimitIsZero() throws Exception {
        LinkedHashMap<String, List<MetadataEntry>> entries = new LinkedHashMap<>();
        entries.put("a", List.of(new MetadataEntry("gauge", "")));
        entries.put("b", List.of(new MetadataEntry("counter", "")));
        entries.put("c", List.of(new MetadataEntry("gauge", "")));
        String body = PrometheusMetadataResponseListener.buildSuccessResponse(entries, 0).content().utf8ToString();
        assertThat(body, not(containsString("warnings")));
        assertThat(body, containsString("\"c\""));
    }

    // --- Error handling ---

    public void testOnFailureBadRequest() {
        FakeRestRequest fakeRequest = new FakeRestRequest();
        FakeRestChannel channel = new FakeRestChannel(fakeRequest, true);
        ActionListener<EsqlQueryResponse> listener = PrometheusMetadataResponseListener.create(channel, 0, 0);

        ElasticsearchStatusException ex = new ElasticsearchStatusException("bad selector syntax", RestStatus.BAD_REQUEST);
        listener.onFailure(ex);

        assertThat(channel.errors().get(), is(1));
        assertThat(channel.capturedResponse().status(), equalTo(RestStatus.BAD_REQUEST));
    }

    public void testOnFailureInternalError() {
        FakeRestRequest fakeRequest = new FakeRestRequest();
        FakeRestChannel channel = new FakeRestChannel(fakeRequest, true);
        ActionListener<EsqlQueryResponse> listener = PrometheusMetadataResponseListener.create(channel, 0, 0);

        listener.onFailure(new RuntimeException("something went wrong"));

        assertThat(channel.errors().get(), is(1));
        assertThat(channel.capturedResponse().status(), equalTo(RestStatus.INTERNAL_SERVER_ERROR));
    }

    public void testOnFailureResponseBodyContainsErrorFields() {
        FakeRestRequest fakeRequest = new FakeRestRequest();
        FakeRestChannel channel = new FakeRestChannel(fakeRequest, true);
        ActionListener<EsqlQueryResponse> listener = PrometheusMetadataResponseListener.create(channel, 0, 0);

        ElasticsearchStatusException ex = new ElasticsearchStatusException("invalid parameter", RestStatus.BAD_REQUEST);
        listener.onFailure(ex);

        String body = channel.capturedResponse().content().utf8ToString();
        assertThat(body, containsString("\"status\":\"error\""));
        assertThat(body, containsString("\"errorType\":\"bad_data\""));
        assertThat(body, containsString("invalid parameter"));
    }
}
