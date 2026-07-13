/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.datastreams;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.common.time.FormatNames;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.ClassRule;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Tests the {@code ES95} TSDB doc values codec inside data stream lifecycles. Covers
 * codec switches across manual rollover boundaries via template updates, and alias
 * level queries across mixed codec backing indices. Per segment format equivalence
 * is covered by {@code ES95VsES819DocValuesDuelTests}.
 */
public class TsdbES95DataStreamLifecycleRestIT extends ESRestTestCase {

    @ClassRule
    public static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .distribution(DistributionType.DEFAULT)
        .setting("xpack.security.enabled", "false")
        .setting("xpack.watcher.enabled", "false")
        .setting("xpack.ml.enabled", "false")
        .build();

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    @Override
    protected Settings restAdminSettings() {
        if (super.restAdminSettings().keySet().contains(ThreadContext.PREFIX + ".Authorization")) {
            return super.restAdminSettings();
        }
        final String token = basicAuthHeaderValue("admin", new SecureString("admin-password".toCharArray()));
        return Settings.builder().put(super.restAdminSettings()).put(ThreadContext.PREFIX + ".Authorization", token).build();
    }

    private static final int MIN_DOC_COUNT = 50;
    private static final int MAX_DOC_COUNT = 200;
    private static final int MIN_ADDITIONAL_DOC_COUNT = 10;
    private static final int MAX_ADDITIONAL_DOC_COUNT = 40;

    private static final long STEP_MS = 1_000L;
    private static final String HOST_A = "host-alpha";
    private static final String HOST_B = "host-beta";
    private static final DateFormatter TS_FORMATTER = DateFormatter.forPattern(FormatNames.STRICT_DATE_TIME.getName());

    @After
    public void cleanUp() throws IOException {
        adminClient().performRequest(new Request("DELETE", "/_data_stream/*?expand_wildcards=hidden"));
        adminClient().performRequest(new Request("DELETE", "/_index_template/*"));
    }

    public void testRolloverFlipsCodecBaselineToES95() throws Exception {
        final String stream = "tsdb-ds-rollover-baseline-to-es95";
        putTSDBTemplate(stream, false);
        createDataStream(stream);

        final int n1 = randomIntBetween(MIN_DOC_COUNT, MAX_DOC_COUNT);
        bulkIndex(stream, n1, 0);
        final String gen1 = backingIndices(stream).get(0);
        assertCodecSetting(gen1, "false");

        putTSDBTemplate(stream, true);
        rollover(stream);
        final int n2 = randomIntBetween(MIN_DOC_COUNT, MAX_DOC_COUNT);
        bulkIndex(stream, n2, n1);
        final String gen2 = backingIndices(stream).get(1);
        assertCodecSetting(gen2, "true");

        refresh(stream);
        final int total = n1 + n2;
        assertDocumentCount(stream, total);
        assertExactHostnameCounts(stream, total);
        assertGaugeSumMatches(stream, expectedGaugeSum(total));
    }

    public void testRolloverFlipsCodecES95ToBaseline() throws Exception {
        final String stream = "tsdb-ds-rollover-es95-to-baseline";
        putTSDBTemplate(stream, true);
        createDataStream(stream);

        final int n1 = randomIntBetween(MIN_DOC_COUNT, MAX_DOC_COUNT);
        bulkIndex(stream, n1, 0);
        final String gen1 = backingIndices(stream).get(0);
        assertCodecSetting(gen1, "true");

        putTSDBTemplate(stream, false);
        rollover(stream);
        final int n2 = randomIntBetween(MIN_DOC_COUNT, MAX_DOC_COUNT);
        bulkIndex(stream, n2, n1);
        final String gen2 = backingIndices(stream).get(1);
        assertCodecSetting(gen2, "false");

        refresh(stream);
        final int total = n1 + n2;
        assertDocumentCount(stream, total);
        assertExactHostnameCounts(stream, total);
        assertGaugeSumMatches(stream, expectedGaugeSum(total));
    }

    public void testAlternatingCodecAcrossFourRollovers() throws Exception {
        final String stream = "tsdb-ds-alternating-codec";
        putTSDBTemplate(stream, false);
        createDataStream(stream);

        final int n0 = randomIntBetween(MIN_DOC_COUNT, MAX_DOC_COUNT);
        bulkIndex(stream, n0, 0);

        putTSDBTemplate(stream, true);
        rollover(stream);
        final int n1 = randomIntBetween(MIN_DOC_COUNT, MAX_DOC_COUNT);
        bulkIndex(stream, n1, n0);

        putTSDBTemplate(stream, false);
        rollover(stream);
        final int n2 = randomIntBetween(MIN_DOC_COUNT, MAX_DOC_COUNT);
        bulkIndex(stream, n2, n0 + n1);

        putTSDBTemplate(stream, true);
        rollover(stream);
        final int n3 = randomIntBetween(MIN_DOC_COUNT, MAX_DOC_COUNT);
        bulkIndex(stream, n3, n0 + n1 + n2);

        final List<String> backing = backingIndices(stream);
        assertThat(backing, Matchers.hasSize(4));
        assertCodecSetting(backing.get(0), "false");
        assertCodecSetting(backing.get(1), "true");
        assertCodecSetting(backing.get(2), "false");
        assertCodecSetting(backing.get(3), "true");

        refresh(stream);
        final int total = n0 + n1 + n2 + n3;
        assertDocumentCount(stream, total);
        assertExactHostnameCounts(stream, total);
        assertGaugeSumMatches(stream, expectedGaugeSum(total));
    }

    public void testQueryAcrossMixedCodecBackingIndicesViaAlias() throws Exception {
        final String stream = "tsdb-ds-mixed-query";
        putTSDBTemplate(stream, false);
        createDataStream(stream);

        final int n1 = randomIntBetween(MIN_DOC_COUNT, MAX_DOC_COUNT);
        bulkIndex(stream, n1, 0);

        putTSDBTemplate(stream, true);
        rollover(stream);
        final int n2 = randomIntBetween(MIN_DOC_COUNT, MAX_DOC_COUNT);
        bulkIndex(stream, n2, n1);
        refresh(stream);

        final List<String> backing = backingIndices(stream);
        assertThat(documentCount(backing.get(0)), Matchers.equalTo((long) n1));
        assertThat(documentCount(backing.get(1)), Matchers.equalTo((long) n2));

        final int total = n1 + n2;
        assertDocumentCount(stream, total);
        assertExactHostnameCounts(stream, total);
        assertGaugeSumMatches(stream, expectedGaugeSum(total));
    }

    public void testRandomAccessDocValuesAcrossBackingIndices() throws Exception {
        final String stream = "tsdb-ds-random-access";
        putTSDBTemplate(stream, false);
        createDataStream(stream);

        final int n1 = randomIntBetween(MIN_DOC_COUNT, MAX_DOC_COUNT);
        bulkIndex(stream, n1, 0);

        putTSDBTemplate(stream, true);
        rollover(stream);
        final int n2 = randomIntBetween(MIN_DOC_COUNT, MAX_DOC_COUNT);
        bulkIndex(stream, n2, n1);
        refresh(stream);

        final int total = n1 + n2;
        final List<String> backing = backingIndices(stream);

        assertTopHitsDocValuesByTimestampDesc(stream, total - 1, Math.min(5, total));
        assertTopHitsDocValuesByTimestampDesc(backing.get(0), n1 - 1, Math.min(5, n1));
        assertTopHitsDocValuesByTimestampDesc(backing.get(1), total - 1, Math.min(5, n2));
    }

    private void putTSDBTemplate(final String streamName, boolean enableES95) throws IOException {
        final String body = String.format(Locale.ROOT, """
            {
              "index_patterns": ["%s"],
              "data_stream": {},
              "template": {
                "settings": {
                  "index.mode": "time_series",
                  "index.routing_path": ["hostname"],
                  "index.time_series.es95_codec.enabled": %s,
                  "index.number_of_shards": 1
                },
                "mappings": {
                  "properties": {
                    "@timestamp": { "type": "date" },
                    "hostname": { "type": "keyword", "time_series_dimension": true },
                    "gauge": { "type": "long", "time_series_metric": "gauge" }
                  }
                }
              }
            }""", streamName, enableES95);
        final Request request = new Request("PUT", "/_index_template/" + streamName);
        request.setJsonEntity(body);
        assertOK(client().performRequest(request));
    }

    private void createDataStream(final String streamName) throws IOException {
        assertOK(client().performRequest(new Request("PUT", "/_data_stream/" + streamName)));
    }

    private void rollover(final String streamName) throws IOException {
        assertOK(client().performRequest(new Request("POST", "/" + streamName + "/_rollover")));
    }

    private void ensureSearchable(final String indexName) throws Exception {
        // Stateless provisions the search shard asynchronously; a read before it is ready returns 503.
        assertBusy(() -> {
            try {
                client().performRequest(new Request("GET", "/" + indexName + "/_search?size=0"));
            } catch (final ResponseException e) {
                if (e.getResponse().getStatusLine().getStatusCode() == 503) {
                    throw new AssertionError(e);
                }
                throw e;
            }
        }, 60, TimeUnit.SECONDS);
    }

    private void bulkIndex(final String streamName, int docCount, int gaugeStart) throws Exception {
        final String writeIdx = writeBackingIndex(streamName);
        ensureSearchable(writeIdx);
        final long startMs = readBackingIndexStartTime(writeIdx).toEpochMilli();
        final long existing = documentCount(writeIdx);
        final StringBuilder bulk = new StringBuilder();
        for (int i = 0; i < docCount; i++) {
            final int gauge = gaugeStart + i;
            final String ts = TS_FORMATTER.format(Instant.ofEpochMilli(startMs + (existing + i) * STEP_MS));
            bulk.append("{\"create\":{}}\n");
            bulk.append("{\"@timestamp\":\"")
                .append(ts)
                .append("\",\"hostname\":\"")
                .append(hostnameForGauge(gauge))
                .append("\",\"gauge\":")
                .append(gauge)
                .append("}\n");
        }
        final Request request = new Request("POST", "/" + streamName + "/_bulk");
        request.setJsonEntity(bulk.toString());
        request.addParameter("refresh", "true");
        final Response response = client().performRequest(request);
        assertOK(response);
        assertThat(entityAsMap(response).get("errors"), Matchers.is(false));
    }

    @SuppressWarnings("unchecked")
    private List<String> backingIndices(final String streamName) throws IOException {
        final Map<String, Object> response = entityAsMap(client().performRequest(new Request("GET", "/_data_stream/" + streamName)));
        final List<Map<String, Object>> streams = (List<Map<String, Object>>) response.get("data_streams");
        final List<Map<String, Object>> indices = (List<Map<String, Object>>) streams.getFirst().get("indices");
        final List<String> names = new ArrayList<>(indices.size());
        for (final Map<String, Object> entry : indices) {
            names.add((String) entry.get("index_name"));
        }
        return names;
    }

    private String writeBackingIndex(final String streamName) throws IOException {
        final List<String> all = backingIndices(streamName);
        return all.getLast();
    }

    private void assertCodecSetting(final String indexName, final String expected) throws IOException {
        final Map<String, Object> settings = getFlatIndexSettings(indexName);
        assertThat(settings.get("index.time_series.es95_codec.enabled"), Matchers.equalTo(expected));
    }

    private Instant readBackingIndexStartTime(final String indexName) throws IOException {
        final String val = (String) getFlatIndexSettings(indexName).get("index.time_series.start_time");
        return Instant.from(TS_FORMATTER.parse(val));
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> getFlatIndexSettings(final String indexName) throws IOException {
        final Request request = new Request("GET", "/" + indexName + "/_settings?flat_settings");
        final Map<String, Object> response = entityAsMap(client().performRequest(request));
        final Map<String, Object> indexResponse = (Map<String, Object>) response.get(indexName);
        return (Map<String, Object>) indexResponse.get("settings");
    }

    private long documentCount(final String target) throws IOException {
        final Map<String, Object> response = entityAsMap(client().performRequest(new Request("GET", "/" + target + "/_count")));
        return ((Number) response.get("count")).longValue();
    }

    private void assertDocumentCount(final String target, long expected) throws IOException {
        assertThat(documentCount(target), Matchers.equalTo(expected));
    }

    @SuppressWarnings("unchecked")
    private long termQueryByHostnameCount(final String target, final String hostname) throws IOException {
        final Request request = new Request("GET", "/" + target + "/_search");
        request.setJsonEntity(String.format(Locale.ROOT, """
            {
              "size": 0,
              "track_total_hits": true,
              "query": { "term": { "hostname": "%s" } }
            }""", hostname));
        final Map<String, Object> response = entityAsMap(client().performRequest(request));
        final Map<String, Object> hits = (Map<String, Object>) response.get("hits");
        final Map<String, Object> total = (Map<String, Object>) hits.get("total");
        return ((Number) total.get("value")).longValue();
    }

    private void assertExactHostnameCounts(final String target, long totalDocs) throws IOException {
        final long expectedAlpha = (totalDocs + 1) / 2;
        final long expectedBeta = totalDocs / 2;
        assertThat(termQueryByHostnameCount(target, HOST_A), Matchers.equalTo(expectedAlpha));
        assertThat(termQueryByHostnameCount(target, HOST_B), Matchers.equalTo(expectedBeta));
    }

    @SuppressWarnings("unchecked")
    private void assertGaugeSumMatches(final String target, long expectedSum) throws IOException {
        final Request request = new Request("GET", "/" + target + "/_search");
        request.setJsonEntity("""
            {
              "size": 0,
              "aggs": {
                "gauge_sum": { "sum": { "field": "gauge" } }
              }
            }""");
        final Map<String, Object> response = entityAsMap(client().performRequest(request));
        final Map<String, Object> aggs = (Map<String, Object>) response.get("aggregations");
        final Map<String, Object> gaugeSum = (Map<String, Object>) aggs.get("gauge_sum");
        assertThat(((Number) gaugeSum.get("value")).longValue(), Matchers.equalTo(expectedSum));
    }

    @SuppressWarnings("unchecked")
    private void assertTopHitsDocValuesByTimestampDesc(final String target, long topExpectedGauge, int topN) throws IOException {
        final Request request = new Request("GET", "/" + target + "/_search");
        request.setJsonEntity(String.format(Locale.ROOT, """
            {
              "size": %s,
              "_source": false,
              "sort": [{ "@timestamp": "desc" }],
              "docvalue_fields": ["gauge", "hostname"]
            }""", topN));
        final Map<String, Object> response = entityAsMap(client().performRequest(request));
        final Map<String, Object> hits = (Map<String, Object>) response.get("hits");
        final List<Map<String, Object>> hitList = (List<Map<String, Object>>) hits.get("hits");
        assertThat(hitList, Matchers.hasSize(topN));

        for (int i = 0; i < topN; i++) {
            final Map<String, Object> hit = hitList.get(i);
            final Map<String, List<Object>> fields = (Map<String, List<Object>>) hit.get("fields");
            final long expectedGauge = topExpectedGauge - i;
            final long actualGauge = ((Number) fields.get("gauge").getFirst()).longValue();
            assertThat("gauge at top-" + i, actualGauge, Matchers.equalTo(expectedGauge));
            final String actualHostname = (String) fields.get("hostname").getFirst();
            assertThat("hostname at top-" + i, actualHostname, Matchers.equalTo(hostnameForGauge((int) expectedGauge)));
        }
    }

    private static long expectedGaugeSum(int docCount) {
        return (long) docCount * (docCount - 1) / 2;
    }

    private static String hostnameForGauge(int gauge) {
        return (gauge % 2 == 0) ? HOST_A : HOST_B;
    }
}
