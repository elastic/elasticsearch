/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.upgrades.tsdb;

import com.carrotsearch.randomizedtesting.annotations.Name;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.common.time.FormatNames;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.FeatureFlag;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.upgrades.FullClusterRestartUpgradeStatus;
import org.elasticsearch.upgrades.ParameterizedFullClusterRestartTestCase;
import org.hamcrest.Matchers;
import org.junit.ClassRule;

import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Full cluster restart tests for TSDB indices using the ES95 codec. Covers pre-upgrade
 * indices staying on the baseline codec across upgrade, opt-in via
 * {@code index.time_series.es95_codec.enabled} at index creation, multi-index searches
 * across both codecs, and force-merge of sibling indices on different codecs. Per
 * segment format correctness is covered by {@code ES95VsES819DocValuesDuelTests}.
 */
public class TimeSeriesES95FullClusterRestartIT extends ParameterizedFullClusterRestartTestCase {

    @ClassRule
    public static final ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .distribution(DistributionType.DEFAULT)
        .version(OLD_CLUSTER_VERSION, isOldClusterDetachedVersion())
        .module("data-streams")
        .module("mapper-extras")
        .setting("xpack.security.enabled", "false")
        .setting("xpack.license.self_generated.type", "trial")
        .feature(FeatureFlag.ES95_CODEC_FEATURE_FLAG)
        .build();

    public TimeSeriesES95FullClusterRestartIT(@Name("cluster") FullClusterRestartUpgradeStatus upgradeStatus) {
        super(upgradeStatus);
    }

    @Override
    protected ElasticsearchCluster getUpgradeCluster() {
        return cluster;
    }

    private static final int MIN_DOC_COUNT = 50;
    private static final int MAX_DOC_COUNT = 200;
    private static final int MIN_ADDITIONAL_DOC_COUNT = 10;
    private static final int MAX_ADDITIONAL_DOC_COUNT = 40;

    private static final DateFormatter TS_FORMATTER = DateFormatter.forPattern(FormatNames.STRICT_DATE_OPTIONAL_TIME.getName());
    private static final String TS_START = "2024-01-01T00:00:00.000Z";
    private static final String TS_END = "2024-12-31T23:59:59.000Z";
    private static final long STEP_MS = 60_000L;
    private static final String HOST_A = "host-alpha";
    private static final String HOST_B = "host-beta";

    public void testPreUpgradeIndexReadableAndQueriesMatch() throws IOException {
        requireES95Codec();
        final String index = "tsdb-readable";
        if (isRunningAgainstOldCluster()) {
            createTSDBIndex(index, false);
            bulkIndexTSDBDocuments(index, randomIntBetween(MIN_DOC_COUNT, MAX_DOC_COUNT));
            refreshIndex(index);
            return;
        }
        assertIndexHealthGreen(index);
        final long total = getDocumentCount(index);
        assertRangeQueryOnTimestampMatchesAll(index, total);
        assertExactHostnameCounts(index, total);
        assertDateHistogramBucketCountByMinute(index, total);
        assertGaugeSumMatches(index, expectedGaugeSum((int) total));
        assertSortByTimestampDescendingMatchesTSDBOrder(index);
        assertFirstDocByTimestampAsc(index);
    }

    public void testPreUpgradeIndexKeepsOriginalCodec() throws IOException {
        requireES95Codec();
        final String index = "tsdb-codec-check";
        if (isRunningAgainstOldCluster()) {
            createTSDBIndex(index, false);
            bulkIndexTSDBDocuments(index, randomIntBetween(MIN_DOC_COUNT, MAX_DOC_COUNT));
            refreshIndex(index);
            return;
        }
        assertIndexHealthGreen(index);
        final long preUpgradeCount = getDocumentCount(index);

        final Map<String, Object> settings = getFlatIndexSettings(index);
        assertThat(settings.get("index.time_series.es95_codec.enabled"), Matchers.nullValue());

        final int additionalDocs = randomIntBetween(MIN_ADDITIONAL_DOC_COUNT, MAX_ADDITIONAL_DOC_COUNT);
        bulkIndexTSDBDocuments(index, additionalDocs, (int) preUpgradeCount);
        refreshIndex(index);

        forceMerge(index, 1);

        final Map<String, Object> settingsAfterForceMerge = getFlatIndexSettings(index);
        assertThat(settingsAfterForceMerge.get("index.time_series.es95_codec.enabled"), Matchers.nullValue());

        final long expectedTotal = preUpgradeCount + additionalDocs;
        assertDocumentCount(index, expectedTotal);
        assertRangeQueryOnTimestampMatchesAll(index, expectedTotal);
        assertGaugeSumMatches(index, expectedGaugeSum((int) expectedTotal));
    }

    public void testNewTSDBIndexUsesES95PostUpgrade() throws IOException {
        requireES95Codec();
        if (isRunningAgainstOldCluster()) {
            return;
        }
        final String index = "tsdb-opt-in";
        final int docCount = randomIntBetween(MIN_DOC_COUNT, MAX_DOC_COUNT);
        createTSDBIndex(index, true);
        bulkIndexTSDBDocuments(index, docCount);
        refreshIndex(index);

        assertIndexHealthGreen(index);
        assertDocumentCount(index, docCount);
        assertRangeQueryOnTimestampMatchesAll(index, docCount);
        assertExactHostnameCounts(index, docCount);
        assertDateHistogramBucketCountByMinute(index, docCount);
        assertGaugeSumMatches(index, expectedGaugeSum(docCount));
        assertSortByTimestampDescendingMatchesTSDBOrder(index);
        assertFirstDocByTimestampAsc(index);

        final Map<String, Object> settings = getFlatIndexSettings(index);
        assertThat(settings.get("index.time_series.es95_codec.enabled"), Matchers.equalTo("true"));
    }

    public void testFinalSettingRejectsLiveUpdate() throws IOException {
        requireES95Codec();
        if (isRunningAgainstOldCluster()) {
            return;
        }
        final String index = "tsdb-final-guard";
        createTSDBIndex(index, false);
        bulkIndexTSDBDocuments(index, randomIntBetween(MIN_DOC_COUNT, MAX_DOC_COUNT));
        refreshIndex(index);

        final ResponseException exception = expectThrows(ResponseException.class, () -> {
            final Request request = new Request("PUT", "/" + index + "/_settings");
            request.setJsonEntity("{\"index.time_series.es95_codec.enabled\":true}");
            client().performRequest(request);
        });
        assertThat(exception.getResponse().getStatusLine().getStatusCode(), Matchers.equalTo(400));
        assertThat(exception.getMessage(), Matchers.containsString("Can't update non dynamic setting"));

        final Map<String, Object> settings = getFlatIndexSettings(index);
        assertThat(settings.get("index.time_series.es95_codec.enabled"), Matchers.nullValue());
    }

    public void testQueryAcrossOldAndNewCodecIndices() throws IOException {
        requireES95Codec();
        final String preIndex = "tsdb-mixed-pre";
        final String postIndex = "tsdb-mixed-post";
        final String target = preIndex + "," + postIndex;
        if (isRunningAgainstOldCluster()) {
            createTSDBIndex(preIndex, false);
            bulkIndexTSDBDocuments(preIndex, randomIntBetween(MIN_DOC_COUNT, MAX_DOC_COUNT));
            refreshIndex(preIndex);
            return;
        }
        assertIndexHealthGreen(preIndex);
        final long preUpgradeCount = getDocumentCount(preIndex);

        final int postUpgradeCount = randomIntBetween(MIN_DOC_COUNT, MAX_DOC_COUNT);
        createTSDBIndex(postIndex, true);
        bulkIndexTSDBDocuments(postIndex, postUpgradeCount, (int) preUpgradeCount);
        refreshIndex(postIndex);
        assertIndexHealthGreen(postIndex);

        final Map<String, Object> oldSettings = getFlatIndexSettings(preIndex);
        assertThat(oldSettings.get("index.time_series.es95_codec.enabled"), Matchers.nullValue());
        final Map<String, Object> newSettings = getFlatIndexSettings(postIndex);
        assertThat(newSettings.get("index.time_series.es95_codec.enabled"), Matchers.equalTo("true"));

        final long expectedTotal = preUpgradeCount + postUpgradeCount;
        assertDocumentCount(target, expectedTotal);
        assertRangeQueryOnTimestampMatchesAll(target, expectedTotal);
        assertExactHostnameCounts(target, expectedTotal);
        assertDateHistogramBucketCountByMinute(target, expectedTotal);
        assertGaugeSumMatches(target, expectedGaugeSum((int) expectedTotal));
        assertSortByTimestampDescendingMatchesTSDBOrder(target);
        assertFirstDocByTimestampAsc(target);
    }

    public void testRandomAccessDocValuesAcrossCodecsAndPhases() throws IOException {
        requireES95Codec();
        final String preIndex = "tsdb-random-pre";
        final String postIndex = "tsdb-random-post";
        final String target = preIndex + "," + postIndex;

        if (isRunningAgainstOldCluster()) {
            createTSDBIndex(preIndex, false);
            bulkIndexTSDBDocuments(preIndex, randomIntBetween(MIN_DOC_COUNT, MAX_DOC_COUNT));
            refreshIndex(preIndex);
            final long preTotal = getDocumentCount(preIndex);
            assertTopHitsDocValuesByTimestampDesc(preIndex, preTotal - 1, Math.min(5, (int) preTotal));
            return;
        }

        assertIndexHealthGreen(preIndex);
        final long preUpgradeCount = getDocumentCount(preIndex);
        assertTopHitsDocValuesByTimestampDesc(preIndex, preUpgradeCount - 1, Math.min(5, (int) preUpgradeCount));

        final int postUpgradeCount = randomIntBetween(MIN_DOC_COUNT, MAX_DOC_COUNT);
        createTSDBIndex(postIndex, true);
        bulkIndexTSDBDocuments(postIndex, postUpgradeCount, (int) preUpgradeCount);
        refreshIndex(postIndex);
        assertIndexHealthGreen(postIndex);

        final long combinedTotal = preUpgradeCount + postUpgradeCount;
        assertTopHitsDocValuesByTimestampDesc(postIndex, combinedTotal - 1, Math.min(5, postUpgradeCount));
        assertTopHitsDocValuesByTimestampDesc(target, combinedTotal - 1, Math.min(5, (int) combinedTotal));
    }

    @SuppressWarnings("unchecked")
    private void assertTopHitsDocValuesByTimestampDesc(final String indexName, long topExpectedGauge, int topN) throws IOException {
        final Request request = new Request("GET", "/" + indexName + "/_search");
        request.setJsonEntity("""
            {
              "size": %s,
              "_source": false,
              "sort": [{ "@timestamp": "desc" }],
              "docvalue_fields": ["gauge", "hostname"]
            }""".formatted(topN));
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
            final String expectedHostname = (expectedGauge % 2 == 0) ? HOST_A : HOST_B;
            final String actualHostname = (String) fields.get("hostname").getFirst();
            assertThat("hostname at top-" + i, actualHostname, Matchers.equalTo(expectedHostname));
        }
    }

    public void testForceMergeBothMixedCodecIndicesToOneSegment() throws IOException {
        requireES95Codec();
        final String preIndex = "tsdb-mixed-fm-pre";
        final String postIndex = "tsdb-mixed-fm-post";
        final String target = preIndex + "," + postIndex;
        if (isRunningAgainstOldCluster()) {
            createTSDBIndex(preIndex, false);
            bulkIndexTSDBDocuments(preIndex, randomIntBetween(MIN_DOC_COUNT, MAX_DOC_COUNT));
            refreshIndex(preIndex);
            return;
        }
        assertIndexHealthGreen(preIndex);
        final long preUpgradeCount = getDocumentCount(preIndex);

        final int postUpgradeCount = randomIntBetween(MIN_DOC_COUNT, MAX_DOC_COUNT);
        createTSDBIndex(postIndex, true);
        bulkIndexTSDBDocuments(postIndex, postUpgradeCount, (int) preUpgradeCount);
        refreshIndex(postIndex);
        assertIndexHealthGreen(postIndex);

        forceMerge(preIndex, 1);
        forceMerge(postIndex, 1);

        final long expectedTotal = preUpgradeCount + postUpgradeCount;
        assertDocumentCount(target, expectedTotal);
        assertRangeQueryOnTimestampMatchesAll(target, expectedTotal);
        assertExactHostnameCounts(target, expectedTotal);
        assertDateHistogramBucketCountByMinute(target, expectedTotal);
        assertGaugeSumMatches(target, expectedGaugeSum((int) expectedTotal));
        assertSortByTimestampDescendingMatchesTSDBOrder(target);
        assertFirstDocByTimestampAsc(target);
    }

    private void createTSDBIndex(final String indexName, boolean enableES95) throws IOException {
        final String extraSetting = enableES95 ? ",\"index.time_series.es95_codec.enabled\":true" : "";
        final String body = String.format(Locale.ROOT, """
            {
              "settings": {
                "index.mode": "time_series",
                "index.routing_path": ["hostname"],
                "index.time_series.start_time": "%s",
                "index.time_series.end_time": "%s",
                "index.number_of_shards": 1,
                "index.number_of_replicas": 0%s
              },
              "mappings": {
                "properties": {
                  "@timestamp": { "type": "date" },
                  "hostname": { "type": "keyword", "time_series_dimension": true },
                  "gauge": { "type": "long", "time_series_metric": "gauge" }
                }
              }
            }""", TS_START, TS_END, extraSetting);
        final Request request = new Request("PUT", "/" + indexName);
        request.setJsonEntity(body);
        assertOK(client().performRequest(request));
    }

    private void bulkIndexTSDBDocuments(final String indexName, int docCount) throws IOException {
        bulkIndexTSDBDocuments(indexName, docCount, 0);
    }

    private void bulkIndexTSDBDocuments(final String indexName, int docCount, int startIndex) throws IOException {
        final StringBuilder bulk = new StringBuilder();
        long startMs = Instant.parse(TS_START).toEpochMilli();
        for (int i = 0; i < docCount; i++) {
            final int absoluteIndex = startIndex + i;
            final String host = (absoluteIndex % 2 == 0) ? HOST_A : HOST_B;
            final String ts = TS_FORMATTER.formatMillis(startMs + (long) absoluteIndex * STEP_MS);
            bulk.append("{\"create\":{}}\n");
            bulk.append("{\"@timestamp\":\"")
                .append(ts)
                .append("\",\"hostname\":\"")
                .append(host)
                .append("\",\"gauge\":")
                .append(absoluteIndex)
                .append("}\n");
        }
        final Request request = new Request("POST", "/" + indexName + "/_bulk");
        request.setJsonEntity(bulk.toString());
        request.addParameter("refresh", "true");
        final Response response = client().performRequest(request);
        assertOK(response);
        assertThat(entityAsMap(response).get("errors"), Matchers.is(false));
    }

    private void refreshIndex(final String indexName) throws IOException {
        assertOK(client().performRequest(new Request("POST", "/" + indexName + "/_refresh")));
    }

    private void forceMerge(final String indexName, int maxSegments) throws IOException {
        final Request request = new Request("POST", "/" + indexName + "/_forcemerge");
        request.addParameter("max_num_segments", Integer.toString(maxSegments));
        assertOK(client().performRequest(request));
        refreshIndex(indexName);
    }

    private long getDocumentCount(final String indexName) throws IOException {
        final Map<String, Object> response = entityAsMap(client().performRequest(new Request("GET", "/" + indexName + "/_count")));
        return ((Number) response.get("count")).longValue();
    }

    private void assertDocumentCount(final String indexName, long expected) throws IOException {
        assertThat(getDocumentCount(indexName), Matchers.equalTo(expected));
    }

    @SuppressWarnings("unchecked")
    private void assertRangeQueryOnTimestampMatchesAll(final String indexName, long expected) throws IOException {
        final Request request = new Request("GET", "/" + indexName + "/_search");
        request.setJsonEntity("""
            {
              "size": 0,
              "track_total_hits": true,
              "query": {
                "range": {
                  "@timestamp": { "gte": "%s", "lte": "%s" }
                }
              }
            }""".formatted(TS_START, TS_END));
        final Map<String, Object> response = entityAsMap(client().performRequest(request));
        final Map<String, Object> hits = (Map<String, Object>) response.get("hits");
        final Map<String, Object> total = (Map<String, Object>) hits.get("total");
        assertThat(((Number) total.get("value")).longValue(), Matchers.equalTo(expected));
    }

    @SuppressWarnings("unchecked")
    private long termQueryByHostnameCount(final String indexName, final String hostname) throws IOException {
        final Request request = new Request("GET", "/" + indexName + "/_search");
        request.setJsonEntity("""
            {
              "size": 0,
              "track_total_hits": true,
              "query": { "term": { "hostname": "%s" } }
            }""".formatted(hostname));
        final Map<String, Object> response = entityAsMap(client().performRequest(request));
        final Map<String, Object> hits = (Map<String, Object>) response.get("hits");
        final Map<String, Object> total = (Map<String, Object>) hits.get("total");
        return ((Number) total.get("value")).longValue();
    }

    private void assertExactHostnameCounts(final String indexName, long totalDocs) throws IOException {
        final long expectedAlpha = (totalDocs + 1) / 2;
        final long expectedBeta = totalDocs / 2;
        assertThat(termQueryByHostnameCount(indexName, HOST_A), Matchers.equalTo(expectedAlpha));
        assertThat(termQueryByHostnameCount(indexName, HOST_B), Matchers.equalTo(expectedBeta));
    }

    private void assertIndexHealthGreen(final String indexName) throws IOException {
        final Request request = new Request("GET", "/_cluster/health/" + indexName);
        request.addParameter("wait_for_status", "green");
        request.addParameter("wait_for_no_relocating_shards", "true");
        request.addParameter("timeout", "30s");
        final Map<String, Object> response = entityAsMap(client().performRequest(request));
        assertThat(response.get("timed_out"), Matchers.is(false));
        assertThat(response.get("status"), Matchers.equalTo("green"));
    }

    @SuppressWarnings("unchecked")
    private void assertGaugeSumMatches(final String indexName, long expectedSum) throws IOException {
        final Request request = new Request("GET", "/" + indexName + "/_search");
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
    private void assertSortByTimestampDescendingMatchesTSDBOrder(final String indexName) throws IOException {
        final Request request = new Request("GET", "/" + indexName + "/_search");
        request.setJsonEntity("""
            {
              "size": 5,
              "_source": false,
              "fields": ["@timestamp"],
              "sort": [{ "@timestamp": "desc" }]
            }""");
        final Map<String, Object> response = entityAsMap(client().performRequest(request));
        final Map<String, Object> hits = (Map<String, Object>) response.get("hits");
        final List<Map<String, Object>> hitList = (List<Map<String, Object>>) hits.get("hits");
        assertThat(hitList, Matchers.not(Matchers.empty()));
        long previousMillis = Long.MAX_VALUE;
        for (final Map<String, Object> hit : hitList) {
            final List<Object> sortValues = (List<Object>) hit.get("sort");
            final long ts = ((Number) sortValues.getFirst()).longValue();
            assertThat("timestamps should be sorted descending", ts, Matchers.lessThanOrEqualTo(previousMillis));
            previousMillis = ts;
        }
    }

    @SuppressWarnings("unchecked")
    private void assertFirstDocByTimestampAsc(final String indexName) throws IOException {
        final Request request = new Request("GET", "/" + indexName + "/_search");
        request.setJsonEntity("""
            {
              "size": 1,
              "sort": [{ "@timestamp": "asc" }]
            }""");
        final Map<String, Object> response = entityAsMap(client().performRequest(request));
        final Map<String, Object> hits = (Map<String, Object>) response.get("hits");
        final List<Map<String, Object>> hitList = (List<Map<String, Object>>) hits.get("hits");
        assertThat(hitList, Matchers.hasSize(1));
        final Map<String, Object> source = (Map<String, Object>) hitList.getFirst().get("_source");
        assertThat(source.get("@timestamp"), Matchers.equalTo(TS_START));
        assertThat(source.get("hostname"), Matchers.equalTo(HOST_A));
        assertThat(((Number) source.get("gauge")).longValue(), Matchers.equalTo(0L));
    }

    private static long expectedGaugeSum(int docCount) {
        return (long) docCount * (docCount - 1) / 2;
    }

    private static void requireES95Codec() {
        assumeTrue("ES95 codec is not available on the OLD cluster", oldClusterHasFeature("index.time_series_es95_codec"));
    }

    @SuppressWarnings("unchecked")
    private void assertDateHistogramBucketCountByMinute(final String indexName, long expectedBuckets) throws IOException {
        final Request request = new Request("GET", "/" + indexName + "/_search");
        request.setJsonEntity("""
            {
              "size": 0,
              "aggs": {
                "by_minute": {
                  "date_histogram": { "field": "@timestamp", "fixed_interval": "1m" }
                }
              }
            }""");
        final Map<String, Object> response = entityAsMap(client().performRequest(request));
        final Map<String, Object> aggs = (Map<String, Object>) response.get("aggregations");
        final Map<String, Object> byMinute = (Map<String, Object>) aggs.get("by_minute");
        final List<Object> buckets = (List<Object>) byMinute.get("buckets");
        assertThat((long) buckets.size(), Matchers.equalTo(expectedBuckets));
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> getFlatIndexSettings(final String indexName) throws IOException {
        final Request request = new Request("GET", "/" + indexName + "/_settings?flat_settings");
        final Map<String, Object> response = entityAsMap(client().performRequest(request));
        final Map<String, Object> indexResponse = (Map<String, Object>) response.get(indexName);
        return (Map<String, Object>) indexResponse.get("settings");
    }
}
