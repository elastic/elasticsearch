/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.analytics.topmetrics;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.network.InetAddresses;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.junit.ClassRule;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;

import static org.elasticsearch.test.MapMatcher.assertMap;
import static org.elasticsearch.test.MapMatcher.matchesMap;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;

// NOTE: The keyword and ip tests for a columnar index are muted until https://github.com/elastic/elasticsearch/issues/162510 is
// fixed. The text, match_only_text and binary tests capture the current rejection of types the top_metrics reference does not list.
// When fixing #162510, decide whether to keep the rejection or extend support, and update these tests and the documentation.
public class TopMetricsColumnarIT extends ESRestTestCase {

    @ClassRule
    public static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .distribution(DistributionType.INTEG_TEST)
        .plugin("x-pack-analytics")
        .module("aggregations")
        .module("mapper-extras")
        .build();

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    private static final DateFormatter DATE_FORMATTER = DateFormatter.forPattern("strict_date_optional_time");
    private static final String TIMESTAMP = "2020-01-01T00:00:00Z";
    private static final List<String> COLUMNAR_MODES = List.of("columnar", "logsdb_columnar");
    private static final Map<String, Object> SAMPLE_VALUES = Map.ofEntries(
        Map.entry("keyword", "abc"),
        Map.entry("ip", "10.0.0.1"),
        Map.entry("text", "abc"),
        Map.entry("match_only_text", "abc"),
        Map.entry("binary", "YWJj"),
        Map.entry("long", 5),
        Map.entry("integer", 5),
        Map.entry("short", 5),
        Map.entry("byte", 5),
        Map.entry("double", 5.5),
        Map.entry("float", 5.5),
        Map.entry("half_float", 5.5),
        Map.entry("scaled_float", 5.5),
        Map.entry("boolean", true),
        Map.entry("date", 0L),
        Map.entry("date_nanos", 0L)
    );

    public void testKeywordOnStandardIndex() throws IOException {
        assertOnStandardIndex(mapping("keyword"), TopMetricsColumnarIT::randomString);
    }

    public void testKeywordOnColumnarIndex() throws IOException {
        assertOnEveryColumnarMode(mapping("keyword"), TopMetricsColumnarIT::randomString);
    }

    public void testIpOnStandardIndex() throws IOException {
        assertOnStandardIndex(mapping("ip"), TopMetricsColumnarIT::randomIpAddress);
    }

    public void testIpOnColumnarIndex() throws IOException {
        assertOnEveryColumnarMode(mapping("ip"), TopMetricsColumnarIT::randomIpAddress);
    }

    public void testTextIsRejectedOnColumnarIndex() throws IOException {
        assertRejectedOnColumnarIndex(mapping("text"), "text");
    }

    public void testMatchOnlyTextIsRejectedOnColumnarIndex() throws IOException {
        assertRejectedOnColumnarIndex(mapping("match_only_text"), "match_only_text");
    }

    public void testBinaryIsRejectedOnColumnarIndex() throws IOException {
        assertRejectedOnColumnarIndex(mapping("binary"), "binary");
    }

    public void testTextWithDocValuesIsRejectedOnStandardIndex() throws IOException {
        assertRejected("standard", Settings.EMPTY, mappingWithDocValues("text"), "text");
    }

    public void testMatchOnlyTextWithDocValuesIsRejectedOnStandardIndex() throws IOException {
        assertRejected("standard", Settings.EMPTY, mappingWithDocValues("match_only_text"), "match_only_text");
    }

    public void testBinaryWithDocValuesIsRejectedOnStandardIndex() throws IOException {
        assertRejected("standard", Settings.EMPTY, mappingWithDocValues("binary"), "binary");
    }

    public void testTextWithFielddataOnStandardIndex() throws IOException {
        assertOnStandardIndex("{\"type\":\"text\",\"fielddata\":true}", TopMetricsColumnarIT::randomLowercaseToken);
    }

    public void testDocValuesDisabledOnStandardIndex() throws IOException {
        for (Map.Entry<String, Object> sample : SAMPLE_VALUES.entrySet()) {
            assertRejectedWithoutDocValues("standard-" + sample.getKey(), Settings.EMPTY, sample.getKey(), sample.getValue());
        }
    }

    public void testTextDocValuesDisabledOnColumnarIndex() throws IOException {
        String mode = randomFrom(COLUMNAR_MODES);
        for (String type : List.of("text", "match_only_text")) {
            assertRejectedWithoutDocValues(mode + "-" + type, columnarSettings(mode), type, SAMPLE_VALUES.get(type));
        }
    }

    public void testMissingMetricOnTopDocument() throws IOException {
        List<Document> documents = List.of(new Document(1, null), new Document(2, 5), new Document(3, 7));
        assertTopMetrics("standard", Settings.EMPTY, mapping("long"), documents, true, Function.identity());
        String mode = randomFrom(COLUMNAR_MODES);
        assertTopMetrics(mode, columnarSettings(mode), mapping("long"), documents, true, Function.identity());
    }

    public void testKeywordMissingMetricOnTopDocumentOnStandardIndex() throws IOException {
        List<Document> documents = List.of(new Document(1, null), new Document(2, "a"), new Document(3, "b"));
        assertTopMetrics("standard", Settings.EMPTY, mapping("keyword"), documents, true, Function.identity());
    }

    public void testKeywordMissingMetricOnTopDocumentOnColumnarIndex() throws IOException {
        List<Document> documents = List.of(new Document(1, null), new Document(2, "a"), new Document(3, "b"));
        for (String mode : COLUMNAR_MODES) {
            assertTopMetrics(mode, columnarSettings(mode), mapping("keyword"), documents, true, Function.identity());
        }
    }

    public void testLong() throws IOException {
        assertOnBothIndices(mapping("long"), ESTestCase::randomLong, TopMetricsColumnarIT::parsedLong);
    }

    public void testInteger() throws IOException {
        assertOnBothIndices(mapping("integer"), ESTestCase::randomInt);
    }

    public void testShort() throws IOException {
        assertOnBothIndices(mapping("short"), ESTestCase::randomShort, TopMetricsColumnarIT::parsedInt);
    }

    public void testByte() throws IOException {
        assertOnBothIndices(mapping("byte"), ESTestCase::randomByte, TopMetricsColumnarIT::parsedInt);
    }

    public void testDouble() throws IOException {
        assertOnBothIndices(mapping("double"), () -> randomDoubleBetween(-1e12, 1e12, true));
    }

    public void testFloat() throws IOException {
        assertOnBothIndices(mapping("float"), ESTestCase::randomFloat, TopMetricsColumnarIT::widenedToDouble);
    }

    public void testHalfFloat() throws IOException {
        assertOnBothIndices(mapping("half_float"), TopMetricsColumnarIT::randomQuarter);
    }

    public void testScaledFloat() throws IOException {
        assertOnBothIndices("{\"type\":\"scaled_float\",\"scaling_factor\":4}", TopMetricsColumnarIT::randomQuarter);
    }

    public void testBoolean() throws IOException {
        assertOnBothIndices(mapping("boolean"), ESTestCase::randomBoolean, String::valueOf);
    }

    public void testDate() throws IOException {
        assertOnBothIndices(mapping("date"), TopMetricsColumnarIT::randomEpochMillis, TopMetricsColumnarIT::formatDate);
    }

    public void testDateNanos() throws IOException {
        assertOnBothIndices(mapping("date_nanos"), TopMetricsColumnarIT::randomEpochMillis, TopMetricsColumnarIT::formatDate);
    }

    private record Document(int sortKey, Object metric) {}

    private void assertOnBothIndices(String metricMapping, Supplier<Object> randomValue) throws IOException {
        assertOnBothIndices(metricMapping, randomValue, Function.identity());
    }

    private void assertOnBothIndices(String metricMapping, Supplier<Object> randomValue, Function<Object, Object> responseValue)
        throws IOException {
        assertOnStandardIndex(metricMapping, randomValue, responseValue);
        assertOnColumnarIndex(metricMapping, randomValue, responseValue);
    }

    private void assertOnStandardIndex(String metricMapping, Supplier<Object> randomValue) throws IOException {
        assertOnStandardIndex(metricMapping, randomValue, Function.identity());
    }

    private void assertOnStandardIndex(String metricMapping, Supplier<Object> randomValue, Function<Object, Object> responseValue)
        throws IOException {
        assertTopMetrics("standard", Settings.EMPTY, metricMapping, randomValue, responseValue);
    }

    private void assertOnColumnarIndex(String metricMapping, Supplier<Object> randomValue) throws IOException {
        assertOnColumnarIndex(metricMapping, randomValue, Function.identity());
    }

    private void assertOnColumnarIndex(String metricMapping, Supplier<Object> randomValue, Function<Object, Object> responseValue)
        throws IOException {
        assertOnColumnarMode(randomFrom(COLUMNAR_MODES), metricMapping, randomValue, responseValue);
    }

    private void assertOnEveryColumnarMode(String metricMapping, Supplier<Object> randomValue) throws IOException {
        for (String mode : COLUMNAR_MODES) {
            assertOnColumnarMode(mode, metricMapping, randomValue, Function.identity());
        }
    }

    private void assertOnColumnarMode(
        String mode,
        String metricMapping,
        Supplier<Object> randomValue,
        Function<Object, Object> responseValue
    ) throws IOException {
        assertTopMetrics(mode, columnarSettings(mode), metricMapping, randomValue, responseValue);
    }

    private void assertRejectedOnColumnarIndex(String metricMapping, String type) throws IOException {
        String mode = randomFrom(COLUMNAR_MODES);
        assertRejected(mode, columnarSettings(mode), metricMapping, type);
    }

    private static Settings columnarSettings(String mode) {
        return Settings.builder().put("index.mode", mode).build();
    }

    private void assertTopMetrics(
        String index,
        Settings indexSettings,
        String metricMapping,
        Supplier<Object> randomValue,
        Function<Object, Object> responseValue
    ) throws IOException {
        assertTopMetrics(index, indexSettings, metricMapping, randomDocuments(randomValue), randomBoolean(), responseValue);
    }

    private void assertTopMetrics(
        String index,
        Settings indexSettings,
        String metricMapping,
        List<Document> documents,
        boolean ascending,
        Function<Object, Object> responseValue
    ) throws IOException {
        createMetricIndex(index, indexSettings, metricMapping);
        indexDocuments(index, documents);

        Comparator<Document> bySortKey = Comparator.comparingInt(Document::sortKey);
        Document top = documents.stream().min(ascending ? bySortKey : bySortKey.reversed()).orElseThrow();

        assertMap(
            search(index, ascending),
            matchesMap().extraOk()
                .entry(
                    "aggregations",
                    matchesMap().entry(
                        "top",
                        matchesMap().entry(
                            "top",
                            List.of(
                                matchesMap().entry("sort", List.of(top.sortKey()))
                                    .entry(
                                        "metrics",
                                        matchesMap().entry("metric", top.metric() == null ? nullValue() : responseValue.apply(top.metric()))
                                    )
                            )
                        )
                    )
                )
        );
    }

    private static List<Document> randomDocuments(Supplier<Object> randomValue) {
        int count = randomIntBetween(5, 20);
        List<Document> documents = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            documents.add(new Document(i * 10 + randomIntBetween(0, 9), rarely() ? null : randomValue.get()));
        }
        return documents;
    }

    private void indexDocuments(String index, List<Document> documents) throws IOException {
        List<Document> shuffled = new ArrayList<>(documents);
        Collections.shuffle(shuffled, random());
        int bulkCount = randomIntBetween(Math.min(3, shuffled.size()), Math.min(5, shuffled.size()));
        int chunkSize = (shuffled.size() + bulkCount - 1) / bulkCount;
        for (int from = 0; from < shuffled.size(); from += chunkSize) {
            bulkIndex(index, shuffled.subList(from, Math.min(shuffled.size(), from + chunkSize)));
        }
    }

    private void bulkIndex(String index, List<Document> documents) throws IOException {
        StringBuilder bulk = new StringBuilder();
        for (Document document : documents) {
            XContentBuilder source = JsonXContent.contentBuilder().startObject();
            source.field("@timestamp", TIMESTAMP).field("sort_key", document.sortKey());
            if (document.metric() != null) {
                source.field("metric", document.metric());
            }
            source.endObject();
            bulk.append("{\"index\":{}}\n").append(Strings.toString(source)).append('\n');
        }
        Request request = new Request("POST", "/" + index + "/_bulk");
        request.addParameter("refresh", "true");
        request.setJsonEntity(bulk.toString());
        assertMap(entityAsMap(client().performRequest(request)), matchesMap().extraOk().entry("errors", false));
    }

    private Map<String, Object> search(String index, boolean ascending) throws IOException {
        XContentBuilder body = JsonXContent.contentBuilder().startObject();
        body.field("size", 0);
        body.startObject("aggs").startObject("top").startObject("top_metrics");
        body.startArray("metrics").startObject().field("field", "metric").endObject().endArray();
        body.startObject("sort").field("sort_key", ascending ? "asc" : "desc").endObject();
        body.endObject().endObject().endObject().endObject();
        Request request = new Request("POST", "/" + index + "/_search");
        request.setJsonEntity(Strings.toString(body));
        Map<String, Object> response = entityAsMap(client().performRequest(request));
        assertMap(response, matchesMap().extraOk().entry("hits", matchesMap().extraOk().entry("hits", hasSize(0))));
        return response;
    }

    private void createMetricIndex(String index, Settings indexSettings, String metricMapping) throws IOException {
        createIndex(index, indexSettings, Strings.format("""
            "properties": {
              "@timestamp": { "type": "date" },
              "sort_key": { "type": "long" },
              "metric": %s
            }
            """, metricMapping));
    }

    private void assertRejected(String index, Settings indexSettings, String metricMapping, String type) throws IOException {
        createMetricIndex(index, indexSettings, metricMapping);
        indexDocuments(index, List.of(new Document(1, SAMPLE_VALUES.get(type))));
        ResponseException e = expectThrows(ResponseException.class, () -> search(index, true));
        assertEquals(e.getMessage(), 400, e.getResponse().getStatusLine().getStatusCode());
    }

    private void assertRejectedWithoutDocValues(String index, Settings indexSettings, String type, Object sample) throws IOException {
        createMetricIndex(index, indexSettings, mappingWithoutDocValues(type));
        indexDocuments(index, List.of(new Document(1, sample)));
        ResponseException e = expectThrows(ResponseException.class, () -> search(index, true));
        assertEquals(e.getMessage(), 400, e.getResponse().getStatusLine().getStatusCode());
        assertThat(e.getMessage(), containsString(rejectionMessage(type)));
        assertThat(e.getMessage(), not(containsString("segment ordinals")));
    }

    private static String rejectionMessage(String type) {
        return switch (type) {
            case "text" -> "Fielddata is disabled";
            case "match_only_text" -> "do not support sorting and aggregations";
            case "keyword", "ip", "binary", "long", "integer", "short", "byte", "double", "float", "half_float", "scaled_float", "boolean",
                "date", "date_nanos" -> "fielddata is unsupported";
            default -> throw new AssertionError("unexpected type [" + type + "]");
        };
    }

    private static String mappingWithDocValues(String type) {
        return Strings.format("{\"type\":\"%s\",\"doc_values\":true}", type);
    }

    private static String mappingWithoutDocValues(String type) {
        String scaling = type.equals("scaled_float") ? ",\"scaling_factor\":4" : "";
        return Strings.format("{\"type\":\"%s\",\"doc_values\":false%s}", type, scaling);
    }

    private static String mapping(String type) {
        return Strings.format("{\"type\":\"%s\"}", type);
    }

    private static Object randomString() {
        return randomRealisticUnicodeOfLengthBetween(1, 20);
    }

    private static Object randomLowercaseToken() {
        return randomAlphaOfLengthBetween(1, 12).toLowerCase(Locale.ROOT);
    }

    private static Object randomIpAddress() {
        return InetAddresses.toAddrString(randomIp(randomBoolean()));
    }

    private static Object randomQuarter() {
        return randomIntBetween(-1000, 1000) / 4.0;
    }

    private static Object randomEpochMillis() {
        return randomLongBetween(0, 4_000_000_000_000L);
    }

    private static Object parsedLong(Object value) {
        long longValue = (Long) value;
        return longValue == (int) longValue ? (Object) (int) longValue : longValue;
    }

    private static Object parsedInt(Object value) {
        return ((Number) value).intValue();
    }

    private static Object widenedToDouble(Object value) {
        return ((Number) value).doubleValue();
    }

    private static Object formatDate(Object epochMillis) {
        return DATE_FORMATTER.format(Instant.ofEpochMilli((Long) epochMillis));
    }
}
