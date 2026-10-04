/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.mixed;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.apache.http.HttpHost;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.test.TestClustersThreadFilter;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.test.rest.ObjectPath;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.junit.ClassRule;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;

import static org.elasticsearch.index.mapper.DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER;
import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

/**
 * PromQL label removal ({@code without}, a classic histogram's {@code le}) while any node is older than
 * {@code esql_timeseries_metadata_unset}: the translation must ask the source for one {@code _timeseries} per exclusion set,
 * never send an older node a {@code TimeSeriesUnset}, and return what the same aggregation grouped {@code by} the remaining
 * labels returns - a translation that never touches {@code _timeseries}. Runs through a current-version coordinator, which
 * has both translations and must pick the older one, and through any node.
 */
@ThreadLeakFilters(filters = TestClustersThreadFilter.class)
public class MixedClusterPromqlTimeSeriesIT extends ESRestTestCase {
    private static final TransportVersion TIMESERIES_METADATA_UNSET = TransportVersion.fromName("esql_timeseries_metadata_unset");
    private static final List<String> REQUIRED_CAPABILITIES = List.of(
        "promql_command_v0",
        "promql_without_grouping",
        "promql_nested_aggregates",
        "promql_histogram_quantile"
    );
    private static final String RANGE = "start=\"2024-04-15T00:00:00Z\" end=\"2024-04-15T00:30:00Z\" step=10m";
    private static final List<String> LE = List.of("0.1", "0.5", "1", "+Inf");

    @ClassRule
    public static ElasticsearchCluster cluster = Clusters.mixedVersionCluster();

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    @Override
    protected boolean preserveClusterUponCompletion() {
        return true;
    }

    public void testWithoutOverRawSelector() throws Exception {
        assertSameAsBy("metrics", "sum without (host) (cpu)", "sum by (cluster, region) (cpu)", List.of("cluster", "region"));
    }

    public void testWithoutOverRate() throws Exception {
        assertSameAsBy(
            "metrics",
            "max without (host, region) (rate(requests[10m]))",
            "max by (cluster) (rate(requests[10m]))",
            List.of("cluster")
        );
    }

    public void testHistogramQuantile() throws Exception {
        assertSameAsBy(
            "buckets",
            "histogram_quantile(0.5, rate(bucket[10m]))",
            "histogram_quantile(0.5, sum by (job, instance, le) (rate(bucket[10m])))",
            List.of("job", "instance")
        );
    }

    /**
     * Runs {@code without} through both coordinators and {@code by} through a current one: the {@code without} rows, keyed by
     * step and the {@code _timeseries} labels, equal the {@code by} rows keyed by step and {@code labels}, and no node edits
     * a {@code _timeseries}.
     */
    private void assertSameAsBy(String dataset, String without, String by, List<String> labels) throws Exception {
        assumeTrue(
            "PromQL label removal not supported on every node",
            clusterHasCapability("POST", "/_query", List.of(), REQUIRED_CAPABILITIES).orElse(false)
        );
        assumeFalse(
            "every node supports esql_timeseries_metadata_unset: this is not a mixed cluster for it",
            minimumTransportVersion().supports(TIMESERIES_METADATA_UNSET)
        );
        String index = indexFor(dataset);
        try (RestClient current = currentNodeClient()) {
            Map<String, Double> expected = rowsBy(runPromql(current, index, by), labels);
            assertThat(expected, aMapWithSize(greaterThan(0)));
            for (RestClient coordinator : List.of(current, client())) {
                Map<String, Object> response = runPromql(coordinator, index, without);
                assertFalse(
                    "no node may run TimeSeriesUnset while any node is older",
                    containsOperator(response.get("profile"), "TimeSeriesUnset")
                );
                Map<String, Double> actual = rowsByTimeSeries(response, labels);
                assertThat(actual.keySet(), equalTo(expected.keySet()));
                for (var entry : expected.entrySet()) {
                    assertThat(entry.getKey(), actual.get(entry.getKey()), closeTo(entry.getValue(), 1e-9));
                }
            }
        }
    }

    private final Map<String, String> indices = new HashMap<>();

    /** The dataset's index, created and loaded once per test cluster. */
    private String indexFor(String dataset) throws Exception {
        String index = indices.get(dataset);
        if (index != null) {
            return index;
        }
        index = "promql_" + dataset + "_" + randomAlphaOfLength(8).toLowerCase(Locale.ROOT);
        if (dataset.equals("metrics")) {
            createIndex(index, """
                "@timestamp": { "type": "date" },
                "host": { "type": "keyword", "time_series_dimension": true },
                "cluster": { "type": "keyword", "time_series_dimension": true },
                "region": { "type": "keyword", "time_series_dimension": true },
                "cpu": { "type": "double", "time_series_metric": "gauge" },
                "requests": { "type": "long", "time_series_metric": "counter" }
                """, "host", "cluster");
            bulk(index, metricDocs());
        } else {
            createIndex(index, """
                "@timestamp": { "type": "date" },
                "job": { "type": "keyword", "time_series_dimension": true },
                "instance": { "type": "keyword", "time_series_dimension": true },
                "le": { "type": "keyword", "time_series_dimension": true },
                "bucket": { "type": "double", "time_series_metric": "counter" }
                """, "job", "instance", "le");
            bulk(index, bucketDocs());
        }
        ensureYellowAndNoInitializingShards(index, "120s");
        indices.put(dataset, index);
        return index;
    }

    private List<String> metricDocs() {
        List<String> docs = new ArrayList<>();
        long start = DEFAULT_DATE_TIME_FORMATTER.parseMillis("2024-04-15T00:00:00Z");
        for (int h = 0; h < 8; h++) {
            String cluster = randomFrom("prod", "qa");
            String region = randomFrom("eu", "us");
            long requests = randomIntBetween(0, 100);
            for (int minute = 0; minute < 30; minute++) {
                requests += randomIntBetween(0, 20);
                docs.add(
                    Strings.format(
                        """
                            {"@timestamp":%d,"host":"host-%d","cluster":"%s","region":"%s","cpu":%d,"requests":%d}""",
                        start + minute * 60_000L,
                        h,
                        cluster,
                        region,
                        randomIntBetween(0, 100),
                        requests
                    )
                );
            }
        }
        return docs;
    }

    private List<String> bucketDocs() {
        List<String> docs = new ArrayList<>();
        long start = DEFAULT_DATE_TIME_FORMATTER.parseMillis("2024-04-15T00:00:00Z");
        for (String job : List.of("api", "worker")) {
            for (int i = 0; i < 3; i++) {
                long[] counts = new long[LE.size()];
                for (int minute = 0; minute < 30; minute++) {
                    // each bucket grows at least as much as the one below it, so the buckets stay cumulative
                    long increment = 0;
                    for (int b = 0; b < LE.size(); b++) {
                        increment += randomIntBetween(0, 5);
                        counts[b] += increment;
                        docs.add(
                            Strings.format(
                                """
                                    {"@timestamp":%d,"job":"%s","instance":"i-%d","le":"%s","bucket":%d}""",
                                start + minute * 60_000L,
                                job,
                                i,
                                LE.get(b),
                                counts[b]
                            )
                        );
                    }
                }
            }
        }
        return docs;
    }

    private void createIndex(String index, String properties, String... routingPath) throws IOException {
        Request request = new Request("PUT", "/" + index);
        try (XContentBuilder routing = XContentFactory.jsonBuilder()) {
            routing.value(List.of(routingPath));
            request.setJsonEntity(Strings.format("""
                {
                  "settings": {
                    "index.mode": "time_series",
                    "index.routing_path": %s,
                    "index.number_of_shards": 4,
                    "index.time_series.start_time": "2024-04-14T00:00:00Z",
                    "index.time_series.end_time": "2024-04-16T00:00:00Z"
                  },
                  "mappings": {
                    "properties": {
                      %s
                    }
                  }
                }
                """, Strings.toString(routing), properties));
        }
        assertOK(client().performRequest(request));
    }

    private void bulk(String index, List<String> docs) throws IOException {
        StringBuilder body = new StringBuilder();
        for (String doc : docs) {
            body.append("{\"create\":{}}\n").append(doc).append('\n');
        }
        Request request = new Request("POST", "/" + index + "/_bulk");
        request.addParameter("refresh", "true");
        request.setJsonEntity(body.toString());
        assertThat(entityAsMap(client().performRequest(request)).get("errors"), equalTo(false));
    }

    private Map<String, Object> runPromql(RestClient client, String index, String expression) throws IOException {
        Request request = new Request("POST", "/_query");
        try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
            builder.startObject();
            builder.field("query", "PROMQL index=" + index + " " + RANGE + " result=(" + expression + ")");
            builder.field("profile", true);
            builder.endObject();
            request.setJsonEntity(Strings.toString(builder));
        }
        return entityAsMap(client.performRequest(request));
    }

    /** The rows of a {@code by} result, keyed by step and {@code labels}, each its own column. */
    private static Map<String, Double> rowsBy(Map<String, Object> response, List<String> labels) {
        List<String> columns = columns(response);
        Map<String, Double> rows = new TreeMap<>();
        for (List<?> row : values(response)) {
            StringBuilder key = new StringBuilder(String.valueOf(row.get(columns.indexOf("step"))));
            for (String label : labels) {
                key.append('|').append(row.get(columns.indexOf(label)));
            }
            rows.put(key.toString(), ((Number) row.get(columns.indexOf("result"))).doubleValue());
        }
        return rows;
    }

    /** The rows of a {@code without} result, keyed by step and {@code labels}, read off each row's {@code _timeseries}. */
    private static Map<String, Double> rowsByTimeSeries(Map<String, Object> response, List<String> labels) {
        List<String> columns = columns(response);
        Map<String, Double> rows = new TreeMap<>();
        for (List<?> row : values(response)) {
            Map<String, Object> identity = XContentHelper.convertToMap(
                JsonXContent.jsonXContent,
                (String) row.get(columns.indexOf("_timeseries")),
                false
            );
            assertThat("the identity carries exactly the remaining labels: " + identity, identity.keySet(), equalTo(new TreeSet<>(labels)));
            StringBuilder key = new StringBuilder(String.valueOf(row.get(columns.indexOf("step"))));
            for (String label : labels) {
                key.append('|').append(identity.get(label));
            }
            rows.put(key.toString(), ((Number) row.get(columns.indexOf("result"))).doubleValue());
        }
        return rows;
    }

    @SuppressWarnings("unchecked")
    private static List<String> columns(Map<String, Object> response) {
        return ((List<Map<String, String>>) response.get("columns")).stream().map(column -> column.get("name")).toList();
    }

    @SuppressWarnings("unchecked")
    private static List<List<?>> values(Map<String, Object> response) {
        return (List<List<?>>) response.get("values");
    }

    private static boolean containsOperator(Object value, String name) {
        return switch (value) {
            case Map<?, ?> map -> map.get("operator") instanceof String operator && operator.contains(name)
                || map.values().stream().anyMatch(child -> containsOperator(child, name));
            case List<?> list -> list.stream().anyMatch(child -> containsOperator(child, name));
            case null, default -> false;
        };
    }

    /** A client pinned to the nodes that have both translations: current-version nodes. */
    private RestClient currentNodeClient() throws IOException {
        ObjectPath nodes = ObjectPath.createFromResponse(client().performRequest(new Request("GET", "/_nodes")));
        Map<String, Object> nodesMap = nodes.evaluate("nodes");
        List<HttpHost> hosts = new ArrayList<>();
        for (String id : nodesMap.keySet()) {
            TransportVersion version = getTransportVersionWithFallback(
                nodes.evaluate("nodes." + id + ".version"),
                nodes.evaluate("nodes." + id + ".transport_version"),
                TransportVersion::minimumCompatible
            );
            if (version.supports(TIMESERIES_METADATA_UNSET)) {
                hosts.add(HttpHost.create(nodes.evaluate("nodes." + id + ".http.publish_address")));
            }
        }
        assertThat("a mixed cluster has current-version nodes", hosts.isEmpty(), equalTo(false));
        return buildClient(restClientSettings(), hosts.toArray(HttpHost[]::new));
    }
}
