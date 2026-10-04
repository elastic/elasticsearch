/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.ccq;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.apache.http.HttpHost;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ListMatcher;
import org.elasticsearch.test.TestClustersThreadFilter;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xpack.esql.AssertWarnings;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.qa.rest.ProfileLogger;
import org.elasticsearch.xpack.esql.qa.rest.RestEsqlTestCase;
import org.hamcrest.Matcher;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.index.mapper.DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER;
import static org.elasticsearch.test.MapMatcher.assertMap;
import static org.elasticsearch.test.MapMatcher.matchesMap;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;

/**
 * PromQL label removal ({@code without}, a classic histogram's {@code le}) across clusters. Each query runs over a local and
 * a remote index, and must return what the same query returns over one local index holding both's data.
 * <p>
 * Which translation runs depends on the clusters: between current clusters each series carries its one {@code _timeseries}
 * and the dropped labels are unset from it ({@code TimeSeriesUnset}); with an older cluster on either side the translation
 * asks the source for one {@code _timeseries} per exclusion set. Against an older remote the cross-cluster query takes the
 * older translation while the single-cluster query takes the current one, so the comparison checks one against the other.
 */
@ThreadLeakFilters(filters = TestClustersThreadFilter.class)
public class MultiClusterPromqlTimeSeriesIT extends ESRestTestCase {

    private static final List<String> REQUIRED_CAPABILITIES = List.of(
        EsqlCapabilities.Cap.PROMQL_COMMAND_V0.capabilityName(),
        EsqlCapabilities.Cap.PROMQL_WITHOUT_GROUPING.capabilityName(),
        EsqlCapabilities.Cap.PROMQL_NESTED_AGGREGATES.capabilityName(),
        EsqlCapabilities.Cap.PROMQL_HISTOGRAM_QUANTILE.capabilityName()
    );
    private static final String UNSET = EsqlCapabilities.Cap.PROMQL_TIMESERIES_METADATA_UNSET.capabilityName();

    private static final String RANGE = "start=\"2024-04-15T00:00:00Z\" end=\"2024-04-15T00:30:00Z\" step=10m";

    static ElasticsearchCluster remoteCluster = Clusters.remoteCluster();
    static ElasticsearchCluster localCluster = Clusters.localCluster(remoteCluster);

    @ClassRule
    public static TestRule clusterRule = RuleChain.outerRule(remoteCluster).around(localCluster);

    @Rule(order = Integer.MIN_VALUE)
    public ProfileLogger profileLogger = new ProfileLogger();

    @Override
    protected String getTestRestCluster() {
        return localCluster.getHttpAddresses();
    }

    @Before
    public void setUpIndices() throws Exception {
        List<String> localMetrics = metricDocs("local");
        List<String> remoteMetrics = metricDocs("remote");
        List<String> localBuckets = bucketDocs("local");
        List<String> remoteBuckets = bucketDocs("remote");
        RestClient local = client();
        createIndex(local, "promql-metrics-local", METRICS_MAPPING, "host", "cluster");
        createIndex(local, "promql-metrics-all", METRICS_MAPPING, "host", "cluster");
        createIndex(local, "promql-buckets-local", BUCKETS_MAPPING, "job", "instance", "le");
        createIndex(local, "promql-buckets-all", BUCKETS_MAPPING, "job", "instance", "le");
        bulk(local, "promql-metrics-local", localMetrics);
        bulk(local, "promql-buckets-local", localBuckets);
        bulk(local, "promql-metrics-all", concat(localMetrics, remoteMetrics));
        bulk(local, "promql-buckets-all", concat(localBuckets, remoteBuckets));
        try (RestClient remote = remoteClusterClient()) {
            createIndex(remote, "promql-metrics-remote", METRICS_MAPPING, "host", "cluster");
            createIndex(remote, "promql-buckets-remote", BUCKETS_MAPPING, "job", "instance", "le");
            bulk(remote, "promql-metrics-remote", remoteMetrics);
            bulk(remote, "promql-buckets-remote", remoteBuckets);
        }
    }

    @After
    public void wipeRemoteIndices() throws Exception {
        try (RestClient remote = remoteClusterClient()) {
            deleteIndex(remote, "promql-metrics-remote");
            deleteIndex(remote, "promql-buckets-remote");
        }
    }

    /** {@code without} over a raw selector: the series collapse by their whole identity, then lose {@code host}. */
    public void testWithoutOverRawSelector() throws IOException {
        assertSameAcrossClusters("metrics", "sum without (host) (cpu)", "_timeseries", true);
    }

    /** Two labels dropped from the per-series rate of a counter. */
    public void testWithoutOverRate() throws IOException {
        assertSameAcrossClusters("metrics", "max without (host, region) (rate(requests[10m]))", "_timeseries", true);
    }

    /** A label no series carries leaves every identity unchanged. */
    public void testWithoutAbsentLabel() throws IOException {
        assertSameAcrossClusters("metrics", "max without (nonexistent) (cpu)", "_timeseries", true);
    }

    /** The {@code le} of a classic histogram, dropped where the buckets merge. */
    public void testHistogramQuantile() throws IOException {
        assertSameAcrossClusters("buckets", "histogram_quantile(0.5, rate(bucket[10m]))", "_timeseries", true);
    }

    /** Buckets already regrouped by promoted labels: {@code le} is a promoted label, nothing to unset. */
    public void testHistogramQuantileOverPromotedLabels() throws IOException {
        assertSameAcrossClusters("buckets", "histogram_quantile(0.5, sum by (job, le) (rate(bucket[10m])))", "job", false);
    }

    /** {@code without} over every label: the empty identity, which only the current translation returns. */
    public void testWithoutEveryLabel() throws IOException {
        assumeTrue("needs " + UNSET + " on both clusters", capabilitiesSupportedNewAndOld(List.of(UNSET)));
        assertSameAcrossClusters("metrics", "sum without (host, cluster, region) (cpu)", "_timeseries", true);
    }

    /** {@code without} over a classic histogram: the older translation cannot plan it, the current one unsets twice. */
    public void testWithoutOverHistogramQuantile() throws IOException {
        assumeTrue("needs " + UNSET + " on both clusters", capabilitiesSupportedNewAndOld(List.of(UNSET)));
        assertSameAcrossClusters("buckets", "max without (instance) (histogram_quantile(0.5, rate(bucket[10m])))", "_timeseries", true);
    }

    /**
     * Runs {@code expression} across clusters and over the single index holding both's data: the same result, and an edited
     * {@code _timeseries} ({@code edits}) exactly when every node of both clusters supports it.
     */
    private void assertSameAcrossClusters(String dataset, String expression, String sort, boolean edits) throws IOException {
        assumeTrue("PromQL label removal not supported", capabilitiesSupportedNewAndOld(REQUIRED_CAPABILITIES));
        String tail = " " + RANGE + " result=(" + expression + ") | SORT " + sort + ", step";
        Map<String, Object> acrossClusters = run("PROMQL index=promql-" + dataset + "-local,*:promql-" + dataset + "-remote" + tail);
        Map<String, Object> singleCluster = run("PROMQL index=promql-" + dataset + "-all" + tail);
        assertThat(acrossClusters.get("values"), not(nullValue()));
        assertThat(((List<?>) acrossClusters.get("values")).isEmpty(), equalTo(false));
        assertMap(
            acrossClusters,
            matchesMap().extraOk().entry("columns", singleCluster.get("columns")).entry("values", matcherFor(singleCluster.get("values")))
        );
        // Between current clusters the series' one _timeseries is edited; with an older cluster it never is.
        boolean unset = capabilitiesSupportedNewAndOld(List.of(UNSET));
        assertThat(expression, containsOperator(acrossClusters.get("profile"), "TimeSeriesUnset"), equalTo(unset && edits));
    }

    private static final String METRICS_MAPPING = """
        "properties": {
          "@timestamp": { "type": "date" },
          "host": { "type": "keyword", "time_series_dimension": true },
          "cluster": { "type": "keyword", "time_series_dimension": true },
          "region": { "type": "keyword", "time_series_dimension": true },
          "cpu": { "type": "double", "time_series_metric": "gauge" },
          "requests": { "type": "long", "time_series_metric": "counter" }
        }
        """;

    private static final String BUCKETS_MAPPING = """
        "properties": {
          "@timestamp": { "type": "date" },
          "job": { "type": "keyword", "time_series_dimension": true },
          "instance": { "type": "keyword", "time_series_dimension": true },
          "le": { "type": "keyword", "time_series_dimension": true },
          "bucket": { "type": "double", "time_series_metric": "counter" }
        }
        """;

    private static final List<String> LE = List.of("0.1", "0.5", "1", "+Inf");

    /** Five hosts per cluster tag, each in one of two clusters and regions, sampled every minute for half an hour. */
    private List<String> metricDocs(String tag) {
        List<String> docs = new ArrayList<>();
        long start = DEFAULT_DATE_TIME_FORMATTER.parseMillis("2024-04-15T00:00:00Z");
        for (int h = 0; h < 5; h++) {
            String host = tag + "-host-" + h;
            String cluster = randomFrom("prod", "qa");
            String region = randomFrom("eu", "us");
            long requests = randomIntBetween(0, 100);
            for (int minute = 0; minute < 30; minute++) {
                requests += randomIntBetween(0, 20);
                docs.add(
                    Strings.format(
                        """
                            {"@timestamp":%d,"host":"%s","cluster":"%s","region":"%s","cpu":%d,"requests":%d}""",
                        start + minute * 60_000L,
                        host,
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

    /** Two jobs with two instances each per cluster tag, cumulative buckets growing every minute for half an hour. */
    private List<String> bucketDocs(String tag) {
        List<String> docs = new ArrayList<>();
        long start = DEFAULT_DATE_TIME_FORMATTER.parseMillis("2024-04-15T00:00:00Z");
        for (String job : List.of("api", "worker")) {
            for (int i = 0; i < 2; i++) {
                String instance = tag + "-" + i;
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
                                    {"@timestamp":%d,"job":"%s","instance":"%s","le":"%s","bucket":%d}""",
                                start + minute * 60_000L,
                                job,
                                instance,
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

    private static List<String> concat(List<String> a, List<String> b) {
        List<String> all = new ArrayList<>(a);
        all.addAll(b);
        return all;
    }

    private void createIndex(RestClient client, String index, String mapping, String... routingPath) throws IOException {
        Request request = new Request("PUT", "/" + index);
        String settings = Settings.builder()
            .put("index.mode", "time_series")
            .putList("index.routing_path", List.of(routingPath))
            .put("index.number_of_shards", randomIntBetween(1, 3))
            .put("index.time_series.start_time", "2024-04-14T00:00:00Z")
            .put("index.time_series.end_time", "2024-04-16T00:00:00Z")
            .build()
            .toString();
        request.setJsonEntity(Strings.format("""
            {
              "settings": %s,
              "mappings": {
                %s
              }
            }
            """, settings, mapping));
        assertOK(client.performRequest(request));
    }

    private void bulk(RestClient client, String index, List<String> docs) throws IOException {
        StringBuilder body = new StringBuilder();
        for (String doc : docs) {
            body.append("{\"create\":{}}\n").append(doc).append('\n');
        }
        Request request = new Request("POST", "/" + index + "/_bulk");
        request.addParameter("refresh", "true");
        request.setJsonEntity(body.toString());
        Map<String, Object> response = entityAsMap(client.performRequest(request));
        assertThat(response.get("errors"), equalTo(false));
    }

    private Map<String, Object> run(String query) throws IOException {
        var request = new RestEsqlTestCase.RequestObjectBuilder().query(query).profile(true);
        Map<String, Object> response = RestEsqlTestCase.runEsqlSync(request, new AssertWarnings.NoWarnings(), profileLogger);
        logger.info("--> query {} response {}", query, response);
        return response;
    }

    private boolean capabilitiesSupportedNewAndOld(List<String> requiredCapabilities) throws IOException {
        boolean supported = clusterHasCapability("POST", "/_query", List.of(), requiredCapabilities).orElse(false);
        try (RestClient remote = remoteClusterClient()) {
            supported = supported && clusterHasCapability(remote, "POST", "/_query", List.of(), requiredCapabilities).orElse(false);
        }
        return supported;
    }

    private static boolean containsOperator(Object value, String name) {
        return switch (value) {
            case Map<?, ?> map -> map.get("operator") instanceof String operator && operator.contains(name)
                || map.values().stream().anyMatch(child -> containsOperator(child, name));
            case List<?> list -> list.stream().anyMatch(child -> containsOperator(child, name));
            case null, default -> false;
        };
    }

    private static Matcher<?> matcherFor(Object value) {
        return switch (value) {
            case null -> nullValue();
            case List<?> list -> {
                ListMatcher matcher = ListMatcher.matchesList();
                for (Object item : list) {
                    matcher = matcher.item(matcherFor(item));
                }
                yield matcher;
            }
            case Double doubleValue -> Matchers.closeTo(doubleValue, 0.0000001);
            default -> equalTo(value);
        };
    }

    private RestClient remoteClusterClient() throws IOException {
        var hosts = parseClusterHosts(remoteCluster.getHttpAddresses());
        return buildClient(restClientSettings(), hosts.toArray(new HttpHost[0]));
    }
}
