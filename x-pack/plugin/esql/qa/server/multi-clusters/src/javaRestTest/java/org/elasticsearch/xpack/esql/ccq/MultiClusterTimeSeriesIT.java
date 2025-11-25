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
import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.test.ListMatcher;
import org.elasticsearch.test.MapMatcher;
import org.elasticsearch.test.TestClustersThreadFilter;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xpack.esql.AssertWarnings;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import static org.elasticsearch.index.mapper.DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER;
import static org.elasticsearch.test.MapMatcher.assertMap;
import static org.elasticsearch.test.MapMatcher.matchesMap;
import static org.hamcrest.Matchers.any;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.nullValue;

@ThreadLeakFilters(filters = TestClustersThreadFilter.class)
public class MultiClusterTimeSeriesIT extends ESRestTestCase {

    static final List<String> REQUIRED_CAPABILITIES = List.of("ts_command_v0");

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

    record TimeSeriesDoc(
        String host,
        String cluster,
        long timestamp,
        long requestCount,
        long cpu,
        ByteSizeValue memory,
        String clusterTag
    ) {}

    final String localIndex = "hosts-local";
    List<TimeSeriesDoc> localDocs = List.of();

    final String remoteIndex = "hosts-remote";
    List<TimeSeriesDoc> remoteDocs = List.of();

    final String allDataIndex = "all-data";

    private Boolean shouldCheckShardCounts = null;

    @Before
    public void setUpTimeSeriesIndices() throws Exception {
        localDocs = getRandomDocs("local");
        RestClient localClient = client();
        createTimeSeriesIndex(localClient, localIndex);
        indexTimeSeriesDocs(localClient, localIndex, localDocs);

        remoteDocs = getRandomDocs("remote");
        try (RestClient remoteClient = remoteClusterClient()) {
            createTimeSeriesIndex(remoteClient, remoteIndex);
            indexTimeSeriesDocs(remoteClient, remoteIndex, remoteDocs);
        }

        createTimeSeriesIndex(localClient, allDataIndex);
        indexTimeSeriesDocs(localClient, allDataIndex, Stream.concat(localDocs.stream(), remoteDocs.stream()).toList());
    }

    @After
    public void wipeIndices() throws Exception {
        try (RestClient remoteClient = remoteClusterClient()) {
            deleteIndex(remoteClient, remoteIndex);
        }
    }

    private List<TimeSeriesDoc> getRandomDocs(String clusterTag) {
        final List<TimeSeriesDoc> docs = new ArrayList<>();

        Map<String, String> hostToClusters = new HashMap<>();
        for (int i = 0; i < 5; i++) {
            hostToClusters.put(clusterTag + "0" + i, randomFrom("qa", "prod"));
        }
        long timestamp = DEFAULT_DATE_TIME_FORMATTER.parseMillis("2024-04-15T00:00:00Z");

        Map<String, Integer> requestCounts = new HashMap<>();
        int numDocs = between(20, 100);
        for (int i = 0; i < numDocs; i++) {
            List<String> hosts = randomSubsetOf(between(1, hostToClusters.size()), hostToClusters.keySet());
            timestamp += between(1, 30) * 1000L;
            for (String host : hosts) {
                var requestCount = requestCounts.compute(host, (k, curr) -> {
                    if (curr == null || randomInt(100) <= 20) {
                        return randomIntBetween(0, 10);
                    } else {
                        return curr + randomIntBetween(1, 10);
                    }
                });
                int cpu = randomIntBetween(0, 100);
                ByteSizeValue memory = ByteSizeValue.ofBytes(randomIntBetween(1024, 1024 * 1024));
                docs.add(new TimeSeriesDoc(host, hostToClusters.get(host), timestamp, requestCount, cpu, memory, clusterTag));
            }
        }

        Randomness.shuffle(docs);

        return docs;
    }

    public void testAvg() throws Exception {
        assumeTrue("TS command not supported", capabilitiesSupportedNewAndOld(REQUIRED_CAPABILITIES));

        boolean includeCCSMetadata = includeCCSMetadata();

        Map<String, Object> multiClusterResult = run("""
            TS hosts-local,*:hosts-remote
            | STATS avg_cpu = AVG(cpu), avg_memory = AVG(memory) BY cluster
            | SORT cluster
            """, includeCCSMetadata);

        Map<String, Object> singleClusterResult = run(
            "TS all-data | STATS avg_cpu = AVG(cpu), avg_memory = AVG(memory) BY cluster | SORT cluster",
            includeCCSMetadata
        );

        assertResultMap(includeCCSMetadata, multiClusterResult, singleClusterResult);
    }

    public void testRateAndTBucket() throws Exception {
        assumeTrue("TS command not supported", capabilitiesSupportedNewAndOld(REQUIRED_CAPABILITIES));

        boolean includeCCSMetadata = includeCCSMetadata();

        Map<String, Object> multiClusterResult = run("""
            TS hosts-local,*:hosts-remote
            | WHERE cluster == "prod"
            | STATS max_rate = MAX(RATE(request_count)) BY tb = TBUCKET(5minute)
            | SORT tb""", includeCCSMetadata);

        Map<String, Object> singleClusterResult = run("""
            TS all-data
            | WHERE cluster == "prod"
            | STATS max_rate = MAX(RATE(request_count)) BY tb = TBUCKET(5minute)
            | SORT tb""", includeCCSMetadata);

        assertResultMap(includeCCSMetadata, multiClusterResult, singleClusterResult);
    }

    public void testAvgOverTime() throws Exception {
        assumeTrue("TS command not supported", capabilitiesSupportedNewAndOld(REQUIRED_CAPABILITIES));

        boolean includeCCSMetadata = includeCCSMetadata();

        Map<String, Object> multiClusterResult = run("""
            TS hosts-local,*:hosts-remote
            | STATS avg_cpu = SUM(AVG_OVER_TIME(cpu)), max_memory = SUM(MAX_OVER_TIME(memory)) BY tb = TBUCKET(10minutes)
            | SORT tb""", includeCCSMetadata);

        Map<String, Object> singleClusterResult = run("""
            TS all-data
            | STATS avg_cpu = SUM(AVG_OVER_TIME(cpu)), max_memory = SUM(MAX_OVER_TIME(memory)) BY tb = TBUCKET(10minutes)
            | SORT tb""", includeCCSMetadata);

        assertResultMap(includeCCSMetadata, multiClusterResult, singleClusterResult);
    }

    public void testIRate() throws Exception {
        assumeTrue("TS command not supported", capabilitiesSupportedNewAndOld(REQUIRED_CAPABILITIES));

        boolean includeCCSMetadata = includeCCSMetadata();

        Map<String, Object> multiClusterResult = run("""
            TS hosts-local,*:hosts-remote
            | STATS irate_req_count = AVG(IRATE(request_count)) BY tb = TBUCKET(1minute)
            | SORT tb""", includeCCSMetadata);

        Map<String, Object> singleClusterResult = run("""
            TS all-data
            | STATS irate_req_count = AVG(IRATE(request_count)) BY tb = TBUCKET(1minute)
            | SORT tb""", includeCCSMetadata);

        assertResultMap(includeCCSMetadata, multiClusterResult, singleClusterResult);
    }

    private void createTimeSeriesIndex(RestClient client, String indexName) throws IOException {
        Request createIndex = new Request("PUT", "/" + indexName);

        String settings = Settings.builder()
            .put("index.mode", "time_series")
            .putList("index.routing_path", List.of("host", "cluster"))
            .put("index.number_of_shards", randomIntBetween(1, 5))
            .put("index.time_series.start_time", "2024-04-14T00:00:00Z")
            .put("index.time_series.end_time", "2024-04-16T00:00:00Z")
            .build()
            .toString();

        final String mapping = """
            "properties": {
              "@timestamp": { "type": "date" },
              "host": { "type": "keyword", "time_series_dimension": true },
              "cluster": { "type": "keyword", "time_series_dimension": true },
              "cpu": { "type": "long", "time_series_metric": "gauge" },
              "memory": { "type": "long", "time_series_metric": "gauge" },
              "request_count": { "type": "long", "time_series_metric": "counter" },
              "cluster_tag": { "type": "keyword" }
            }
            """;

        createIndex.setJsonEntity(Strings.format("""
            {
              "settings": %s,
              "mappings": {
                %s
              }
            }
            """, settings, mapping));
        assertOK(client.performRequest(createIndex));
    }

    private void indexTimeSeriesDocs(RestClient client, String index, List<TimeSeriesDoc> docs) throws IOException {
        logger.info("--> indexing {} time series docs to index {}", docs.size(), index);
        for (TimeSeriesDoc doc : docs) {
            Request createDoc = new Request("POST", "/" + index + "/_doc");
            createDoc.addParameter("refresh", "true");
            createDoc.setJsonEntity(Strings.format("""
                {
                  "@timestamp": %d,
                  "host": "%s",
                  "cluster": "%s",
                  "cpu": %d,
                  "memory": %d,
                  "request_count": %d,
                  "cluster_tag": "%s"
                }
                """, doc.timestamp, doc.host, doc.cluster, doc.cpu, doc.memory.getBytes(), doc.requestCount, doc.clusterTag));
            assertOK(client.performRequest(createDoc));
        }
        refresh(client, index);
    }

    private Map<String, Object> run(String query, boolean includeCCSMetadata) throws IOException {
        var queryBuilder = new RestEsqlTestCase.RequestObjectBuilder().query(query).profile(true);
        if (includeCCSMetadata) {
            queryBuilder.includeCCSMetadata(true);
        }
        Map<String, Object> resp = runEsql(queryBuilder.build());
        logger.info("--> query {} response {}", queryBuilder, resp);
        return resp;
    }

    private Map<String, Object> runEsql(RestEsqlTestCase.RequestObjectBuilder requestObject) throws IOException {
        return RestEsqlTestCase.runEsqlSync(requestObject, new AssertWarnings.NoWarnings(), profileLogger);
    }

    private boolean checkShardCounts() {
        if (shouldCheckShardCounts == null) {
            try {
                shouldCheckShardCounts = capabilitiesSupportedNewAndOld(List.of("correct_skipped_shard_count"));
            } catch (IOException e) {
                shouldCheckShardCounts = false;
            }
        }
        return shouldCheckShardCounts;
    }

    private boolean capabilitiesSupportedNewAndOld(List<String> requiredCapabilities) throws IOException {
        boolean isSupported = clusterHasCapability("POST", "/_query", List.of(), requiredCapabilities).orElse(false);
        try (RestClient remoteClient = remoteClusterClient()) {
            isSupported = isSupported
                && clusterHasCapability(remoteClient, "POST", "/_query", List.of(), requiredCapabilities).orElse(false);
        }
        return isSupported;
    }

    private void assertResultMap(boolean includeCCSMetadata, Map<String, Object> result, Map<String, Object> expectedResult) {
        MapMatcher mapMatcher = getResultMatcher(result.containsKey("is_partial"), result.containsKey("documents_found")).extraOk();
        if (includeCCSMetadata) {
            mapMatcher = mapMatcher.entry("_clusters", any(Map.class));
        }

        assertMap(
            result,
            mapMatcher.entry("columns", expectedResult.get("columns")).entry("values", matcherFor(expectedResult.get("values")))
        );

        if (includeCCSMetadata) {
            assertClusterDetailsMap(result);
        }
    }

    /**
     * Converts an unknown {@link Object} to an equality {@link Matcher}
     * for the public API methods that take {@linkplain Object}.
     * <br/>
     * This is a copy of org.elasticsearch.test.MapMatcher#matcherFor(java.lang.Object) to add support for Double values comparison
     * with a given error.
     */
    private static Matcher<?> matcherFor(Object value) {
        return switch (value) {
            case null -> nullValue();
            case List<?> list -> matchesList(list);
            case Map<?, ?> map -> matchesMap(map);
            case Matcher<?> matcher -> matcher;
            case Double doubleValue -> Matchers.closeTo(doubleValue, 0.0000001);
            default -> equalTo(value);
        };
    }

    public static ListMatcher matchesList(List<?> list) {
        ListMatcher matcher = ListMatcher.matchesList();
        for (Object item : list) {
            matcher = matcher.item(matcherFor(matcherFor(item)));
        }
        return matcher;
    }

    private void assertClusterDetailsMap(Map<String, Object> result) {
        @SuppressWarnings("unchecked")
        Map<String, Object> clusters = (Map<String, Object>) result.get("_clusters");
        assertThat(clusters.size(), equalTo(7));
        assertThat(clusters.keySet(), equalTo(Set.of("total", "successful", "running", "skipped", "partial", "failed", "details")));
        int expectedNumClusters = 2;
        Set<String> expectedClusterAliases = Set.of("remote_cluster", "(local)");

        assertThat(clusters.get("total"), equalTo(expectedNumClusters));
        assertThat(clusters.get("successful"), equalTo(expectedNumClusters));
        assertThat(clusters.get("running"), equalTo(0));
        assertThat(clusters.get("skipped"), equalTo(0));
        assertThat(clusters.get("partial"), equalTo(0));
        assertThat(clusters.get("failed"), equalTo(0));

        @SuppressWarnings("unchecked")
        Map<String, Object> details = (Map<String, Object>) clusters.get("details");
        assertThat(details.keySet(), equalTo(expectedClusterAliases));

        @SuppressWarnings("unchecked")
        Map<String, Object> remoteCluster = (Map<String, Object>) details.get("remote_cluster");
        assertThat(remoteCluster.keySet(), equalTo(Set.of("status", "indices", "took", "_shards")));
        assertThat(remoteCluster.get("status"), equalTo("successful"));
        assertThat(remoteCluster.get("indices"), equalTo("hosts-remote"));
        assertThat((Integer) remoteCluster.get("took"), greaterThanOrEqualTo(0));

        @SuppressWarnings("unchecked")
        Map<String, Object> remoteClusterShards = (Map<String, Object>) remoteCluster.get("_shards");
        assertThat(
            remoteClusterShards,
            matchesMap().entry("total", greaterThanOrEqualTo(0))
                .entry("successful", greaterThanOrEqualTo(0))
                .entry("skipped", greaterThanOrEqualTo(0))
                .entry("failed", 0)
        );
        if (checkShardCounts()) {
            assertThat(
                (int) remoteClusterShards.get("successful") + (int) remoteClusterShards.get("skipped"),
                equalTo(remoteClusterShards.get("total"))
            );
        }
        if (false == false) {
            @SuppressWarnings("unchecked")
            Map<String, Object> localCluster = (Map<String, Object>) details.get("(local)");
            assertThat(localCluster.keySet(), equalTo(Set.of("status", "indices", "took", "_shards")));
            assertThat(localCluster.get("status"), equalTo("successful"));
            assertThat(localCluster.get("indices"), equalTo("hosts-local"));
            assertThat((Integer) localCluster.get("took"), greaterThanOrEqualTo(0));

            @SuppressWarnings("unchecked")
            Map<String, Object> localClusterShards = (Map<String, Object>) localCluster.get("_shards");
            assertThat(
                localClusterShards,
                matchesMap().entry("total", greaterThanOrEqualTo(0))
                    .entry("successful", greaterThanOrEqualTo(0))
                    .entry("skipped", greaterThanOrEqualTo(0))
                    .entry("failed", 0)
            );
            if (checkShardCounts()) {
                assertThat(
                    (int) localClusterShards.get("successful") + (int) localClusterShards.get("skipped"),
                    equalTo(localClusterShards.get("total"))
                );
            }
        }
    }

    private RestClient remoteClusterClient() throws IOException {
        var clusterHosts = parseClusterHosts(remoteCluster.getHttpAddresses());
        return buildClient(restClientSettings(), clusterHosts.toArray(new HttpHost[0]));
    }

    private static boolean includeCCSMetadata() {
        return randomBoolean();
    }
}
