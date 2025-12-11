/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.ccq;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.apache.http.HttpHost;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.Version;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.logging.LoggerMessageFormat;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.MapMatcher;
import org.elasticsearch.test.TestClustersThreadFilter;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.test.rest.TestFeatureService;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.esql.AssertWarnings;
import org.elasticsearch.xpack.esql.qa.rest.ProfileLogger;
import org.elasticsearch.xpack.esql.qa.rest.RestEsqlTestCase;
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
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.elasticsearch.test.MapMatcher.assertMap;
import static org.elasticsearch.test.MapMatcher.matchesMap;
import static org.elasticsearch.xpack.esql.ccq.Clusters.REMOTE_CLUSTER_NAME;
import static org.hamcrest.Matchers.any;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

@ThreadLeakFilters(filters = TestClustersThreadFilter.class)
public class MultiClustersIT extends ESRestTestCase {
    static ElasticsearchCluster remoteCluster = Clusters.remoteCluster();
    static ElasticsearchCluster localCluster = Clusters.localCluster(remoteCluster);

    @ClassRule
    public static TestRule clusterRule = RuleChain.outerRule(remoteCluster).around(localCluster);

    @Rule(order = Integer.MIN_VALUE)
    public ProfileLogger profileLogger = new ProfileLogger();

    private static TestFeatureService remoteFeaturesService;

    @Override
    protected String getTestRestCluster() {
        return localCluster.getHttpAddresses();
    }

    record Doc(int id, String color, long data) {

    }

    final String localIndex = "test-local-index";
    List<Doc> localDocs = List.of();
    final String remoteIndex = "test-remote-index";
    List<Doc> remoteDocs = List.of();
    final String lookupIndexLocal = "test-lookup-index-local";
    final String lookupIndexRemote = "test-lookup-index-remote";
    final String lookupAlias = "test-lookup-index";
    private Boolean shouldCheckShardCounts = null;

    @Before
    public void setUpIndices() throws Exception {
        final String mapping = """
             "properties": {
               "data": { "type": "long" },
               "color": { "type": "keyword" }
             }
            """;
        RestClient localClient = client();
        localDocs = IntStream.range(0, between(1, 500))
            .mapToObj(n -> new Doc(n, randomFrom("red", "yellow", "green"), randomIntBetween(1, 1000)))
            .toList();
        createIndex(
            localClient,
            localIndex,
            Settings.builder().put("index.number_of_shards", randomIntBetween(1, 5)).build(),
            mapping,
            null
        );
        indexDocs(localClient, localIndex, localDocs);

        remoteDocs = IntStream.range(0, between(1, 500))
            .mapToObj(n -> new Doc(n, randomFrom("red", "yellow", "green"), randomIntBetween(1, 1000)))
            .toList();
        try (RestClient remoteClient = remoteClusterClient()) {
            createIndex(
                remoteClient,
                remoteIndex,
                Settings.builder().put("index.number_of_shards", randomIntBetween(1, 5)).build(),
                mapping,
                null
            );
            indexDocs(remoteClient, remoteIndex, remoteDocs);
        }
    }

    private void setupLookupIndices() throws IOException {
        RestClient localClient = client();
        final String mapping = """
             "properties": {
               "data": { "type": "long" },
               "morecolor": { "type": "keyword" }
             }
            """;
        var randomDocsData = new ArrayList<Integer>();
        var lookupDocs = IntStream.range(0, between(1, 5)).mapToObj(n -> {
            String color = randomFrom("red", "yellow", "green");
            int data = randomValueOtherThanMany(i -> randomDocsData.contains(i), () -> randomIntBetween(1, 1000));
            randomDocsData.add(data);
            return new Doc(n, color, data);
        }).toList();
        createIndex(
            localClient,
            lookupIndexLocal,
            Settings.builder().put("index.number_of_shards", 1).put("index.mode", "lookup").build(),
            mapping,
            "\"" + lookupAlias + "\":{}"
        );
        indexDocs(localClient, lookupIndexLocal, lookupDocs);
        try (RestClient remoteClient = remoteClusterClient()) {
            createIndex(
                remoteClient,
                lookupIndexRemote,
                Settings.builder().put("index.number_of_shards", 1).put("index.mode", "lookup").build(),
                mapping,
                "\"" + lookupAlias + "\":{}"
            );
            indexDocs(remoteClient, lookupIndexRemote, lookupDocs);
        }
    }

    public void wipeLookupIndices() throws IOException {
        try (RestClient remoteClient = remoteClusterClient()) {
            deleteIndex(remoteClient, lookupIndexRemote);
        }
        deleteIndex(client(), lookupIndexLocal);
    }

    @After
    public void wipeIndices() throws Exception {
        try (RestClient remoteClient = remoteClusterClient()) {
            deleteIndex(remoteClient, remoteIndex);
        }
    }

    void indexDocs(RestClient client, String index, List<Doc> docs) throws IOException {
        logger.info("--> indexing {} docs to index {}", docs.size(), index);
        long total = 0;
        for (Doc doc : docs) {
            Request createDoc = new Request("POST", "/" + index + "/_doc/id_" + doc.id);
            if (randomInt(100) < 10) {
                createDoc.addParameter("refresh", "true");
            }
            createDoc.setJsonEntity(Strings.format("""
                { "color": "%s", "data": %s}
                """, doc.color, doc.data));
            assertOK(client.performRequest(createDoc));
            total += doc.data;
        }
        logger.info("--> index={} total={}", index, total);
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

    private Map<String, Object> runWithColumnarAndIncludeCCSMetadata(String query) throws IOException {
        Map<String, Object> resp = runEsql(
            new RestEsqlTestCase.RequestObjectBuilder().query(query).includeCCSMetadata(true).columnar(true).build()
        );
        logger.info("--> query {} response {}", query, resp);
        return resp;
    }

    protected boolean supportsAsync() {
        return false; // TODO: Version.CURRENT.onOrAfter(Version.V_8_13_0); ?? // the Async API was introduced in 8.13.0
    }

    private Map<String, Object> runEsql(RestEsqlTestCase.RequestObjectBuilder requestObject) throws IOException {
        if (supportsAsync()) {
            return RestEsqlTestCase.runEsqlAsync(requestObject, new AssertWarnings.NoWarnings(), profileLogger);
        } else {
            return RestEsqlTestCase.runEsqlSync(requestObject, new AssertWarnings.NoWarnings(), profileLogger);
        }
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

    private <C, V> void assertResultMapWithCapabilities(
        boolean includeCCSMetadata,
        Map<String, Object> result,
        C columns,
        V values,
        boolean remoteOnly,
        List<String> fullResultCapabilities
    ) throws IOException {
        // the feature is completely supported if both local and remote clusters support it
        // otherwise we expect a partial result, and will not check the data
        boolean isSupported = capabilitiesSupportedNewAndOld(fullResultCapabilities);
        if (isSupported) {
            assertResultMap(includeCCSMetadata, result, columns, values, remoteOnly);
        } else {
            logger.info(
                "-->  skipping data check for a test, cluster does not support all of [{}] capabilities",
                String.join(",", fullResultCapabilities)
            );
            // just verify that we did not get a partial result
            var clusters = result.get("_clusters");
            var reason = "unexpected partial results" + (clusters != null ? ": _clusters=" + clusters : "");
            assertThat(reason, result.get("is_partial"), anyOf(nullValue(), is(false)));
        }
    }

    private boolean capabilitiesSupportedNewAndOld(List<String> requiredCapabilities) throws IOException {
        boolean isSupported = clusterHasCapability("POST", "/_query", List.of(), requiredCapabilities).orElse(false);
        try (RestClient remoteClient = remoteClusterClient()) {
            isSupported = isSupported
                && clusterHasCapability(remoteClient, "POST", "/_query", List.of(), requiredCapabilities).orElse(false);
        }
        return isSupported;
    }

    private <C, V> void assertResultMap(boolean includeCCSMetadata, Map<String, Object> result, C columns, V values, boolean remoteOnly) {
        MapMatcher mapMatcher = getResultMatcher(
            result.containsKey("is_partial"),
            result.containsKey("documents_found"),
            result.containsKey("start_time_in_millis")
        ).extraOk();
        if (includeCCSMetadata) {
            mapMatcher = mapMatcher.entry("_clusters", any(Map.class));
        }
        assertMap(result, mapMatcher.entry("columns", columns).entry("values", values));
        if (includeCCSMetadata) {
            assertClusterDetailsMap(result, remoteOnly);
        }
    }

    public void testCount() throws Exception {
        {
            boolean includeCCSMetadata = includeCCSMetadata();
            Map<String, Object> result = run("FROM test-local-index,*:test-remote-index | STATS c = COUNT(*)", includeCCSMetadata);
            var columns = List.of(Map.of("name", "c", "type", "long"));
            var values = List.of(List.of(localDocs.size() + remoteDocs.size()));

            assertResultMap(includeCCSMetadata, result, columns, values, false);
        }
        {
            boolean includeCCSMetadata = includeCCSMetadata();
            Map<String, Object> result = run("FROM *:test-remote-index | STATS c = COUNT(*)", includeCCSMetadata);
            var columns = List.of(Map.of("name", "c", "type", "long"));
            var values = List.of(List.of(remoteDocs.size()));

            assertResultMap(includeCCSMetadata, result, columns, values, true);
        }
    }

    public void testUngroupedAggs() throws Exception {
        {
            boolean includeCCSMetadata = includeCCSMetadata();
            Map<String, Object> result = run("FROM test-local-index,*:test-remote-index | STATS total = SUM(data)", includeCCSMetadata);
            var columns = List.of(Map.of("name", "total", "type", "long"));
            long sum = Stream.concat(localDocs.stream(), remoteDocs.stream()).mapToLong(d -> d.data).sum();
            var values = List.of(List.of(Math.toIntExact(sum)));

            // check all sections of map except _cluster/details
            assertResultMap(includeCCSMetadata, result, columns, values, false);
        }
        {
            boolean includeCCSMetadata = includeCCSMetadata();
            Map<String, Object> result = run("FROM *:test-remote-index | STATS total = SUM(data)", includeCCSMetadata);
            var columns = List.of(Map.of("name", "total", "type", "long"));
            long sum = remoteDocs.stream().mapToLong(d -> d.data).sum();
            var values = List.of(List.of(Math.toIntExact(sum)));

            assertResultMap(includeCCSMetadata, result, columns, values, true);
        }
        {
            Map<String, Object> result = runWithColumnarAndIncludeCCSMetadata("FROM *:test-remote-index | STATS total = SUM(data)");
            var columns = List.of(Map.of("name", "total", "type", "long"));
            long sum = remoteDocs.stream().mapToLong(d -> d.data).sum();
            var values = List.of(List.of(Math.toIntExact(sum)));

            assertResultMap(true, result, columns, values, true);
        }
    }

    private void assertClusterDetailsMap(Map<String, Object> result, boolean remoteOnly) {
        @SuppressWarnings("unchecked")
        Map<String, Object> clusters = (Map<String, Object>) result.get("_clusters");
        assertThat(clusters.size(), equalTo(7));
        assertThat(clusters.keySet(), equalTo(Set.of("total", "successful", "running", "skipped", "partial", "failed", "details")));
        int expectedNumClusters = remoteOnly ? 1 : 2;
        Set<String> expectedClusterAliases = remoteOnly ? Set.of("remote_cluster") : Set.of("remote_cluster", "(local)");

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
        assertThat(remoteCluster.get("indices"), equalTo("test-remote-index"));
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
        if (remoteOnly == false) {
            @SuppressWarnings("unchecked")
            Map<String, Object> localCluster = (Map<String, Object>) details.get("(local)");
            assertThat(localCluster.keySet(), equalTo(Set.of("status", "indices", "took", "_shards")));
            assertThat(localCluster.get("status"), equalTo("successful"));
            assertThat(localCluster.get("indices"), equalTo("test-local-index"));
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

    public void testGroupedAggs() throws Exception {
        {
            boolean includeCCSMetadata = includeCCSMetadata();
            Map<String, Object> result = run(
                "FROM test-local-index,*:test-remote-index | STATS total = SUM(data) BY color | SORT color",
                includeCCSMetadata
            );
            var columns = List.of(Map.of("name", "total", "type", "long"), Map.of("name", "color", "type", "keyword"));
            var values = Stream.concat(localDocs.stream(), remoteDocs.stream())
                .collect(Collectors.toMap(d -> d.color, Doc::data, Long::sum))
                .entrySet()
                .stream()
                .sorted(Map.Entry.comparingByKey())
                .map(e -> List.of(Math.toIntExact(e.getValue()), e.getKey()))
                .toList();

            assertResultMap(includeCCSMetadata, result, columns, values, false);
        }
        {
            boolean includeCCSMetadata = includeCCSMetadata();
            Map<String, Object> result = run(
                "FROM *:test-remote-index | STATS total = SUM(data) by color | SORT color",
                includeCCSMetadata
            );
            var columns = List.of(Map.of("name", "total", "type", "long"), Map.of("name", "color", "type", "keyword"));
            var values = remoteDocs.stream()
                .collect(Collectors.toMap(d -> d.color, Doc::data, Long::sum))
                .entrySet()
                .stream()
                .sorted(Map.Entry.comparingByKey())
                .map(e -> List.of(Math.toIntExact(e.getValue()), e.getKey()))
                .toList();

            // check all sections of map except _clusters/details
            assertResultMap(includeCCSMetadata, result, columns, values, true);
        }
    }

    public void testIndexPattern() throws Exception {
        {
            String indexPattern = randomFrom(
                "test-local-index,*:test-remote-index",
                "test-local-index,*:test-remote-*",
                "test-local-index,*:test-*",
                "test-*,*:test-remote-index"
            );
            Map<String, Object> result = run("FROM " + indexPattern + " | STATS c = COUNT(*)", false);
            var columns = List.of(Map.of("name", "c", "type", "long"));
            var values = List.of(List.of(localDocs.size() + remoteDocs.size()));

            assertResultMap(false, result, columns, values, false);
        }
        {
            String indexPattern = randomFrom("*:test-remote-index", "*:test-remote-*", "*:test-*");
            Map<String, Object> result = run("FROM " + indexPattern + " | STATS c = COUNT(*)", false);
            var columns = List.of(Map.of("name", "c", "type", "long"));
            var values = List.of(List.of(remoteDocs.size()));

            assertResultMap(false, result, columns, values, false);
        }
    }

    @SuppressWarnings("unchecked")
    public void testStats() throws IOException {
        Request caps = new Request("GET", "_capabilities?method=GET&path=_cluster/stats&capabilities=esql-stats");
        Response capsResponse = client().performRequest(caps);
        Map<String, Object> capsResult = entityAsMap(capsResponse.getEntity());
        assumeTrue("esql stats capability missing", capsResult.get("supported").equals(true));

        run("FROM test-local-index,*:test-remote-index | STATS total = SUM(data) BY color | SORT color", includeCCSMetadata());
        Request stats = new Request("GET", "_cluster/stats");
        Response statsResponse = client().performRequest(stats);
        Map<String, Object> result = entityAsMap(statsResponse.getEntity());
        assertThat(result, hasKey("ccs"));
        Map<String, Object> ccs = (Map<String, Object>) result.get("ccs");
        assertThat(ccs, hasKey("_esql"));
        Map<String, Object> esql = (Map<String, Object>) ccs.get("_esql");
        assertThat(esql, hasKey("total"));
        assertThat(esql, hasKey("success"));
        assertThat(esql, hasKey("took"));
        assertThat(esql, hasKey("remotes_per_search_max"));
        assertThat(esql, hasKey("remotes_per_search_avg"));
        assertThat(esql, hasKey("failure_reasons"));
        assertThat(esql, hasKey("features"));
        assertThat(esql, hasKey("clusters"));
        Map<String, Object> clusters = (Map<String, Object>) esql.get("clusters");
        assertThat(clusters, hasKey(REMOTE_CLUSTER_NAME));
        assertThat(clusters, hasKey("(local)"));
        Map<String, Object> clusterData = (Map<String, Object>) clusters.get(REMOTE_CLUSTER_NAME);
        assertThat(clusterData, hasKey("total"));
        assertThat(clusterData, hasKey("skipped"));
        assertThat(clusterData, hasKey("took"));
    }

    public void testLookupJoinAliases() throws IOException {
        assumeTrue(
            "Local cluster does not support multiple LOOKUP JOIN aliases",
            supportsLookupJoinAliases(Clusters.localClusterVersion())
        );
        assumeTrue(
            "Remote cluster does not support multiple LOOKUP JOIN aliases",
            supportsLookupJoinAliases(Clusters.remoteClusterVersion())
        );
        try {
            setupLookupIndices();
            Map<String, Object> result = run(
                "FROM test-local-index,*:test-remote-index | LOOKUP JOIN test-lookup-index ON data | STATS c = COUNT(*)",
                true
            );
            var columns = List.of(Map.of("name", "c", "type", "long"));
            var values = List.of(List.of(localDocs.size() + remoteDocs.size()));
            assertResultMap(true, result, columns, values, false);
        } finally {
            wipeLookupIndices();
        }
    }

    public void testLookupJoinAliasesSkipOld() throws IOException {
        assumeTrue(
            "Local cluster does not support multiple LOOKUP JOIN aliases",
            supportsLookupJoinAliases(Clusters.localClusterVersion())
        );
        assumeFalse(
            "Remote cluster should not support multiple LOOKUP JOIN aliases",
            supportsLookupJoinAliases(Clusters.remoteClusterVersion())
        );
        try {
            setupLookupIndices();
            Map<String, Object> result = run(
                "FROM test-local-index,*:test-remote-index | LOOKUP JOIN test-lookup-index ON data | STATS c = COUNT(*)",
                true
            );
            var columns = List.of(Map.of("name", "c", "type", "long"));
            var values = List.of(List.of(localDocs.size()));

            MapMatcher mapMatcher = getResultMatcher(
                false,
                result.containsKey("documents_found"),
                result.containsKey("start_time_in_millis")
            ).extraOk();
            mapMatcher = mapMatcher.entry("_clusters", any(Map.class));
            mapMatcher = mapMatcher.entry("is_partial", true);
            assertMap(result, mapMatcher.entry("columns", columns).entry("values", values));
            // check that the remote is skipped
            @SuppressWarnings("unchecked")
            Map<String, Object> clusters = (Map<String, Object>) result.get("_clusters");
            @SuppressWarnings("unchecked")
            Map<String, Object> details = (Map<String, Object>) clusters.get("details");
            @SuppressWarnings("unchecked")
            Map<String, Object> remoteCluster = (Map<String, Object>) details.get("remote_cluster");
            assertThat(remoteCluster.get("status"), equalTo("skipped"));
        } finally {
            wipeLookupIndices();
        }
    }

    public void testLikeIndex() throws Exception {

        boolean includeCCSMetadata = includeCCSMetadata();
        Map<String, Object> result = run("""
            FROM test-local-index,*:test-remote-index METADATA _index
            | WHERE _index LIKE "*remote*"
            | STATS c = COUNT(*) BY _index
            | SORT _index ASC
            """, includeCCSMetadata);
        var columns = List.of(Map.of("name", "c", "type", "long"), Map.of("name", "_index", "type", "keyword"));
        var values = List.of(List.of(remoteDocs.size(), REMOTE_CLUSTER_NAME + ":" + remoteIndex));
        assertResultMapWithCapabilities(includeCCSMetadata, result, columns, values, false, List.of("like_on_index_fields"));
    }

    public void testLikeIndexLegacySettingNoResults() throws Exception {
        // the feature is completely supported if both local and remote clusters support it
        assumeTrue("not supported", capabilitiesSupportedNewAndOld(List.of("like_on_index_fields")));
        try (
            ClusterSettingToggle ignored = new ClusterSettingToggle(adminClient(), "esql.query.string_like_on_index", false, true);
            RestClient remoteClient = remoteClusterClient();
            ClusterSettingToggle ignored2 = new ClusterSettingToggle(remoteClient, "esql.query.string_like_on_index", false, true)
        ) {
            // test code with the setting changed
            boolean includeCCSMetadata = includeCCSMetadata();
            Map<String, Object> result = run("""
                FROM test-local-index,*:test-remote-index METADATA _index
                | WHERE _index LIKE "*remote*"
                | STATS c = COUNT(*) BY _index
                | SORT _index ASC
                """, includeCCSMetadata);
            var columns = List.of(Map.of("name", "c", "type", "long"), Map.of("name", "_index", "type", "keyword"));
            // we expect empty result, since the setting is false
            var values = List.of();
            assertResultMapWithCapabilities(includeCCSMetadata, result, columns, values, false, List.of("like_on_index_fields"));
        }
    }

    public void testLikeIndexLegacySettingResults() throws Exception {
        // we require that the admin client supports the like_on_index_fields capability
        // otherwise we will get an error when trying to toggle the setting
        // the remote client does not have to support it
        assumeTrue("not supported", capabilitiesSupportedNewAndOld(List.of("like_on_index_fields")));
        try (
            ClusterSettingToggle ignored = new ClusterSettingToggle(adminClient(), "esql.query.string_like_on_index", false, true);
            RestClient remoteClient = remoteClusterClient();
            ClusterSettingToggle ignored2 = new ClusterSettingToggle(remoteClient, "esql.query.string_like_on_index", false, true)
        ) {
            boolean includeCCSMetadata = includeCCSMetadata();
            Map<String, Object> result = run("""
                FROM test-local-index,*:test-remote-index METADATA _index
                | WHERE _index LIKE "*remote*:*remote*"
                | STATS c = COUNT(*) BY _index
                | SORT _index ASC
                """, includeCCSMetadata);
            var columns = List.of(Map.of("name", "c", "type", "long"), Map.of("name", "_index", "type", "keyword"));
            // we expect results, since the setting is false, but there is : in the LIKE query
            var values = List.of(List.of(remoteDocs.size(), REMOTE_CLUSTER_NAME + ":" + remoteIndex));
            assertResultMapWithCapabilities(includeCCSMetadata, result, columns, values, false, List.of("like_on_index_fields"));
        }
    }

    public void testNotLikeIndex() throws Exception {
        boolean includeCCSMetadata = includeCCSMetadata();
        Map<String, Object> result = run("""
            FROM test-local-index,*:test-remote-index METADATA _index
            | WHERE _index NOT LIKE "*remote*"
            | STATS c = COUNT(*) BY _index
            | SORT _index ASC
            """, includeCCSMetadata);
        var columns = List.of(Map.of("name", "c", "type", "long"), Map.of("name", "_index", "type", "keyword"));
        var values = List.of(List.of(localDocs.size(), localIndex));
        assertResultMapWithCapabilities(includeCCSMetadata, result, columns, values, false, List.of("like_on_index_fields"));
    }

    public void testLikeListIndex() throws Exception {
        // the feature is completely supported if both local and remote clusters support it
        assumeTrue("not supported", capabilitiesSupportedNewAndOld(List.of("like_list_on_index_fields")));
        boolean includeCCSMetadata = includeCCSMetadata();
        Map<String, Object> result = run("""
            FROM test-local-index,*:test-remote-index METADATA _index
            | WHERE _index LIKE ("*remote*", "not-exist*")
            | STATS c = COUNT(*) BY _index
            | SORT _index ASC
            """, includeCCSMetadata);
        var columns = List.of(Map.of("name", "c", "type", "long"), Map.of("name", "_index", "type", "keyword"));
        var values = List.of(List.of(remoteDocs.size(), REMOTE_CLUSTER_NAME + ":" + remoteIndex));
        assertResultMapWithCapabilities(
            includeCCSMetadata,
            result,
            columns,
            values,
            false,
            List.of("like_on_index_fields", "like_list_on_index_fields")
        );
    }

    public void testNotLikeListIndex() throws Exception {
        assumeTrue("not supported", capabilitiesSupportedNewAndOld(List.of("like_list_on_index_fields")));
        boolean includeCCSMetadata = includeCCSMetadata();
        Map<String, Object> result = run("""
            FROM test-local-index,*:test-remote-index METADATA _index
            | WHERE _index NOT LIKE ("*remote*", "not-exist*")
            | STATS c = COUNT(*) BY _index
            | SORT _index ASC
            """, includeCCSMetadata);
        var columns = List.of(Map.of("name", "c", "type", "long"), Map.of("name", "_index", "type", "keyword"));
        var values = List.of(List.of(localDocs.size(), localIndex));
        assertResultMapWithCapabilities(
            includeCCSMetadata,
            result,
            columns,
            values,
            false,
            List.of("like_on_index_fields", "like_list_on_index_fields")
        );
    }

    public void testNotLikeListKeyword() throws Exception {
        assumeTrue("not supported", capabilitiesSupportedNewAndOld(List.of("like_with_list_of_patterns")));
        boolean includeCCSMetadata = includeCCSMetadata();
        Map<String, Object> result = run("""
            FROM test-local-index,*:test-remote-index METADATA _index
            | WHERE color NOT LIKE ("*blue*", "*red*")
            | STATS c = COUNT(*) BY _index
            | SORT _index ASC
            """, includeCCSMetadata);
        var columns = List.of(Map.of("name", "c", "type", "long"), Map.of("name", "_index", "type", "keyword"));
        Predicate<Doc> filter = d -> false == (d.color.contains("blue") || d.color.contains("red"));

        var values = new ArrayList<>();
        int remoteCount = (int) remoteDocs.stream().filter(filter).count();
        int localCount = (int) localDocs.stream().filter(filter).count();
        if (remoteCount > 0) {
            values.add(List.of(remoteCount, REMOTE_CLUSTER_NAME + ":" + remoteIndex));
        }
        if (localCount > 0) {
            values.add(List.of(localCount, localIndex));
        }
        assertResultMapWithCapabilities(
            includeCCSMetadata,
            result,
            columns,
            values,
            false,
            List.of("like_on_index_fields", "like_list_on_index_fields")
        );
    }

    public void testRLikeIndex() throws Exception {
        boolean includeCCSMetadata = includeCCSMetadata();
        Map<String, Object> result = run("""
            FROM test-local-index,*:test-remote-index METADATA _index
            | WHERE _index RLIKE ".*remote.*"
            | STATS c = COUNT(*) BY _index
            | SORT _index ASC
            """, includeCCSMetadata);
        var columns = List.of(Map.of("name", "c", "type", "long"), Map.of("name", "_index", "type", "keyword"));
        var values = List.of(List.of(remoteDocs.size(), REMOTE_CLUSTER_NAME + ":" + remoteIndex));
        assertResultMapWithCapabilities(includeCCSMetadata, result, columns, values, false, List.of("like_on_index_fields"));
    }

    public void testNotRLikeIndex() throws Exception {
        boolean includeCCSMetadata = includeCCSMetadata();
        Map<String, Object> result = run("""
            FROM test-local-index,*:test-remote-index METADATA _index
            | WHERE _index NOT RLIKE ".*remote.*"
            | STATS c = COUNT(*) BY _index
            | SORT _index ASC
            """, includeCCSMetadata);
        var columns = List.of(Map.of("name", "c", "type", "long"), Map.of("name", "_index", "type", "keyword"));
        var values = List.of(List.of(localDocs.size(), localIndex));
        assertResultMapWithCapabilities(includeCCSMetadata, result, columns, values, false, List.of("like_on_index_fields"));
    }

    public void testRLikeListIndex() throws Exception {
        assumeTrue("not supported", capabilitiesSupportedNewAndOld(List.of("rlike_with_list_of_patterns")));
        boolean includeCCSMetadata = includeCCSMetadata();
        Map<String, Object> result = run("""
            FROM test-local-index,*:test-remote-index METADATA _index
            | WHERE _index RLIKE (".*remote.*", ".*not-exist.*")
            | STATS c = COUNT(*) BY _index
            | SORT _index ASC
            """, includeCCSMetadata);
        var columns = List.of(Map.of("name", "c", "type", "long"), Map.of("name", "_index", "type", "keyword"));
        var values = List.of(List.of(remoteDocs.size(), REMOTE_CLUSTER_NAME + ":" + remoteIndex));
        // we depend on the code in like_on_index_fields to serialize an ExpressionQueryBuilder
        assertResultMapWithCapabilities(includeCCSMetadata, result, columns, values, false, List.of("like_on_index_fields"));
    }

    public void testNotRLikeListIndex() throws Exception {
        assumeTrue("not supported", capabilitiesSupportedNewAndOld(List.of("rlike_with_list_of_patterns")));
        boolean includeCCSMetadata = includeCCSMetadata();
        Map<String, Object> result = run("""
            FROM test-local-index,*:test-remote-index METADATA _index
            | WHERE _index NOT RLIKE (".*remote.*", ".*not-exist.*")
            | STATS c = COUNT(*) BY _index
            | SORT _index ASC
            """, includeCCSMetadata);
        var columns = List.of(Map.of("name", "c", "type", "long"), Map.of("name", "_index", "type", "keyword"));
        var values = List.of(List.of(localDocs.size(), localIndex));
        // we depend on the code in like_on_index_fields to serialize an ExpressionQueryBuilder
        assertResultMapWithCapabilities(includeCCSMetadata, result, columns, values, false, List.of("like_on_index_fields"));
    }

    public void testSubqueryInFromNewDataTypeErrorOutOnMixedClusterVersions() throws IOException {
        // local cluster supports subquery in from command and new data type
        assumeTrue(
            "local cluster has subquery_in_from capability",
            clusterHasCapability("POST", "/_query", List.of(), List.of("subquery_in_from_command")).orElse(false)
        );
        assumeTrue(
            "local cluster dense_vector_field_type_released capability",
            clusterHasCapability("POST", "/_query", List.of(), List.of("dense_vector_field_type_released")).orElse(false)
        );
        // remote cluster does not support new data type
        assumeTrue("remote cluster is on 9.0 with no dense_vector capability", doesNotSupportDenseVector(Clusters.remoteClusterVersion()));

        String localIndexWithNewDataType = "new-data-type-index";
        try {
            setupIndexForSubquery(localIndexWithNewDataType);

            // one cluster supports the new data type, however the other cluster does not, verifier will fail the query
            String query = LoggerMessageFormat.format(null, """
                FROM
                    (FROM {}),
                    (FROM {}:{}),
                    (FROM {}, {})
                | WHERE float_vector IS NOT NULL
                | STATS total = COUNT(*)
                """, localIndexWithNewDataType, REMOTE_CLUSTER_NAME, remoteIndex, localIndex, localIndexWithNewDataType);

            assertErrorMessage(query, List.of("[float_vector] has conflicting or unsupported data types in subqueries: [[dense_vector]"));

            query = LoggerMessageFormat.format(null, """
                FROM
                    (FROM {}),
                    (FROM {}:{}),
                    (FROM {}, {})
                | WHERE float_vector IS NOT NULL AND color IS NOT NULL
                | KEEP float_vector, color
                """, localIndexWithNewDataType, REMOTE_CLUSTER_NAME, remoteIndex, localIndex, localIndexWithNewDataType);

            assertErrorMessage(
                query,
                List.of(
                    "[float_vector] has conflicting or unsupported data types in subqueries: [[dense_vector]",
                    "[color] has conflicting or unsupported data types in subqueries: [[dense_vector]"
                )
            );
        } finally {
            deleteIndex(client(), localIndexWithNewDataType);
        }
    }

    public void testSubqueryInFrom() throws IOException {
        // local cluster supports subquery in from command and new data type
        assumeTrue(
            "local cluster has subquery_in_from capability",
            clusterHasCapability("POST", "/_query", List.of(), List.of("subquery_in_from_command")).orElse(false)
        );
        assumeTrue(
            "local cluster dense_vector_field_type_released capability",
            clusterHasCapability("POST", "/_query", List.of(), List.of("dense_vector_field_type_released")).orElse(false)
        );
        // remote cluster does not support new data type
        assumeTrue("remote cluster is on 9.0 with no dense_vector capability", doesNotSupportDenseVector(Clusters.remoteClusterVersion()));

        String localIndexWithNewDataType = "new-data-type-index";
        try {
            setupIndexForSubquery(localIndexWithNewDataType);

            String query = LoggerMessageFormat.format(null, """
                FROM
                    (FROM {} | WHERE knn(float_vector, [1.0, 2.0, 3.0])),
                    (FROM {}:{}),
                    (FROM {} | WHERE knn(float_vector, [1.0, 2.0, 3.0]))
                | WHERE float_vector IS NOT NULL
                | KEEP float_vector
                """, localIndexWithNewDataType, REMOTE_CLUSTER_NAME, remoteIndex, localIndexWithNewDataType);

            Map<String, Object> result = run(query, false);
            var columns = List.of(Map.of("name", "float_vector", "type", "dense_vector"));
            var denseVector = List.of(1.0, 2.0, 3.0);
            var values = List.of(List.of(denseVector), List.of(denseVector));

            assertResultMap(false, result, columns, values, false);

            query = LoggerMessageFormat.format(null, """
                FROM
                    (FROM {}),
                    (FROM {}:{}),
                    (FROM {}, {})
                | WHERE knn(float_vector, [1.0, 2.0, 3.0])
                | KEEP float_vector
                """, localIndexWithNewDataType, REMOTE_CLUSTER_NAME, remoteIndex, localIndex, localIndexWithNewDataType);
            result = run(query, false);
            assertResultMap(false, result, columns, values, false);

        } finally {
            deleteIndex(client(), localIndexWithNewDataType);
        }
    }

    private void setupIndexForSubquery(String indexName) throws IOException {
        RestClient client = client();
        // create an index on local cluster with aggregate_metric_double type
        String mappingForSubquery = """
            "properties": {
                "float_vector": {
                    "type" : "dense_vector",
                    "similarity": "l2_norm",
                    "index_options": {
                        "type": "hnsw",
                        "m": 16,
                        "ef_construction": 100
                     }
                },
                "color": {
                    "type" : "dense_vector",
                    "similarity": "l2_norm",
                    "index_options": {
                        "type": "hnsw",
                        "m": 16,
                        "ef_construction": 100
                     }
                }
            }
            """;
        createIndex(
            client,
            indexName,
            Settings.builder().put("index.number_of_shards", randomIntBetween(1, 5)).build(),
            mappingForSubquery,
            null
        );

        XContentBuilder doc = JsonXContent.contentBuilder().startObject();
        doc.startArray("float_vector");
        doc.value(1.0);
        doc.value(2.0);
        doc.value(3.0);
        doc.endArray();
        doc.startArray("color");
        doc.value(4.0);
        doc.value(5.0);
        doc.value(6.0);
        doc.endArray();
        doc.endObject();
        Request createDoc = new Request("POST", "/" + indexName + "/_doc");
        createDoc.addParameter("refresh", "true");
        createDoc.setJsonEntity(Strings.toString(doc));
        assertOK(client.performRequest(createDoc));
        refresh(client, indexName);
    }

    private boolean doesNotSupportDenseVector(Version version) {
        return version.onOrAfter(Version.V_9_0_0) && version.before(Version.V_9_1_0);
    }

    private void assertErrorMessage(String query, List<String> errorMessages) throws IOException {
        ResponseException re = expectThrows(ResponseException.class, () -> run(query, false));
        String errorMessage = EntityUtils.toString(re.getResponse().getEntity()).replaceAll("\\\\\n\s+\\\\", "");
        assertThat(re.getResponse().getStatusLine().getStatusCode(), equalTo(400));
        errorMessages.forEach(s -> assertThat(errorMessage, containsString(s)));
    }

    private RestClient remoteClusterClient() throws IOException {
        var clusterHosts = parseClusterHosts(remoteCluster.getHttpAddresses());
        return buildClient(restClientSettings(), clusterHosts.toArray(new HttpHost[0]));
    }

    private static boolean supportsLookupJoinAliases(Version version) {
        return version.onOrAfter(Version.V_9_2_0);
    }

    private static boolean includeCCSMetadata() {
        return randomBoolean();
    }

    public static class ClusterSettingToggle implements AutoCloseable {
        private final RestClient client;
        private final String settingKey;
        private final Object originalValue;

        public ClusterSettingToggle(RestClient client, String settingKey, Object newValue, Object restoreValue) throws IOException {
            this.client = client;
            this.settingKey = settingKey;
            this.originalValue = restoreValue;
            setValue(newValue);
        }

        private void setValue(Object value) throws IOException {
            Request set = new Request("PUT", "/_cluster/settings");
            set.setJsonEntity("{\"persistent\": {\"" + settingKey + "\": " + value + "}}");
            ESRestTestCase.assertOK(client.performRequest(set));
        }

        @Override
        public void close() throws IOException {
            setValue(originalValue == null ? "null" : originalValue);
        }
    }
}
