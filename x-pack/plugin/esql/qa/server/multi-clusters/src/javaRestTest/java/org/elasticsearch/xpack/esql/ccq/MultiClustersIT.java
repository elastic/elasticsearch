/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.ccq;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.apache.http.HttpHost;
import org.elasticsearch.Version;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.MapMatcher;
import org.elasticsearch.test.TestClustersThreadFilter;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.test.rest.TestFeatureService;
import org.elasticsearch.xpack.esql.qa.rest.RestEsqlTestCase;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.elasticsearch.test.MapMatcher.assertMap;
import static org.elasticsearch.test.MapMatcher.matchesMap;
import static org.elasticsearch.xpack.esql.ccq.Clusters.REMOTE_CLUSTER_NAME;
import static org.hamcrest.Matchers.any;
import static org.hamcrest.Matchers.anyOf;
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
            return RestEsqlTestCase.runEsqlAsync(requestObject);
        } else {
            return RestEsqlTestCase.runEsqlSync(requestObject);
        }
    }

    private <C, V> void assertResultMapForLike(
        boolean includeCCSMetadata,
        Map<String, Object> result,
        C columns,
        V values,
        boolean remoteOnly,
        boolean requireLikeListCapability
    ) throws IOException {
        List<String> requiredCapabilities = new ArrayList<>(List.of("like_on_index_fields"));
        if (requireLikeListCapability) {
            requiredCapabilities.add("like_list_on_index_fields");
        }
        // the feature is completely supported if both local and remote clusters support it
        boolean isSupported = capabilitiesSupportedNewAndOld(requiredCapabilities);

        if (isSupported) {
            assertResultMap(includeCCSMetadata, result, columns, values, remoteOnly);
        } else {
            logger.info("-->  skipping data check for like index test, cluster does not support like index feature");
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

        }
        return isSupported;
    }

    private <C, V> void assertResultMap(boolean includeCCSMetadata, Map<String, Object> result, C columns, V values, boolean remoteOnly) {
        MapMatcher mapMatcher = getResultMatcher(
            ccsMetadataAvailable(),
            result.containsKey("is_partial"),
            result.containsKey("documents_found")
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
            assumeTrue("requires ccs metadata", ccsMetadataAvailable());
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
                .entry("successful", remoteClusterShards.get("total"))
                .entry("skipped", greaterThanOrEqualTo(0))
                .entry("failed", 0)
        );

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
                    .entry("successful", localClusterShards.get("total"))
                    .entry("skipped", greaterThanOrEqualTo(0))
                    .entry("failed", 0)
            );
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
        assumeTrue("capabilities endpoint is not available", capabilitiesEndpointAvailable());

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
        assertResultMapForLike(includeCCSMetadata, result, columns, values, false, false);
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
        assertResultMapForLike(includeCCSMetadata, result, columns, values, false, false);
    }

    public void testLikeListIndex() throws Exception {
        List<String> requiredCapabilities = new ArrayList<>(List.of("like_list_on_index_fields"));
        // the feature is completely supported if both local and remote clusters support it
        if (capabilitiesSupportedNewAndOld(requiredCapabilities) == false) {
            logger.info("-->  skipping testNotLikeListIndex, due to missing capability");
            return;
        }
        boolean includeCCSMetadata = includeCCSMetadata();
        Map<String, Object> result = run("""
            FROM test-local-index,*:test-remote-index METADATA _index
            | WHERE _index LIKE ("*remote*", "not-exist*")
            | STATS c = COUNT(*) BY _index
            | SORT _index ASC
            """, includeCCSMetadata);
        var columns = List.of(Map.of("name", "c", "type", "long"), Map.of("name", "_index", "type", "keyword"));
        var values = List.of(List.of(remoteDocs.size(), REMOTE_CLUSTER_NAME + ":" + remoteIndex));
        assertResultMapForLike(includeCCSMetadata, result, columns, values, false, true);
    }

    public void testNotLikeListIndex() throws Exception {
        List<String> requiredCapabilities = new ArrayList<>(List.of("like_list_on_index_fields"));
        // the feature is completely supported if both local and remote clusters support it
        if (capabilitiesSupportedNewAndOld(requiredCapabilities) == false) {
            logger.info("-->  skipping testNotLikeListIndex, due to missing capability");
            return;
        }
        boolean includeCCSMetadata = includeCCSMetadata();
        Map<String, Object> result = run("""
            FROM test-local-index,*:test-remote-index METADATA _index
            | WHERE _index NOT LIKE ("*remote*", "not-exist*")
            | STATS c = COUNT(*) BY _index
            | SORT _index ASC
            """, includeCCSMetadata);
        var columns = List.of(Map.of("name", "c", "type", "long"), Map.of("name", "_index", "type", "keyword"));
        var values = List.of(List.of(localDocs.size(), localIndex));
        assertResultMapForLike(includeCCSMetadata, result, columns, values, false, true);
    }

    public void testNotLikeListKeyWord() throws Exception {
        List<String> requiredCapabilities = new ArrayList<>(List.of("like_list_on_index_fields"));
        // the feature is completely supported if both local and remote clusters support it
        if (capabilitiesSupportedNewAndOld(requiredCapabilities) == false) {
            logger.info("-->  skipping testNotLikeListIndex, due to missing capability");
            return;
        }
        boolean includeCCSMetadata = includeCCSMetadata();
        Map<String, Object> result = run("""
            FROM test-local-index,*:test-remote-index METADATA _index
            | WHERE color NOT LIKE ("*blue*", "*red*")
            | STATS c = COUNT(*) BY _index
            | SORT _index ASC
            """, includeCCSMetadata);
        var columns = List.of(Map.of("name", "c", "type", "long"), Map.of("name", "_index", "type", "keyword"));
        var values = List.of(List.of(localDocs.size(), localIndex));
        assertResultMapForLike(includeCCSMetadata, result, columns, values, false, true);
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
        assertResultMapForLike(includeCCSMetadata, result, columns, values, false, false);
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
        assertResultMapForLike(includeCCSMetadata, result, columns, values, false, false);
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
        assertResultMapForLike(includeCCSMetadata, result, columns, values, false, false);
    }

    public void testLikeIndexLegacySettingNoHit() throws Exception {
        try (ClusterSettingToggle ignored = new ClusterSettingToggle(adminClient(), "esql.query.string_like_on_index", false, true)) {
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
            assertResultMapForLike(includeCCSMetadata, result, columns, values, false, false);
        }
    }

    public void testLikeIndexLegacySettingHit() throws Exception {
        try (ClusterSettingToggle ignored = new ClusterSettingToggle(adminClient(), "esql.query.string_like_on_index", false, true)) {
            boolean includeCCSMetadata = includeCCSMetadata();
            Map<String, Object> result = run("""
                FROM test-local-index,*:test-remote-index METADATA _index
                | WHERE _index LIKE "*remote*:*remote*"
                | STATS c = COUNT(*) BY _index
                | SORT _index ASC
                """, includeCCSMetadata);
            var columns = List.of(Map.of("name", "c", "type", "long"), Map.of("name", "_index", "type", "keyword"));
            var values = List.of(List.of(remoteDocs.size(), REMOTE_CLUSTER_NAME + ":" + remoteIndex));
            assertResultMapForLike(includeCCSMetadata, result, columns, values, false, false);
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
        assertResultMapForLike(includeCCSMetadata, result, columns, values, false, false);
    }

    public void testLikeListIndex() throws Exception {
        List<String> requiredCapabilities = new ArrayList<>(List.of("like_list_on_index_fields"));
        // the feature is completely supported if both local and remote clusters support it
        if (capabilitiesSupportedNewAndOld(requiredCapabilities) == false) {
            logger.info("-->  skipping testNotLikeListIndex, due to missing capability");
            return;
        }
        boolean includeCCSMetadata = includeCCSMetadata();
        Map<String, Object> result = run("""
            FROM test-local-index,*:test-remote-index METADATA _index
            | WHERE _index LIKE ("*remote*", "not-exist*")
            | STATS c = COUNT(*) BY _index
            | SORT _index ASC
            """, includeCCSMetadata);
        var columns = List.of(Map.of("name", "c", "type", "long"), Map.of("name", "_index", "type", "keyword"));
        var values = List.of(List.of(remoteDocs.size(), REMOTE_CLUSTER_NAME + ":" + remoteIndex));
        assertResultMapForLike(includeCCSMetadata, result, columns, values, false, true);
    }

    public void testNotLikeListIndex() throws Exception {
        List<String> requiredCapabilities = new ArrayList<>(List.of("like_list_on_index_fields"));
        // the feature is completely supported if both local and remote clusters support it
        if (capabilitiesSupportedNewAndOld(requiredCapabilities) == false) {
            logger.info("-->  skipping testNotLikeListIndex, due to missing capability");
            return;
        }
        boolean includeCCSMetadata = includeCCSMetadata();
        Map<String, Object> result = run("""
            FROM test-local-index,*:test-remote-index METADATA _index
            | WHERE _index NOT LIKE ("*remote*", "not-exist*")
            | STATS c = COUNT(*) BY _index
            | SORT _index ASC
            """, includeCCSMetadata);
        var columns = List.of(Map.of("name", "c", "type", "long"), Map.of("name", "_index", "type", "keyword"));
        var values = List.of(List.of(localDocs.size(), localIndex));
        assertResultMapForLike(includeCCSMetadata, result, columns, values, false, true);
    }

    public void testNotLikeListKeyWord() throws Exception {
        List<String> requiredCapabilities = new ArrayList<>(List.of("like_list_on_index_fields"));
        // the feature is completely supported if both local and remote clusters support it
        if (capabilitiesSupportedNewAndOld(requiredCapabilities) == false) {
            logger.info("-->  skipping testNotLikeListIndex, due to missing capability");
            return;
        }
        boolean includeCCSMetadata = includeCCSMetadata();
        Map<String, Object> result = run("""
            FROM test-local-index,*:test-remote-index METADATA _index
            | WHERE color NOT LIKE ("*blue*", "*red*")
            | STATS c = COUNT(*) BY _index
            | SORT _index ASC
            """, includeCCSMetadata);
        var columns = List.of(Map.of("name", "c", "type", "long"), Map.of("name", "_index", "type", "keyword"));
        var values = List.of(List.of(localDocs.size(), localIndex));
        assertResultMapForLike(includeCCSMetadata, result, columns, values, false, true);
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
        assertResultMapForLike(includeCCSMetadata, result, columns, values, false, false);
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
        assertResultMapForLike(includeCCSMetadata, result, columns, values, false, false);
    }

    private RestClient remoteClusterClient() throws IOException {
        var clusterHosts = parseClusterHosts(remoteCluster.getHttpAddresses());
        return buildClient(restClientSettings(), clusterHosts.toArray(new HttpHost[0]));
    }

    private static boolean ccsMetadataAvailable() {
        return Clusters.localClusterVersion().onOrAfter(Version.V_8_16_0);
    }

    private static boolean capabilitiesEndpointAvailable() {
        return Clusters.localClusterVersion().onOrAfter(Version.V_8_15_0);
    }

    private static boolean includeCCSMetadata() {
        return ccsMetadataAvailable() && randomBoolean();
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
