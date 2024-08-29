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
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.elasticsearch.test.MapMatcher.assertMap;
import static org.elasticsearch.test.MapMatcher.matchesMap;

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
        assumeTrue("CCS requires its own resolve_fields API", remoteFeaturesService().clusterHasFeature("esql.resolve_fields_api"));
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

    private Map<String, Object> run(String query) throws IOException {
        Map<String, Object> resp = runEsql(new RestEsqlTestCase.RequestObjectBuilder().query(query).build());
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

    public void testCount() throws Exception {
        {
            Map<String, Object> result = run("FROM test-local-index,*:test-remote-index | STATS c = COUNT(*)");
            var columns = List.of(Map.of("name", "c", "type", "long"));
            var values = List.of(List.of(localDocs.size() + remoteDocs.size()));
            assertMap(result, matchesMap().entry("columns", columns).entry("values", values));
        }
        {
            Map<String, Object> result = run("FROM *:test-remote-index | STATS c = COUNT(*)");
            var columns = List.of(Map.of("name", "c", "type", "long"));
            var values = List.of(List.of(remoteDocs.size()));
            assertMap(result, matchesMap().entry("columns", columns).entry("values", values));
        }
    }

    public void testUngroupedAggs() throws Exception {
        {
            Map<String, Object> result = run("FROM test-local-index,*:test-remote-index | STATS total = SUM(data)");
            var columns = List.of(Map.of("name", "total", "type", "long"));
            long sum = Stream.concat(localDocs.stream(), remoteDocs.stream()).mapToLong(d -> d.data).sum();
            var values = List.of(List.of(Math.toIntExact(sum)));
            assertMap(result, matchesMap().entry("columns", columns).entry("values", values));
        }
        {
            Map<String, Object> result = run("FROM *:test-remote-index | STATS total = SUM(data)");
            var columns = List.of(Map.of("name", "total", "type", "long"));
            long sum = remoteDocs.stream().mapToLong(d -> d.data).sum();
            var values = List.of(List.of(Math.toIntExact(sum)));
            assertMap(result, matchesMap().entry("columns", columns).entry("values", values));
        }
    }

    public void testGroupedAggs() throws Exception {
        {
            Map<String, Object> result = run("FROM test-local-index,*:test-remote-index | STATS total = SUM(data) BY color | SORT color");
            var columns = List.of(Map.of("name", "total", "type", "long"), Map.of("name", "color", "type", "keyword"));
            var values = Stream.concat(localDocs.stream(), remoteDocs.stream())
                .collect(Collectors.toMap(d -> d.color, Doc::data, Long::sum))
                .entrySet()
                .stream()
                .sorted(Map.Entry.comparingByKey())
                .map(e -> List.of(Math.toIntExact(e.getValue()), e.getKey()))
                .toList();
            assertMap(result, matchesMap().entry("columns", columns).entry("values", values));
        }
        {
            Map<String, Object> result = run("FROM *:test-remote-index | STATS total = SUM(data) by color | SORT color");
            var columns = List.of(Map.of("name", "total", "type", "long"), Map.of("name", "color", "type", "keyword"));
            var values = remoteDocs.stream()
                .collect(Collectors.toMap(d -> d.color, Doc::data, Long::sum))
                .entrySet()
                .stream()
                .sorted(Map.Entry.comparingByKey())
                .map(e -> List.of(Math.toIntExact(e.getValue()), e.getKey()))
                .toList();
            assertMap(result, matchesMap().entry("columns", columns).entry("values", values));
        }
    }

    private RestClient remoteClusterClient() throws IOException {
        var clusterHosts = parseClusterHosts(remoteCluster.getHttpAddresses());
        return buildClient(restClientSettings(), clusterHosts.toArray(new HttpHost[0]));
    }

    private TestFeatureService remoteFeaturesService() throws IOException {
        if (remoteFeaturesService == null) {
            try (RestClient remoteClient = remoteClusterClient()) {
                var remoteNodeVersions = readVersionsFromNodesInfo(remoteClient);
                var semanticNodeVersions = remoteNodeVersions.stream()
                    .map(ESRestTestCase::parseLegacyVersion)
                    .flatMap(Optional::stream)
                    .collect(Collectors.toSet());
                remoteFeaturesService = createTestFeatureService(getClusterStateFeatures(remoteClient), semanticNodeVersions);
            }
        }
        return remoteFeaturesService;
    }
}
