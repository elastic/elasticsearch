/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.ccq;

import org.apache.http.HttpHost;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xpack.esql.qa.rest.RestEsqlTestCase;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.elasticsearch.test.MapMatcher.assertMap;
import static org.elasticsearch.test.MapMatcher.matchesMap;
import static org.elasticsearch.xpack.esql.qa.rest.RestEsqlTestCase.runEsql;

public class MultiClustersIT extends ESRestTestCase {

    record Doc(int id, String color, long data) {

    }

    final String localIndex = "test-local-index";
    List<Doc> localDocs = List.of();
    final String remoteIndex = "test-remote-index";
    List<Doc> remoteDocs = List.of();

    @Before
    public void setUpIndices() throws Exception {
        assumeFalse("Skip the first step", Fixtures.testAgainstRemoteClusterOnly());
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
            Settings.builder().put("index.number_of_shards", ESTestCase.randomIntBetween(1, 5)).build(),
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
                Settings.builder().put("index.number_of_shards", ESTestCase.randomIntBetween(1, 5)).build(),
                mapping,
                null
            );
            indexDocs(remoteClient, remoteIndex, remoteDocs);
        }
    }

    @After
    public void wipeIndices() throws Exception {
        if (Fixtures.testAgainstRemoteClusterOnly()) {
            return;
        }
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
        assumeFalse("Skip the first step", Fixtures.testAgainstRemoteClusterOnly());
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
        String remoteCluster = System.getProperty("tests.rest.remote_cluster");
        if (remoteCluster == null) {
            throw new RuntimeException("remote_cluster wasn't specified");
        }
        var clusterHosts = parseClusterHosts(remoteCluster);
        return buildClient(restClientSettings(), clusterHosts.toArray(new HttpHost[0]));
    }
}
