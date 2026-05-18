/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.analysis.common;

import org.elasticsearch.action.admin.indices.analyze.AnalyzeAction.AnalyzeToken;
import org.elasticsearch.action.admin.indices.analyze.AnalyzeAction.Response;
import org.elasticsearch.action.admin.indices.analyze.ReloadAnalyzersRequest;
import org.elasticsearch.action.admin.indices.analyze.ReloadAnalyzersResponse;
import org.elasticsearch.action.admin.indices.analyze.TransportReloadAnalyzersAction;
import org.elasticsearch.action.admin.indices.close.CloseIndexRequest;
import org.elasticsearch.action.admin.indices.open.OpenIndexRequest;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.elasticsearch.indices.breaker.HierarchyCircuitBreakerService.FIELDDATA_CIRCUIT_BREAKER_TYPE_SETTING;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.hamcrest.Matchers.instanceOf;

public class ReloadSynonymAnalyzerIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(CommonAnalysisPlugin.class);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        Settings.Builder builder = Settings.builder().put(super.nodeSettings(nodeOrdinal, otherSettings));
        // Force the fielddata circuit breaker to always be a memory CB. We have a test that uses it in this test suite and
        // InternalTestCluster randomly sets it to a noop CB 10% of the time, breaking the test.
        builder.put(FIELDDATA_CIRCUIT_BREAKER_TYPE_SETTING.getKey(), "memory");
        return builder.build();
    }

    public void testSynonymsUpdateable() throws FileNotFoundException, IOException, InterruptedException {
        testSynonymsUpdate(false);
    }

    public void testSynonymsWithPreview() throws FileNotFoundException, IOException, InterruptedException {
        testSynonymsUpdate(true);
    }

    private void testSynonymsUpdate(boolean preview) throws FileNotFoundException, IOException, InterruptedException {
        Path config = internalCluster().getInstance(Environment.class).configDir();
        String synonymsFileName = "synonyms.txt";
        Path synonymsFile = config.resolve(synonymsFileName);
        writeFile(synonymsFile, "foo, baz");

        assertAcked(
            indicesAdmin().prepareCreate("test")
                .setSettings(
                    indexSettings(cluster().numDataNodes() * 2, 1).put("analysis.analyzer.my_synonym_analyzer.tokenizer", "standard")
                        .put("analysis.analyzer.my_synonym_analyzer.filter", "my_synonym_filter")
                        .put("analysis.filter.my_synonym_filter.type", "synonym")
                        .put("analysis.filter.my_synonym_filter.updateable", "true")
                        .put("analysis.filter.my_synonym_filter.synonyms_path", synonymsFileName)
                )
                .setMapping("field", "type=text,analyzer=standard,search_analyzer=my_synonym_analyzer")
        );

        prepareIndex("test").setId("1").setSource("field", "foo").get();
        assertNoFailures(indicesAdmin().prepareRefresh("test").get());

        assertHitCount(prepareSearch("test").setQuery(QueryBuilders.matchQuery("field", "baz")), 1L);
        assertHitCount(prepareSearch("test").setQuery(QueryBuilders.matchQuery("field", "buzz")), 0L);
        assertAnalysis("test", "my_synonym_analyzer", "foo", Set.of("foo", "baz"));

        // now update synonyms file several times and trigger reloading
        for (int i = 0; i < 10; i++) {
            String testTerm = randomAlphaOfLength(10);
            writeFile(synonymsFile, "foo, baz, " + testTerm, StandardOpenOption.WRITE);

            ReloadAnalyzersResponse reloadResponse = client().execute(
                TransportReloadAnalyzersAction.TYPE,
                new ReloadAnalyzersRequest(null, preview, "test")
            ).actionGet();
            assertReloadAnalyzers(reloadResponse, cluster().numDataNodes(), Map.of("test", Set.of("my_synonym_analyzer")));

            Set<String> expectedTokens = preview ? Set.of("foo", "baz") : Set.of("foo", "baz", testTerm);
            assertAnalysis("test", "my_synonym_analyzer", "foo", expectedTokens);

            assertHitCount(prepareSearch("test").setQuery(QueryBuilders.matchQuery("field", "baz")), 1L);
            long expectedHitCount = preview ? 0L : 1L;
            assertHitCount(prepareSearch("test").setQuery(QueryBuilders.matchQuery("field", testTerm)), expectedHitCount);
        }
    }

    public void testSynonymsUpdateInvalid() throws IOException {
        final String indexName = "test-invalid";
        final String synonymsFileName = "synonyms.txt";
        final String fieldName = "field";

        Path config = internalCluster().getInstance(Environment.class).configDir();
        Path synonymsFile = config.resolve(synonymsFileName);
        writeFile(synonymsFile, "foo, baz");

        assertAcked(
            indicesAdmin().prepareCreate(indexName)
                .setSettings(
                    indexSettings(cluster().numDataNodes(), 0).put("analysis.analyzer.my_synonym_analyzer.tokenizer", "standard")
                        .putList("analysis.analyzer.my_synonym_analyzer.filter", "my_stop_filter", "my_synonym_filter")
                        .put("analysis.filter.my_stop_filter.type", "stop")
                        .put("analysis.filter.my_stop_filter.stopwords", "bar")
                        .put("analysis.filter.my_synonym_filter.type", "synonym")
                        .put("analysis.filter.my_synonym_filter.updateable", "true")
                        .put("analysis.filter.my_synonym_filter.synonyms_path", synonymsFileName)
                )
                .setMapping(fieldName, "type=text,analyzer=standard,search_analyzer=my_synonym_analyzer")
        );
        ensureGreen(indexName);

        prepareIndex(indexName).setId("1").setSource(fieldName, "foo").get();
        assertNoFailures(indicesAdmin().prepareRefresh(indexName).get());

        assertHitCount(prepareSearch(indexName).setQuery(QueryBuilders.matchQuery(fieldName, "baz")), 1);
        assertHitCount(prepareSearch(indexName).setQuery(QueryBuilders.matchQuery(fieldName, "buzz")), 0);
        assertAnalysis(indexName, "my_synonym_analyzer", "foo", Set.of("foo", "baz"));

        // Add an invalid synonym to the file and reload analyzers
        writeFile(synonymsFile, "foo, baz, bar, buzz");
        ReloadAnalyzersResponse reloadResponse = client().execute(
            TransportReloadAnalyzersAction.TYPE,
            new ReloadAnalyzersRequest(null, false, indexName)
        ).actionGet();
        assertReloadAnalyzers(reloadResponse, cluster().numDataNodes(), Map.of(indexName, Set.of("my_synonym_analyzer")));

        ensureGreen(indexName);
        assertHitCount(prepareSearch(indexName).setQuery(QueryBuilders.matchQuery(fieldName, "baz")), 1);
        assertHitCount(prepareSearch(indexName).setQuery(QueryBuilders.matchQuery(fieldName, "buzz")), 1);
        assertAnalysis(indexName, "my_synonym_analyzer", "foo", Set.of("foo", "baz", "buzz"));

        // Reload the index
        reloadIndex(indexName);
        ensureGreen(indexName);
        assertHitCount(prepareSearch(indexName).setQuery(QueryBuilders.matchQuery(fieldName, "baz")), 1);
        assertHitCount(prepareSearch(indexName).setQuery(QueryBuilders.matchQuery(fieldName, "buzz")), 1);
        assertAnalysis(indexName, "my_synonym_analyzer", "foo", Set.of("foo", "baz", "buzz"));
    }

    public void testSynonymsUpdateTripsCircuitBreakerWithLenientTrue() throws Exception {
        final String synonymsIndex = "synonyms-index";
        final String noSynonymsIndex = "no-synonyms-index";

        Path config = internalCluster().getInstance(Environment.class).configDir();
        String synonymsFileName = "synonyms.txt";
        Path synonymsFile = config.resolve(synonymsFileName);
        writeFile(synonymsFile, "foo, baz");

        String dataNodeName = internalCluster().getRandomDataNodeName();
        assertAcked(
            indicesAdmin().prepareCreate(synonymsIndex)
                .setSettings(
                    indexSettings(1, 0).put("index.routing.allocation.include._name", dataNodeName)  // Pin the shard to a specific node
                        .put("analysis.analyzer.my_synonym_analyzer.tokenizer", "standard")
                        .put("analysis.analyzer.my_synonym_analyzer.filter", "my_synonym_filter")
                        .put("analysis.filter.my_synonym_filter.type", "synonym")
                        .put("analysis.filter.my_synonym_filter.updateable", "true")
                        .put("analysis.filter.my_synonym_filter.lenient", "true")
                        .put("analysis.filter.my_synonym_filter.synonyms_path", synonymsFileName)
                )
                .setMapping("field", "type=text,analyzer=standard,search_analyzer=my_synonym_analyzer")
        );
        assertAcked(indicesAdmin().prepareCreate(noSynonymsIndex).setSettings(indexSettings(1, 0)).setMapping("field", "type=text"));

        prepareIndex(synonymsIndex).setId("1").setSource("field", "foo").get();
        prepareIndex(noSynonymsIndex).setId("2").setSource("field", "bar").get();
        assertNoFailures(indicesAdmin().prepareRefresh(synonymsIndex, noSynonymsIndex).get());

        assertHitCount(prepareSearch(synonymsIndex).setQuery(QueryBuilders.matchQuery("field", "baz")), 1L);
        assertHitCount(prepareSearch(synonymsIndex).setQuery(QueryBuilders.matchQuery("field", "buzz")), 0L);
        assertHitCount(prepareSearch(noSynonymsIndex).setQuery(QueryBuilders.matchQuery("field", "bar")), 1L);
        assertAnalysis(synonymsIndex, "my_synonym_analyzer", "foo", Set.of("foo", "baz"));

        // Check that a circuit breaker trip uses an empty synonyms map when lenient is true
        try (var ignored = fullyAllocateCircuitBreakerOnNode(dataNodeName, CircuitBreaker.FIELDDATA)) {
            String testTerm = randomAlphaOfLength(10);
            writeFile(synonymsFile, "foo, baz, " + testTerm, StandardOpenOption.WRITE);

            ReloadAnalyzersResponse reloadResponse = client().execute(
                TransportReloadAnalyzersAction.TYPE,
                new ReloadAnalyzersRequest(null, false, synonymsIndex, noSynonymsIndex)
            ).actionGet();
            assertReloadAnalyzers(reloadResponse, 2, Map.of(synonymsIndex, Set.of("my_synonym_analyzer"), noSynonymsIndex, Set.of()));

            // Index stays green because lenient mode returns an empty synonyms map
            ensureGreen(synonymsIndex, noSynonymsIndex);
            ensureStableCluster(internalCluster().size());

            // Synonyms are empty, so synonym-based matches no longer work
            assertHitCount(prepareSearch(synonymsIndex).setQuery(QueryBuilders.matchQuery("field", "baz")), 0L);
            assertHitCount(prepareSearch(synonymsIndex).setQuery(QueryBuilders.matchQuery("field", "foo")), 1L);
            assertAnalysis(synonymsIndex, "my_synonym_analyzer", "foo", Set.of("foo"));

            // Non-synonym index remains fully accessible
            assertHitCount(prepareSearch(noSynonymsIndex).setQuery(QueryBuilders.matchQuery("field", "bar")), 1L);
        }

        // Check that the index recovers once the circuit breaker is released
        String testTerm = randomAlphaOfLength(10);
        writeFile(synonymsFile, "foo, baz, " + testTerm, StandardOpenOption.WRITE);

        ReloadAnalyzersResponse reloadResponse = client().execute(
            TransportReloadAnalyzersAction.TYPE,
            new ReloadAnalyzersRequest(null, false, synonymsIndex, noSynonymsIndex)
        ).actionGet();
        assertReloadAnalyzers(reloadResponse, 2, Map.of(synonymsIndex, Set.of("my_synonym_analyzer"), noSynonymsIndex, Set.of()));
        assertAnalysis(synonymsIndex, "my_synonym_analyzer", "foo", Set.of("foo", "baz", testTerm));
        ensureGreen(synonymsIndex, noSynonymsIndex);
        ensureStableCluster(internalCluster().size());
        assertHitCount(prepareSearch(synonymsIndex).setQuery(QueryBuilders.matchQuery("field", testTerm)), 1L);
        assertHitCount(prepareSearch(noSynonymsIndex).setQuery(QueryBuilders.matchQuery("field", "bar")), 1L);
    }

    public void testSynonymsUpdateTripsCircuitBreakerWithLenientFalse() throws Exception {
        final String synonymsIndex = "synonyms-index";
        final String noSynonymsIndex = "no-synonyms-index";

        Path config = internalCluster().getInstance(Environment.class).configDir();
        String synonymsFileName = "synonyms.txt";
        Path synonymsFile = config.resolve(synonymsFileName);
        writeFile(synonymsFile, "foo, baz");

        assertAcked(
            indicesAdmin().prepareCreate(synonymsIndex)
                .setSettings(
                    indexSettings(cluster().numDataNodes(), 0).put("analysis.analyzer.my_synonym_analyzer.tokenizer", "standard")
                        .put("analysis.analyzer.my_synonym_analyzer.filter", "my_synonym_filter")
                        .put("analysis.filter.my_synonym_filter.type", "synonym")
                        .put("analysis.filter.my_synonym_filter.updateable", "true")
                        .put("analysis.filter.my_synonym_filter.lenient", "false")
                        .put("analysis.filter.my_synonym_filter.synonyms_path", synonymsFileName)
                )
                .setMapping("field", "type=text,analyzer=standard,search_analyzer=my_synonym_analyzer")
        );
        assertAcked(
            indicesAdmin().prepareCreate(noSynonymsIndex)
                .setSettings(indexSettings(cluster().numDataNodes(), 0))
                .setMapping("field", "type=text")
        );

        prepareIndex(synonymsIndex).setId("1").setSource("field", "foo").get();
        prepareIndex(noSynonymsIndex).setId("2").setSource("field", "bar").get();
        assertNoFailures(indicesAdmin().prepareRefresh(synonymsIndex, noSynonymsIndex).get());

        assertHitCount(prepareSearch(synonymsIndex).setQuery(QueryBuilders.matchQuery("field", "baz")), 1L);
        assertHitCount(prepareSearch(synonymsIndex).setQuery(QueryBuilders.matchQuery("field", "buzz")), 0L);
        assertHitCount(prepareSearch(noSynonymsIndex).setQuery(QueryBuilders.matchQuery("field", "bar")), 1L);
        assertAnalysis(synonymsIndex, "my_synonym_analyzer", "foo", Set.of("foo", "baz"));

        // Check that a circuit breaker trip sets the index to red state when lenient is false
        String dataNodeName = internalCluster().getRandomDataNodeName();
        try (var ignored = fullyAllocateCircuitBreakerOnNode(dataNodeName, CircuitBreaker.FIELDDATA)) {
            String testTerm = randomAlphaOfLength(10);
            writeFile(synonymsFile, "foo, baz, " + testTerm, StandardOpenOption.WRITE);

            ReloadAnalyzersResponse reloadResponse = client().execute(
                TransportReloadAnalyzersAction.TYPE,
                new ReloadAnalyzersRequest(null, false, synonymsIndex, noSynonymsIndex)
            ).actionGet();
            assertEquals(1, reloadResponse.getFailedShards());
            assertEquals(1, reloadResponse.getShardFailures().length);
            assertThat(reloadResponse.getShardFailures()[0].getCause().getCause().getCause(), instanceOf(CircuitBreakingException.class));

            ensureRed(synonymsIndex);
            ensureGreen(noSynonymsIndex);
            ensureStableCluster(internalCluster().size());
            assertHitCount(prepareSearch(noSynonymsIndex).setQuery(QueryBuilders.matchQuery("field", "bar")), 1L);
        }

        // Check that the index can recover from the circuit breaker trip
        String testTerm = randomAlphaOfLength(10);
        writeFile(synonymsFile, "foo, baz, " + testTerm, StandardOpenOption.WRITE);

        ReloadAnalyzersResponse reloadResponse = client().execute(
            TransportReloadAnalyzersAction.TYPE,
            new ReloadAnalyzersRequest(null, false, synonymsIndex, noSynonymsIndex)
        ).actionGet();
        assertReloadAnalyzers(
            reloadResponse,
            cluster().numDataNodes() * 2,
            Map.of(synonymsIndex, Set.of("my_synonym_analyzer"), noSynonymsIndex, Set.of())
        );
        assertAnalysis(synonymsIndex, "my_synonym_analyzer", "foo", Set.of("foo", "baz", testTerm));
        ensureGreen(synonymsIndex, noSynonymsIndex);
        ensureStableCluster(internalCluster().size());
        assertHitCount(prepareSearch(synonymsIndex).setQuery(QueryBuilders.matchQuery("field", testTerm)), 1L);
        assertHitCount(prepareSearch(noSynonymsIndex).setQuery(QueryBuilders.matchQuery("field", "bar")), 1L);
    }

    private static void writeFile(Path path, String contents, OpenOption... options) throws IOException {
        try (PrintWriter out = new PrintWriter(new OutputStreamWriter(Files.newOutputStream(path, options), StandardCharsets.UTF_8))) {
            out.println(contents);
        }
    }

    private static void assertAnalysis(String indexName, String analyzerName, String input, Set<String> expectedTokens) {
        Response response = indicesAdmin().prepareAnalyze(indexName, input).setAnalyzer(analyzerName).get();
        Set<String> actualTokens = response.getTokens().stream().map(AnalyzeToken::getTerm).collect(Collectors.toSet());
        assertEquals(expectedTokens, actualTokens);
    }

    private static void assertReloadAnalyzers(
        ReloadAnalyzersResponse response,
        int expectedSuccessfulShards,
        Map<String, Set<String>> expectedIndicesAndAnalyzers
    ) {
        assertNoFailures(response);
        assertEquals(expectedSuccessfulShards, response.getSuccessfulShards());

        assertEquals(expectedIndicesAndAnalyzers.size(), response.getReloadDetails().size());
        for (var expectedIndexAndAnalyzers : expectedIndicesAndAnalyzers.entrySet()) {
            String expectedIndexName = expectedIndexAndAnalyzers.getKey();
            Set<String> expectedAnalyzers = expectedIndexAndAnalyzers.getValue();

            ReloadAnalyzersResponse.ReloadDetails reloadDetails = response.getReloadDetails().get(expectedIndexName);
            assertNotNull(reloadDetails);
            assertEquals(expectedAnalyzers, reloadDetails.getReloadedAnalyzers());
        }
    }

    private static void reloadIndex(String indexName) {
        final TimeValue timeout = TimeValue.timeValueSeconds(30);
        assertAcked(indicesAdmin().close(new CloseIndexRequest(indexName)).actionGet(timeout));
        assertAcked(indicesAdmin().open(new OpenIndexRequest(indexName)).actionGet(timeout));
    }
}
