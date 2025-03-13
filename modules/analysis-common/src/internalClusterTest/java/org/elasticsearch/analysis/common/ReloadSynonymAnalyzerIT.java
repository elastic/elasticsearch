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

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;

public class ReloadSynonymAnalyzerIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(CommonAnalysisPlugin.class);
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
