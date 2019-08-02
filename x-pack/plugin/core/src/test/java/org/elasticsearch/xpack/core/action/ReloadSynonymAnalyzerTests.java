/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.action;

import org.elasticsearch.action.admin.indices.analyze.AnalyzeAction.AnalyzeToken;
import org.elasticsearch.action.admin.indices.analyze.AnalyzeAction.Response;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.analysis.common.CommonAnalysisPlugin;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.mapper.MapperException;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.xpack.core.LocalStateCompositeXPackPlugin;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;

public class ReloadSynonymAnalyzerTests extends ESSingleNodeTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return Arrays.asList(LocalStateCompositeXPackPlugin.class, CommonAnalysisPlugin.class);
    }

    public void testSynonymsUpdateable() throws FileNotFoundException, IOException {
        String synonymsFileName = "synonyms.txt";
        Path configDir = node().getEnvironment().configFile();
        if (Files.exists(configDir) == false) {
            Files.createDirectory(configDir);
        }
        Path synonymsFile = configDir.resolve(synonymsFileName);
        if (Files.exists(synonymsFile) == false) {
            Files.createFile(synonymsFile);
        }
        try (PrintWriter out = new PrintWriter(
                new OutputStreamWriter(Files.newOutputStream(synonymsFile, StandardOpenOption.WRITE), StandardCharsets.UTF_8))) {
            out.println("foo, baz");
        }

        final String indexName = "test";
        final String synonymAnalyzerName = "synonym_analyzer";
        final String synonymGraphAnalyzerName = "synonym_graph_analyzer";
        assertAcked(client().admin().indices().prepareCreate(indexName).setSettings(Settings.builder()
                .put("index.number_of_shards", 5)
                .put("index.number_of_replicas", 0)
                .put("analysis.analyzer." + synonymAnalyzerName + ".tokenizer", "standard")
                .putList("analysis.analyzer." + synonymAnalyzerName + ".filter", "lowercase", "synonym_filter")
                .put("analysis.analyzer." + synonymGraphAnalyzerName + ".tokenizer", "standard")
                .putList("analysis.analyzer." + synonymGraphAnalyzerName + ".filter", "lowercase", "synonym_graph_filter")
                .put("analysis.filter.synonym_filter.type", "synonym")
                .put("analysis.filter.synonym_filter.updateable", "true")
                .put("analysis.filter.synonym_filter.synonyms_path", synonymsFileName)
                .put("analysis.filter.synonym_graph_filter.type", "synonym_graph")
                .put("analysis.filter.synonym_graph_filter.updateable", "true")
                .put("analysis.filter.synonym_graph_filter.synonyms_path", synonymsFileName))
                .addMapping("_doc", "field", "type=text,analyzer=standard,search_analyzer=" + synonymAnalyzerName));

        client().prepareIndex(indexName, "_doc", "1").setSource("field", "Foo").get();
        assertNoFailures(client().admin().indices().prepareRefresh(indexName).execute().actionGet());

        SearchResponse response = client().prepareSearch(indexName).setQuery(QueryBuilders.matchQuery("field", "baz")).get();
        assertHitCount(response, 1L);
        response = client().prepareSearch(indexName).setQuery(QueryBuilders.matchQuery("field", "buzz")).get();
        assertHitCount(response, 0L);

        {
            for (String analyzerName : new String[] { synonymAnalyzerName, synonymGraphAnalyzerName }) {
                Response analyzeResponse = client().admin().indices().prepareAnalyze(indexName, "foo").setAnalyzer(analyzerName)
                        .get();
                assertEquals(2, analyzeResponse.getTokens().size());
                Set<String> tokens = new HashSet<>();
                analyzeResponse.getTokens().stream().map(AnalyzeToken::getTerm).forEach(t -> tokens.add(t));
                assertTrue(tokens.contains("foo"));
                assertTrue(tokens.contains("baz"));
            }
        }

        // now update synonyms file and trigger reloading
        try (PrintWriter out = new PrintWriter(
                new OutputStreamWriter(Files.newOutputStream(synonymsFile, StandardOpenOption.WRITE), StandardCharsets.UTF_8))) {
            out.println("foo, baz, buzz");
        }
        ReloadAnalyzersResponse reloadResponse = client().execute(ReloadAnalyzerAction.INSTANCE, new ReloadAnalyzersRequest(indexName))
                .actionGet();
        assertNoFailures(reloadResponse);
        Set<String> reloadedAnalyzers = reloadResponse.getReloadDetails().get(indexName).getReloadedAnalyzers();
        assertEquals(2, reloadedAnalyzers.size());
        assertTrue(reloadedAnalyzers.contains(synonymAnalyzerName));
        assertTrue(reloadedAnalyzers.contains(synonymGraphAnalyzerName));

        {
            for (String analyzerName : new String[] { synonymAnalyzerName, synonymGraphAnalyzerName }) {
                Response analyzeResponse = client().admin().indices().prepareAnalyze(indexName, "foo").setAnalyzer(analyzerName)
                        .get();
                assertEquals(3, analyzeResponse.getTokens().size());
                Set<String> tokens = new HashSet<>();
                analyzeResponse.getTokens().stream().map(AnalyzeToken::getTerm).forEach(t -> tokens.add(t));
                assertTrue(tokens.contains("foo"));
                assertTrue(tokens.contains("baz"));
                assertTrue(tokens.contains("buzz"));
            }
        }

        response = client().prepareSearch(indexName).setQuery(QueryBuilders.matchQuery("field", "baz")).get();
        assertHitCount(response, 1L);
        response = client().prepareSearch(indexName).setQuery(QueryBuilders.matchQuery("field", "buzz")).get();
        assertHitCount(response, 1L);
    }

    public void testUpdateableSynonymsRejectedAtIndexTime() throws FileNotFoundException, IOException {
        String synonymsFileName = "synonyms.txt";
        Path configDir = node().getEnvironment().configFile();
        if (Files.exists(configDir) == false) {
            Files.createDirectory(configDir);
        }
        Path synonymsFile = configDir.resolve(synonymsFileName);
        if (Files.exists(synonymsFile) == false) {
            Files.createFile(synonymsFile);
        }
        try (PrintWriter out = new PrintWriter(
                new OutputStreamWriter(Files.newOutputStream(synonymsFile, StandardOpenOption.WRITE), StandardCharsets.UTF_8))) {
            out.println("foo, baz");
        }

        final String indexName = "test";
        final String analyzerName = "my_synonym_analyzer";
        MapperException ex = expectThrows(MapperException.class, () -> client().admin().indices().prepareCreate(indexName)
                .setSettings(Settings.builder()
                .put("index.number_of_shards", 5)
                .put("index.number_of_replicas", 0)
                .put("analysis.analyzer." + analyzerName + ".tokenizer", "standard")
                .putList("analysis.analyzer." + analyzerName + ".filter", "lowercase", "synonym_filter")
                .put("analysis.filter.synonym_filter.type", "synonym")
                .put("analysis.filter.synonym_filter.updateable", "true")
                .put("analysis.filter.synonym_filter.synonyms_path", synonymsFileName))
                .addMapping("_doc", "field", "type=text,analyzer=" + analyzerName).get());

        assertEquals(
                "Failed to parse mapping [_doc]: analyzer [my_synonym_analyzer] "
                + "contains filters [synonym_filter] that are not allowed to run in all mode.",
                ex.getMessage());
    }
}