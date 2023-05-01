/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.action;

import org.elasticsearch.action.admin.indices.analyze.AnalyzeAction;
import org.elasticsearch.action.admin.indices.analyze.AnalyzeAction.AnalyzeToken;
import org.elasticsearch.action.admin.indices.analyze.AnalyzeAction.Response;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.analysis.common.CommonAnalysisPlugin;
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
import java.util.List;
import java.util.Set;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;

public class ReloadAnalyzerTests extends ESSingleNodeTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return Arrays.asList(LocalStateCompositeXPackPlugin.class, CommonAnalysisPlugin.class);
    }

    public void testSynonymsUpdateable() throws IOException {
        String synonymsFileName = "synonyms.txt";
        Path synonymsFile = setupResourceFile(synonymsFileName, "foo, baz");

        final String indexName = "test";
        final String synonymAnalyzerName = "synonym_analyzer";
        final String synonymGraphAnalyzerName = "synonym_graph_analyzer";
        assertAcked(
            client().admin()
                .indices()
                .prepareCreate(indexName)
                .setSettings(
                    indexSettings(5, 0).put("analysis.analyzer." + synonymAnalyzerName + ".tokenizer", "standard")
                        .putList("analysis.analyzer." + synonymAnalyzerName + ".filter", "lowercase", "synonym_filter")
                        .put("analysis.analyzer." + synonymGraphAnalyzerName + ".tokenizer", "standard")
                        .putList("analysis.analyzer." + synonymGraphAnalyzerName + ".filter", "lowercase", "synonym_graph_filter")
                        .put("analysis.filter.synonym_filter.type", "synonym")
                        .put("analysis.filter.synonym_filter.updateable", "true")
                        .put("analysis.filter.synonym_filter.synonyms_path", synonymsFileName)
                        .put("analysis.filter.synonym_graph_filter.type", "synonym_graph")
                        .put("analysis.filter.synonym_graph_filter.updateable", "true")
                        .put("analysis.filter.synonym_graph_filter.synonyms_path", synonymsFileName)
                )
                .setMapping("field", "type=text,analyzer=standard,search_analyzer=" + synonymAnalyzerName)
        );

        client().prepareIndex(indexName).setId("1").setSource("field", "Foo").get();
        assertNoFailures(client().admin().indices().prepareRefresh(indexName).execute().actionGet());

        SearchResponse response = client().prepareSearch(indexName).setQuery(QueryBuilders.matchQuery("field", "baz")).get();
        assertHitCount(response, 1L);
        response = client().prepareSearch(indexName).setQuery(QueryBuilders.matchQuery("field", "buzz")).get();
        assertHitCount(response, 0L);

        {
            for (String analyzerName : new String[] { synonymAnalyzerName, synonymGraphAnalyzerName }) {
                Response analyzeResponse = client().admin().indices().prepareAnalyze(indexName, "foo").setAnalyzer(analyzerName).get();
                assertEquals(2, analyzeResponse.getTokens().size());
                Set<String> tokens = new HashSet<>();
                analyzeResponse.getTokens().stream().map(AnalyzeToken::getTerm).forEach(t -> tokens.add(t));
                assertTrue(tokens.contains("foo"));
                assertTrue(tokens.contains("baz"));
            }
        }

        // now update synonyms file and trigger reloading
        try (
            PrintWriter out = new PrintWriter(
                new OutputStreamWriter(Files.newOutputStream(synonymsFile, StandardOpenOption.WRITE), StandardCharsets.UTF_8)
            )
        ) {
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
                Response analyzeResponse = client().admin().indices().prepareAnalyze(indexName, "foo").setAnalyzer(analyzerName).get();
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

    public void testSynonymsInMultiplexerUpdateable() throws FileNotFoundException, IOException {
        String synonymsFileName = "synonyms.txt";
        Path synonymsFile = setupResourceFile(synonymsFileName, "foo, baz");

        final String indexName = "test";
        final String synonymAnalyzerName = "synonym_in_multiplexer_analyzer";
        assertAcked(
            client().admin()
                .indices()
                .prepareCreate(indexName)
                .setSettings(
                    indexSettings(5, 0).put("analysis.analyzer." + synonymAnalyzerName + ".tokenizer", "whitespace")
                        .putList("analysis.analyzer." + synonymAnalyzerName + ".filter", "my_multiplexer")
                        .put("analysis.filter.synonym_filter.type", "synonym")
                        .put("analysis.filter.synonym_filter.updateable", "true")
                        .put("analysis.filter.synonym_filter.synonyms_path", synonymsFileName)
                        .put("analysis.filter.my_multiplexer.type", "multiplexer")
                        .putList("analysis.filter.my_multiplexer.filters", "synonym_filter")
                )
                .setMapping("field", "type=text,analyzer=standard,search_analyzer=" + synonymAnalyzerName)
        );

        client().prepareIndex(indexName).setId("1").setSource("field", "foo").get();
        assertNoFailures(client().admin().indices().prepareRefresh(indexName).execute().actionGet());

        SearchResponse response = client().prepareSearch(indexName).setQuery(QueryBuilders.matchQuery("field", "baz")).get();
        assertHitCount(response, 1L);
        response = client().prepareSearch(indexName).setQuery(QueryBuilders.matchQuery("field", "buzz")).get();
        assertHitCount(response, 0L);

        Response analyzeResponse = client().admin().indices().prepareAnalyze(indexName, "foo").setAnalyzer(synonymAnalyzerName).get();
        assertEquals(2, analyzeResponse.getTokens().size());
        final Set<String> tokens = new HashSet<>();
        analyzeResponse.getTokens().stream().map(AnalyzeToken::getTerm).forEach(t -> tokens.add(t));
        assertTrue(tokens.contains("foo"));
        assertTrue(tokens.contains("baz"));

        // now update synonyms file and trigger reloading
        try (
            PrintWriter out = new PrintWriter(
                new OutputStreamWriter(Files.newOutputStream(synonymsFile, StandardOpenOption.WRITE), StandardCharsets.UTF_8)
            )
        ) {
            out.println("foo, baz, buzz");
        }
        ReloadAnalyzersResponse reloadResponse = client().execute(ReloadAnalyzerAction.INSTANCE, new ReloadAnalyzersRequest(indexName))
            .actionGet();
        assertNoFailures(reloadResponse);
        Set<String> reloadedAnalyzers = reloadResponse.getReloadDetails().get(indexName).getReloadedAnalyzers();
        assertEquals(1, reloadedAnalyzers.size());
        assertTrue(reloadedAnalyzers.contains(synonymAnalyzerName));

        analyzeResponse = client().admin().indices().prepareAnalyze(indexName, "foo").setAnalyzer(synonymAnalyzerName).get();
        assertEquals(3, analyzeResponse.getTokens().size());
        tokens.clear();
        analyzeResponse.getTokens().stream().map(AnalyzeToken::getTerm).forEach(t -> tokens.add(t));
        assertTrue(tokens.contains("foo"));
        assertTrue(tokens.contains("baz"));
        assertTrue(tokens.contains("buzz"));

        response = client().prepareSearch(indexName).setQuery(QueryBuilders.matchQuery("field", "baz")).get();
        assertHitCount(response, 1L);
        response = client().prepareSearch(indexName).setQuery(QueryBuilders.matchQuery("field", "buzz")).get();
        assertHitCount(response, 1L);
    }

    public void testUpdateableSynonymsRejectedAtIndexTime() throws FileNotFoundException, IOException {
        String synonymsFileName = "synonyms.txt";
        setupResourceFile(synonymsFileName, "foo, baz");
        Path configDir = node().getEnvironment().configFile();
        if (Files.exists(configDir) == false) {
            Files.createDirectory(configDir);
        }
        Path synonymsFile = configDir.resolve(synonymsFileName);
        if (Files.exists(synonymsFile) == false) {
            Files.createFile(synonymsFile);
        }
        try (
            PrintWriter out = new PrintWriter(
                new OutputStreamWriter(Files.newOutputStream(synonymsFile, StandardOpenOption.WRITE), StandardCharsets.UTF_8)
            )
        ) {
            out.println("foo, baz");
        }

        final String indexName = "test";
        final String analyzerName = "my_synonym_analyzer";

        MapperException ex = expectThrows(
            MapperException.class,
            () -> client().admin()
                .indices()
                .prepareCreate(indexName)
                .setSettings(
                    indexSettings(5, 0).put("analysis.analyzer." + analyzerName + ".tokenizer", "standard")
                        .putList("analysis.analyzer." + analyzerName + ".filter", "lowercase", "synonym_filter")
                        .put("analysis.filter.synonym_filter.type", "synonym")
                        .put("analysis.filter.synonym_filter.updateable", "true")
                        .put("analysis.filter.synonym_filter.synonyms_path", synonymsFileName)
                )
                .setMapping("field", "type=text,analyzer=" + analyzerName)
                .get()
        );

        assertEquals(
            "Failed to parse mapping: analyzer [my_synonym_analyzer] "
                + "contains filters [synonym_filter] that are not allowed to run in index time mode.",
            ex.getMessage()
        );

        // same for synonym filters in multiplexer chain
        ex = expectThrows(
            MapperException.class,
            () -> client().admin()
                .indices()
                .prepareCreate(indexName)
                .setSettings(
                    indexSettings(5, 0).put("analysis.analyzer." + analyzerName + ".tokenizer", "whitespace")
                        .putList("analysis.analyzer." + analyzerName + ".filter", "my_multiplexer")
                        .put("analysis.filter.synonym_filter.type", "synonym")
                        .put("analysis.filter.synonym_filter.updateable", "true")
                        .put("analysis.filter.synonym_filter.synonyms_path", synonymsFileName)
                        .put("analysis.filter.my_multiplexer.type", "multiplexer")
                        .putList("analysis.filter.my_multiplexer.filters", "synonym_filter")
                )
                .setMapping("field", "type=text,analyzer=" + analyzerName)
                .get()
        );

        assertEquals(
            "Failed to parse mapping: analyzer [my_synonym_analyzer] "
                + "contains filters [my_multiplexer] that are not allowed to run in index time mode.",
            ex.getMessage()
        );
    }

    public void testKeywordMarkerUpdateable() throws IOException {
        String fileName = "example_word_list.txt";
        Path file = setupResourceFile(fileName, "running");

        final String indexName = "test";
        final String analyzerName = "keyword_maker_analyzer";
        assertAcked(
            client().admin()
                .indices()
                .prepareCreate(indexName)
                .setSettings(
                    indexSettings(5, 0).put("analysis.analyzer." + analyzerName + ".tokenizer", "whitespace")
                        .putList("analysis.analyzer." + analyzerName + ".filter", "keyword_marker_filter", "stemmer")
                        .put("analysis.filter.keyword_marker_filter.type", "keyword_marker")
                        .put("analysis.filter.keyword_marker_filter.updateable", "true")
                        .put("analysis.filter.keyword_marker_filter.keywords_path", fileName)
                )
                .setMapping("field", "type=text,analyzer=standard,search_analyzer=" + analyzerName)
        );

        AnalyzeAction.Response analysisResponse = client().admin()
            .indices()
            .prepareAnalyze("test", "running jumping")
            .setAnalyzer(analyzerName)
            .get();
        List<AnalyzeToken> tokens = analysisResponse.getTokens();
        assertEquals("running", tokens.get(0).getTerm());
        assertEquals("jump", tokens.get(1).getTerm());

        // now update keyword marker file and trigger reloading
        try (
            PrintWriter out = new PrintWriter(
                new OutputStreamWriter(Files.newOutputStream(file, StandardOpenOption.WRITE), StandardCharsets.UTF_8)
            )
        ) {
            out.println("running");
            out.println("jumping");
        }

        ReloadAnalyzersResponse reloadResponse = client().execute(ReloadAnalyzerAction.INSTANCE, new ReloadAnalyzersRequest(indexName))
            .actionGet();
        assertNoFailures(reloadResponse);
        Set<String> reloadedAnalyzers = reloadResponse.getReloadDetails().get(indexName).getReloadedAnalyzers();
        assertEquals(1, reloadedAnalyzers.size());
        assertTrue(reloadedAnalyzers.contains(analyzerName));

        analysisResponse = client().admin().indices().prepareAnalyze("test", "running jumping").setAnalyzer(analyzerName).get();
        tokens = analysisResponse.getTokens();
        assertEquals("running", tokens.get(0).getTerm());
        assertEquals("jumping", tokens.get(1).getTerm());
    }

    private Path setupResourceFile(String fileName, String... content) throws IOException {
        Path configDir = node().getEnvironment().configFile();
        if (Files.exists(configDir) == false) {
            Files.createDirectory(configDir);
        }
        Path file = configDir.resolve(fileName);
        if (Files.exists(file) == false) {
            Files.createFile(file);
        }
        try (
            PrintWriter out = new PrintWriter(
                new OutputStreamWriter(Files.newOutputStream(file, StandardOpenOption.WRITE), StandardCharsets.UTF_8)
            )
        ) {
            for (String item : content) {
                out.println(item);
            }
        }
        return file;
    }

}
