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
        final String analyzerName = "my_synonym_analyzer";
        assertAcked(client().admin().indices().prepareCreate(indexName).setSettings(Settings.builder()
                .put("index.number_of_shards", 5)
                .put("index.number_of_replicas", 0)
                .put("analysis.analyzer." + analyzerName + ".tokenizer", "standard")
                .putList("analysis.analyzer." + analyzerName + ".filter", "lowercase", "my_synonym_filter")
                .put("analysis.filter.my_synonym_filter.type", "synonym")
                .put("analysis.filter.my_synonym_filter.updateable", "true")
                .put("analysis.filter.my_synonym_filter.synonyms_path", synonymsFileName))
                .addMapping("_doc", "field", "type=text,analyzer=standard,search_analyzer=" + analyzerName));

        client().prepareIndex(indexName, "_doc", "1").setSource("field", "Foo").get();
        assertNoFailures(client().admin().indices().prepareRefresh(indexName).execute().actionGet());

        SearchResponse response = client().prepareSearch(indexName).setQuery(QueryBuilders.matchQuery("field", "baz")).get();
        assertHitCount(response, 1L);
        response = client().prepareSearch(indexName).setQuery(QueryBuilders.matchQuery("field", "buzz")).get();
        assertHitCount(response, 0L);
        Response analyzeResponse = client().admin().indices().prepareAnalyze(indexName, "foo").setAnalyzer(analyzerName).get();
        assertEquals(2, analyzeResponse.getTokens().size());
        assertEquals("foo", analyzeResponse.getTokens().get(0).getTerm());
        assertEquals("baz", analyzeResponse.getTokens().get(1).getTerm());

        // now update synonyms file and trigger reloading
        try (PrintWriter out = new PrintWriter(
                new OutputStreamWriter(Files.newOutputStream(synonymsFile, StandardOpenOption.WRITE), StandardCharsets.UTF_8))) {
            out.println("foo, baz, buzz");
        }
        ReloadAnalyzersResponse reloadResponse = client().execute(ReloadAnalyzerAction.INSTANCE, new ReloadAnalyzersRequest(indexName))
                .actionGet();
        assertNoFailures(reloadResponse);
        Set<String> reloadedAnalyzers = reloadResponse.getReloadDetails().get(indexName).getReloadedAnalyzers();
        assertEquals(1, reloadedAnalyzers.size());
        assertTrue(reloadedAnalyzers.contains(analyzerName));

        analyzeResponse = client().admin().indices().prepareAnalyze(indexName, "Foo").setAnalyzer(analyzerName).get();
        assertEquals(3, analyzeResponse.getTokens().size());
        Set<String> tokens = new HashSet<>();
        analyzeResponse.getTokens().stream().map(AnalyzeToken::getTerm).forEach(t -> tokens.add(t));
        assertTrue(tokens.contains("foo"));
        assertTrue(tokens.contains("baz"));
        assertTrue(tokens.contains("buzz"));

        response = client().prepareSearch(indexName).setQuery(QueryBuilders.matchQuery("field", "baz")).get();
        assertHitCount(response, 1L);
        response = client().prepareSearch(indexName).setQuery(QueryBuilders.matchQuery("field", "buzz")).get();
        assertHitCount(response, 1L);
    }
}