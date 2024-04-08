/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.analysis.common;

import org.elasticsearch.action.admin.indices.analyze.AnalyzeAction.AnalyzeToken;
import org.elasticsearch.action.admin.indices.analyze.AnalyzeAction.Response;
import org.elasticsearch.action.admin.indices.analyze.ReloadAnalyzersRequest;
import org.elasticsearch.action.admin.indices.analyze.ReloadAnalyzersResponse;
import org.elasticsearch.action.admin.indices.analyze.TransportReloadAnalyzersAction;
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
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

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
        Path config = internalCluster().getInstance(Environment.class).configFile();
        String synonymsFileName = "synonyms.txt";
        Path synonymsFile = config.resolve(synonymsFileName);
        try (PrintWriter out = new PrintWriter(new OutputStreamWriter(Files.newOutputStream(synonymsFile), StandardCharsets.UTF_8))) {
            out.println("foo, baz");
        }
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
        Response analyzeResponse = indicesAdmin().prepareAnalyze("test", "foo").setAnalyzer("my_synonym_analyzer").get();
        assertEquals(2, analyzeResponse.getTokens().size());
        assertEquals("foo", analyzeResponse.getTokens().get(0).getTerm());
        assertEquals("baz", analyzeResponse.getTokens().get(1).getTerm());

        // now update synonyms file several times and trigger reloading
        for (int i = 0; i < 10; i++) {
            String testTerm = randomAlphaOfLength(10);
            try (
                PrintWriter out = new PrintWriter(
                    new OutputStreamWriter(Files.newOutputStream(synonymsFile, StandardOpenOption.WRITE), StandardCharsets.UTF_8)
                )
            ) {
                out.println("foo, baz, " + testTerm);
            }
            ReloadAnalyzersResponse reloadResponse = client().execute(
                TransportReloadAnalyzersAction.TYPE,
                new ReloadAnalyzersRequest(null, preview, "test")
            ).actionGet();
            assertNoFailures(reloadResponse);
            assertEquals(cluster().numDataNodes(), reloadResponse.getSuccessfulShards());
            assertTrue(reloadResponse.getReloadDetails().containsKey("test"));
            assertEquals("test", reloadResponse.getReloadDetails().get("test").getIndexName());
            assertEquals(
                Collections.singleton("my_synonym_analyzer"),
                reloadResponse.getReloadDetails().get("test").getReloadedAnalyzers()
            );

            analyzeResponse = indicesAdmin().prepareAnalyze("test", "foo").setAnalyzer("my_synonym_analyzer").get();
            int expectedTokens = preview ? 2 : 3;
            assertEquals(expectedTokens, analyzeResponse.getTokens().size());
            Set<String> tokens = new HashSet<>();
            analyzeResponse.getTokens().stream().map(AnalyzeToken::getTerm).forEach(t -> tokens.add(t));
            assertTrue(tokens.contains("foo"));
            assertTrue(tokens.contains("baz"));
            if (preview == false) {
                assertTrue(tokens.contains(testTerm));
            }

            assertHitCount(prepareSearch("test").setQuery(QueryBuilders.matchQuery("field", "baz")), 1L);
            long expectedHitCount = preview ? 0L : 1L;
            assertHitCount(prepareSearch("test").setQuery(QueryBuilders.matchQuery("field", testTerm)), expectedHitCount);
        }
    }
}
