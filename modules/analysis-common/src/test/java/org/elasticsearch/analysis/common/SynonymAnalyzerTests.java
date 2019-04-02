/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.analysis.common;

import org.elasticsearch.action.admin.indices.analyze.AnalyzeResponse;
import org.elasticsearch.action.admin.indices.analyze.AnalyzeResponse.AnalyzeToken;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESSingleNodeTestCase;

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

public class SynonymAnalyzerTests extends ESSingleNodeTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return Arrays.asList(CommonAnalysisPlugin.class);
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

        assertAcked(client().admin().indices().prepareCreate("test").setSettings(Settings.builder()
                .put("index.number_of_shards", 1)
                .put("index.number_of_replicas", 0)
                .put("analysis.analyzer.my_synonym_analyzer.tokenizer", "standard")
                .putList("analysis.analyzer.my_synonym_analyzer.filter", "lowercase", "my_synonym_filter")
                .put("analysis.filter.my_synonym_filter.type", "synonym")
                .put("analysis.filter.my_synonym_filter.updateable", "true")
                .put("analysis.filter.my_synonym_filter.synonyms_path", synonymsFileName))
                .addMapping("_doc", "field", "type=text,analyzer=standard,search_analyzer=my_synonym_analyzer"));

        client().prepareIndex("test", "_doc", "1").setSource("field", "Foo").get();
        assertNoFailures(client().admin().indices().prepareRefresh("test").execute().actionGet());

        SearchResponse response = client().prepareSearch("test").setQuery(QueryBuilders.matchQuery("field", "baz")).get();
        assertHitCount(response, 1L);
        response = client().prepareSearch("test").setQuery(QueryBuilders.matchQuery("field", "buzz")).get();
        assertHitCount(response, 0L);
        AnalyzeResponse analyzeResponse = client().admin().indices().prepareAnalyze("test", "foo").setAnalyzer("my_synonym_analyzer").get();
        assertEquals(2, analyzeResponse.getTokens().size());
        assertEquals("foo", analyzeResponse.getTokens().get(0).getTerm());
        assertEquals("baz", analyzeResponse.getTokens().get(1).getTerm());

        // now update synonyms file and trigger reloading
        try (PrintWriter out = new PrintWriter(
                new OutputStreamWriter(Files.newOutputStream(synonymsFile, StandardOpenOption.WRITE), StandardCharsets.UTF_8))) {
            out.println("foo, baz, buzz");
        }
        // TODO don't use refresh here but something more specific
        assertNoFailures(client().admin().indices().prepareRefresh("test").execute().actionGet());

        analyzeResponse = client().admin().indices().prepareAnalyze("test", "Foo").setAnalyzer("my_synonym_analyzer").get();
        assertEquals(3, analyzeResponse.getTokens().size());
        Set<String> tokens = new HashSet<>();
        analyzeResponse.getTokens().stream().map(AnalyzeToken::getTerm).forEach(t -> tokens.add(t));
        assertTrue(tokens.contains("foo"));
        assertTrue(tokens.contains("baz"));
        assertTrue(tokens.contains("buzz"));

        response = client().prepareSearch("test").setQuery(QueryBuilders.matchQuery("field", "baz")).get();
        assertHitCount(response, 1L);
        response = client().prepareSearch("test").setQuery(QueryBuilders.matchQuery("field", "buzz")).get();
        assertHitCount(response, 1L);
    }
}