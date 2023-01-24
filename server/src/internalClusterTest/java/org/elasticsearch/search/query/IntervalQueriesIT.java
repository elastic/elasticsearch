/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.query;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.core.KeywordTokenizer;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.analysis.AnalyzerProvider;
import org.elasticsearch.index.analysis.AnalyzerScope;
import org.elasticsearch.index.query.IntervalQueryBuilder;
import org.elasticsearch.index.query.IntervalsSourceProvider;
import org.elasticsearch.indices.analysis.AnalysisModule;
import org.elasticsearch.plugins.AnalysisPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.InternalSettingsPlugin;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;

import static java.util.Collections.singletonMap;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;

public class IntervalQueriesIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(InternalSettingsPlugin.class, MockAnalysisPlugin.class);
    }

    public void testEmptyIntervalsWithNestedMappings() throws InterruptedException {
        assertAcked(prepareCreate("nested").setMapping("""
            { "_doc" : {
                "properties" : {
                    "empty_text" : { "type" : "text", "analyzer" : "empty" },
                    "text" : { "type" : "text" },
                    "nested" : { "type" : "nested", "properties" : { "nt" : { "type" : "text" } } }
                }
            }}
            """));

        indexRandom(
            true,
            client().prepareIndex("nested").setId("1").setSource("text", "the quick brown fox jumps"),
            client().prepareIndex("nested").setId("2").setSource("text", "quick brown"),
            client().prepareIndex("nested").setId("3").setSource("text", "quick")
        );

        SearchResponse resp = client().prepareSearch("nested")
            .setQuery(
                new IntervalQueryBuilder("empty_text", new IntervalsSourceProvider.Match("an empty query", 0, true, null, null, null))
            )
            .get();
        assertEquals(0, resp.getFailedShards());
    }

    private static class EmptyAnalyzer extends Analyzer {

        @Override
        protected TokenStreamComponents createComponents(String fieldName) {
            Tokenizer source = new KeywordTokenizer();
            TokenStream sink = new TokenStream() {
                @Override
                public boolean incrementToken() throws IOException {
                    return false;
                }
            };
            return new TokenStreamComponents(source, sink);
        }
    }

    public static class MockAnalysisPlugin extends Plugin implements AnalysisPlugin {

        @Override
        public Map<String, AnalysisModule.AnalysisProvider<AnalyzerProvider<? extends Analyzer>>> getAnalyzers() {
            return singletonMap("empty", (indexSettings, environment, name, settings) -> new AnalyzerProvider<>() {
                @Override
                public String name() {
                    return "empty";
                }

                @Override
                public AnalyzerScope scope() {
                    return AnalyzerScope.GLOBAL;
                }

                @Override
                public Analyzer get() {
                    return new EmptyAnalyzer();
                }
            });
        }
    }
}
