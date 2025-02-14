/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.query;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.core.KeywordTokenizer;
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
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchHits;

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
            prepareIndex("nested").setId("1").setSource("text", "the quick brown fox jumps"),
            prepareIndex("nested").setId("2").setSource("text", "quick brown"),
            prepareIndex("nested").setId("3").setSource("text", "quick")
        );

        assertNoFailures(
            prepareSearch("nested").setQuery(
                new IntervalQueryBuilder("empty_text", new IntervalsSourceProvider.Match("an empty query", 0, true, null, null, null))
            )
        );
    }

    public void testPreserveInnerGap() {
        assertAcked(prepareCreate("index").setMapping("""
            {
                "_doc" : {
                    "properties" : {
                        "text" : { "type" : "text" }
                    }
                }
            }
            """));

        indexRandom(true, prepareIndex("index").setId("1").setSource("text", "w1 w2 w3 w4 w5"));

        // ordered
        {
            var res = prepareSearch("index").setQuery(
                new IntervalQueryBuilder(
                    "text",
                    new IntervalsSourceProvider.Combine(
                        Arrays.asList(
                            new IntervalsSourceProvider.Match("w1 w4", -1, true, null, null, null),
                            new IntervalsSourceProvider.Match("w5", -1, true, null, null, null)
                        ),
                        true,
                        1,
                        null
                    )
                )
            );
            assertSearchHits(res, "1");
        }

        // unordered
        {
            var res = prepareSearch("index").setQuery(
                new IntervalQueryBuilder(
                    "text",
                    new IntervalsSourceProvider.Combine(
                        Arrays.asList(
                            new IntervalsSourceProvider.Match("w3", 0, false, null, null, null),
                            new IntervalsSourceProvider.Match("w4 w1", -1, false, null, null, null)
                        ),
                        false,
                        0,
                        null
                    )
                )
            );
            assertSearchHits(res, "1");
        }
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
