/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.analysis;

import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class IndexAnalyzersTests extends ESTestCase {

    public void testAnalyzerDefaults() throws IOException {
        Map<String, NamedAnalyzer> analyzers = new HashMap<>();
        NamedAnalyzer analyzer = new NamedAnalyzer("default", AnalyzerScope.INDEX, new StandardAnalyzer());
        analyzers.put(AnalysisRegistry.DEFAULT_ANALYZER_NAME, analyzer);

        // if only "default" is set in the map, all getters should return the same analyzer
        try (IndexAnalyzers indexAnalyzers = IndexAnalyzers.of(analyzers)) {
            assertSame(analyzer, indexAnalyzers.getDefaultIndexAnalyzer());
            assertSame(analyzer, indexAnalyzers.getDefaultSearchAnalyzer());
            assertSame(analyzer, indexAnalyzers.getDefaultSearchQuoteAnalyzer());
        }

        analyzers.put(
            AnalysisRegistry.DEFAULT_SEARCH_ANALYZER_NAME,
            new NamedAnalyzer("my_search_analyzer", AnalyzerScope.INDEX, new StandardAnalyzer())
        );
        try (IndexAnalyzers indexAnalyzers = IndexAnalyzers.of(analyzers)) {
            assertSame(analyzer, indexAnalyzers.getDefaultIndexAnalyzer());
            assertEquals("my_search_analyzer", indexAnalyzers.getDefaultSearchAnalyzer().name());
            assertEquals("my_search_analyzer", indexAnalyzers.getDefaultSearchQuoteAnalyzer().name());
        }

        analyzers.put(
            AnalysisRegistry.DEFAULT_SEARCH_QUOTED_ANALYZER_NAME,
            new NamedAnalyzer("my_search_quote_analyzer", AnalyzerScope.INDEX, new StandardAnalyzer())
        );
        try (IndexAnalyzers indexAnalyzers = IndexAnalyzers.of(analyzers)) {
            assertSame(analyzer, indexAnalyzers.getDefaultIndexAnalyzer());
            assertEquals("my_search_analyzer", indexAnalyzers.getDefaultSearchAnalyzer().name());
            assertEquals("my_search_quote_analyzer", indexAnalyzers.getDefaultSearchQuoteAnalyzer().name());
        }
    }

    public void testClose() throws IOException {

        AtomicInteger closes = new AtomicInteger(0);
        NamedAnalyzer a = new NamedAnalyzer("default", AnalyzerScope.INDEX, new WhitespaceAnalyzer()) {
            @Override
            public void close() {
                super.close();
                closes.incrementAndGet();
            }
        };

        NamedAnalyzer n = new NamedAnalyzer("keyword_normalizer", AnalyzerScope.INDEX, new KeywordAnalyzer()) {
            @Override
            public void close() {
                super.close();
                closes.incrementAndGet();
            }
        };

        NamedAnalyzer w = new NamedAnalyzer("whitespace_normalizer", AnalyzerScope.INDEX, new WhitespaceAnalyzer()) {
            @Override
            public void close() {
                super.close();
                closes.incrementAndGet();
            }
        };

        IndexAnalyzers ia = IndexAnalyzers.of(Map.of("default", a), Map.of("n", n), Map.of("w", w));
        ia.close();
        assertEquals(3, closes.get());

    }

}
