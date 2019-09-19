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

package org.elasticsearch.index.analysis;

import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class IndexAnalyzersTests extends ESTestCase {

    /**
     * test the checks in the constructor
     */
    public void testAnalyzerMapChecks() {
        Map<String, NamedAnalyzer> analyzers = new HashMap<>();
        {
            NullPointerException ex = expectThrows(NullPointerException.class,
                    () -> new IndexAnalyzers(analyzers, Collections.emptyMap(), Collections.emptyMap()));
            assertEquals("the default analyzer must be set", ex.getMessage());
        }
        {
            analyzers.put(AnalysisRegistry.DEFAULT_ANALYZER_NAME,
                    new NamedAnalyzer("otherName", AnalyzerScope.INDEX, new StandardAnalyzer()));
            IllegalStateException ex = expectThrows(IllegalStateException.class,
                    () -> new IndexAnalyzers(analyzers, Collections.emptyMap(), Collections.emptyMap()));
            assertEquals("default analyzer must have the name [default] but was: [otherName]", ex.getMessage());
        }
    }

    public void testAnalyzerDefaults() throws IOException {
        Map<String, NamedAnalyzer> analyzers = new HashMap<>();
        NamedAnalyzer analyzer = new NamedAnalyzer("default", AnalyzerScope.INDEX, new StandardAnalyzer());
        analyzers.put(AnalysisRegistry.DEFAULT_ANALYZER_NAME, analyzer);

        // if only "default" is set in the map, all getters should return the same analyzer
        try (IndexAnalyzers indexAnalyzers = new IndexAnalyzers(analyzers, Collections.emptyMap(), Collections.emptyMap())) {
            assertSame(analyzer, indexAnalyzers.getDefaultIndexAnalyzer());
            assertSame(analyzer, indexAnalyzers.getDefaultSearchAnalyzer());
            assertSame(analyzer, indexAnalyzers.getDefaultSearchQuoteAnalyzer());
        }

        analyzers.put(AnalysisRegistry.DEFAULT_SEARCH_ANALYZER_NAME,
                new NamedAnalyzer("my_search_analyzer", AnalyzerScope.INDEX, new StandardAnalyzer()));
        try (IndexAnalyzers indexAnalyzers = new IndexAnalyzers(analyzers, Collections.emptyMap(), Collections.emptyMap())) {
            assertSame(analyzer, indexAnalyzers.getDefaultIndexAnalyzer());
            assertEquals("my_search_analyzer", indexAnalyzers.getDefaultSearchAnalyzer().name());
            assertEquals("my_search_analyzer", indexAnalyzers.getDefaultSearchQuoteAnalyzer().name());
        }

        analyzers.put(AnalysisRegistry.DEFAULT_SEARCH_QUOTED_ANALYZER_NAME,
                new NamedAnalyzer("my_search_quote_analyzer", AnalyzerScope.INDEX, new StandardAnalyzer()));
        try (IndexAnalyzers indexAnalyzers = new IndexAnalyzers(analyzers, Collections.emptyMap(), Collections.emptyMap())) {
            assertSame(analyzer, indexAnalyzers.getDefaultIndexAnalyzer());
            assertEquals("my_search_analyzer", indexAnalyzers.getDefaultSearchAnalyzer().name());
            assertEquals("my_search_quote_analyzer", indexAnalyzers.getDefaultSearchQuoteAnalyzer().name());
        }
    }

    public void testClose() throws IOException {

        AtomicInteger closes = new AtomicInteger(0);
        NamedAnalyzer a = new NamedAnalyzer("default", AnalyzerScope.INDEX, new WhitespaceAnalyzer()){
            @Override
            public void close() {
                super.close();
                closes.incrementAndGet();
            }
        };

        NamedAnalyzer n = new NamedAnalyzer("keyword_normalizer", AnalyzerScope.INDEX, new KeywordAnalyzer()){
            @Override
            public void close() {
                super.close();
                closes.incrementAndGet();
            }
        };

        NamedAnalyzer w = new NamedAnalyzer("whitespace_normalizer", AnalyzerScope.INDEX, new WhitespaceAnalyzer()){
            @Override
            public void close() {
                super.close();
                closes.incrementAndGet();
            }
        };

        IndexAnalyzers ia = new IndexAnalyzers(Map.of("default", a), Map.of("n", n), Map.of("w", w));
        ia.close();
        assertEquals(3, closes.get());

    }

}
