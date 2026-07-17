/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.analysis;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.standard.StandardTokenizer;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class ElasticsearchAnalyzerWrapperTests extends ESTestCase {

    /**
     * Verifies that {@link ElasticsearchAnalyzerWrapper} selects a reload-aware reuse strategy when wrapping a
     * {@link ReloadableCustomAnalyzer}. After the delegate is reloaded, the wrapper must invalidate its cached
     * token stream and rebuild it from the new components — otherwise reloadable analyzer's
     * updates are silently ignored.
     */
    public void testWrapperPropagatesReloadFromReloadableDelegate() throws Exception {
        TokenizerFactory tokenizerFactory = TokenizerFactory.newFactory("standard", StandardTokenizer::new);
        AtomicInteger createCallCount = new AtomicInteger(0);
        AbstractTokenFilterFactory trackingFilter = new AbstractTokenFilterFactory("tracking") {
            @Override
            public AnalysisMode getAnalysisMode() {
                return AnalysisMode.SEARCH_TIME;
            }

            @Override
            public TokenStream create(TokenStream tokenStream) {
                createCallCount.incrementAndGet();
                return tokenStream;
            }
        };
        AnalyzerComponents components = new AnalyzerComponents(
            tokenizerFactory,
            new CharFilterFactory[0],
            new TokenFilterFactory[] { trackingFilter }
        );

        try (
            ReloadableCustomAnalyzer delegate = new ReloadableCustomAnalyzer(components, 0, -1);
            ElasticsearchAnalyzerWrapper wrapper = new ElasticsearchAnalyzerWrapper(delegate) {
                @Override
                protected Analyzer getWrappedAnalyzer(String fieldName) {
                    return delegate;
                }
            }
        ) {
            consume(wrapper);
            assertEquals("first use should create components", 1, createCallCount.get());

            consume(wrapper);
            assertEquals("second use without reload should reuse cached components", 1, createCallCount.get());

            delegate.reload(
                "test_analyzer",
                Settings.builder().put("tokenizer", "standard").putList("filter", "tracking").build(),
                Map.of("standard", tokenizerFactory),
                Map.of(),
                Map.of("tracking", trackingFilter)
            );

            consume(wrapper);
            assertEquals("reload must force the wrapper to rebuild components", 2, createCallCount.get());
        }
    }

    private static void consume(Analyzer analyzer) throws Exception {
        try (TokenStream ts = analyzer.tokenStream("field", "hello world")) {
            ts.reset();
            while (ts.incrementToken()) {
            }
            ts.end();
        }
    }
}
