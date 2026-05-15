/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.analysis;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

import static org.hamcrest.Matchers.containsString;

public class TokenCountingAnalyzerTests extends ESTestCase {

    public void testTokenCountingBelowLimit() throws IOException {
        try (StandardAnalyzer delegate = new StandardAnalyzer()) {
            TokenCountingAnalyzer analyzer = new TokenCountingAnalyzer(delegate, 100);
            analyzer.resetTokenCount();
            try (TokenStream ts = analyzer.tokenStream("field", "hello world foo bar")) {
                ts.reset();
                int count = 0;
                while (ts.incrementToken()) {
                    count++;
                }
                ts.end();
                assertEquals(4, count);
                assertEquals(4, analyzer.getTokenCount());
            }
        }
    }

    public void testTokenCountingExceedsLimit() throws IOException {
        try (StandardAnalyzer delegate = new StandardAnalyzer()) {
            TokenCountingAnalyzer analyzer = new TokenCountingAnalyzer(delegate, 3);
            analyzer.resetTokenCount();
            try (TokenStream ts = analyzer.tokenStream("field", "hello world foo bar baz")) {
                ts.reset();
                TokenCountingAnalyzer.DocumentTokenCountExceededException ex = expectThrows(
                    TokenCountingAnalyzer.DocumentTokenCountExceededException.class,
                    () -> {
                        while (ts.incrementToken()) {
                            // consume tokens
                        }
                    }
                );
                assertThat(ex.getMessage(), containsString("[3]"));
                assertThat(ex.getMessage(), containsString("index.mapping.total_tokens_per_document.limit"));
                assertEquals(3, ex.getMaxTokenCount());
            }
        }
    }

    public void testTokenCountAccumulatesAcrossFields() throws IOException {
        try (StandardAnalyzer delegate = new StandardAnalyzer()) {
            TokenCountingAnalyzer analyzer = new TokenCountingAnalyzer(delegate, 5);
            analyzer.resetTokenCount();

            // First field: 3 tokens
            try (TokenStream ts = analyzer.tokenStream("field1", "one two three")) {
                ts.reset();
                while (ts.incrementToken()) {
                    // consume
                }
                ts.end();
            }
            assertEquals(3, analyzer.getTokenCount());

            // Second field: 2 more tokens (total 5, at the limit)
            try (TokenStream ts = analyzer.tokenStream("field2", "four five")) {
                ts.reset();
                while (ts.incrementToken()) {
                    // consume
                }
                ts.end();
            }
            assertEquals(5, analyzer.getTokenCount());

            // Third field: 1 more token should exceed limit
            try (TokenStream ts = analyzer.tokenStream("field3", "six")) {
                ts.reset();
                expectThrows(TokenCountingAnalyzer.DocumentTokenCountExceededException.class, () -> {
                    while (ts.incrementToken()) {
                        // consume
                    }
                });
            }
        }
    }

    public void testResetClearsCounter() throws IOException {
        try (StandardAnalyzer delegate = new StandardAnalyzer()) {
            TokenCountingAnalyzer analyzer = new TokenCountingAnalyzer(delegate, 3);

            // Use up the limit
            analyzer.resetTokenCount();
            try (TokenStream ts = analyzer.tokenStream("field", "one two three")) {
                ts.reset();
                while (ts.incrementToken()) {
                    // consume
                }
                ts.end();
            }
            assertEquals(3, analyzer.getTokenCount());

            // Reset and verify we can analyze again
            analyzer.resetTokenCount();
            assertEquals(0, analyzer.getTokenCount());
            try (TokenStream ts = analyzer.tokenStream("field", "a b c")) {
                ts.reset();
                int count = 0;
                while (ts.incrementToken()) {
                    count++;
                }
                ts.end();
                assertEquals(3, count);
            }
        }
    }

    public void testDelegatesPerFieldAnalysis() throws IOException {
        // Verify the wrapper properly delegates to the underlying analyzer
        try (StandardAnalyzer delegate = new StandardAnalyzer()) {
            TokenCountingAnalyzer analyzer = new TokenCountingAnalyzer(delegate, 1000);
            analyzer.resetTokenCount();
            try (TokenStream ts = analyzer.tokenStream("test_field", "hello world")) {
                ts.reset();
                CharTermAttribute attr = ts.addAttribute(CharTermAttribute.class);
                assertTrue(ts.incrementToken());
                assertEquals("hello", attr.toString());
                assertTrue(ts.incrementToken());
                assertEquals("world", attr.toString());
                assertFalse(ts.incrementToken());
                ts.end();
            }
        }
    }
}
