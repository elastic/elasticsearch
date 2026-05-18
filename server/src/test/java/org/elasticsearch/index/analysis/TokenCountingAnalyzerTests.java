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
            try (TokenStream ts = analyzer.tokenStream("field", "hello world foo bar")) {
                ts.reset();
                int count = 0;
                while (ts.incrementToken()) {
                    count++;
                }
                ts.end();
                assertEquals(4, count);
            }
        }
    }

    public void testTokenCountingExceedsLimit() throws IOException {
        try (StandardAnalyzer delegate = new StandardAnalyzer()) {
            TokenCountingAnalyzer analyzer = new TokenCountingAnalyzer(delegate, 3);
            try (TokenStream ts = analyzer.tokenStream("field", "hello world foo bar baz")) {
                ts.reset();
                TokenCountingAnalyzer.FieldTokenCountExceededException ex = expectThrows(
                    TokenCountingAnalyzer.FieldTokenCountExceededException.class,
                    () -> {
                        while (ts.incrementToken()) {
                            // consume tokens
                        }
                    }
                );
                assertThat(ex.getMessage(), containsString("[3]"));
                assertThat(ex.getMessage(), containsString("[field]"));
                assertThat(ex.getMessage(), containsString("index.mapping.tokens_per_field.limit"));
                assertEquals(3, ex.getMaxTokenCount());
                assertEquals("field", ex.getFieldName());
            }
        }
    }

    public void testCounterResetsPerField() throws IOException {
        try (StandardAnalyzer delegate = new StandardAnalyzer()) {
            // Limit of 3: each field has 3 tokens, should succeed because counter resets per field
            TokenCountingAnalyzer analyzer = new TokenCountingAnalyzer(delegate, 3);

            // First field: 3 tokens (at the limit)
            try (TokenStream ts = analyzer.tokenStream("field1", "one two three")) {
                ts.reset();
                while (ts.incrementToken()) {
                    // consume
                }
                ts.end();
            }

            // Second field: 3 tokens again — should succeed because counter resets
            try (TokenStream ts = analyzer.tokenStream("field2", "four five six")) {
                ts.reset();
                while (ts.incrementToken()) {
                    // consume
                }
                ts.end();
            }
        }
    }

    public void testCounterResetsOnSameFieldNameReuse() throws IOException {
        try (StandardAnalyzer delegate = new StandardAnalyzer()) {
            // With PER_FIELD_REUSE_STRATEGY, the same field name reuses the same TokenStreamComponents
            // (and thus the same filter instance). Verify that reset() properly zeroes the counter.
            TokenCountingAnalyzer analyzer = new TokenCountingAnalyzer(delegate, 3);

            // First use: 3 tokens (at the limit)
            try (TokenStream ts = analyzer.tokenStream("field", "one two three")) {
                ts.reset();
                while (ts.incrementToken()) {
                    // consume
                }
                ts.end();
            }

            // Second use of same field name: 3 tokens again — should succeed because reset() zeroes counter
            try (TokenStream ts = analyzer.tokenStream("field", "four five six")) {
                ts.reset();
                while (ts.incrementToken()) {
                    // consume
                }
                ts.end();
            }
        }
    }

    public void testExactBoundary() throws IOException {
        try (StandardAnalyzer delegate = new StandardAnalyzer()) {
            TokenCountingAnalyzer analyzer = new TokenCountingAnalyzer(delegate, 5);

            // Exactly at the limit: 5 tokens with limit 5 should succeed
            try (TokenStream ts = analyzer.tokenStream("field", "one two three four five")) {
                ts.reset();
                int count = 0;
                while (ts.incrementToken()) {
                    count++;
                }
                ts.end();
                assertEquals(5, count);
            }

            // One over the limit: 6 tokens with limit 5 should fail
            try (TokenStream ts = analyzer.tokenStream("field", "one two three four five six")) {
                ts.reset();
                expectThrows(TokenCountingAnalyzer.FieldTokenCountExceededException.class, () -> {
                    while (ts.incrementToken()) {
                        // consume
                    }
                });
            }
        }
    }

    public void testNoLimitWhenDisabled() throws IOException {
        try (StandardAnalyzer delegate = new StandardAnalyzer()) {
            // -1 means no limit
            TokenCountingAnalyzer analyzer = new TokenCountingAnalyzer(delegate, -1);
            try (TokenStream ts = analyzer.tokenStream("field", "one two three four five six seven eight nine ten")) {
                ts.reset();
                int count = 0;
                while (ts.incrementToken()) {
                    count++;
                }
                ts.end();
                assertEquals(10, count);
            }
        }
    }

    public void testDelegatesPerFieldAnalysis() throws IOException {
        // Verify the wrapper properly delegates to the underlying analyzer
        try (StandardAnalyzer delegate = new StandardAnalyzer()) {
            TokenCountingAnalyzer analyzer = new TokenCountingAnalyzer(delegate, 1000);
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
