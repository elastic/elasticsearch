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
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.hamcrest.Matchers.containsString;

public class TokenCountingAnalyzerTests extends ESTestCase {

    public void testTokenCountingWithinOrAtLimit() throws IOException {
        int limit = randomIntBetween(1, 100);
        int tokenCount = randomIntBetween(1, limit*2);
        String fieldName = "foo";
        String input = generateWords(tokenCount);

        try (StandardAnalyzer delegate = new StandardAnalyzer()) {
            TokenCountingAnalyzer analyzer = new TokenCountingAnalyzer(delegate, limit);
            try (TokenStream ts = analyzer.tokenStream(fieldName, input)) {
                ts.reset();
                if( tokenCount > limit ) {
                    TokenCountingAnalyzer.FieldTokenCountExceededException ex = expectThrows(
                        TokenCountingAnalyzer.FieldTokenCountExceededException.class,
                        () -> {
                            while (ts.incrementToken()) {
                                // consume tokens
                            }
                        }
                    );
                    assertThat(ex.getMessage(), containsString("[" + limit + "]"));
                    assertThat(ex.getMessage(), containsString("[" + fieldName + "]"));
                    assertThat(ex.getMessage(), containsString("index.mapping.tokens_per_field.limit"));
                    assertEquals(limit, ex.getMaxTokenCount());
                    assertEquals(fieldName, ex.getFieldName());
                } else {
                    int count = 0;
                    while (ts.incrementToken()) {
                        count++;
                    }
                    ts.end();
                    assertEquals(tokenCount, count);
                }
            }
        }
    }

    public void testNoLimitWhenDisabled() throws IOException {
        int tokenCount = 50;
        String input = generateWords(tokenCount);

        try (StandardAnalyzer delegate = new StandardAnalyzer()) {
            TokenCountingAnalyzer analyzer = new TokenCountingAnalyzer(delegate, -1);
            try (TokenStream ts = analyzer.tokenStream("field", input)) {
                ts.reset();
                int count = 0;
                while (ts.incrementToken()) {
                    count++;
                }
                ts.end();
                assertEquals(tokenCount, count);
            }
        }
    }

    public void testCounterResetsOnReuse() throws IOException {
        // With PER_FIELD_REUSE_STRATEGY, the same field name reuses the same TokenStreamComponents
        // (and thus the same filter instance). Verify that reset() properly zeroes the counter.
        int limit = randomIntBetween(3, 20);
        String input = generateWords(limit);

        try (StandardAnalyzer delegate = new StandardAnalyzer()) {
            TokenCountingAnalyzer analyzer = new TokenCountingAnalyzer(delegate, limit);

            // First use: exactly at the limit
            try (TokenStream ts = analyzer.tokenStream("field", input)) {
                ts.reset();
                while (ts.incrementToken()) {
                    // consume
                }
                ts.end();
            }

            // Second use of same field name: should succeed because reset() zeroes counter
            try (TokenStream ts = analyzer.tokenStream("field", input)) {
                ts.reset();
                while (ts.incrementToken()) {
                    // consume
                }
                ts.end();
            }
        }
    }

    private static String generateWords(int count) {
        return IntStream.range(0, count).mapToObj(i -> "word" + i).collect(Collectors.joining(" "));
    }
}
