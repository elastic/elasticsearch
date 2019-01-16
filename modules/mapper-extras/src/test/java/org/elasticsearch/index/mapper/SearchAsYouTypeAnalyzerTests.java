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

package org.elasticsearch.index.mapper;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.core.SimpleAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.elasticsearch.index.mapper.SearchAsYouTypeFieldMapper.SearchAsYouTypeAnalyzer;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static java.util.Arrays.asList;
import static org.hamcrest.Matchers.equalTo;

public class SearchAsYouTypeAnalyzerTests extends ESTestCase {

    private static final Analyzer SIMPLE = new SimpleAnalyzer();

    public static List<String> analyze(SearchAsYouTypeAnalyzer analyzer, String text) throws IOException  {
        final List<String> tokens = new ArrayList<>();
        try (TokenStream tokenStream = analyzer.tokenStream("field", text)) {
            final CharTermAttribute charTermAttribute = tokenStream.addAttribute(CharTermAttribute.class);
            tokenStream.reset();
            while (tokenStream.incrementToken()) {
                tokens.add(charTermAttribute.toString());
            }
        }
        return tokens;
    }

    public void testSingleTermNGrams() throws IOException {
        for (int shingleSize = 2; shingleSize <= 4; shingleSize++) {
            final List<String> tokens = analyze(SearchAsYouTypeAnalyzer.withShingleAndPrefix(SIMPLE, shingleSize), "quick");
            final List<String> expectedTokens = new ArrayList<>(asList("q", "qu", "qui", "quic", "quick"));
            for (int i = 1; i < shingleSize; i++) {
                expectedTokens.add("quick" + spaces(i));
            }
            assertThat("analyzed correctly with [" + shingleSize + "] shingles and ngrams", tokens, equalTo(expectedTokens));
            logger.error("TOKENS " + tokens);
        }
    }

    private static String spaces(int count) {
        final StringBuilder builder = new StringBuilder();
        for (int i = 0; i < count; i++) {
            builder.append(" ");
        }
        return builder.toString();
    }
}
