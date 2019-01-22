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
import java.util.function.Function;
import java.util.stream.IntStream;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.stream.Collectors.toList;
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

    private void testCase(String text,
                                 Function<Integer, SearchAsYouTypeAnalyzer> analyzerFunction,
                                 Function<Integer, List<String>> expectedTokensFunction) throws IOException {

        for (int shingleSize = 2; shingleSize <= 4; shingleSize++) {
            final SearchAsYouTypeAnalyzer analyzer = analyzerFunction.apply(shingleSize);
            final List<String> expectedTokens = expectedTokensFunction.apply(shingleSize);
            final List<String> actualTokens = analyze(analyzer, text);
            assertThat("analyzed correctly with " + analyzer, actualTokens, equalTo(expectedTokens));
        }
    }

    public void testSingleTermShingles() throws IOException {
        testCase(
            "quick",
            shingleSize -> SearchAsYouTypeAnalyzer.withShingle(SIMPLE, shingleSize),
            shingleSize -> emptyList()
        );
    }

    public void testMultiTermShingles() throws IOException {
        testCase(
            "quick red fox lazy brown",
            shingleSize -> SearchAsYouTypeAnalyzer.withShingle(SIMPLE, shingleSize),
            shingleSize -> {
                if (shingleSize == 2) {
                    return asList("quick red", "red fox", "fox lazy", "lazy brown");
                } else if (shingleSize == 3) {
                    return asList("quick red fox", "red fox lazy", "fox lazy brown");
                } else if (shingleSize == 4) {
                    return asList("quick red fox lazy", "red fox lazy brown");
                }
                throw new IllegalArgumentException();
            }
        );
    }

    public void testSingleTermPrefix() throws IOException {
        testCase(
            "quick",
            shingleSize -> SearchAsYouTypeAnalyzer.withShingleAndPrefix(SIMPLE, shingleSize),
            shingleSize -> {
                final List<String> tokens = new ArrayList<>(asList("q", "qu", "qui", "quic", "quick"));
                tokens.addAll(tokenWithSpaces("quick", shingleSize));
                return tokens;
            }
        );
    }

    public void testMultiTermPrefix() throws IOException {
        testCase(
            "quick red fox lazy brown",
            shingleSize -> SearchAsYouTypeAnalyzer.withShingleAndPrefix(SIMPLE, shingleSize),
            shingleSize -> {
                if (shingleSize == 2) {
                    final List<String> tokens = new ArrayList<>(asList(
                        "q", "qu", "qui", "quic", "quick", "quick ", "quick r", "quick re", "quick red",
                        "r", "re", "red", "red ", "red f", "red fo", "red fox",
                        "f", "fo", "fox", "fox ", "fox l", "fox la", "fox laz", "fox lazy",
                        "l", "la", "laz", "lazy", "lazy ", "lazy b", "lazy br", "lazy bro", "lazy brow", "lazy brown"
                    ));
                    tokens.addAll(asList("b", "br", "bro", "brow", "brown"));
                    tokens.addAll(tokenWithSpaces("brown", shingleSize));
                    return tokens;
                } else if (shingleSize == 3) {
                    final List<String> tokens = new ArrayList<>(asList(
                        "q", "qu", "qui", "quic", "quick", "quick ", "quick r", "quick re", "quick red", "quick red ", "quick red f",
                        "quick red fo", "quick red fox",
                        "r", "re", "red", "red ", "red f", "red fo", "red fox", "red fox ", "red fox l", "red fox la", "red fox laz",
                        "red fox lazy",
                        "f", "fo", "fox", "fox ", "fox l", "fox la", "fox laz", "fox lazy", "fox lazy ", "fox lazy b", "fox lazy br",
                        "fox lazy bro", "fox lazy brow", "fox lazy brown"
                    ));
                    tokens.addAll(asList("l", "la", "laz", "lazy", "lazy ", "lazy b", "lazy br", "lazy bro", "lazy brow", "lazy brown"));
                    tokens.addAll(tokenWithSpaces("lazy brown", shingleSize - 1));
                    tokens.addAll(asList("b", "br", "bro", "brow", "brown"));
                    tokens.addAll(tokenWithSpaces("brown", shingleSize));
                    return tokens;
                } else if (shingleSize == 4) {
                    final List<String> tokens = new ArrayList<>(asList(
                        "q", "qu", "qui", "quic", "quick", "quick ", "quick r", "quick re", "quick red", "quick red ", "quick red f",
                        "quick red fo", "quick red fox", "quick red fox ", "quick red fox l", "quick red fox la", "quick red fox laz",
                        "quick red fox lazy",
                        "r", "re", "red", "red ", "red f", "red fo", "red fox", "red fox ", "red fox l", "red fox la", "red fox laz",
                        "red fox lazy", "red fox lazy ", "red fox lazy b", "red fox lazy br", "red fox lazy bro", "red fox lazy brow",
                        "red fox lazy brown"

                    ));
                    tokens.addAll(asList(
                        "f", "fo", "fox", "fox ", "fox l", "fox la", "fox laz", "fox lazy", "fox lazy ", "fox lazy b", "fox lazy br",
                        "fox lazy bro", "fox lazy brow", "fox lazy brown"
                    ));
                    tokens.addAll(tokenWithSpaces("fox lazy brown", shingleSize - 2));
                    tokens.addAll(asList("l", "la", "laz", "lazy", "lazy ", "lazy b", "lazy br", "lazy bro", "lazy brow", "lazy brown"));
                    tokens.addAll(tokenWithSpaces("lazy brown", shingleSize - 1));
                    tokens.addAll(asList("b", "br", "bro", "brow", "brown"));
                    tokens.addAll(tokenWithSpaces("brown", shingleSize));
                    return tokens;
                }

                throw new IllegalArgumentException();
            }
        );
    }

    private static List<String> tokenWithSpaces(String text, int maxShingleSize) {
        return IntStream.range(1, maxShingleSize).mapToObj(i -> text + spaces(i)).collect(toList());
    }

    private static String spaces(int count) {
        final StringBuilder builder = new StringBuilder();
        for (int i = 0; i < count; i++) {
            builder.append(" ");
        }
        return builder.toString();
    }
}
