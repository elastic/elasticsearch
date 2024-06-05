/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper.extras;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.core.SimpleAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.elasticsearch.index.mapper.extras.SearchAsYouTypeFieldMapper.SearchAsYouTypeAnalyzer;
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

    public static List<String> analyze(SearchAsYouTypeAnalyzer analyzer, String text) throws IOException {
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

    private void testCase(
        String text,
        Function<Integer, SearchAsYouTypeAnalyzer> analyzerFunction,
        Function<Integer, List<String>> expectedTokensFunction
    ) throws IOException {

        for (int shingleSize = 2; shingleSize <= 4; shingleSize++) {
            final SearchAsYouTypeAnalyzer analyzer = analyzerFunction.apply(shingleSize);
            final List<String> expectedTokens = expectedTokensFunction.apply(shingleSize);
            final List<String> actualTokens = analyze(analyzer, text);
            assertThat("analyzed correctly with " + analyzer, actualTokens, equalTo(expectedTokens));
        }
    }

    public void testSingleTermShingles() throws IOException {
        testCase("quick", shingleSize -> SearchAsYouTypeAnalyzer.withShingle(SIMPLE, shingleSize), shingleSize -> emptyList());
    }

    public void testMultiTermShingles() throws IOException {
        testCase("quick brown fox jump lazy", shingleSize -> SearchAsYouTypeAnalyzer.withShingle(SIMPLE, shingleSize), shingleSize -> {
            if (shingleSize == 2) {
                return asList("quick brown", "brown fox", "fox jump", "jump lazy");
            } else if (shingleSize == 3) {
                return asList("quick brown fox", "brown fox jump", "fox jump lazy");
            } else if (shingleSize == 4) {
                return asList("quick brown fox jump", "brown fox jump lazy");
            }
            throw new IllegalArgumentException();
        });
    }

    public void testSingleTermPrefix() throws IOException {
        testCase("quick", shingleSize -> SearchAsYouTypeAnalyzer.withShingleAndPrefix(SIMPLE, shingleSize), shingleSize -> {
            final List<String> tokens = new ArrayList<>(asList("q", "qu", "qui", "quic", "quick"));
            tokens.addAll(tokenWithSpaces("quick", shingleSize));
            return tokens;
        });
    }

    public void testMultiTermPrefix() throws IOException {
        testCase(
            // "quick red fox lazy brown",
            "quick brown fox jump lazy",
            shingleSize -> SearchAsYouTypeAnalyzer.withShingleAndPrefix(SIMPLE, shingleSize),
            shingleSize -> {
                if (shingleSize == 2) {
                    final List<String> tokens = new ArrayList<>();
                    tokens.addAll(
                        asList("q", "qu", "qui", "quic", "quick", "quick ", "quick b", "quick br", "quick bro", "quick brow", "quick brown")
                    );
                    tokens.addAll(asList("b", "br", "bro", "brow", "brown", "brown ", "brown f", "brown fo", "brown fox"));
                    tokens.addAll(asList("f", "fo", "fox", "fox ", "fox j", "fox ju", "fox jum", "fox jump"));
                    tokens.addAll(asList("j", "ju", "jum", "jump", "jump ", "jump l", "jump la", "jump laz", "jump lazy"));
                    tokens.addAll(asList("l", "la", "laz", "lazy"));
                    tokens.addAll(tokenWithSpaces("lazy", shingleSize));
                    return tokens;
                } else if (shingleSize == 3) {
                    final List<String> tokens = new ArrayList<>();
                    tokens.addAll(
                        asList(
                            "q",
                            "qu",
                            "qui",
                            "quic",
                            "quick",
                            "quick ",
                            "quick b",
                            "quick br",
                            "quick bro",
                            "quick brow",
                            "quick brown",
                            "quick brown ",
                            "quick brown f",
                            "quick brown fo",
                            "quick brown fox"
                        )
                    );
                    tokens.addAll(
                        asList(
                            "b",
                            "br",
                            "bro",
                            "brow",
                            "brown",
                            "brown ",
                            "brown f",
                            "brown fo",
                            "brown fox",
                            "brown fox ",
                            "brown fox j",
                            "brown fox ju",
                            "brown fox jum",
                            "brown fox jump"
                        )
                    );
                    tokens.addAll(
                        asList(
                            "f",
                            "fo",
                            "fox",
                            "fox ",
                            "fox j",
                            "fox ju",
                            "fox jum",
                            "fox jump",
                            "fox jump ",
                            "fox jump l",
                            "fox jump la",
                            "fox jump laz",
                            "fox jump lazy"
                        )
                    );
                    tokens.addAll(asList("j", "ju", "jum", "jump", "jump ", "jump l", "jump la", "jump laz", "jump lazy"));
                    tokens.addAll(tokenWithSpaces("jump lazy", shingleSize - 1));
                    tokens.addAll(asList("l", "la", "laz", "lazy"));
                    tokens.addAll(tokenWithSpaces("lazy", shingleSize));
                    return tokens;
                } else if (shingleSize == 4) {
                    final List<String> tokens = new ArrayList<>();
                    tokens.addAll(
                        asList(
                            "q",
                            "qu",
                            "qui",
                            "quic",
                            "quick",
                            "quick ",
                            "quick b",
                            "quick br",
                            "quick bro",
                            "quick brow",
                            "quick brown",
                            "quick brown ",
                            "quick brown f",
                            "quick brown fo",
                            "quick brown fox",
                            "quick brown fox ",
                            "quick brown fox j",
                            "quick brown fox ju",
                            "quick brown fox jum",
                            "quick brown fox jump"
                        )
                    );
                    tokens.addAll(
                        asList(
                            "b",
                            "br",
                            "bro",
                            "brow",
                            "brown",
                            "brown ",
                            "brown f",
                            "brown fo",
                            "brown fox",
                            "brown fox ",
                            "brown fox j",
                            "brown fox ju",
                            "brown fox jum",
                            "brown fox jump",
                            "brown fox jump ",
                            "brown fox jump l",
                            "brown fox jump la",
                            "brown fox jump laz",
                            "brown fox jump lazy"
                        )
                    );
                    tokens.addAll(
                        asList(
                            "f",
                            "fo",
                            "fox",
                            "fox ",
                            "fox j",
                            "fox ju",
                            "fox jum",
                            "fox jump",
                            "fox jump ",
                            "fox jump l",
                            "fox jump la",
                            "fox jump laz",
                            "fox jump lazy"
                        )
                    );
                    tokens.addAll(tokenWithSpaces("fox jump lazy", shingleSize - 2));
                    tokens.addAll(asList("j", "ju", "jum", "jump", "jump ", "jump l", "jump la", "jump laz", "jump lazy"));
                    tokens.addAll(tokenWithSpaces("jump lazy", shingleSize - 1));
                    tokens.addAll(asList("l", "la", "laz", "lazy"));
                    tokens.addAll(tokenWithSpaces("lazy", shingleSize));
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
