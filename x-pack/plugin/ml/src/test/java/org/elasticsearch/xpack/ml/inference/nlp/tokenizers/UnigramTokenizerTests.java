/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.nlp.tokenizers;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.tests.analysis.BaseTokenStreamTestCase;
import org.apache.lucene.util.BytesRef;

import java.io.IOException;
import java.io.Reader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.elasticsearch.xpack.ml.inference.nlp.tokenizers.UnigramTokenizer.PREFIX;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;

public class UnigramTokenizerTests extends BaseTokenStreamTestCase {
    private static final String UNKNOWN_TOKEN = "<unk>";
    private static final List<String> NEVER_SPLIT = List.of("<mask>");

    public void testSimpleTokenization() throws IOException {
        TestNLPAnalyzer analyzer = new TestNLPAnalyzer(
            List.of(UNKNOWN_TOKEN, PREFIX + "a", "b", "c", "d", "cd", PREFIX + "ab", PREFIX + "abc", PREFIX + "abcd", "<mask>"),
            List.of(0.0, 0.0, 0.0, 0.0, 0.0, 1.0, 2.0, 5.0, 10.0, 0.0),
            UNKNOWN_TOKEN,
            new PrecompiledCharMapNormalizer.Config(new int[0], "")
        );

        assertAnalyzesToNoCharFilter(analyzer, "", new String[0]);
        assertAnalyzesToNoCharFilter(analyzer, "abcd", new String[] { PREFIX + "abcd" });
    }

    public void testLessSimpleTokenization() throws IOException {
        TestNLPAnalyzer analyzer = new TestNLPAnalyzer(
            List.of(
                UNKNOWN_TOKEN,
                PREFIX + "ab",
                "cd",
                PREFIX + "abc",
                "a",
                "b",
                "c",
                "ABC",
                "abcdabcd",
                "q",
                "r",
                "qr",
                "<mask>",
                "aa",
                "aaaa"
            ),
            List.of(0.0, 0.0, -0.1, -0.2, -0.3, -0.4, -0.5, -0.5, 20.0, 20.5, 20.5, -0.5, 0.0, -13.5467, -14.9644),
            UNKNOWN_TOKEN,
            new PrecompiledCharMapNormalizer.Config(new int[0], "")
        );

        assertAnalyzesToNoCharFilter(analyzer, "", new String[0]);
        assertAnalyzesToNoCharFilter(analyzer, "abcd", new String[] { PREFIX + "ab", "cd" });
        assertAnalyzesToNoCharFilter(analyzer, "abc", new String[] { PREFIX + "abc" });
        assertAnalyzesToNoCharFilter(analyzer, "AB", new String[] { PREFIX + "AB" });
        assertAnalyzesToNoCharFilter(analyzer, "abcc", new String[] { PREFIX + "abc", "c" });
        assertAnalyzesToNoCharFilter(analyzer, "  \nabcd \n\n   abcc   \n", new String[] { PREFIX + "ab", "cd", PREFIX + "abc", "c" });
    }

    public void testLessSimpleTokenizationForRepeatingCharacters() throws IOException {
        TestNLPAnalyzer analyzer = new TestNLPAnalyzer(
            List.of(UNKNOWN_TOKEN, "HH", "HHHH", PREFIX + "H", "HHH", PREFIX + "HH", PREFIX, PREFIX + "HHH"),
            List.of(0.0, -13.5467, -14.9644, -9.17478, -15.1165, -13.201, -7.97025, -15.602),
            UNKNOWN_TOKEN,
            PrecompiledCharMapNormalizer.fromBase64EncodedResource(
                "/org/elasticsearch/xpack/ml/inference.nlp.tokenizers/spm_precompiled_normalizer.txt"
            )
        );

        assertAnalyzesToNoCharFilter(analyzer, "HHHHHHHHHHHH", new String[] { PREFIX, "HHHH", "HHHH", "HHHH" });
        assertAnalyzesToNoCharFilter(analyzer, "HHHHHHHHHHH", new String[] { PREFIX + "HHH", "HHHH", "HHHH" });
        assertAnalyzesToNoCharFilter(analyzer, "HHHHHHHHHH", new String[] { PREFIX + "HH", "HHHH", "HHHH" });
        assertAnalyzesToNoCharFilter(analyzer, "HHHHHHHHH", new String[] { PREFIX + "H", "HHHH", "HHHH" });
        assertAnalyzesToNoCharFilter(analyzer, "HHHHHHHH", new String[] { PREFIX, "HHHH", "HHHH" });
        assertAnalyzesToNoCharFilter(analyzer, "HHHHHHH", new String[] { PREFIX + "HHH", "HHHH" });
        assertAnalyzesToNoCharFilter(analyzer, "HHHHHH", new String[] { PREFIX + "HH", "HHHH" });
        assertAnalyzesToNoCharFilter(analyzer, "HHHHH", new String[] { PREFIX + "H", "HHHH" });
        assertAnalyzesToNoCharFilter(analyzer, "HHHH", new String[] { PREFIX, "HHHH" });
        assertAnalyzesToNoCharFilter(analyzer, "HHH", new String[] { PREFIX + "HHH" });
        assertAnalyzesToNoCharFilter(analyzer, "HH", new String[] { PREFIX + "HH" });
        assertAnalyzesToNoCharFilter(analyzer, "H", new String[] { PREFIX + "H" });

    }

    public void testLessSimpleTokenizationWithNeverSplit() throws IOException {
        TestNLPAnalyzer analyzer = new TestNLPAnalyzer(
            List.of(
                UNKNOWN_TOKEN,
                PREFIX + "ab",
                "cd",
                PREFIX + "cd",
                PREFIX + "abc",
                "a",
                "b",
                "c",
                "ABC",
                "abcdabcd",
                "q",
                "r",
                "qr",
                "<mask>"
            ),
            List.of(0.0, 0.0, -0.1, -0.2, -0.2, -0.3, -0.4, -0.5, -0.5, 20.0, 20.5, 20.5, -0.5, 0.0),
            UNKNOWN_TOKEN,
            new PrecompiledCharMapNormalizer.Config(new int[0], "")
        );

        assertAnalyzesToNoCharFilter(analyzer, "<mask>", new String[] { "<mask>" });
        assertAnalyzesToNoCharFilter(analyzer, "<mask>abcd<mask>", new String[] { "<mask>", PREFIX + "ab", "cd", "<mask>" });
        assertAnalyzesToNoCharFilter(
            analyzer,
            "<mask>  \nab<mask>cd \n\n   abcc<mask>  \n",
            new String[] { "<mask>", PREFIX + "ab", "<mask>", PREFIX + "cd", PREFIX + "abc", "c", "<mask>" }
        );
    }

    public void testTriePrefixMatch() {
        List<BytesRef> inputs = new ArrayList<>(
            List.of(
                new BytesRef("a"),
                new BytesRef("b"),
                new BytesRef("c"),
                new BytesRef("d"),
                new BytesRef("cd"),
                new BytesRef("ab"),
                new BytesRef("abc"),
                new BytesRef("abcd")
            )
        );
        Collections.shuffle(inputs, random());
        UnigramTokenizer.BytesTrie bytesTrie = UnigramTokenizer.BytesTrie.build(inputs);
        String input = "abcd";
        assertThat(
            bytesTrie.matchingPrefixes(new BytesRef(input)).stream().map(BytesRef::utf8ToString).toList(),
            contains("a", "ab", "abc", "abcd")
        );
        input = "bcd";
        assertThat(bytesTrie.matchingPrefixes(new BytesRef(input)).stream().map(BytesRef::utf8ToString).toList(), contains("b"));
        input = "cd";
        assertThat(bytesTrie.matchingPrefixes(new BytesRef(input)).stream().map(BytesRef::utf8ToString).toList(), contains("c", "cd"));
        input = "d";
        assertThat(bytesTrie.matchingPrefixes(new BytesRef(input)).stream().map(BytesRef::utf8ToString).toList(), contains("d"));
        input = "";
        assertThat(bytesTrie.matchingPrefixes(new BytesRef(input)).stream().map(BytesRef::utf8ToString).toList(), empty());
        input = "zabcd";
        assertThat(bytesTrie.matchingPrefixes(new BytesRef(input)).stream().map(BytesRef::utf8ToString).toList(), empty());
        input = "azbcd";
        assertThat(bytesTrie.matchingPrefixes(new BytesRef(input)).stream().map(BytesRef::utf8ToString).toList(), contains("a"));
        input = "abzcd";
        assertThat(bytesTrie.matchingPrefixes(new BytesRef(input)).stream().map(BytesRef::utf8ToString).toList(), contains("a", "ab"));
        input = "abcdz";
        assertThat(
            bytesTrie.matchingPrefixes(new BytesRef(input)).stream().map(BytesRef::utf8ToString).toList(),
            contains("a", "ab", "abc", "abcd")
        );
    }

    private static class TestNLPAnalyzer extends Analyzer {
        private final List<String> dictionary;
        private final double[] scores;
        private final String unknownToken;
        private final PrecompiledCharMapNormalizer.Config normalizer;

        TestNLPAnalyzer(List<String> dictionary, List<Double> scores, String unknownToken, PrecompiledCharMapNormalizer.Config normalizer) {
            this.dictionary = dictionary;
            this.scores = new double[scores.size()];
            int i = 0;
            for (Double s : scores) {
                this.scores[i++] = s;
            }
            this.unknownToken = unknownToken;
            this.normalizer = normalizer;
        }

        @Override
        protected Reader initReader(String fieldName, Reader reader) {
            if (normalizer.offsets().length > 0) {
                return new PrecompiledCharMapNormalizer(normalizer.offsets(), normalizer.utf8str(), reader);
            }
            return reader;
        }

        @Override
        protected TokenStreamComponents createComponents(String fieldName) {
            UnigramTokenizer tokenizer = UnigramTokenizer.build(NEVER_SPLIT, dictionary, scores, unknownToken, false);
            return new TokenStreamComponents(tokenizer);
        }
    }

    private static void assertAnalyzesToNoCharFilter(Analyzer a, String input, String[] output) throws IOException {
        assertTokenStreamContents(a.tokenStream("dummy", input), output, null, null, null, null, null, input.length());
        checkResetException(a, input);
        // We don't allow the random char filter because our offsets aren't corrected appropriately due to "never_split"
        // If we could figure out a way to pass "never_split" through whichever passed char_filter there was, then it would work
        checkAnalysisConsistency(random(), a, false, input);
    }

}
