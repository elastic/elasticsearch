/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.nlp.tokenizers;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.tests.analysis.BaseTokenStreamTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

public class BpeTokenizerTests extends BaseTokenStreamTestCase {
    public static final List<String> TEST_CASED_VOCAB = List.of(
        "<s>",
        "<pad>",
        "</s>",
        "<unk>",
        "<mask>",
        "!",
        "\"",
        "#",
        "$",
        "%",
        "&",
        "'",
        "(",
        ")",
        "*",
        "+",
        ",",
        "-",
        ".",
        "/",
        "0",
        "1",
        "2",
        "3",
        "4",
        "5",
        "6",
        "7",
        "8",
        "9",
        ":",
        ";",
        "<",
        "=",
        ">",
        "?",
        "@",
        "A",
        "B",
        "C",
        "D",
        "E",
        "F",
        "G",
        "H",
        "I",
        "J",
        "K",
        "L",
        "M",
        "N",
        "O",
        "P",
        "Q",
        "R",
        "S",
        "T",
        "U",
        "V",
        "W",
        "X",
        "Y",
        "Z",
        "[",
        "\\",
        "]",
        "^",
        "_",
        "`",
        "a",
        "b",
        "c",
        "d",
        "e",
        "f",
        "g",
        "h",
        "i",
        "j",
        "k",
        "l",
        "m",
        "n",
        "o",
        "p",
        "q",
        "r",
        "s",
        "t",
        "u",
        "v",
        "w",
        "x",
        "y",
        "z",
        "{",
        "|",
        "}",
        "~",
        "¡",
        "¢",
        "£",
        "¤",
        "¥",
        "¦",
        "§",
        "¨",
        "©",
        "ª",
        "«",
        "¬",
        "®",
        "¯",
        "°",
        "±",
        "²",
        "³",
        "´",
        "µ",
        "¶",
        "·",
        "¸",
        "¹",
        "º",
        "»",
        "¼",
        "½",
        "¾",
        "¿",
        "À",
        "Á",
        "Â",
        "Ã",
        "Ä",
        "Å",
        "Æ",
        "Ç",
        "È",
        "É",
        "Ê",
        "Ë",
        "Ì",
        "Í",
        "Î",
        "Ï",
        "Ð",
        "Ñ",
        "Ò",
        "Ó",
        "Ô",
        "Õ",
        "Ö",
        "×",
        "Ø",
        "Ù",
        "Ú",
        "Û",
        "Ü",
        "Ý",
        "Þ",
        "ß",
        "à",
        "á",
        "â",
        "ã",
        "ä",
        "å",
        "æ",
        "ç",
        "è",
        "é",
        "ê",
        "ë",
        "ì",
        "í",
        "î",
        "ï",
        "ð",
        "ñ",
        "ò",
        "ó",
        "ô",
        "õ",
        "ö",
        "÷",
        "ø",
        "ù",
        "ú",
        "û",
        "ü",
        "ý",
        "þ",
        "ÿ",
        "Ā",
        "ā",
        "Ă",
        "ă",
        "Ą",
        "ą",
        "Ć",
        "ć",
        "Ĉ",
        "ĉ",
        "Ċ",
        "ċ",
        "Č",
        "č",
        "Ď",
        "ď",
        "Đ",
        "đ",
        "Ē",
        "ē",
        "Ĕ",
        "ĕ",
        "Ė",
        "ė",
        "Ę",
        "ę",
        "Ě",
        "ě",
        "Ĝ",
        "ĝ",
        "Ğ",
        "ğ",
        "Ġ",
        "ġ",
        "Ģ",
        "ģ",
        "Ĥ",
        "ĥ",
        "Ħ",
        "ħ",
        "Ĩ",
        "ĩ",
        "Ī",
        "ī",
        "Ĭ",
        "ĭ",
        "Į",
        "į",
        "İ",
        "ı",
        "Ĳ",
        "ĳ",
        "Ĵ",
        "ĵ",
        "Ķ",
        "ķ",
        "ĸ",
        "Ĺ",
        "ĺ",
        "Ļ",
        "ļ",
        "Ľ",
        "ľ",
        "Ŀ",
        "ŀ",
        "Ł",
        "ł",
        "Ń",
        "ar",
        "it",
        "la",
        "Go",
        "an",
        "dz",
        "fu",
        "il",
        "wit",
        "ĠGo",
        "Ġfu",
        "Ġwit",
        "dzil",
        "ĠGodzil",
        "Ġfun",
        "Ġwith",
        "ĠGodzilla",
        "Ela",
        "ak",
        "ch",
        "cs",
        "car",
        "cak",
        "ed",
        "ear",
        "ics",
        "le",
        "lit",
        "red",
        "st",
        "tle",
        "Ġa",
        "Ġcar",
        "Ġlit",
        "Ġred",
        "ancak",
        "Elast",
        "earch",
        "icsearch",
        "Ġlittle",
        "ancake",
        "Elasticsearch"
    );
    public static final List<String> TEST_CASE_MERGE = List.of(
        "a r",
        "i t",
        "l a",
        "G o",
        "a n",
        "d z",
        "f u",
        "i l",
        "w it",
        "Ġ Go",
        "Ġ fu",
        "Ġ wit",
        "dz il",
        "ĠGo dzil",
        "Ġfu n",
        "Ġwit h",
        "ĠGodzil la",
        "E la",
        "a k",
        "c h",
        "c s",
        "c ar",
        "c ak",
        "e d",
        "e ar",
        "i cs",
        "l e",
        "l it",
        "r ed",
        "s t",
        "t le",
        "Ġ a",
        "Ġ car",
        "Ġ lit",
        "Ġ red",
        "an cak",
        "Ela st",
        "ear ch",
        "ics earch",
        "Ġlit tle",
        "ancak e"
    );

    public void testNeverSplit() throws IOException {
        Analyzer analyzer = analyzerFromSettings(List.of("<mask>"), false);
        assertAnalyzesToNoCharFilter(analyzer, "Elasticsearch <<mask>.", new String[] { "Elast", "icsearch", "Ġ", "<", "<mask>", "." });
        assertAnalyzesToNoCharFilter(analyzer, "Elasticsearch < red", new String[] { "Elast", "icsearch", "Ġ", "<", "Ġred" });
        assertAnalyzesToNoCharFilter(analyzer, "Elasticsearch <mask>.", new String[] { "Elast", "icsearch", "<mask>", "." });
        assertAnalyzesToNoCharFilter(
            analyzer,
            "Elasticsearch<mask><mask>~redElasticsearch",
            new String[] { "Elast", "icsearch", "<mask>", "<mask>", "~", "red", "Elast", "icsearch" }
        );
        assertAnalyzesToNoCharFilter(
            analyzer,
            "Elasticsearch red~<mask>.",
            new String[] { "Elast", "icsearch", "Ġred", "~", "<mask>", "." }
        );
        assertAnalyzesToNoCharFilter(
            analyzer,
            "Elasticsearch red~<mask><mask>.",
            new String[] { "Elast", "icsearch", "Ġred", "~", "<mask>", "<mask>", "." }
        );
        assertAnalyzesToNoCharFilter(analyzer, "Elasticsearch <mask>!!", new String[] { "Elast", "icsearch", "<mask>", "!", "!" });
    }

    public void testTokenization() throws IOException {
        try (BpeAnalyzer analyzer = analyzerFromSettings(List.of("<mask>"), false)) {
            String inputText = "Elasticsearch Godzilla car";
            List<Integer> tokenPositionMap = new ArrayList<>();
            try (TokenStream ts = analyzer.tokenStream("input", inputText)) {
                ts.reset();
                PositionIncrementAttribute tokenPos = ts.addAttribute(PositionIncrementAttribute.class);
                int currPos = -1;
                while (ts.incrementToken()) {
                    currPos += tokenPos.getPositionIncrement();
                    tokenPositionMap.add(currPos);
                }
            }
            List<BpeTokenizer.BpeToken> tokens = analyzer.getTokens();
            List<Integer> tokenIds = tokens.stream().map(BpeTokenizer.BpeToken::getEncoding).collect(Collectors.toList());
            List<String> tokenValues = tokens.stream().map(BpeTokenizer.BpeToken::toString).collect(Collectors.toList());
            assertThat(tokenIds, equalTo(List.of(297, 299, 277, 293)));
            assertThat(tokenPositionMap, equalTo(List.of(0, 0, 1, 2)));
            assertThat(tokenValues, equalTo(List.of("Elast", "icsearch", "ĠGodzilla", "Ġcar")));

            // Verify offsets handle whitespace and partitioned words
            assertThat(inputText.substring(tokens.get(0).startOffset(), tokens.get(0).endOffset()), equalTo("Elasticsearch"));
            assertThat(inputText.substring(tokens.get(1).startOffset(), tokens.get(1).endOffset()), equalTo("Elasticsearch"));

            assertThat(inputText.substring(tokens.get(2).startOffset(), tokens.get(2).endOffset()), equalTo("Godzilla"));
            assertThat(inputText.substring(tokens.get(3).startOffset(), tokens.get(3).endOffset()), equalTo("car"));
        }
    }

    public void testTokenizationOffsetsWithPrefixWhitespace() throws IOException {
        try (BpeAnalyzer analyzer = analyzerFromSettings(List.of("<mask>"), true)) {
            String inputText = "Godzilla car";
            try (TokenStream ts = analyzer.tokenStream("input", inputText)) {
                ts.reset();
                while (ts.incrementToken()) {
                }
            }
            List<BpeTokenizer.BpeToken> tokens = analyzer.getTokens();
            assertThat(inputText.substring(tokens.get(0).startOffset(), tokens.get(0).endOffset()), equalTo("Godzilla"));
            assertThat(inputText.substring(tokens.get(1).startOffset(), tokens.get(1).endOffset()), equalTo("car"));
        }
    }

    public void testTokenizationEmpty() throws IOException {
        try (BpeAnalyzer analyzer = analyzerFromSettings(List.of("<mask>"), random().nextBoolean())) {
            List<Integer> tokenPositionMap = new ArrayList<>();
            try (TokenStream ts = analyzer.tokenStream("input", "")) {
                ts.reset();
                PositionIncrementAttribute tokenPos = ts.addAttribute(PositionIncrementAttribute.class);
                int currPos = -1;
                while (ts.incrementToken()) {
                    currPos += tokenPos.getPositionIncrement();
                    tokenPositionMap.add(currPos);
                }
            }
            assertThat(tokenPositionMap, hasSize(0));
            assertThat(analyzer.getTokens(), hasSize(0));
        }
    }

    public static BpeAnalyzer analyzerFromSettings(List<String> neverSplit, boolean prefixSpace) {
        return new BpeAnalyzer(TEST_CASED_VOCAB, TEST_CASE_MERGE, neverSplit, prefixSpace, "<unk>");
    }

    private static void assertAnalyzesToNoCharFilter(Analyzer a, String input, String[] output) throws IOException {
        assertTokenStreamContents(a.tokenStream("dummy", input), output, null, null, null, null, null, input.length());
        checkResetException(a, input);
        // We don't allow the random char filter because our offsets aren't corrected appropriately due to "never_split"
        // If we could figure out a way to pass "never_split" through whichever passed char_filter there was, then it would work
        checkAnalysisConsistency(random(), a, false, input);
    }

}
