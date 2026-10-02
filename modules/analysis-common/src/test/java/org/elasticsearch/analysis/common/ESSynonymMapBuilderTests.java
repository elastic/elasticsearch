/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.analysis.common;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.core.WhitespaceTokenizer;
import org.apache.lucene.analysis.synonym.SynonymFilter;
import org.apache.lucene.analysis.synonym.SynonymMap;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.util.CharsRef;
import org.apache.lucene.util.CharsRefBuilder;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.test.ESTokenStreamTestCase;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

/**
 * Comparison tests that feed identical rules to both {@link ESSynonymMapBuilder} and Lucene's
 * {@link SynonymMap.Builder}, then assert identical output. Catches drift on Lucene upgrades.
 */
public class ESSynonymMapBuilderTests extends ESTokenStreamTestCase {

    private static final CircuitBreaker NOOP_CIRCUIT_BREAKER = new NoopCircuitBreaker("test");

    private record SynonymRule(CharsRef input, CharsRef output, boolean includeOrig) {}

    private record TokenInfo(String term, int positionIncrement) {}

    private static CharsRef word(String s) {
        return new CharsRef(s);
    }

    private static CharsRef phrase(String... words) {
        return SynonymMap.Builder.join(words, new CharsRefBuilder());
    }

    private SynonymMap buildWithLucene(boolean dedup, SynonymRule... rules) throws IOException {
        SynonymMap.Builder builder = new SynonymMap.Builder(dedup);
        for (SynonymRule rule : rules) {
            builder.add(rule.input(), rule.output(), rule.includeOrig());
        }
        return builder.build();
    }

    private SynonymMap buildWithES(boolean dedup, SynonymRule... rules) throws IOException {
        ESSynonymMapBuilder builder = new ESSynonymMapBuilder(dedup, NOOP_CIRCUIT_BREAKER);
        for (SynonymRule rule : rules) {
            builder.add(rule.input(), rule.output(), rule.includeOrig());
        }
        return builder.build();
    }

    private List<TokenInfo> collectTokens(SynonymMap map, String inputText) throws IOException {
        Tokenizer tokenizer = new WhitespaceTokenizer();
        tokenizer.setReader(new StringReader(inputText));
        try (TokenStream ts = new SynonymFilter(tokenizer, map, false)) {
            CharTermAttribute termAtt = ts.addAttribute(CharTermAttribute.class);
            PositionIncrementAttribute posIncAtt = ts.addAttribute(PositionIncrementAttribute.class);
            List<TokenInfo> tokens = new ArrayList<>();
            ts.reset();
            while (ts.incrementToken()) {
                tokens.add(new TokenInfo(termAtt.toString(), posIncAtt.getPositionIncrement()));
            }
            ts.end();
            return tokens;
        }
    }

    private void assertBuildersMatch(boolean dedup, SynonymRule[] rules, String... testInputs) throws IOException {
        SynonymMap luceneMap = buildWithLucene(dedup, rules);
        SynonymMap esMap = buildWithES(dedup, rules);

        assertEquals("maxHorizontalContext mismatch", luceneMap.maxHorizontalContext, esMap.maxHorizontalContext);

        for (String input : testInputs) {
            List<TokenInfo> luceneTokens = collectTokens(luceneMap, input);
            List<TokenInfo> esTokens = collectTokens(esMap, input);
            assertEquals("Token mismatch for input: [" + input + "]", luceneTokens, esTokens);
        }
    }

    // --- Deterministic edge cases that random tests can't reliably hit ---

    public void testBeyondBmpCharacters() throws IOException {
        String chicken = "🐔";
        assertBuildersMatch(true, new SynonymRule[] { new SynonymRule(word(chicken), word("chicken"), false) }, chicken, "chicken");
    }

    public void testMixedBmpAndBeyondBmpInSameToken() throws IOException {
        String mixed = "hello🐔world";
        assertBuildersMatch(true, new SynonymRule[] { new SynonymRule(word(mixed), word("poultry"), false) }, mixed, "poultry");
    }

    public void testBmpBoundaryCharacters() throws IOException {
        String lastBmp = "\uFFFD";
        String firstSupplementary = new String(Character.toChars(0x10000));
        assertBuildersMatch(
            true,
            new SynonymRule[] {
                new SynonymRule(word(lastBmp), word("replacement"), false),
                new SynonymRule(word(firstSupplementary), word("supplementary"), false) },
            lastBmp,
            firstSupplementary
        );
    }

    public void testUtf16VsUtf8SortOrder() throws IOException {
        // These sort differently under UTF-16 vs UTF-8; exercises the comparator
        String privateUse = "\uE000";
        String chicken = "🐔";
        assertBuildersMatch(
            true,
            new SynonymRule[] {
                new SynonymRule(word(privateUse), word("private"), false),
                new SynonymRule(word(chicken), word("chicken"), false) },
            privateUse,
            chicken
        );
    }

    public void testEmptyBuilder() throws IOException {
        SynonymMap luceneMap = buildWithLucene(true);
        SynonymMap esMap = buildWithES(true);

        assertEquals("maxHorizontalContext mismatch", luceneMap.maxHorizontalContext, esMap.maxHorizontalContext);
        assertEquals("fst null mismatch", luceneMap.fst == null, esMap.fst == null);
    }

    public void testMultipleOutputsSameInput() throws IOException {
        assertBuildersMatch(
            true,
            new SynonymRule[] { new SynonymRule(word("a"), word("b"), false), new SynonymRule(word("a"), word("c"), false) },
            "a",
            "b",
            "c"
        );
    }

    public void testMixedIncludeOrigForSameInput() throws IOException {
        assertBuildersMatch(
            true,
            new SynonymRule[] { new SynonymRule(word("fast"), word("quick"), false), new SynonymRule(word("fast"), word("speedy"), true) },
            "fast",
            "quick",
            "speedy"
        );
    }

    public void testOverlappingMultiwordPrefixes() throws IOException {
        assertBuildersMatch(
            true,
            new SynonymRule[] {
                new SynonymRule(phrase("a", "b"), word("ab"), false),
                new SynonymRule(phrase("a", "b", "c"), word("abc"), false) },
            "a b",
            "a b c",
            "a b c d"
        );
    }

    public void testAddValidationRejectsEmptyInput() {
        ESSynonymMapBuilder builder = new ESSynonymMapBuilder(true, NOOP_CIRCUIT_BREAKER);
        expectThrows(IllegalArgumentException.class, () -> builder.add(word(""), word("output"), false));
        expectThrows(IllegalArgumentException.class, () -> builder.add(word("input"), word(""), false));
    }

    public void testHasHolesAssertionFires() {
        assumeTrue("requires assertions enabled", ESSynonymMapBuilderTests.class.desiredAssertionStatus());
        ESSynonymMapBuilder builder = new ESSynonymMapBuilder(true, NOOP_CIRCUIT_BREAKER);
        final char S = SynonymMap.WORD_SEPARATOR;
        // consecutive WORD_SEPARATORs (hole in the middle)
        CharsRef consecutive = new CharsRef(new char[] { 'f', 'o', 'o', S, S, 'b', 'a', 'r' }, 0, 8);
        expectThrows(AssertionError.class, () -> builder.add(consecutive, word("output"), false));
        expectThrows(AssertionError.class, () -> builder.add(word("input"), consecutive, false));
        // leading WORD_SEPARATOR
        CharsRef leading = new CharsRef(new char[] { S, 'f', 'o', 'o' }, 0, 4);
        expectThrows(AssertionError.class, () -> builder.add(leading, word("output"), false));
        expectThrows(AssertionError.class, () -> builder.add(word("input"), leading, false));
        // trailing WORD_SEPARATOR
        CharsRef trailing = new CharsRef(new char[] { 'f', 'o', 'o', S }, 0, 4);
        expectThrows(AssertionError.class, () -> builder.add(trailing, word("output"), false));
        expectThrows(AssertionError.class, () -> builder.add(word("input"), trailing, false));
    }

    public void testSelfReferencingSynonyms() throws IOException {
        assertBuildersMatch(
            true,
            new SynonymRule[] {
                new SynonymRule(word("zoo"), word("zoo"), false),
                new SynonymRule(word("zoo"), phrase("zoo", "park"), true),
                new SynonymRule(phrase("zoo", "park"), word("zoo"), false) },
            "zoo",
            "zoo park",
            "park"
        );
    }

    public void testOutputLongerThanInput() throws IOException {
        assertBuildersMatch(true, new SynonymRule[] { new SynonymRule(word("a"), phrase("a", "b", "c"), false) }, "a", "a b c");
    }

    public void testGreedyLongestMatch() throws IOException {
        assertBuildersMatch(
            true,
            new SynonymRule[] {
                new SynonymRule(word("a"), word("x"), false),
                new SynonymRule(phrase("a", "b"), word("y"), false),
                new SynonymRule(phrase("a", "b", "c"), word("z"), false) },
            "a b c d",
            "a b d",
            "a d"
        );
    }

    public void testDedupRemovesDuplicateOutputs() throws IOException {
        assertBuildersMatch(
            true,
            new SynonymRule[] { new SynonymRule(word("fast"), word("quick"), false), new SynonymRule(word("fast"), word("quick"), false) },
            "fast"
        );
    }

    // --- Randomized tests ---

    public void testRandomMixedRules() throws IOException {
        int numRules = randomIntBetween(1, 50);
        boolean dedup = randomBoolean();
        SynonymRule[] rules = new SynonymRule[numRules];
        List<String> testInputs = new ArrayList<>();

        for (int i = 0; i < numRules; i++) {
            int inputWords = randomIntBetween(1, 4);
            String[] inputArr = new String[inputWords];
            for (int j = 0; j < inputWords; j++) {
                inputArr[j] = randomAlphaOfLengthBetween(1, 8);
            }

            String[] outputArr;
            if (rarely()) {
                outputArr = inputArr.clone();
            } else {
                int outputWords = randomIntBetween(1, 4);
                outputArr = new String[outputWords];
                for (int j = 0; j < outputWords; j++) {
                    outputArr[j] = randomAlphaOfLengthBetween(1, 8);
                }
            }

            CharsRef input = inputWords == 1 ? word(inputArr[0]) : phrase(inputArr);
            CharsRef output = outputArr.length == 1 ? word(outputArr[0]) : phrase(outputArr);
            rules[i] = new SynonymRule(input, output, randomBoolean());
            testInputs.add(String.join(" ", inputArr));
        }

        assertBuildersMatch(dedup, rules, testInputs.toArray(new String[0]));
    }

    public void testRandomBeyondBmpRules() throws IOException {
        int numRules = randomIntBetween(1, 20);
        boolean dedup = randomBoolean();
        SynonymRule[] rules = new SynonymRule[numRules];
        List<String> testInputs = new ArrayList<>();

        for (int i = 0; i < numRules; i++) {
            int inputWords = randomIntBetween(1, 4);
            String[] inputArr = new String[inputWords];
            for (int j = 0; j < inputWords; j++) {
                inputArr[j] = randomWordWithSupplementary();
            }

            int outputWords = randomIntBetween(1, 3);
            String[] outputArr = new String[outputWords];
            for (int j = 0; j < outputWords; j++) {
                outputArr[j] = randomWordWithSupplementary();
            }

            CharsRef input = inputWords == 1 ? word(inputArr[0]) : phrase(inputArr);
            CharsRef output = outputWords == 1 ? word(outputArr[0]) : phrase(outputArr);
            rules[i] = new SynonymRule(input, output, randomBoolean());
            testInputs.add(String.join(" ", inputArr));
        }

        assertBuildersMatch(dedup, rules, testInputs.toArray(new String[0]));
    }

    public void testRandomUnicodeRules() throws IOException {
        int numRules = randomIntBetween(1, 30);
        boolean dedup = randomBoolean();
        SynonymRule[] rules = new SynonymRule[numRules];
        List<String> testInputs = new ArrayList<>();

        for (int i = 0; i < numRules; i++) {
            int inputWords = randomIntBetween(1, 3);
            String[] inputArr = new String[inputWords];
            for (int j = 0; j < inputWords; j++) {
                inputArr[j] = randomUnicodeWord();
            }

            int outputWords = randomIntBetween(1, 3);
            String[] outputArr = new String[outputWords];
            for (int j = 0; j < outputWords; j++) {
                outputArr[j] = randomUnicodeWord();
            }

            CharsRef input = inputWords == 1 ? word(inputArr[0]) : phrase(inputArr);
            CharsRef output = outputWords == 1 ? word(outputArr[0]) : phrase(outputArr);
            rules[i] = new SynonymRule(input, output, randomBoolean());
            testInputs.add(String.join(" ", inputArr));
        }

        assertBuildersMatch(dedup, rules, testInputs.toArray(new String[0]));
    }

    public void testRandomLargeRuleSet() throws IOException {
        int numRules = randomIntBetween(200, 500);
        boolean dedup = randomBoolean();
        SynonymRule[] rules = new SynonymRule[numRules];
        List<String> testInputs = new ArrayList<>();

        for (int i = 0; i < numRules; i++) {
            int inputWords = randomIntBetween(1, 3);
            String[] inputArr = new String[inputWords];
            for (int j = 0; j < inputWords; j++) {
                inputArr[j] = randomAlphaOfLengthBetween(1, 6);
            }

            int outputWords = randomIntBetween(1, 3);
            String[] outputArr = new String[outputWords];
            for (int j = 0; j < outputWords; j++) {
                outputArr[j] = randomAlphaOfLengthBetween(1, 6);
            }

            CharsRef input = inputWords == 1 ? word(inputArr[0]) : phrase(inputArr);
            CharsRef output = outputWords == 1 ? word(outputArr[0]) : phrase(outputArr);
            rules[i] = new SynonymRule(input, output, randomBoolean());
            testInputs.add(String.join(" ", inputArr));
        }

        assertBuildersMatch(dedup, rules, testInputs.toArray(new String[0]));
    }

    public void testRandomManyOutputsSameInput() throws IOException {
        boolean dedup = randomBoolean();
        String inputWord = randomAlphaOfLengthBetween(2, 8);
        int numOutputs = randomIntBetween(3, 8);
        SynonymRule[] rules = new SynonymRule[numOutputs];

        for (int i = 0; i < numOutputs; i++) {
            int outputWords = randomIntBetween(1, 3);
            String[] outputArr = new String[outputWords];
            for (int j = 0; j < outputWords; j++) {
                outputArr[j] = randomAlphaOfLengthBetween(1, 8);
            }
            CharsRef output = outputWords == 1 ? word(outputArr[0]) : phrase(outputArr);
            rules[i] = new SynonymRule(word(inputWord), output, randomBoolean());
        }

        assertBuildersMatch(dedup, rules, inputWord);
    }

    private String randomWordWithSupplementary() {
        StringBuilder sb = new StringBuilder();
        int codePointCount = randomIntBetween(1, 5);
        for (int i = 0; i < codePointCount; i++) {
            if (randomBoolean()) {
                sb.appendCodePoint(randomIntBetween(0x1F300, 0x1F5FF));
            } else {
                sb.append(randomAlphaOfLength(1));
            }
        }
        return sb.toString();
    }

    private String randomUnicodeWord() {
        StringBuilder sb = new StringBuilder();
        int codePointCount = randomIntBetween(1, 5);
        for (int i = 0; i < codePointCount; i++) {
            int cp;
            do {
                cp = randomIntBetween(0x21, 0x1FFFF);
            } while (Character.isWhitespace(cp) || cp == SynonymMap.WORD_SEPARATOR || (cp >= 0xD800 && cp <= 0xDFFF));
            sb.appendCodePoint(cp);
        }
        return sb.toString();
    }
}
