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
import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.analysis.tokenattributes.TermToBytesRefAttribute;
import org.apache.lucene.analysis.tokenattributes.TypeAttribute;
import org.apache.lucene.tests.analysis.BaseTokenStreamTestCase;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.AttributeSource;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.io.StringReader;
import java.util.Locale;

/**
 * {@link AsciiStandardAnalyzer} must be indistinguishable from a {@link StandardAnalyzer} without stop words: same terms, offsets,
 * types and position increments, including after {@code end()}. Lucene's {@code StandardAnalyzer} is the oracle in every test here,
 * so these tests also fail if a Lucene upgrade changes the word break rules for ASCII text.
 */
public class AsciiStandardAnalyzerTests extends ESTestCase {

    private static StandardAnalyzer oracle(int maxTokenLength) {
        StandardAnalyzer analyzer = new StandardAnalyzer(CharArraySet.EMPTY_SET);
        analyzer.setMaxTokenLength(maxTokenLength);
        return analyzer;
    }

    /** One line per token plus the state after {@code end()}. */
    private static String describe(Analyzer analyzer, String text) throws IOException {
        StringBuilder sb = new StringBuilder();
        try (TokenStream ts = analyzer.tokenStream("field", text)) {
            CharTermAttribute term = ts.addAttribute(CharTermAttribute.class);
            OffsetAttribute offset = ts.addAttribute(OffsetAttribute.class);
            PositionIncrementAttribute posInc = ts.addAttribute(PositionIncrementAttribute.class);
            TypeAttribute type = ts.addAttribute(TypeAttribute.class);
            TermToBytesRefAttribute termBytes = ts.addAttribute(TermToBytesRefAttribute.class);
            ts.reset();
            while (ts.incrementToken()) {
                // The indexing chain reads the UTF-8 bytes, so they are part of what must be the same.
                sb.append('[').append(term).append("] ").append(termBytes.getBytesRef().toString()).append(' ');
                sb.append(offset.startOffset()).append('-').append(offset.endOffset()).append(' ');
                sb.append(type.type()).append(" +").append(posInc.getPositionIncrement()).append('\n');
            }
            ts.end();
            sb.append("end ").append(offset.startOffset()).append('-').append(offset.endOffset());
            sb.append(" +").append(posInc.getPositionIncrement());
        }
        return sb.toString();
    }

    private static void assertSame(Analyzer expected, Analyzer actual, String text) throws IOException {
        String expectedTokens = describe(expected, text);
        String actualTokens = describe(actual, text);
        if (expectedTokens.equals(actualTokens) == false) {
            fail("different tokens for [" + escape(text) + "]\nexpected:\n" + expectedTokens + "\nactual:\n" + actualTokens);
        }
    }

    private static String escape(String text) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < text.length(); i++) {
            char c = text.charAt(i);
            if (c >= 32 && c < 127) {
                sb.append(c);
            } else {
                sb.append(String.format(Locale.ROOT, "\\u%04x", (int) c));
            }
        }
        return sb.toString();
    }

    public void testSimpleLogLine() throws IOException {
        String line = "2024-01-01T00:00:00.123Z INFO [main] org.example.Foo: user_id=42 took 1,234.5ms from 10.0.0.1:9200 (can't stop)";
        assertSame(oracle(255), new AsciiStandardAnalyzer(), line);
        BaseTokenStreamTestCase.assertAnalyzesTo(
            new AsciiStandardAnalyzer(),
            "Foo_Bar 1.5 a'b c:d 1:2",
            new String[] { "foo_bar", "1.5", "a'b", "c:d", "1", "2" },
            new String[] { "<ALPHANUM>", "<NUM>", "<ALPHANUM>", "<ALPHANUM>", "<NUM>", "<NUM>" }
        );
    }

    /** Every string up to a few chars over the char classes that matter, so every state machine transition is checked. */
    public void testExhaustiveShortStrings() throws IOException {
        char[] alphabet = { 'a', 'B', '7', '_', '.', '\'', ':', ',', ';', ' ' };
        Analyzer expected = oracle(255);
        Analyzer actual = new AsciiStandardAnalyzer();
        int maxLength = 6;
        int[] digits = new int[maxLength];
        for (int length = 0; length <= maxLength; length++) {
            long total = 1;
            for (int i = 0; i < length; i++) {
                total *= alphabet.length;
            }
            char[] text = new char[length];
            for (long n = 0; n < total; n++) {
                long rest = n;
                for (int i = 0; i < length; i++) {
                    digits[i] = (int) (rest % alphabet.length);
                    rest /= alphabet.length;
                    text[i] = alphabet[digits[i]];
                }
                assertSame(expected, actual, new String(text));
            }
        }
    }

    public void testEveryAsciiCharInContext() throws IOException {
        Analyzer expected = oracle(255);
        Analyzer actual = new AsciiStandardAnalyzer();
        for (char c = 0; c < 128; c++) {
            String s = String.valueOf(c);
            for (String template : new String[] {
                "%s",
                "a%sa",
                "1%s1",
                "a%s1",
                "1%sa",
                "%sa%s",
                "a%s%sa",
                "1%s%s1",
                "a_%sb",
                "a%s_b",
                " %s ",
                "A1%s2b%s" }) {
                assertSame(expected, actual, template.replace("%s", s));
            }
        }
    }

    public void testRandomAscii() throws IOException {
        // Weighted towards the chars that change the tokenization.
        String interesting = "aAzZ09_.':,; -/=[]\t\n\"";
        Analyzer expected = oracle(255);
        Analyzer actual = new AsciiStandardAnalyzer();
        for (int i = 0; i < 20_000; i++) {
            int length = randomIntBetween(0, 200);
            StringBuilder sb = new StringBuilder(length);
            for (int j = 0; j < length; j++) {
                sb.append(randomInt(3) == 0 ? (char) randomIntBetween(0, 127) : interesting.charAt(randomInt(interesting.length() - 1)));
            }
            assertSame(expected, actual, sb.toString());
        }
    }

    /** Tokens that are longer than max_token_length are chopped by Lucene, not dropped. */
    public void testLongTokens() throws IOException {
        for (int max : new int[] { 1, 2, 3, 4, 5, 7, 16, 255, 256 }) {
            Analyzer expected = oracle(max);
            Analyzer actual = new AsciiStandardAnalyzer(max);
            for (String text : new String[] {
                "a".repeat(600),
                "1".repeat(300) + " " + "b".repeat(300),
                "_".repeat(300) + "a",
                "_".repeat(300),
                "abc.def",
                "1.1.1",
                "a.b.c.d.e.f.g.h.i.j.k.l.m.n.o.p.q.r.s.t.u.v.w.x.y.z",
                "a_".repeat(200),
                "ab'".repeat(100),
                "12,".repeat(100) + "x" }) {
                assertSame(expected, actual, text);
            }
            for (int i = 0; i < 2_000; i++) {
                String interesting = "ab1_.:,'";
                StringBuilder sb = new StringBuilder();
                for (int j = randomIntBetween(0, 80); j > 0; j--) {
                    sb.append(interesting.charAt(randomInt(interesting.length() - 1)));
                }
                assertSame(expected, actual, sb.toString());
            }
        }
    }

    public void testNonAscii() throws IOException {
        Analyzer expected = oracle(255);
        Analyzer actual = new AsciiStandardAnalyzer();
        for (String text : new String[] {
            "café au lait",
            "CAFÉ",
            "áb c",   // combining mark extends the ASCII token before it
            "foo こんにちは bar",
            "中文 text",
            "emoji 😀 smile 1⃣",
            "ascii only first then é",
            "é then ascii only",
            "a‍b",
            "\ud83d",   // lone surrogate
            "Straße 1.5" }) {
            assertSame(expected, actual, text);
        }
        for (int i = 0; i < 5_000; i++) {
            assertSame(expected, actual, randomUnicodeMixedWithAscii());
        }
    }

    private String randomUnicodeMixedWithAscii() {
        StringBuilder sb = new StringBuilder();
        for (int i = randomIntBetween(0, 20); i > 0; i--) {
            if (randomBoolean()) {
                sb.append(randomAlphaOfLengthBetween(0, 12)).append(randomFrom(" ", "_", ".", "'", ":", ",", "1"));
            } else {
                sb.append(TestUtil.randomUnicodeString(random(), 6));
            }
        }
        return sb.toString();
    }

    /** One analyzer instance (so one reused tokenizer) must give the right answer when ASCII and non-ASCII values alternate. */
    public void testReuseAcrossFastAndFallbackValues() throws IOException {
        Analyzer expected = oracle(255);
        Analyzer actual = new AsciiStandardAnalyzer();
        String[] values = { "plain ascii 1.5", "café latte", "more ascii", "", "中", "x", "Á", "last ascii" };
        for (int i = 0; i < 100; i++) {
            assertSame(expected, actual, randomFrom(values));
        }
    }

    /** Values that do not fit in the buffer are handed to Lucene's tokenizer together with the part we already read. */
    public void testValueLargerThanBuffer() throws IOException {
        int maxBuffered = 64;
        Analyzer actual = new Analyzer() {
            @Override
            protected TokenStreamComponents createComponents(String fieldName) {
                return new TokenStreamComponents(new AsciiStandardTokenizer(maxBuffered));
            }
        };
        Analyzer expected = oracle(255);
        for (int length : new int[] { 0, 1, 63, 64, 65, 127, 128, 129, 1000 }) {
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < length; i++) {
                sb.append(randomFrom("ab", "1", " ", "_", ".", "x"));
            }
            String ascii = sb.toString();
            assertSame(expected, actual, ascii);
            assertSame(expected, actual, ascii + "é" + ascii);
        }
    }

    /** A cloned attribute source must keep its own term bytes, the indexing chain and other consumers can hold on to a clone. */
    public void testClonedAttributesKeepTheirOwnBytes() throws IOException {
        try (TokenStream ts = new AsciiStandardAnalyzer().tokenStream("field", "first second \u00e9clair third")) {
            TermToBytesRefAttribute bytes = ts.addAttribute(TermToBytesRefAttribute.class);
            ts.reset();
            assertTrue(ts.incrementToken());
            AttributeSource firstClone = ts.cloneAttributes();
            BytesRef first = BytesRef.deepCopyOf(bytes.getBytesRef());
            assertTrue(ts.incrementToken());
            assertEquals(first, firstClone.addAttribute(TermToBytesRefAttribute.class).getBytesRef());
            assertEquals("first", first.utf8ToString());
            assertEquals("second", bytes.getBytesRef().utf8ToString());
            assertTrue(ts.incrementToken());
            assertEquals("\u00e9clair", bytes.getBytesRef().utf8ToString());
            ts.end();
        }
    }

    public void testNormalize() {
        assertEquals(oracle(255).normalize("field", "FoO_BarÉ"), new AsciiStandardAnalyzer().normalize("field", "FoO_BarÉ"));
    }

    public void testInvalidMaxTokenLengthFailsLikeStandardAnalyzer() {
        for (int invalid : new int[] { 0, -1, 1024 * 1024 + 1 }) {
            IllegalArgumentException expected = expectThrows(
                IllegalArgumentException.class,
                () -> oracle(invalid).tokenStream("f", new StringReader("a"))
            );
            IllegalArgumentException actual = expectThrows(
                IllegalArgumentException.class,
                () -> new AsciiStandardAnalyzer(invalid).tokenStream("f", new StringReader("a"))
            );
            assertEquals(expected.getMessage(), actual.getMessage());
        }
    }

    /** Lucene's own consistency checks: offsets, reuse, reset and end contract, on random text. */
    public void testLuceneRandomChecks() throws IOException {
        BaseTokenStreamTestCase.checkRandomData(random(), new AsciiStandardAnalyzer(), 200 * RANDOM_MULTIPLIER);
        BaseTokenStreamTestCase.checkRandomData(random(), new AsciiStandardAnalyzer(5), 200 * RANDOM_MULTIPLIER);
    }
}
