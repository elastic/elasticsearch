/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.logsdb.patterntext;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;

public class PatternTextUtf8SplitterTests extends ESTestCase {

    /**
     * Asserts that {@link PatternTextUtf8Splitter#split} produces the same results as
     * {@link PatternTextValueProcessor#split} for the given string.
     */
    private static void assertMatchesRowPath(String input) throws IOException {
        final PatternTextValueProcessor.Parts parts = PatternTextValueProcessor.split(input);
        final PatternTextUtf8Splitter splitter = new PatternTextUtf8Splitter();
        final PatternTextUtf8Splitter.Result result = splitter.split(new BytesRef(input));

        // Both agree on whether the value exceeded the length limit.
        assertEquals(parts.useBinaryDocValuesForRawText(), result == PatternTextUtf8Splitter.Result.LENGTH_EXCEEDED);

        // templateId must match in both cases (including the truncation/lone-surrogate case).
        assertEquals("templateId mismatch for: " + input, parts.templateId(), splitter.templateId().utf8ToString());

        if (result == PatternTextUtf8Splitter.Result.TEMPLATED) {
            // Template bytes.
            assertBytesEqual("template", parts.template(), splitter.template());

            // argsInfo bytes.
            String expectedArgsInfo = Arg.encodeInfo(parts.argsInfo());
            assertEquals("argsInfo mismatch for: " + input, expectedArgsInfo, splitter.argsInfo().utf8ToString());

            // joinedArgs: only meaningful when there are args.
            if (splitter.argCount() > 0) {
                String expectedJoined = Arg.encodeRemainingArgs(parts);
                assertEquals("joinedArgs mismatch for: " + input, expectedJoined, splitter.joinedArgs().utf8ToString());
            } else {
                assertEquals("argCount should be 0 when row path has no args for: " + input, 0, parts.args().size());
            }
        }
    }

    private static void assertBytesEqual(String label, String expectedUtf8String, BytesRef actual) {
        byte[] expected = expectedUtf8String.getBytes(StandardCharsets.UTF_8);
        byte[] got = Arrays.copyOfRange(actual.bytes, actual.offset, actual.offset + actual.length);
        assertArrayEquals(label + " bytes mismatch", expected, got);
    }

    public void testSimpleLogLine() throws IOException {
        assertMatchesRowPath("Error 123 at line 456");
    }

    public void testNoArgs() throws IOException {
        assertMatchesRowPath("No numbers here at all");
    }

    public void testAllArgs() throws IOException {
        assertMatchesRowPath("123 456 789");
    }

    public void testEmptyString() throws IOException {
        assertMatchesRowPath("");
    }

    public void testSingleWord() throws IOException {
        assertMatchesRowPath("hello");
    }

    public void testSingleDigit() throws IOException {
        assertMatchesRowPath("5");
    }

    public void testSingleDelimiter() throws IOException {
        assertMatchesRowPath(" ");
    }

    public void testAllDelimiters() throws IOException {
        assertMatchesRowPath("   \t\n\r");
    }

    public void testEveryDelimiterChar() throws IOException {
        // Each of the 8 delimiter characters in isolation.
        assertMatchesRowPath(" space");
        assertMatchesRowPath("\ttab");
        assertMatchesRowPath("\nnewline");
        assertMatchesRowPath("vertical");
        assertMatchesRowPath("\fformfeed");
        assertMatchesRowPath("\rcarriage");
        assertMatchesRowPath("[bracket");
        assertMatchesRowPath("]bracket");
    }

    public void testBrackets() throws IOException {
        assertMatchesRowPath("Error [123] occurred");
        assertMatchesRowPath("[key=val] message");
        assertMatchesRowPath("msg [id=42][code=0]");
    }

    public void testLeadingDelimiters() throws IOException {
        assertMatchesRowPath("  leading");
        assertMatchesRowPath("  leading 123");
        assertMatchesRowPath("\t\tleading");
    }

    public void testTrailingDelimiters() throws IOException {
        assertMatchesRowPath("trailing  ");
        assertMatchesRowPath("trailing 123  ");
    }

    public void testConsecutiveDelimiters() throws IOException {
        assertMatchesRowPath("a  b  c");
        assertMatchesRowPath("x\t\ty");
    }

    public void testOnlyDelimiters() throws IOException {
        assertMatchesRowPath("   ");
        assertMatchesRowPath("[ ]");
        assertMatchesRowPath("\t \r\n");
    }

    public void testPureDigitToken() throws IOException {
        assertMatchesRowPath("value 42");
        assertMatchesRowPath("value 0");
        assertMatchesRowPath("val 9999");
    }

    public void testMixedAlphaDigit() throws IOException {
        assertMatchesRowPath("id abc123");
        assertMatchesRowPath("tag v1.2.3");
    }

    public void testNoDigits() throws IOException {
        assertMatchesRowPath("abc def ghi");
    }

    public void testNonAsciiNoDigits() throws IOException {
        // Multi-byte sequences with no digit code points.
        assertMatchesRowPath("Привет мир");    // Cyrillic (3-byte sequences)
        assertMatchesRowPath("你好世界");          // CJK (3-byte sequences)
        assertMatchesRowPath("café latte");   // Latin extended (2-byte sequences)
    }

    public void testBmpNonAsciiDigits() throws IOException {
        // Arabic-Indic digits (U+0660–0669): should be treated as args.
        assertMatchesRowPath("count ١٢٣");  // ١٢٣
        // Fullwidth digits (U+FF10–FF19): should be treated as args.
        assertMatchesRowPath("id １２３");     // １２３
    }

    public void testSupplementaryDigitsNotArgs() throws IOException {
        // U+1D7CE MATHEMATICAL BOLD DIGIT ZERO is a supplementary (4-byte) code point.
        // The row path calls Character.isDigit(char) on each UTF-16 code unit (two surrogates),
        // and surrogates are never digits, so this token is NOT an arg.
        // U+1D7CE = surrogate pair U+D835 U+DFCE.
        String supplementaryDigit = new String(Character.toChars(0x1D7CE));
        // Put it as a standalone token.
        String input = "prefix " + supplementaryDigit + " suffix";
        assertMatchesRowPath(input);
        // Verify our conclusion: the row path also says it's NOT an arg.
        PatternTextValueProcessor.Parts parts = PatternTextValueProcessor.split(input);
        assertEquals("supplementary digit token should not be an arg", 0, parts.args().size());
    }

    public void testEmoji() throws IOException {
        assertMatchesRowPath("error 😀 occurred");  // emoji (4-byte, surrogates)
        assertMatchesRowPath("🚀 launch 42");
    }

    public void testBufferReuseAcrossCalls() throws IOException {
        // Ensure the splitter correctly resets its state between calls.
        PatternTextUtf8Splitter splitter = new PatternTextUtf8Splitter();

        // First call: many args and a long template.
        String first = "a 1 b 2 c 3 d 4 e 5";
        splitter.split(new BytesRef(first));
        assertEquals(5, splitter.argCount());
        String t1 = splitter.template().utf8ToString();
        String ai1 = splitter.argsInfo().utf8ToString();

        // Second call: no args.
        String second = "no args here at all";
        splitter.split(new BytesRef(second));
        assertEquals(0, splitter.argCount());
        String t2 = splitter.template().utf8ToString();

        // Third call: same as the first — must match.
        splitter.split(new BytesRef(first));
        assertEquals(5, splitter.argCount());
        assertEquals(t1, splitter.template().utf8ToString());
        assertEquals(ai1, splitter.argsInfo().utf8ToString());

        // Differential check for all three.
        assertMatchesRowPath(first);
        assertMatchesRowPath(second);

        // The second call's template should match the second input (no args left over from first).
        assertEquals(second, t2);
    }

    public void testExactlyAtLimit() throws IOException {
        // Exactly MAX_LOG_LEN_TO_STORE_AS_DOC_VALUE ASCII chars → TEMPLATED.
        String atLimit = "a".repeat(PatternTextValueProcessor.MAX_LOG_LEN_TO_STORE_AS_DOC_VALUE);
        assertMatchesRowPath(atLimit);
        PatternTextUtf8Splitter splitter = new PatternTextUtf8Splitter();
        assertEquals(PatternTextUtf8Splitter.Result.TEMPLATED, splitter.split(new BytesRef(atLimit)));
    }

    public void testOneOverLimit() throws IOException {
        // MAX + 1 ASCII chars → LENGTH_EXCEEDED.
        String overLimit = "a".repeat(PatternTextValueProcessor.MAX_LOG_LEN_TO_STORE_AS_DOC_VALUE + 1);
        assertMatchesRowPath(overLimit);
        PatternTextUtf8Splitter splitter = new PatternTextUtf8Splitter();
        assertEquals(PatternTextUtf8Splitter.Result.LENGTH_EXCEEDED, splitter.split(new BytesRef(overLimit)));
    }

    public void testMultiByteAtLimitCharsButOverLimitBytes() throws IOException {
        // Build a value that is exactly MAX chars but has some 2-byte sequences so the byte
        // count exceeds MAX. This exercises the "byte count > limit but char count ≤ limit"
        // branch — the value should be TEMPLATED.
        int max = PatternTextValueProcessor.MAX_LOG_LEN_TO_STORE_AS_DOC_VALUE;
        // Use café-style é (U+00E9, 2 bytes) to exceed the byte limit while staying under char limit.
        // Each é is 2 bytes but 1 char. We'll mix some to ensure byte count > max.
        String base = "é".repeat(max); // max chars, 2*max bytes
        assertMatchesRowPath(base);
        PatternTextUtf8Splitter splitter = new PatternTextUtf8Splitter();
        assertEquals(PatternTextUtf8Splitter.Result.TEMPLATED, splitter.split(new BytesRef(base)));
    }

    public void testMultiByteOverLimit() throws IOException {
        // More than MAX chars of multi-byte text → LENGTH_EXCEEDED.
        String overLimit = "é".repeat(PatternTextValueProcessor.MAX_LOG_LEN_TO_STORE_AS_DOC_VALUE + 1);
        assertMatchesRowPath(overLimit);
        PatternTextUtf8Splitter splitter = new PatternTextUtf8Splitter();
        assertEquals(PatternTextUtf8Splitter.Result.LENGTH_EXCEEDED, splitter.split(new BytesRef(overLimit)));
    }

    public void testSurrogatePairStraddlingTruncationPoint() throws IOException {
        // Build a string of exactly MAX chars where the last char before the limit is the high
        // surrogate of a supplementary code point (the pair straddles the truncation boundary).
        // The row path calls CharBuffer.subSequence(0, 8192) which cuts the pair, leaving a lone
        // high surrogate, and String.getBytes(UTF_8) encodes it as '?' (0x3F).
        // Our byte path must produce the same templateId.
        int max = PatternTextValueProcessor.MAX_LOG_LEN_TO_STORE_AS_DOC_VALUE;
        // Fill max-1 ASCII chars, then one supplementary code point (2 UTF-16 units = 4 UTF-8 bytes).
        // Total chars = (max-1) + 2 = max+1, which exceeds the limit; truncation cuts at max,
        // leaving max-1 ASCII + the high surrogate.
        StringBuilder sb = new StringBuilder(max + 2);
        for (int i = 0; i < max - 1; i++) {
            sb.append('a');
        }
        sb.appendCodePoint(0x1F600); // U+1F600 GRINNING FACE, surrogate pair: D83D DE00
        String input = sb.toString();
        assertEquals(max + 1, input.length()); // max-1 + 2 surrogates
        assertMatchesRowPath(input);
        PatternTextUtf8Splitter splitter = new PatternTextUtf8Splitter();
        assertEquals(PatternTextUtf8Splitter.Result.LENGTH_EXCEEDED, splitter.split(new BytesRef(input)));
    }

    public void testSupplementaryCharEndingBeforeTruncationPoint() throws IOException {
        // Supplementary char fits fully inside the truncation window: no lone surrogate, no '?'.
        int max = PatternTextValueProcessor.MAX_LOG_LEN_TO_STORE_AS_DOC_VALUE;
        // max-2 ASCII + 1 supplementary (2 chars) + extra chars to trigger LENGTH_EXCEEDED.
        StringBuilder sb = new StringBuilder(max + 5);
        for (int i = 0; i < max - 2; i++) {
            sb.append('b');
        }
        sb.appendCodePoint(0x1F600); // fits at positions max-2 and max-1 → fully inside window
        sb.append("extra"); // push total length over the limit
        String input = sb.toString();
        assertTrue(input.length() > max);
        assertMatchesRowPath(input);
    }

    public void testRandomLogLines() throws IOException {
        for (int i = 0; i < 200; i++) {
            String input = randomLogLine();
            assertMatchesRowPath(input);
        }
    }

    public void testRandomRealisticUnicode() throws IOException {
        for (int i = 0; i < 100; i++) {
            // Use ESTestCase's realistic unicode generator (may produce multi-byte characters).
            String input = randomRealisticUnicodeOfLength(randomIntBetween(0, 200));
            assertMatchesRowPath(input);
        }
    }

    public void testRandomNearLimitStrings() throws IOException {
        int max = PatternTextValueProcessor.MAX_LOG_LEN_TO_STORE_AS_DOC_VALUE;
        for (int i = 0; i < 50; i++) {
            int len = randomIntBetween(max - 5, max + 5);
            // ASCII only, to ensure byte count equals char count for controlled length.
            String input = "a".repeat(len);
            assertMatchesRowPath(input);
        }
    }

    public void testArgsInfoEmptyArgs() throws IOException {
        // The row path always emits argsInfo even for no-args values. Verify byte parity.
        PatternTextValueProcessor.Parts parts = PatternTextValueProcessor.split("no args here");
        assertEquals(List.of(), parts.args());

        PatternTextUtf8Splitter splitter = new PatternTextUtf8Splitter();
        splitter.split(new BytesRef("no args here"));

        String expectedArgsInfo = Arg.encodeInfo(parts.argsInfo());
        assertEquals("argsInfo for empty args should match", expectedArgsInfo, splitter.argsInfo().utf8ToString());
    }

    // ── Zero-copy fast paths ──────────────────────────────────────────────────────────────────
    //
    // These tests verify that template() / joinedArgs() point directly into the source BytesRef
    // (assertSame on the backing array) when the single-slice fast path applies, and that the
    // fallback path (assertNotSame) is taken when multiple non-empty complement slices exist.
    //
    // For "123 456" the template is just the space " " — a single contiguous slice — so it IS
    // zero-copy even though there are two args.
    // For "[42]" the template is "[" and "]" — two non-contiguous slices — so it is NOT.

    public void testTemplateSingleSliceZeroCopy() throws IOException {
        PatternTextUtf8Splitter splitter = new PatternTextUtf8Splitter();

        // "Error 123": template = "Error " — one non-empty slice before the single arg.
        BytesRef v = new BytesRef("Error 123");
        splitter.split(v);
        assertSame("template should point into source for single-slice case", v.bytes, splitter.template().bytes);
        assertMatchesRowPath("Error 123");

        // " 42": template = " " — one non-empty slice before the arg.
        v = new BytesRef(" 42");
        splitter.split(v);
        assertSame("template should point into source for single-slice case", v.bytes, splitter.template().bytes);
        assertMatchesRowPath(" 42");

        // "42 ": template = " " — one non-empty slice after the arg.
        v = new BytesRef("42 ");
        splitter.split(v);
        assertSame("template should point into source for single-slice case", v.bytes, splitter.template().bytes);
        assertMatchesRowPath("42 ");

        // "123 456": template = " " (the space between two args) — one non-empty slice.
        v = new BytesRef("123 456");
        splitter.split(v);
        assertSame("template should point into source for single-slice case", v.bytes, splitter.template().bytes);
        assertMatchesRowPath("123 456");

        // "No numbers here": no args, template == whole input — one non-empty slice.
        v = new BytesRef("No numbers here");
        splitter.split(v);
        assertSame("template should point into source when no args", v.bytes, splitter.template().bytes);
        assertMatchesRowPath("No numbers here");
    }

    public void testTemplateZeroLengthZeroCopy() throws IOException {
        PatternTextUtf8Splitter splitter = new PatternTextUtf8Splitter();

        // "123": all-arg, template is empty — zero non-empty slices, still zero-copy path.
        BytesRef v = new BytesRef("123");
        splitter.split(v);
        assertSame("template should point into source even when template is empty", v.bytes, splitter.template().bytes);
        assertEquals(0, splitter.template().length);
        assertMatchesRowPath("123");

        // "": empty input — template is empty.
        v = new BytesRef("");
        splitter.split(v);
        assertSame("template should point into source for empty input", v.bytes, splitter.template().bytes);
        assertEquals(0, splitter.template().length);
        assertMatchesRowPath("");
    }

    public void testTemplateMultiSliceFallback() throws IOException {
        PatternTextUtf8Splitter splitter = new PatternTextUtf8Splitter();

        // "Error 123 at line 456": two args → three template slices ("Error ", " at line ", "")
        // = two non-empty slices → fallback path.
        BytesRef v = new BytesRef("Error 123 at line 456");
        splitter.split(v);
        assertNotSame("template should use scratch buffer for multi-slice case", v.bytes, splitter.template().bytes);
        assertMatchesRowPath("Error 123 at line 456");

        // "[42]": template = "[" and "]" — two non-empty slices → fallback path.
        v = new BytesRef("[42]");
        splitter.split(v);
        assertNotSame("template should use scratch buffer for [42] (two non-empty slices)", v.bytes, splitter.template().bytes);
        assertMatchesRowPath("[42]");
    }

    public void testArgsSingleArgZeroCopy() throws IOException {
        PatternTextUtf8Splitter splitter = new PatternTextUtf8Splitter();

        // Single arg: joinedArgs() should point into the source.
        BytesRef v = new BytesRef("abc 42");
        splitter.split(v);
        assertEquals(1, splitter.argCount());
        assertSame("joinedArgs should point into source for single arg", v.bytes, splitter.joinedArgs().bytes);
        assertMatchesRowPath("abc 42");

        v = new BytesRef("42");
        splitter.split(v);
        assertEquals(1, splitter.argCount());
        assertSame("joinedArgs should point into source for bare single arg", v.bytes, splitter.joinedArgs().bytes);
        assertMatchesRowPath("42");
    }

    public void testArgsMultiArgFallback() throws IOException {
        PatternTextUtf8Splitter splitter = new PatternTextUtf8Splitter();

        // Multiple args: joinedArgs() must use internal scratch.
        BytesRef v = new BytesRef("a 1 b 2");
        splitter.split(v);
        assertEquals(2, splitter.argCount());
        assertNotSame("joinedArgs should use scratch buffer for multiple args", v.bytes, splitter.joinedArgs().bytes);
        assertMatchesRowPath("a 1 b 2");
    }

    public void testZeroCopyThenFallbackThenZeroCopyReuse() throws IOException {
        // Verifies that templateRef is correctly reset on each call: a zero-copy ref from call N
        // must not bleed into call N+1, and the fallback buffer must not corrupt call N+2's
        // zero-copy ref.
        PatternTextUtf8Splitter splitter = new PatternTextUtf8Splitter();

        // Call 1: single-slice zero-copy.
        BytesRef v1 = new BytesRef("Error 123");
        splitter.split(v1);
        assertSame(v1.bytes, splitter.template().bytes);
        String t1 = splitter.template().utf8ToString(); // capture before next call

        // Call 2: multi-slice fallback.
        BytesRef v2 = new BytesRef("Error 123 at line 456");
        splitter.split(v2);
        assertNotSame(v2.bytes, splitter.template().bytes);
        // Also must not still point at v1's bytes.
        assertNotSame(v1.bytes, splitter.template().bytes);
        String t2 = splitter.template().utf8ToString();

        // Call 3: single-slice zero-copy again — must point at v3, not v1 or templateBuf.
        BytesRef v3 = new BytesRef("Warning 789");
        splitter.split(v3);
        assertSame(v3.bytes, splitter.template().bytes);
        String t3 = splitter.template().utf8ToString();

        // Parity checks.
        assertMatchesRowPath("Error 123");
        assertMatchesRowPath("Error 123 at line 456");
        assertMatchesRowPath("Warning 789");

        // The captured strings must also match the row path individually.
        assertEquals(PatternTextValueProcessor.split("Error 123").template(), t1);
        assertEquals(PatternTextValueProcessor.split("Error 123 at line 456").template(), t2);
        assertEquals(PatternTextValueProcessor.split("Warning 789").template(), t3);
    }

    /** Generates a random log-like string mixing words, numbers, and delimiters. */
    private String randomLogLine() {
        StringBuilder sb = new StringBuilder();
        int wordCount = randomIntBetween(0, 15);
        for (int i = 0; i < wordCount; i++) {
            if (i > 0) {
                // Random delimiter
                sb.append(randomFrom(' ', '\t', '\n', '[', ']'));
            }
            if (randomBoolean()) {
                // A "number-ish" arg token
                sb.append(randomIntBetween(0, 999999));
            } else {
                // A plain word (letters only)
                int wlen = randomIntBetween(1, 12);
                for (int j = 0; j < wlen; j++) {
                    sb.append((char) ('a' + randomIntBetween(0, 25)));
                }
            }
        }
        return sb.toString();
    }
}
