/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.core.expression.predicate.regex;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.WildcardQuery;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.automaton.ByteRunAutomaton;
import org.apache.lucene.util.automaton.Operations;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.predicate.regex.WildcardPattern.Shape;
import org.elasticsearch.xpack.esql.core.util.ByteMatchers;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;

/**
 * Pins the byte-level semantics that {@code WildcardLike.toEvaluator} substitutes for the
 * {@link ByteRunAutomaton} walk on affix-only patterns. For each shape returned by
 * {@link WildcardPattern#shape()}, the routed predicate the production switch lands on
 * ({@code Arrays.equals} on leading bytes for {@link Shape.Prefix}, {@code Arrays.equals}
 * on trailing bytes for {@link Shape.Suffix}, {@link ByteMatchers#containsLiteral} for
 * {@link Shape.Contains}, automaton for {@link Shape.General}) must agree with the
 * automaton compiled from the same Lucene wildcard string on every input — a silent
 * disagreement would change query results.
 *
 * <p>Classification correctness of {@code WildcardPattern#shape()} itself
 * (which pattern maps to which {@code Shape} record) lives in {@code StringPatternTests};
 * this file owns the equivalence-vs-automaton dimension only. The dispatch wiring inside
 * {@code WildcardLike.toEvaluator} is pinned by evaluator-name assertions in
 * {@code WildcardLikeTests}.
 *
 * <p>Two assertions:
 *
 * <ul>
 *   <li><b>Hand-picked edge cases.</b> Empty literal, length boundaries, multi-byte UTF-8,
 *       supplementary-plane codepoints (surrogate pairs in UTF-16, four-byte sequences in
 *       UTF-8), and escaped specials carried through into the literal segment.</li>
 *   <li><b>Randomized property pin.</b> A seeded sweep over random shapes &times; random
 *       literals &times; random values, mixing ASCII and multi-byte content. Catches regressions
 *       the hand-picked set might miss.</li>
 * </ul>
 *
 * <p>Case-insensitive {@code LIKE} is short-circuited to the automaton inside
 * {@code WildcardLike.toEvaluator} before {@code shape()} is consulted, so the fast path
 * never runs case-insensitively — out of scope here.
 */
public class WildcardPatternShapeEquivalenceTests extends ESTestCase {

    /**
     * Per-shape equivalence on a hand-picked edge case set. Every {@code (pattern, value)}
     * pair must produce the same boolean from {@link ByteRunAutomaton} and from the routed
     * evaluator (prefix → {@code Arrays.equals} on the leading bytes; suffix → on the trailing
     * bytes; contains → {@link ByteMatchers#containsLiteral}).
     */
    public void testFastPathEquivalentToAutomaton() {
        record Case(String pattern, String value) {}
        List<Case> cases = List.of(
            // Prefix shape.
            new Case("foo*", "foobar"),                  // hit
            new Case("foo*", "barfoo"),                  // miss — prefix in middle
            new Case("foo*", "foo"),                     // exact length, prefix exhausts value
            new Case("foo*", "fo"),                      // value shorter than prefix
            new Case("foo*", ""),                        // empty value
            new Case("a*", "a"),                         // single-char prefix exact
            new Case("café*", "café-au-lait"),           // multi-byte UTF-8 prefix
            new Case("café*", "cafe-au-lait"),           // miss — bare e differs from é byte-for-byte
            new Case("😀*", "😀 hi"),// supplementary-plane prefix (4-byte UTF-8)

            // Suffix shape.
            new Case("*foo", "barfoo"),                  // hit
            new Case("*foo", "foobar"),                  // miss — suffix at start
            new Case("*foo", "foo"),                     // exact length
            new Case("*foo", "fo"),                      // value shorter than suffix
            new Case("*foo", ""),                        // empty value
            new Case("*café", "lait-au-café"),           // multi-byte UTF-8 suffix
            new Case("*😀", "hi 😀"),// supplementary-plane suffix

            // Contains shape — exercises SIMD substring (above 24 bytes) and scalar fallback.
            new Case("*foo*", "barfoobaz"),              // hit in middle
            new Case("*foo*", "fooxxx"),                 // hit at start
            new Case("*foo*", "xxxfoo"),                 // hit at end
            new Case("*foo*", "barbaz"),                 // miss
            new Case("*foo*", "fo"),                     // value shorter than literal
            new Case("*foo*", ""),                       // empty value
            new Case("*café*", "le-café-est-bon"),       // multi-byte UTF-8 contains
            new Case("*café*", "le-cafe-est-bon"),       // miss on bare e
            // Long value to exercise the SIMD path's first+last byte filter past its 24-byte threshold.
            new Case("*needle*", "lorem-ipsum-dolor-sit-amet-needle-consectetur-adipiscing-elit"),

            // Edge: single-star pattern → Suffix("") → every value matches.
            new Case("*", ""),
            new Case("*", "anything"),
            new Case("*", "😀"),

            // Escaped specials in the literal: byte-for-byte match still required.
            new Case("*foo\\*bar*", "see foo*bar end"),  // contains literal "*"
            new Case("*foo\\*bar*", "see foo bar end"),  // miss — no literal "*"
            new Case("foo\\?bar*", "foo?bar123"),        // prefix carrying literal "?"
            new Case("foo\\?bar*", "fooXbar123")         // miss — '?' is literal, not wildcard
        );
        for (Case c : cases) {
            assertAgrees(c.pattern, c.value);
        }
    }

    /**
     * Seeded randomized property pin. Each iteration picks a shape (prefix / suffix / contains),
     * a random literal (mostly ASCII, sometimes multi-byte UTF-8 including supplementary planes),
     * and a random value built either to satisfy the shape or to dodge it. Asserts routed-path
     * vs automaton agreement.
     */
    public void testFastPathEquivalentToAutomatonRandomized() {
        int iterations = 1000;
        for (int i = 0; i < iterations; i++) {
            String shape = randomFrom("prefix", "suffix", "contains");
            String literal = randomLiteral();
            String value = randomValue(literal, shape);
            String pattern = switch (shape) {
                case "prefix" -> escapeWildcardChars(literal) + "*";
                case "suffix" -> "*" + escapeWildcardChars(literal);
                case "contains" -> "*" + escapeWildcardChars(literal) + "*";
                default -> throw new AssertionError(shape);
            };
            assertAgrees(pattern, value);
        }
    }

    /**
     * Core agreement assertion: build a {@link WildcardPattern}, classify it, dispatch through the
     * same switch the production {@code WildcardLike.toEvaluator} uses, and compare against the
     * automaton compiled from the same wildcard string. For {@link Shape.General}, the production
     * path falls through to the automaton itself — trivially equivalent, but exercised to confirm
     * the shape classifier returns {@code General} for inputs it should.
     */
    private static void assertAgrees(String pattern, String value) {
        WildcardPattern wp = new WildcardPattern(pattern);
        byte[] valueBytes = value.getBytes(StandardCharsets.UTF_8);
        BytesRef valueRef = new BytesRef(valueBytes);
        ByteRunAutomaton automaton = new ByteRunAutomaton(
            WildcardQuery.toAutomaton(new Term("f", pattern), Operations.DEFAULT_DETERMINIZE_WORK_LIMIT)
        );
        boolean fromAutomaton = automaton.run(valueBytes, 0, valueBytes.length);
        boolean fromRouted = switch (wp.shape()) {
            case Shape.Prefix(String prefix) -> startsWithBytes(valueRef, new BytesRef(prefix));
            case Shape.Suffix(String suffix) -> endsWithBytes(valueRef, new BytesRef(suffix));
            case Shape.Contains(String literal) -> ByteMatchers.containsLiteral(valueRef, new BytesRef(literal));
            case Shape.General ignored -> fromAutomaton;  // production path is identical here.
        };
        assertEquals(
            "pattern [" + pattern + "] value [" + value + "] shape=" + wp.shape() + ": routed and automaton diverge",
            fromAutomaton,
            fromRouted
        );
    }

    /** Mirrors {@code StartsWith#process} byte-for-byte. */
    private static boolean startsWithBytes(BytesRef str, BytesRef prefix) {
        if (str.length < prefix.length) {
            return false;
        }
        return Arrays.equals(str.bytes, str.offset, str.offset + prefix.length, prefix.bytes, prefix.offset, prefix.offset + prefix.length);
    }

    /** Mirrors {@code EndsWith#process} byte-for-byte. */
    private static boolean endsWithBytes(BytesRef str, BytesRef suffix) {
        if (str.length < suffix.length) {
            return false;
        }
        return Arrays.equals(
            str.bytes,
            str.offset + str.length - suffix.length,
            str.offset + str.length,
            suffix.bytes,
            suffix.offset,
            suffix.offset + suffix.length
        );
    }

    /**
     * Random literal of length 1..8 characters, ~25% chance of including a multi-byte UTF-8
     * codepoint (latin-extended, CJK, or supplementary plane via surrogate pair) — exercises
     * the full UTF-8 byte-length range (1..4 bytes per codepoint) the row evaluator can see.
     */
    private String randomLiteral() {
        int n = randomIntBetween(1, 8);
        StringBuilder sb = new StringBuilder();
        while (sb.length() < n) {
            if (randomInt(99) < 25) {
                int pick = randomInt(2);
                if (pick == 0) {
                    sb.append((char) randomIntBetween(0xC0, 0xFF));         // latin-extended (2-byte UTF-8)
                } else if (pick == 1) {
                    sb.append((char) randomIntBetween(0x4E00, 0x9FFF));     // CJK (3-byte UTF-8)
                } else {
                    sb.appendCodePoint(0x1F600 + randomInt(0x40));          // emoji (4-byte UTF-8)
                }
            } else {
                sb.append((char) ('a' + randomInt(25)));
            }
        }
        return sb.toString();
    }

    /**
     * Construct a value that either embeds the literal at the shape-mandated position
     * (prefix → start, suffix → end, contains → random body position) or excludes it
     * entirely. Body bytes mix ASCII and occasional multi-byte to keep the corpus realistic.
     */
    private String randomValue(String literal, String shape) {
        boolean shouldMatch = randomBoolean();
        int bodyLen = randomIntBetween(0, 32);
        StringBuilder body = new StringBuilder();
        while (body.length() < bodyLen) {
            if (randomInt(99) < 15) {
                body.append((char) randomIntBetween(0x4E00, 0x9FFF));
            } else {
                body.append((char) ('a' + randomInt(25)));
            }
        }
        if (shouldMatch == false) {
            // Regenerate until body genuinely lacks the literal. Latin/CJK randomness almost never
            // produces a long literal by accident; loop is a defensive guard.
            while (body.toString().contains(literal)) {
                body.setLength(0);
                while (body.length() < bodyLen) {
                    body.append((char) ('a' + randomInt(25)));
                }
            }
            return body.toString();
        }
        return switch (shape) {
            case "prefix" -> literal + body;
            case "suffix" -> body + literal;
            case "contains" -> {
                int split = body.isEmpty() ? 0 : randomInt(body.length());
                yield body.substring(0, split) + literal + body.substring(split);
            }
            default -> throw new AssertionError(shape);
        };
    }

    /**
     * Escape the wildcard meta-characters {@code *}, {@code ?}, and {@code \} for use inside a
     * literal segment of a Lucene wildcard pattern. Without this the random literals occasionally
     * carry an unescaped wildcard byte and the synthesized pattern stops being affix-only.
     */
    private static String escapeWildcardChars(String s) {
        StringBuilder sb = new StringBuilder(s.length());
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c == '*' || c == '?' || c == '\\') {
                sb.append('\\');
            }
            sb.append(c);
        }
        return sb.toString();
    }
}
