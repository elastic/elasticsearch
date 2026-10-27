/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.simdvec.ESVectorUtil;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

import static org.hamcrest.Matchers.greaterThan;

/**
 * Every way a value can spell a term it holds. The point of these is completeness: a spelling left out of the
 * patterns would rule out a value that holds the term, so each is walked for every character of Unicode that
 * another lowers onto, against a list of those characters this test works out for itself.
 */
public class CaseSpellingsTests extends ESTestCase {

    private static final int MOST = 16;

    /** Characters that lower onto {@code lower}, worked out here rather than asked of the thing under test. */
    private static int[] loweringOnto(int lower) {
        final List<Integer> sources = new ArrayList<>();
        for (int codePoint = 0; codePoint <= Character.MAX_CODE_POINT; codePoint++) {
            if (Character.isDefined(codePoint) && codePoint != lower && Character.toLowerCase(codePoint) == lower) {
                sources.add(codePoint);
            }
        }
        final int[] all = new int[sources.size()];
        for (int i = 0; i < all.length; i++) {
            all[i] = sources.get(i);
        }
        return all;
    }

    /**
     * The whole of it: for every character another lowers onto, a value spelling that other character is found by
     * one of the patterns the term comes to.
     */
    public void testEverySpellingOfEveryCharacterIsLookedFor() {
        // One pass over Unicode, keeping what lowers onto what, so the walk below is not quadratic.
        final List<int[]> pairs = new ArrayList<>();
        for (int codePoint = 0; codePoint <= Character.MAX_CODE_POINT; codePoint++) {
            if (Character.isDefined(codePoint) == false) {
                continue;
            }
            final int lower = Character.toLowerCase(codePoint);
            if (lower != codePoint) {
                pairs.add(new int[] { lower, codePoint });
            }
        }
        assertThat("Unicode has characters whose case differs", pairs.size(), greaterThan(1000));

        for (int[] pair : pairs) {
            final int lower = pair[0];
            final int spelledAs = pair[1];
            final BytesRef term = new BytesRef(new String(Character.toChars(lower)));
            final List<BytesRef> needles = CaseSpellings.of(term, MOST);
            assertNotNull("one character is never spelled more ways than " + MOST, needles);
            final BytesRef value = new BytesRef("x" + new String(Character.toChars(spelledAs)) + "y");
            assertTrue(
                describe(lower) + " is spelled by " + describe(spelledAs) + ", which none of " + needles + " looks for",
                anyNeedleFound(value, needles)
            );
        }
    }

    /** The term as it stands is always looked for, which is what an analyzer that lowers nothing leaves. */
    public void testTheTermAsItStandsIsLookedFor() {
        for (String term : List.of("fox", "FOX", "Café", "CAFÉ", "Ą", "ą", "K", "İ")) {
            final List<BytesRef> needles = CaseSpellings.of(new BytesRef(term), MOST);
            assertNotNull(needles);
            assertTrue(term + " is not looked for by " + needles, anyNeedleFound(new BytesRef("x" + term + "y"), needles));
        }
    }

    /** Two characters of a term, each spelled two ways, are four patterns, and every one of them is looked for. */
    public void testTheSpellingsOfOneCharacterMultiplyWithAnother() {
        final List<BytesRef> needles = CaseSpellings.of(new BytesRef("ąę"), MOST);
        assertNotNull(needles);
        for (String spelling : List.of("ąę", "Ąę", "ąĘ", "ĄĘ")) {
            assertTrue(spelling + " is not looked for by " + needles, anyNeedleFound(new BytesRef("x" + spelling + "y"), needles));
        }
    }

    /** Comparing the ASCII letters without their case already takes most spellings for the same. */
    public void testWhatTheComparisonOfAsciiCaseAlreadyCovers() {
        assertEquals("a term of ASCII comes to one pattern", 1, CaseSpellings.of(new BytesRef("brown"), MOST).size());
        assertEquals("and so does one of Latin letters with accents", 1, CaseSpellings.of(new BytesRef("café"), MOST).size());
        assertEquals(1, CaseSpellings.of(new BytesRef("züber"), MOST).size());
        assertEquals(
            "while a term holding an i is spelled with the character that lowers onto it too",
            2,
            CaseSpellings.of(new BytesRef("zeppelin"), MOST).size()
        );
        assertEquals("and a term holding both a k and an i is spelled four ways", 4, CaseSpellings.of(new BytesRef("kelvin"), MOST).size());
        assertThat(
            "while a letter whose case the bytes do not fold together is more than one",
            CaseSpellings.of(new BytesRef("ą"), MOST).size(),
            greaterThan(1)
        );
    }

    /** A term spelled more ways than a value is worth reading for is not read for in part. */
    public void testTooManySpellingsAreNotReadForAtAll() {
        assertNull(CaseSpellings.of(new BytesRef("ąęćłńóśżź"), MOST));
        assertNull("nor under a count of one", CaseSpellings.of(new BytesRef("ąę"), 1));
        assertNotNull("while a term spelled one way fits a count of one", CaseSpellings.of(new BytesRef("brown"), 1));
    }

    /**
     * Lowering a character leaves it the same number of chars, which is what an analyzer lowering a token in place
     * relies on, and so does every pattern worked out here.
     */
    public void testLoweringACharacterKeepsItsLength() {
        for (int codePoint = 0; codePoint <= Character.MAX_CODE_POINT; codePoint++) {
            if (Character.isDefined(codePoint) == false) {
                continue;
            }
            assertEquals(
                describe(codePoint) + " changes length when lowered",
                Character.charCount(codePoint),
                Character.charCount(Character.toLowerCase(codePoint))
            );
        }
    }

    private static boolean anyNeedleFound(BytesRef value, List<BytesRef> needles) {
        for (BytesRef needle : needles) {
            if (ESVectorUtil.mayContainIgnoringAsciiCase(
                value.bytes,
                value.offset,
                value.length,
                needle.bytes,
                needle.offset,
                needle.length
            )) {
                return true;
            }
        }
        return false;
    }

    private static String describe(int codePoint) {
        return String.format(Locale.ROOT, "U+%04X", codePoint);
    }
}
