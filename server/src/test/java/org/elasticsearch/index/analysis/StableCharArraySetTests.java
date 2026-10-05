/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.analysis;

import org.apache.lucene.analysis.CharArraySet;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Tests for {@link Analysis.StableCharArraySet} — the wrapper class that gives a
 * {@link CharArraySet} a stable {@code hashCode} suitable for use as a HashMap key.
 *
 * <p>The Lucene {@link CharArraySet}'s own {@code hashCode} is broken (it varies across calls on
 * the same instance because the underlying {@code CharArrayMap.MapEntry#getKey} clones the key
 * {@code char[]}, and {@code char[].hashCode} is identity-based). This wrapper precomputes a
 * stable hash at construction and delegates equality to the underlying set.
 */
public class StableCharArraySetTests extends ESTestCase {

    public void testEmptySetHashCodeIsStable() {
        Analysis.StableCharArraySet a = new Analysis.StableCharArraySet(CharArraySet.EMPTY_SET);
        Analysis.StableCharArraySet b = new Analysis.StableCharArraySet(CharArraySet.EMPTY_SET);
        assertEquals(a, b);
        assertEquals(a.hashCode(), b.hashCode());
        // The empty hash is the same regardless of how many times you call it
        for (int i = 0; i < 10; i++) {
            assertEquals(a.hashCode(), a.hashCode());
        }
    }

    public void testNullSetTreatedAsEmpty() {
        Analysis.StableCharArraySet wrap = new Analysis.StableCharArraySet(null);
        assertEquals(0, wrap.hashCode());
    }

    /**
     * Regression: a null-set instance and an empty/non-null-set instance hash to the same value (0),
     * so they collide in a HashMap and {@code equals} is invoked. Calling {@code equals} with the
     * null-set instance as the receiver must NOT throw a NullPointerException — it must simply report
     * the two unequal (forgoing the sharing opportunity is safe; sharing them wrongly is not).
     */
    public void testEqualsNullVsEmptyDoesNotNpe() {
        Analysis.StableCharArraySet nullSet = new Analysis.StableCharArraySet(null);
        Analysis.StableCharArraySet emptySet = new Analysis.StableCharArraySet(CharArraySet.EMPTY_SET);
        Analysis.StableCharArraySet nonEmpty = new Analysis.StableCharArraySet(new CharArraySet(List.of("the"), false));

        // Same hash → they would collide in a HashMap and reach equals().
        assertEquals(nullSet.hashCode(), emptySet.hashCode());

        // The null-set instance as the receiver must not NPE, in either direction.
        assertNotEquals(nullSet, emptySet);
        assertNotEquals(emptySet, nullSet);
        assertNotEquals(nullSet, nonEmpty);
        assertNotEquals(nonEmpty, nullSet);

        // Two null-set instances are equal (the == short-circuit), still no NPE.
        assertEquals(nullSet, new Analysis.StableCharArraySet(null));

        // And it survives being used as a HashMap key alongside a colliding empty-set key.
        Map<Analysis.StableCharArraySet, String> map = new HashMap<>();
        map.put(emptySet, "empty");
        assertNull("null-set lookup must not match the empty-set key, and must not NPE", map.get(nullSet));
    }

    public void testSameContentDifferentInstancesAreEqual() {
        CharArraySet a = new CharArraySet(List.of("the", "and", "of"), false);
        CharArraySet b = new CharArraySet(List.of("of", "and", "the"), false);  // different insertion order
        Analysis.StableCharArraySet wa = new Analysis.StableCharArraySet(a);
        Analysis.StableCharArraySet wb = new Analysis.StableCharArraySet(b);
        assertEquals(wa, wb);
        assertEquals(wa.hashCode(), wb.hashCode());
    }

    public void testDifferentContentNotEqual() {
        CharArraySet a = new CharArraySet(List.of("the", "and"), false);
        CharArraySet b = new CharArraySet(List.of("the", "or"), false);
        Analysis.StableCharArraySet wa = new Analysis.StableCharArraySet(a);
        Analysis.StableCharArraySet wb = new Analysis.StableCharArraySet(b);
        assertNotEquals(wa, wb);
    }

    public void testHashCodeStableAcrossManyCalls() {
        CharArraySet src = new CharArraySet(List.of("alpha", "beta", "gamma", "delta", "epsilon"), false);
        Analysis.StableCharArraySet wrap = new Analysis.StableCharArraySet(src);
        int firstHash = wrap.hashCode();
        for (int i = 0; i < 100; i++) {
            assertEquals("hashCode must be stable across calls", firstHash, wrap.hashCode());
        }
    }

    /**
     * Regression: the raw {@link CharArraySet#hashCode()} is NOT stable across calls (this is the
     * Lucene quirk our wrapper exists to mask). This test pins that behavior so a future Lucene
     * change that fixes it can be noticed and the wrapper potentially simplified.
     */
    public void testRawCharArraySetHashCodeIsStillUnstable() {
        CharArraySet set = new CharArraySet(List.of("alpha", "beta", "gamma"), false);
        int h1 = set.hashCode();
        int h2 = set.hashCode();
        // If this assertion ever fires it would be GREAT news — Lucene fixed the unstable hash —
        // and {@link Analysis.StableCharArraySet} may no longer be needed. Update accordingly.
        assertNotEquals(
            "If this assertion fires, Lucene CharArraySet#hashCode is now stable and the wrapper "
                + "in Analysis.StableCharArraySet may be simplifiable to just hold the set.",
            h1,
            h2
        );
    }

    public void testWorksAsHashMapKey() {
        Map<Analysis.StableCharArraySet, String> map = new HashMap<>();
        CharArraySet sourceA = new CharArraySet(List.of("one", "two"), false);
        CharArraySet sourceB = new CharArraySet(List.of("one", "two"), false);  // same content, distinct instance
        Analysis.StableCharArraySet keyA = new Analysis.StableCharArraySet(sourceA);
        Analysis.StableCharArraySet keyB = new Analysis.StableCharArraySet(sourceB);

        map.put(keyA, "valueA");
        assertEquals("valueA", map.get(keyB));  // lookup by equal-but-distinct key
        map.put(keyB, "overwritten");  // put on the equal key should overwrite, not add
        assertEquals(1, map.size());
        assertEquals("overwritten", map.get(keyA));
    }

    /**
     * Case sensitivity is behavior-affecting. A case-insensitive {@link CharArraySet} stores its
     * keys folded to lower-case, so for already-lower-case word lists (the common case for
     * keep/stop/article lists) the {@code ignoreCase=true} and {@code ignoreCase=false} variants
     * hold IDENTICAL stored content — the content-only {@link CharArraySet#equals} then reports
     * them equal even though they match differently. The wrapper must fold the case flag into its
     * identity so two analysis factories differing only in a {@code *_case} / {@code ignore_case}
     * setting do not wrongly share one analyzer instance (which, for an index-time filter, would
     * change how data is tokenized). Guards the false-share fix.
     */
    public void testCaseSensitivityIsPartOfIdentity() {
        // Lower-case words: stored content is identical regardless of the ignoreCase flag, so only
        // the flag distinguishes the two sets. This is exactly the case the content hash misses.
        CharArraySet sensitiveSet = new CharArraySet(List.of("apple", "banana"), false);
        CharArraySet insensitiveSet = new CharArraySet(List.of("apple", "banana"), true);
        Analysis.StableCharArraySet sensitive = new Analysis.StableCharArraySet(sensitiveSet, false);
        Analysis.StableCharArraySet insensitive = new Analysis.StableCharArraySet(insensitiveSet, true);
        assertNotEquals("same stored content but different case mode must not be equal", sensitive, insensitive);
        assertNotEquals("case mode must perturb the hash", sensitive.hashCode(), insensitive.hashCode());
        // Same content AND same case mode are still equal.
        assertEquals(insensitive, new Analysis.StableCharArraySet(new CharArraySet(List.of("banana", "apple"), true), true));
        // The single-argument constructor is case-sensitive, matching the two-arg false form.
        assertEquals(sensitive, new Analysis.StableCharArraySet(sensitiveSet));
    }

    public void testEqualsContract() {
        CharArraySet s = new CharArraySet(List.of("x"), false);
        Analysis.StableCharArraySet wrap = new Analysis.StableCharArraySet(s);
        assertEquals(wrap, wrap);                       // reflexive
        assertNotEquals(wrap, null);                    // null
        assertNotEquals(wrap, "not a wrapper");         // different type
        Analysis.StableCharArraySet other = new Analysis.StableCharArraySet(s);
        assertEquals(wrap, other);                      // symmetric
        assertEquals(other, wrap);
    }

    public void testLargeSetStableAndUnique() {
        CharArraySet a = new CharArraySet(randomWords(1000), false);
        CharArraySet b = new CharArraySet(randomWords(1000), false);  // different random content
        Analysis.StableCharArraySet wa = new Analysis.StableCharArraySet(a);
        Analysis.StableCharArraySet wb = new Analysis.StableCharArraySet(b);
        // Two random 1000-word sets should essentially never collide.
        assertNotEquals(wa, wb);
        assertNotEquals(wa.hashCode(), wb.hashCode());
    }

    private List<String> randomWords(int count) {
        List<String> words = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            words.add(randomAlphaOfLength(between(3, 12)) + i);
        }
        return words;
    }
}
