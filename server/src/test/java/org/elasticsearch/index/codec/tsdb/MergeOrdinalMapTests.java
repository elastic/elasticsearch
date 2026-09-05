/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.index.codec.tsdb;

import org.apache.lucene.index.BaseTermsEnum;
import org.apache.lucene.index.ImpactsEnum;
import org.apache.lucene.index.OrdinalMap;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.TermState;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LongValues;
import org.apache.lucene.util.packed.PackedInts;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Arrays;
import java.util.Locale;
import java.util.TreeSet;

/**
 * Unit tests for {@link MergeOrdinalMap}.
 *
 * <p>Each test builds a {@link MergeOrdinalMap} and an equivalent Lucene {@link OrdinalMap}
 * from the same per-segment term arrays, then asserts that:
 * <ul>
 *   <li>{@link MergeOrdinalMap#getValueCount()} equals {@code OrdinalMap.getValueCount()}</li>
 *   <li>{@link MergeOrdinalMap#getGlobalOrds(int)} maps every segment ordinal identically</li>
 *   <li>{@link MergeOrdinalMap#mergedTermsEnum} yields the same terms in the same order</li>
 * </ul>
 */
public class MergeOrdinalMapTests extends ESTestCase {

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    /**
     * A minimal {@link TermsEnum} over a pre-sorted {@link BytesRef} array that supports
     * {@code ord()} — required by both {@link OrdinalMap#build} and {@link MergeOrdinalMap}.
     */
    private static TermsEnum arrayTermsEnum(BytesRef[] sortedTerms) {
        return new BaseTermsEnum() {
            int pos = -1;

            @Override
            public BytesRef next() {
                pos++;
                return pos < sortedTerms.length ? sortedTerms[pos] : null;
            }

            @Override
            public BytesRef term() {
                return sortedTerms[pos];
            }

            @Override
            public long ord() {
                return pos;
            }

            @Override
            public TermsEnum.SeekStatus seekCeil(BytesRef text) {
                throw new UnsupportedOperationException();
            }

            @Override
            public void seekExact(long ord) {
                pos = (int) ord;
            }

            @Override
            public int docFreq() {
                throw new UnsupportedOperationException();
            }

            @Override
            public long totalTermFreq() {
                throw new UnsupportedOperationException();
            }

            @Override
            public PostingsEnum postings(PostingsEnum reuse, int flags) {
                throw new UnsupportedOperationException();
            }

            @Override
            public ImpactsEnum impacts(int flags) {
                throw new UnsupportedOperationException();
            }

            @Override
            public TermState termState() {
                throw new UnsupportedOperationException();
            }
        };
    }

    /** Converts a string array to a sorted {@link BytesRef} array. */
    private static BytesRef[] toSortedBytes(String... terms) {
        BytesRef[] refs = Arrays.stream(terms).map(BytesRef::new).toArray(BytesRef[]::new);
        Arrays.sort(refs);
        return refs;
    }

    /**
     * Builds both a {@link MergeOrdinalMap} and a reference {@link OrdinalMap} from the given
     * term arrays (one per segment), then asserts they are equivalent on all observable outputs.
     */
    private void assertEquivalent(BytesRef[][] segmentTerms) throws IOException {
        int numSubs = segmentTerms.length;
        long[] weights = new long[numSubs];
        for (int i = 0; i < numSubs; i++) {
            weights[i] = segmentTerms[i].length;
        }

        // Build MergeOrdinalMap
        TermsEnum[] mapSubs = new TermsEnum[numSubs];
        for (int i = 0; i < numSubs; i++) {
            mapSubs[i] = arrayTermsEnum(segmentTerms[i]);
        }
        MergeOrdinalMap mergeMap = new MergeOrdinalMap(mapSubs, weights);

        // Build reference OrdinalMap from identical subs
        TermsEnum[] refSubs = new TermsEnum[numSubs];
        for (int i = 0; i < numSubs; i++) {
            refSubs[i] = arrayTermsEnum(segmentTerms[i]);
        }
        OrdinalMap refMap = OrdinalMap.build(null, refSubs, weights, PackedInts.COMPACT);

        // valueCount must match
        assertEquals("getValueCount", refMap.getValueCount(), mergeMap.getValueCount());

        // getGlobalOrds must match for every segment and every segment ordinal
        for (int seg = 0; seg < numSubs; seg++) {
            LongValues refGO = refMap.getGlobalOrds(seg);
            LongValues mergeGO = mergeMap.getGlobalOrds(seg);
            for (int segOrd = 0; segOrd < segmentTerms[seg].length; segOrd++) {
                assertEquals("seg=" + seg + " segOrd=" + segOrd, refGO.get(segOrd), mergeGO.get(segOrd));
            }
        }

        // mergedTermsEnum must yield the same terms in the same global-ord order
        TermsEnum[] enumSubs = new TermsEnum[numSubs];
        for (int i = 0; i < numSubs; i++) {
            enumSubs[i] = arrayTermsEnum(segmentTerms[i]);
        }
        TermsEnum merged = mergeMap.mergedTermsEnum(enumSubs);
        long expectedOrd = 0;
        for (BytesRef term = merged.next(); term != null; term = merged.next()) {
            assertEquals("ord", expectedOrd, merged.ord());
            expectedOrd++;
        }
        assertEquals("valueCount from enum", mergeMap.getValueCount(), expectedOrd);
    }

    // -------------------------------------------------------------------------
    // Tests
    // -------------------------------------------------------------------------

    public void testSingleSegmentAllTermsDistinct() throws IOException {
        BytesRef[][] segs = { toSortedBytes("apple", "banana", "cherry") };
        assertEquivalent(segs);
    }

    public void testTwoSegmentsNoOverlap() throws IOException {
        BytesRef[][] segs = { toSortedBytes("aaa", "bbb"), toSortedBytes("ccc", "ddd") };
        assertEquivalent(segs);
    }

    public void testTwoSegmentsFullOverlap() throws IOException {
        BytesRef[][] segs = { toSortedBytes("foo", "bar"), toSortedBytes("foo", "bar") };
        assertEquivalent(segs);
    }

    public void testTwoSegmentsPartialOverlap() throws IOException {
        BytesRef[][] segs = { toSortedBytes("apple", "cherry", "elderberry"), toSortedBytes("banana", "cherry", "fig") };
        assertEquivalent(segs);
    }

    public void testThreeSegmentsMixedOverlap() throws IOException {
        BytesRef[][] segs = { toSortedBytes("a", "b", "c", "d"), toSortedBytes("b", "c", "e"), toSortedBytes("a", "c", "f") };
        assertEquivalent(segs);
    }

    /**
     * Simulates a realistic scenario: many segments with partial overlap on a high-cardinality
     * field (e.g. a {@code keyword} or {@code _tsid} field). Verifies equivalence across a
     * 32-segment merge to mirror the incident configuration (N=32).
     */
    public void testThirtyTwoSegmentsMerge() throws IOException {
        int numSegs = 32;
        int termsPerSeg = 1000;
        int totalDistinct = 5000; // ~20% overlap between segments

        // Generate all distinct terms
        String[] allTerms = new String[totalDistinct];
        for (int i = 0; i < totalDistinct; i++) {
            allTerms[i] = String.format(Locale.ROOT, "term%08d", i);
        }

        BytesRef[][] segs = new BytesRef[numSegs][];
        for (int seg = 0; seg < numSegs; seg++) {
            // Each segment gets a random subset of terms; reproducible via randomLong()
            TreeSet<String> chosen = new TreeSet<>();
            int offset = seg * (totalDistinct / numSegs);
            for (int t = 0; t < termsPerSeg; t++) {
                chosen.add(allTerms[(offset + t) % totalDistinct]);
            }
            segs[seg] = chosen.stream().map(BytesRef::new).toArray(BytesRef[]::new);
        }
        assertEquivalent(segs);
    }

    /** An empty segment contributes no terms and must not disturb the global ord sequence. */
    public void testEmptySegment() throws IOException {
        BytesRef[][] segs = { toSortedBytes("alpha", "beta"), new BytesRef[0], toSortedBytes("beta", "gamma") };
        assertEquivalent(segs);
    }

    /** All segments are empty — value count must be 0. */
    public void testAllSegmentsEmpty() throws IOException {
        BytesRef[][] segs = { new BytesRef[0], new BytesRef[0] };
        assertEquivalent(segs);
        MergeOrdinalMap m = new MergeOrdinalMap(
            new TermsEnum[] { arrayTermsEnum(new BytesRef[0]), arrayTermsEnum(new BytesRef[0]) },
            new long[] { 0, 0 }
        );
        assertEquals(0L, m.getValueCount());
    }

    /** A single segment with zero terms. */
    public void testSingleEmptySegment() throws IOException {
        BytesRef[][] segs = { new BytesRef[0] };
        assertEquivalent(segs);
    }

    /**
     * Verifies that {@link MergeOrdinalMap#mergedTermsEnum} can be called multiple times on the
     * same map and that each call returns an independent iterator.
     */
    public void testMultipleMergedTermsEnumCallsAreIndependent() throws IOException {
        BytesRef[] terms = toSortedBytes("aaa", "bbb", "ccc");
        BytesRef[][] segs = { terms, toSortedBytes("aaa", "ddd") };
        long[] weights = { terms.length, 2 };

        TermsEnum[] mapSubs = { arrayTermsEnum(segs[0]), arrayTermsEnum(segs[1]) };
        MergeOrdinalMap map = new MergeOrdinalMap(mapSubs, weights);

        // First enum: advance to the end
        TermsEnum e1 = map.mergedTermsEnum(new TermsEnum[] { arrayTermsEnum(segs[0]), arrayTermsEnum(segs[1]) });
        while (e1.next() != null) {
            /* drain */ }

        // Second enum: must start fresh at ord 0
        TermsEnum e2 = map.mergedTermsEnum(new TermsEnum[] { arrayTermsEnum(segs[0]), arrayTermsEnum(segs[1]) });
        BytesRef first = e2.next();
        assertNotNull("second enum should start fresh", first);
        assertEquals(0L, e2.ord());
        assertEquals(new BytesRef("aaa"), first);
    }

    /** Randomized stress test: random number of segments, random terms with random overlap. */
    public void testRandomized() throws IOException {
        int numSegs = randomIntBetween(1, 16);
        int distinctCount = randomIntBetween(1, 500);

        // Generate a pool of distinct terms
        String[] pool = new String[distinctCount];
        for (int i = 0; i < distinctCount; i++) {
            pool[i] = randomAlphaOfLengthBetween(1, 20) + i; // suffix ensures uniqueness
        }
        Arrays.sort(pool);

        BytesRef[][] segs = new BytesRef[numSegs][];
        for (int seg = 0; seg < numSegs; seg++) {
            int segSize = randomIntBetween(0, distinctCount);
            TreeSet<String> chosen = new TreeSet<>();
            for (int t = 0; t < segSize; t++) {
                chosen.add(pool[randomIntBetween(0, distinctCount - 1)]);
            }
            segs[seg] = chosen.stream().map(BytesRef::new).toArray(BytesRef[]::new);
        }
        assertEquivalent(segs);
    }
}
