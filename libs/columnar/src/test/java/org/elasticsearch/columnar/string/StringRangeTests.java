/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar.string;

import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.TwoPhaseIterator;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.elasticsearch.columnar.ColumnarTestUtils.randomValidBlockSize;

/**
 * Range matching over every column shape that answers it differently: bisection over sorted values,
 * ordinal-range evaluation over a dictionary, and a per-value byte scan over unsorted plain. Each
 * path is checked against the documents themselves, and windowed collection is verified to agree
 * with per-document iteration.
 */
public class StringRangeTests extends ColumnarStringTestCase {

    public void testSortedPlain() throws IOException {
        assertRange(sorted(repeated(between(400, 2000))), DictionaryPolicy.NONE);
    }

    public void testUnsortedPlain() throws IOException {
        assertRange(repeated(between(400, 2000)), DictionaryPolicy.NONE);
    }

    public void testDictionary() throws IOException {
        assertDictionaryRange(repeated(between(400, 2000)));
    }

    public void testDictionaryWithEscapes() throws IOException {
        final BytesRef[] docValues = repeated(between(600, 2000));
        for (int d = 0; d < docValues.length; d += 60) {
            docValues[d] = new BytesRef("escaped-" + d);
        }
        assertDictionaryRange(docValues);
    }

    public void testSparse() throws IOException {
        final BytesRef[] docValues = repeated(between(400, 2000));
        for (int d = 0; d < docValues.length; d++) {
            if (randomBoolean()) {
                docValues[d] = null;
            }
        }
        assertRange(docValues, DictionaryPolicy.NONE);
    }

    public void testSortedAndSparse() throws IOException {
        final BytesRef[] docValues = sorted(repeated(between(400, 2000)));
        for (int d = 0; d < docValues.length; d += 7) {
            docValues[d] = null;
        }
        assertRange(docValues, DictionaryPolicy.NONE);
    }

    public void testOpenBounds() throws IOException {
        final BytesRef[] docValues = repeated(between(400, 2000));
        for (DictionaryPolicy policy : List.of(DictionaryPolicy.NONE, dictionaryPolicy())) {
            withColumn(docValues, randomValidBlockSize(), randomChunkCodec(), randomTargetChunkBytes(), policy, (metadata, reader) -> {
                for (BytesRef bound : bounds()) {
                    for (boolean inclusive : new boolean[] { true, false }) {
                        assertEquals(
                            "open lower, upper=" + bound + " inclusive=" + inclusive + " policy=" + policy,
                            expectedInRange(docValues, null, true, bound, inclusive),
                            matched(reader.matchRange(null, true, bound, inclusive))
                        );
                        assertEquals(
                            "open upper, lower=" + bound + " inclusive=" + inclusive + " policy=" + policy,
                            expectedInRange(docValues, bound, inclusive, null, true),
                            matched(reader.matchRange(bound, inclusive, null, true))
                        );
                    }
                }
                assertEquals(
                    "both bounds open policy=" + policy,
                    expectedInRange(docValues, null, true, null, true),
                    matched(reader.matchRange(null, true, null, true))
                );
            });
        }
    }

    public void testEmptyStringBound() throws IOException {
        final BytesRef[] docValues = new BytesRef[between(400, 1500)];
        for (int d = 0; d < docValues.length; d++) {
            docValues[d] = d % 10 == 0 ? new BytesRef("") : new BytesRef(TERMS[d % (TERMS.length - 1)]);
        }
        for (DictionaryPolicy policy : List.of(DictionaryPolicy.NONE, dictionaryPolicy())) {
            withColumn(docValues, randomValidBlockSize(), randomChunkCodec(), randomTargetChunkBytes(), policy, (metadata, reader) -> {
                assertEquals(
                    "range from empty to alpha inclusive",
                    expectedInRange(docValues, new BytesRef(""), true, new BytesRef("alpha"), true),
                    matched(reader.matchRange(new BytesRef(""), true, new BytesRef("alpha"), true))
                );
                assertEquals(
                    "range from empty exclusive to alpha exclusive",
                    expectedInRange(docValues, new BytesRef(""), false, new BytesRef("alpha"), false),
                    matched(reader.matchRange(new BytesRef(""), false, new BytesRef("alpha"), false))
                );
            });
        }
    }

    public void testEmptyColumn() throws IOException {
        final BytesRef[] docValues = new BytesRef[between(400, 1500)];
        for (DictionaryPolicy policy : List.of(DictionaryPolicy.NONE, dictionaryPolicy())) {
            withColumn(docValues, randomValidBlockSize(), randomChunkCodec(), randomTargetChunkBytes(), policy, (metadata, reader) -> {
                assertEquals(
                    "empty column",
                    List.of(),
                    matched(reader.matchRange(new BytesRef("alpha"), true, new BytesRef("bravo"), true))
                );
            });
        }
    }

    public void testWindowedCollectionAgreesWithPerDocument() throws IOException {
        record Shape(String name, BytesRef[] values, DictionaryPolicy policy) {}
        final BytesRef[] withEscapes = new BytesRef[between(600, 1500)];
        for (int d = 0; d < withEscapes.length; d++) {
            withEscapes[d] = d % 40 == 3 ? new BytesRef("escaped-" + d) : new BytesRef(TERMS[d % (TERMS.length - 1)]);
        }
        final List<Shape> shapes = List.of(
            new Shape("plain", repeated(between(400, 1500)), DictionaryPolicy.NONE),
            new Shape("dictionary", repeated(between(400, 1500)), dictionaryPolicy()),
            new Shape("dictionary with escapes", withEscapes, dictionaryPolicy())
        );
        for (Shape shape : shapes) {
            withColumn(
                shape.values(),
                randomValidBlockSize(),
                randomChunkCodec(),
                randomTargetChunkBytes(),
                shape.policy(),
                (metadata, reader) -> {
                    for (RangeCase rc : rangeCases()) {
                        assertWindowedAgrees(
                            shape.name() + " " + rc,
                            shape.values().length,
                            () -> reader.matchRange(rc.lower, rc.includeLower, rc.upper, rc.includeUpper)
                        );
                    }
                }
            );
        }
    }

    public void testMultiValuedRange() throws IOException {
        final BytesRef[][] docSlots = {
            { new BytesRef("alpha"), new BytesRef("delta") },
            { new BytesRef("charlie"), new BytesRef("echo") },
            { null, new BytesRef("bravo") },
            null };
        for (DictionaryPolicy policy : new DictionaryPolicy[] { DictionaryPolicy.NONE, dictionaryPolicy() }) {
            withColumn(docSlots, randomValidBlockSize(), randomChunkCodec(), randomTargetChunkBytes(), policy, (metadata, reader) -> {
                final List<Integer> matches = matched(reader.matchRange(new BytesRef("alpha"), true, new BytesRef("bravo"), true));
                assertEquals("multi-valued policy=" + policy, List.of(0, 2), matches);
            });
        }
    }

    public void testEmptyUpperBoundIsAnInvertedRange() throws IOException {
        final BytesRef[] docValues = repeated(between(400, 2000));
        for (DictionaryPolicy policy : List.of(DictionaryPolicy.NONE, dictionaryPolicy())) {
            withColumn(
                docValues,
                randomValidBlockSize(),
                randomChunkCodec(),
                randomTargetChunkBytes(),
                policy,
                (metadata, reader) -> assertEquals(
                    "empty upper bound policy=" + policy,
                    List.of(),
                    matched(reader.matchRange(new BytesRef("alpha"), true, new BytesRef(), true))
                )
            );
        }
    }

    private static DictionaryPolicy dictionaryPolicy() {
        // NOTE: coverage runs from the production bar down rather than up. Every shape here repeats its terms,
        // so a stricter bar than production rejects nothing a looser one admits, and the escaped values the
        // escape shapes hold stay well inside the share the bar leaves over.
        return randomBoolean()
            ? StringColumnOptions.DEFAULT_DICTIONARY
            : new DictionaryPolicy(
                between(4 * 1024, 512 * 1024),
                randomDoubleBetween(0.5, StringColumnOptions.DEFAULT_DICTIONARY.minCoverage(), true),
                randomDoubleBetween(0.2, 0.5, true)
            );
    }

    private static final String[] TERMS = { "alpha", "alpine", "bravo", "charlie", "delta" };

    private static BytesRef[] bounds() {
        final BytesRef[] result = new BytesRef[TERMS.length + 1];
        for (int i = 0; i < TERMS.length; i++) {
            result[i] = new BytesRef(TERMS[i]);
        }
        result[TERMS.length] = new BytesRef("zzz");
        return result;
    }

    private static BytesRef[] repeated(int count) {
        final BytesRef[] values = new BytesRef[count];
        for (int d = 0; d < count; d++) {
            values[d] = new BytesRef(TERMS[d % TERMS.length]);
        }
        return values;
    }

    private static BytesRef[] sorted(BytesRef[] values) {
        final BytesRef[] copy = values.clone();
        Arrays.sort(copy, (a, b) -> a == null ? (b == null ? 0 : -1) : (b == null ? 1 : a.compareTo(b)));
        return copy;
    }

    private static List<RangeCase> rangeCases() {
        final List<RangeCase> cases = new ArrayList<>();
        final BytesRef[] b = bounds();
        for (int i = 0; i < b.length; i++) {
            for (int j = i; j < b.length; j++) {
                cases.add(new RangeCase(b[i], true, b[j], true));
                cases.add(new RangeCase(b[i], false, b[j], false));
                cases.add(new RangeCase(b[i], true, b[j], false));
            }
        }
        return cases;
    }

    private record RangeCase(BytesRef lower, boolean includeLower, BytesRef upper, boolean includeUpper) {
        @Override
        public String toString() {
            return (includeLower ? "[" : "{") + lower + " TO " + upper + (includeUpper ? "]" : "}");
        }
    }

    private static List<Integer> expectedInRange(
        BytesRef[] docValues,
        BytesRef lower,
        boolean includeLower,
        BytesRef upper,
        boolean includeUpper
    ) {
        final List<Integer> docs = new ArrayList<>();
        for (int d = 0; d < docValues.length; d++) {
            final BytesRef value = docValues[d];
            if (value == null) {
                continue;
            }
            if (lower != null) {
                final int cmp = value.compareTo(lower);
                if (cmp < 0 || (cmp == 0 && includeLower == false)) {
                    continue;
                }
            }
            if (upper != null) {
                final int cmp = value.compareTo(upper);
                if (cmp > 0 || (cmp == 0 && includeUpper == false)) {
                    continue;
                }
            }
            docs.add(d);
        }
        return docs;
    }

    /** As {@link #assertRange}, checking the values earned a dictionary so the ordinal paths are the ones run. */
    private void assertDictionaryRange(BytesRef[] docValues) throws IOException {
        assertRange(docValues, dictionaryPolicy(), true);
    }

    private void assertRange(BytesRef[] docValues, DictionaryPolicy policy) throws IOException {
        assertRange(docValues, policy, false);
    }

    private void assertRange(BytesRef[] docValues, DictionaryPolicy policy, boolean expectDictionary) throws IOException {
        withColumn(docValues, randomValidBlockSize(), randomChunkCodec(), randomTargetChunkBytes(), policy, (metadata, reader) -> {
            if (expectDictionary) {
                assertTrue("expected a dictionary, got " + metadata.layout(), reader.hasDictionary());
            }
            for (RangeCase rc : rangeCases()) {
                assertEquals(
                    "range " + rc,
                    expectedInRange(docValues, rc.lower, rc.includeLower, rc.upper, rc.includeUpper),
                    matched(reader.matchRange(rc.lower, rc.includeLower, rc.upper, rc.includeUpper))
                );
            }
            assertEquals(
                "empty range exclusive equal bounds",
                List.of(),
                matched(reader.matchRange(new BytesRef("alpha"), false, new BytesRef("alpha"), false))
            );
            assertEquals(
                "inverted bounds",
                List.of(),
                matched(reader.matchRange(new BytesRef("bravo"), true, new BytesRef("alpha"), true))
            );
        });
    }

    private void assertWindowedAgrees(String label, int docCount, Match match) throws IOException {
        final List<Integer> oneAtATime = matched(match.get());
        for (int window : new int[] { 1, 7, 64, 128, 512, docCount + 1 }) {
            final TwoPhaseIterator twoPhase = TwoPhaseIterator.unwrap(match.get());
            if (twoPhase == null) {
                continue;
            }
            final FixedBitSet bits = new FixedBitSet(docCount);
            final DocIdSetIterator approximation = twoPhase.approximation();
            approximation.nextDoc();
            for (int upTo = window; approximation.docID() != DocIdSetIterator.NO_MORE_DOCS; upTo += window) {
                twoPhase.intoBitSet(Math.min(upTo, docCount), bits, 0);
                if (upTo >= docCount) {
                    break;
                }
            }
            final List<Integer> windowed = new ArrayList<>();
            for (int d = bits.nextSetBit(0); d != DocIdSetIterator.NO_MORE_DOCS; d = d + 1 < bits.length()
                ? bits.nextSetBit(d + 1)
                : DocIdSetIterator.NO_MORE_DOCS) {
                windowed.add(d);
            }
            assertEquals(label + " collected in windows of " + window, oneAtATime, windowed);
        }
    }

    private static List<Integer> matched(DocIdSetIterator matches) throws IOException {
        final List<Integer> docs = new ArrayList<>();
        for (int doc = matches.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = matches.nextDoc()) {
            docs.add(doc);
        }
        return docs;
    }

    @FunctionalInterface
    private interface Match {
        DocIdSetIterator get() throws IOException;
    }
}
