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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.NavigableSet;
import java.util.Objects;
import java.util.TreeSet;

import static org.elasticsearch.columnar.ColumnarTestUtils.randomValidBlockSize;

/**
 * Term-set matching over every column shape that answers it differently: ordinal-set evaluation over a dictionary,
 * byte scanning over plain. Each path is checked against the documents themselves.
 */
public class StringAnyOfTests extends ColumnarStringTestCase {

    public void testPlain() throws IOException {
        assertAnyOf(repeated(between(400, 2000)), DictionaryPolicy.NONE);
    }

    public void testDictionary() throws IOException {
        assertDictionaryAnyOf(repeated(between(400, 2000)));
    }

    public void testDictionaryWithEscapes() throws IOException {
        final BytesRef[] docValues = repeated(between(600, 2000));
        for (int d = 0; d < docValues.length; d += 60) {
            docValues[d] = new BytesRef("escaped-" + d);
        }
        assertDictionaryAnyOf(docValues);
    }

    public void testSortedPlain() throws IOException {
        assertSortedAnyOf(sorted(repeated(between(400, 2000))), DictionaryPolicy.NONE);
    }

    public void testSortedDictionary() throws IOException {
        assertSortedAnyOf(sorted(repeated(between(400, 2000))), dictionaryPolicy());
    }

    public void testSortedAndSparse() throws IOException {
        final BytesRef[] docValues = sorted(repeated(between(400, 2000)));
        for (int d = 0; d < docValues.length; d += 7) {
            docValues[d] = null;
        }
        assertSortedAnyOf(docValues, DictionaryPolicy.NONE);
        assertSortedAnyOf(docValues, dictionaryPolicy());
    }

    public void testSparse() throws IOException {
        final BytesRef[] docValues = repeated(between(400, 2000));
        for (int d = 0; d < docValues.length; d++) {
            if (randomBoolean()) {
                docValues[d] = null;
            }
        }
        assertAnyOf(docValues, DictionaryPolicy.NONE);
    }

    public void testEmptyTermSet() throws IOException {
        final BytesRef[] docValues = repeated(between(400, 1500));
        for (DictionaryPolicy policy : List.of(DictionaryPolicy.NONE, dictionaryPolicy())) {
            withColumn(docValues, randomValidBlockSize(), randomChunkCodec(), randomTargetChunkBytes(), policy, (metadata, reader) -> {
                assertEquals("empty term set", List.of(), matched(reader.matchAnyOf(new TreeSet<>())));
            });
        }
    }

    public void testEmptyColumn() throws IOException {
        final BytesRef[] docValues = new BytesRef[between(400, 1500)];
        for (DictionaryPolicy policy : List.of(DictionaryPolicy.NONE, dictionaryPolicy())) {
            withColumn(docValues, randomValidBlockSize(), randomChunkCodec(), randomTargetChunkBytes(), policy, (metadata, reader) -> {
                assertEquals("empty column", List.of(), matched(reader.matchAnyOf(termsOf("alpha", "bravo"))));
            });
        }
    }

    public void testEscapeMatchedByBytes() throws IOException {
        final BytesRef escaped = new BytesRef("escaped-unique-42");
        final BytesRef[] docValues = new BytesRef[between(600, 1500)];
        for (int d = 0; d < docValues.length; d++) {
            docValues[d] = new BytesRef(TERMS[d % TERMS.length]);
        }
        final int escapeDoc = docValues.length / 2;
        docValues[escapeDoc] = escaped;

        withColumn(
            docValues,
            randomValidBlockSize(),
            randomChunkCodec(),
            randomTargetChunkBytes(),
            dictionaryPolicy(),
            (metadata, reader) -> {
                final List<Integer> found = matched(reader.matchAnyOf(termsOf(escaped.utf8ToString())));
                assertEquals("escape matched by bytes", List.of(escapeDoc), found);
            }
        );
    }

    public void testMultiValuedAnyOf() throws IOException {
        final BytesRef[][] docSlots = {
            { new BytesRef("alpha"), new BytesRef("delta") },
            { new BytesRef("bravo"), new BytesRef("echo") },
            { null, new BytesRef("charlie") },
            null };
        final NavigableSet<BytesRef> queryTerms = termsOf("alpha", "charlie");
        for (DictionaryPolicy policy : new DictionaryPolicy[] { DictionaryPolicy.NONE, dictionaryPolicy() }) {
            withColumn(docSlots, randomValidBlockSize(), randomChunkCodec(), randomTargetChunkBytes(), policy, (metadata, reader) -> {
                final List<Integer> matches = matched(reader.matchAnyOf(queryTerms));
                assertEquals("multi-valued policy=" + policy, List.of(0, 2), matches);
            });
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

    public void testTermsInOneRunOfOrdinalsSettleFromTheOrdinals() throws IOException {
        // NOTE: the dictionary is in term order, so terms that sort next to each other take consecutive
        // ordinals and the window over them needs no per-document verification.
        final BytesRef[] docValues = repeated(between(600, 2000));
        final NavigableSet<BytesRef> adjacent = termsOf("alpha", "alpine");
        withColumn(docValues, randomValidBlockSize(), randomChunkCodec(), randomTargetChunkBytes(), dictionaryPolicy(), (m, reader) -> {
            assertEquals(expectedAnyOf(docValues, adjacent), matched(reader.matchAnyOf(adjacent)));
            final TwoPhaseIterator twoPhase = TwoPhaseIterator.unwrap(reader.matchAnyOf(adjacent));
            assertNotNull(twoPhase);
            assertEquals("one run of ordinals settles it", 0f, twoPhase.matchCost(), 0f);
        });
    }

    public void testTermsInManyRunsKeepTheOrdinalBitset() throws IOException {
        final String[] vocabulary = new String[80];
        for (int t = 0; t < vocabulary.length; t++) {
            vocabulary[t] = String.format(Locale.ROOT, "host-%03d", t);
        }
        final BytesRef[] docValues = new BytesRef[between(800, 2000)];
        for (int d = 0; d < docValues.length; d++) {
            docValues[d] = new BytesRef(vocabulary[d % vocabulary.length]);
        }
        // Every other term, so the ordinals fall into more runs than a window is built for.
        final NavigableSet<BytesRef> scattered = new TreeSet<>();
        for (int t = 0; t < vocabulary.length; t += 2) {
            scattered.add(new BytesRef(vocabulary[t]));
        }
        withColumn(docValues, randomValidBlockSize(), randomChunkCodec(), randomTargetChunkBytes(), dictionaryPolicy(), (m, reader) -> {
            assertEquals(expectedAnyOf(docValues, scattered), matched(reader.matchAnyOf(scattered)));
            final TwoPhaseIterator twoPhase = TwoPhaseIterator.unwrap(reader.matchAnyOf(scattered));
            assertNotNull(twoPhase);
            assertEquals("too many runs for a window, so the ordinals are tested per slot", 3f, twoPhase.matchCost(), 0f);
        });
    }

    private static final String[] TERMS = { "alpha", "alpine", "bravo", "charlie", "delta" };

    private static BytesRef[] repeated(int count) {
        final BytesRef[] values = new BytesRef[count];
        for (int d = 0; d < count; d++) {
            values[d] = new BytesRef(TERMS[d % TERMS.length]);
        }
        return values;
    }

    public void testASetOrderedByAnExplicitComparator() throws IOException {
        final BytesRef[] docValues = repeated(between(400, 2000));
        final NavigableSet<BytesRef> naturalOrder = new TreeSet<>(Comparator.naturalOrder());
        naturalOrder.add(new BytesRef(TERMS[0]));
        naturalOrder.add(new BytesRef(TERMS[1]));
        final NavigableSet<BytesRef> reversed = new TreeSet<>(Comparator.reverseOrder());
        reversed.addAll(naturalOrder);
        final DictionaryPolicy policy = randomBoolean() ? DictionaryPolicy.NONE : dictionaryPolicy();
        withColumn(docValues, randomValidBlockSize(), randomChunkCodec(), randomTargetChunkBytes(), policy, (metadata, reader) -> {
            // NOTE: ordering by bytes is the requirement, which a comparator meets as much as natural ordering.
            assertEquals(expectedAnyOf(docValues, naturalOrder), matched(reader.matchAnyOf(naturalOrder)));
            expectThrows(AssertionError.class, () -> reader.matchAnyOf(reversed));
        });
    }

    private static NavigableSet<BytesRef> termsOf(String... values) {
        final NavigableSet<BytesRef> set = new TreeSet<>();
        for (String v : values) {
            set.add(new BytesRef(v));
        }
        return set;
    }

    private static List<Integer> expectedAnyOf(BytesRef[] docValues, NavigableSet<BytesRef> terms) {
        final List<Integer> docs = new ArrayList<>();
        for (int d = 0; d < docValues.length; d++) {
            if (docValues[d] != null && terms.contains(docValues[d])) {
                docs.add(d);
            }
        }
        return docs;
    }

    /** As {@link #assertAnyOf}, checking the values earned a dictionary so the ordinal paths are the ones run. */
    private void assertDictionaryAnyOf(BytesRef[] docValues) throws IOException {
        withColumn(
            docValues,
            randomValidBlockSize(),
            randomChunkCodec(),
            randomTargetChunkBytes(),
            dictionaryPolicy(),
            (metadata, reader) -> {
                assertTrue("expected a dictionary, got " + metadata.layout(), reader.hasDictionary());
                checkAnyOf(docValues, reader);
            }
        );
    }

    private void assertAnyOf(BytesRef[] docValues, DictionaryPolicy policy) throws IOException {
        withColumn(
            docValues,
            randomValidBlockSize(),
            randomChunkCodec(),
            randomTargetChunkBytes(),
            policy,
            (metadata, reader) -> checkAnyOf(docValues, reader)
        );
    }

    private void assertSortedAnyOf(BytesRef[] docValues, DictionaryPolicy policy) throws IOException {
        withColumn(docValues, randomValidBlockSize(), randomChunkCodec(), randomTargetChunkBytes(), policy, (metadata, reader) -> {
            assertTrue("column written in term order", reader.valuesSorted());
            // NOTE: the results match the unordered path, so the iterator's shape is what proves the bisection ran:
            // a dense column answers with the runs themselves, a sparse one with a rank check costing 1.
            final TwoPhaseIterator twoPhase = TwoPhaseIterator.unwrap(reader.matchAnyOf(termsOf("alpha", "delta")));
            if (Arrays.stream(docValues).allMatch(Objects::nonNull)) {
                assertNull("dense sorted column answers without a per-document check", twoPhase);
            } else {
                assertEquals("sparse sorted column checks ranks only", 1f, twoPhase.matchCost(), 0f);
            }
            checkAnyOf(docValues, reader);
            // NOTE: alpha and delta are not adjacent in term order, so their runs leave a gap advance has to cross.
            for (NavigableSet<BytesRef> terms : List.of(termsOf("alpha", "delta"), termsOf("alpine", "bravo", "delta"), termsOf(TERMS))) {
                assertAdvanceAgrees("advance " + terms, expectedAnyOf(docValues, terms), docValues.length, () -> reader.matchAnyOf(terms));
            }
        });
    }

    private void checkAnyOf(BytesRef[] docValues, StringColumnReader reader) throws IOException {
        for (String t : TERMS) {
            final NavigableSet<BytesRef> single = termsOf(t);
            assertEquals("single term " + t, expectedAnyOf(docValues, single), matched(reader.matchAnyOf(single)));
        }
        final NavigableSet<BytesRef> all = termsOf(TERMS);
        assertEquals("all terms", expectedAnyOf(docValues, all), matched(reader.matchAnyOf(all)));
        final NavigableSet<BytesRef> subset = termsOf("alpha", "delta");
        assertEquals("subset terms", expectedAnyOf(docValues, subset), matched(reader.matchAnyOf(subset)));
        final NavigableSet<BytesRef> absent = termsOf("zzz-not-present");
        assertEquals("absent term", List.of(), matched(reader.matchAnyOf(absent)));
        final NavigableSet<BytesRef> partlyAbsent = termsOf("aardvark", "bravo", "zzz-not-present");
        assertEquals("partly absent terms", expectedAnyOf(docValues, partlyAbsent), matched(reader.matchAnyOf(partlyAbsent)));
    }

    private void assertAdvanceAgrees(String label, List<Integer> expected, int docCount, Match match) throws IOException {
        final DocIdSetIterator matches = match.get();
        int target = between(0, 5);
        while (target < docCount) {
            final int doc = matches.advance(target);
            assertEquals(label + " advance(" + target + ")", firstAtOrAfter(expected, target), doc);
            if (doc == DocIdSetIterator.NO_MORE_DOCS) {
                return;
            }
            target = doc + between(1, 40);
        }
    }

    private static int firstAtOrAfter(List<Integer> docs, int target) {
        for (int doc : docs) {
            if (doc >= target) {
                return doc;
            }
        }
        return DocIdSetIterator.NO_MORE_DOCS;
    }

    private static BytesRef[] sorted(BytesRef[] values) {
        final BytesRef[] copy = values.clone();
        Arrays.sort(copy);
        return copy;
    }

    private interface Match {
        DocIdSetIterator get() throws IOException;
    }

    private static List<Integer> matched(DocIdSetIterator matches) throws IOException {
        final List<Integer> docs = new ArrayList<>();
        for (int doc = matches.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = matches.nextDoc()) {
            docs.add(doc);
        }
        return docs;
    }
}
