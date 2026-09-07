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
import org.apache.lucene.util.automaton.ByteRunAutomaton;
import org.apache.lucene.util.automaton.Operations;
import org.apache.lucene.util.automaton.RegExp;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;

import static org.elasticsearch.columnar.ColumnarTestUtils.randomValidBlockSize;

/**
 * Matching a term or a prefix, over every shape that answers it differently: bisected over ordered values,
 * matched over ordinals, or compared value by value. Each is checked against the documents themselves, so
 * the three paths have to agree with the column and with each other.
 */
public class StringMatchTests extends ColumnarStringTestCase {

    private static final DictionaryPolicy ROOMY = new DictionaryPolicy(512 * 1024, 0.5, 0.2);

    /** Values in term order and no dictionary: the run is found by bisecting the values. */
    public void testSortedPlain() throws IOException {
        assertMatches(sorted(repeated(between(400, 2000))), DictionaryPolicy.NONE, Path.BISECT);
    }

    /** Nothing to bisect and no ordinals: the values are compared. */
    public void testUnsortedPlain() throws IOException {
        assertMatches(repeated(between(400, 2000)), DictionaryPolicy.NONE, Path.SCAN);
    }

    /** A dictionary, so a term is one ordinal and the values are never touched. */
    public void testDictionary() throws IOException {
        assertMatches(repeated(between(400, 2000)), ROOMY, Path.ORDINALS);
    }

    /** A dictionary that let values escape: those can only be found by their bytes. */
    public void testDictionaryWithEscapes() throws IOException {
        final BytesRef[] docValues = repeated(between(600, 2000));
        for (int d = 0; d < docValues.length; d += 60) {
            docValues[d] = new BytesRef("escaped-" + d);
        }
        assertMatches(docValues, ROOMY, Path.ORDINALS);
    }

    /** Documents without a value, so a rank is not a document id and the matches have to be mapped back. */
    public void testSparse() throws IOException {
        final BytesRef[] docValues = repeated(between(400, 2000));
        for (int d = 0; d < docValues.length; d++) {
            if (randomBoolean()) {
                docValues[d] = null;
            }
        }
        assertMatches(docValues, DictionaryPolicy.NONE, Path.SCAN);
    }

    /** Sorted and sparse at once, which is the shape an index sort with missing values produces. */
    public void testSortedAndSparse() throws IOException {
        final BytesRef[] docValues = sorted(repeated(between(400, 2000)));
        for (int d = 0; d < docValues.length; d += 7) {
            docValues[d] = null;
        }
        assertMatches(docValues, DictionaryPolicy.NONE, Path.BISECT);
    }

    /** Values of no bytes are terms like any other, including as a prefix of everything. */
    public void testEmptyValues() throws IOException {
        final BytesRef[] docValues = repeated(between(400, 1500));
        for (int d = 0; d < docValues.length; d += 11) {
            docValues[d] = new BytesRef("");
        }
        assertMatches(docValues, DictionaryPolicy.NONE, Path.SCAN);
    }

    /**
     * A search splits a segment into document ranges and runs them concurrently, so each range advances the
     * iterator to its start and stops at its end. A range holding no match must not pay for the rest of the
     * column, so every range is asked for its own documents and the answers have to add up to the whole.
     */
    public void testAdvanceBySliceOnSortedAndSparse() throws IOException {
        final BytesRef[] docValues = sorted(repeated(between(400, 2000)));
        for (int d = 0; d < docValues.length; d += 7) {
            docValues[d] = null;
        }
        withColumn(
            docValues,
            randomValidBlockSize(),
            randomChunkCodec(),
            randomTargetChunkBytes(),
            DictionaryPolicy.NONE,
            (metadata, reader) -> {
                assertTrue("expected to bisect", reader.valuesSorted() && metadata.multiValued() == false);
                for (String probe : TERMS) {
                    for (int slices : new int[] { 2, 3, 8, 32 }) {
                        final int width = Math.max(1, docValues.length / slices);
                        final List<Integer> found = new ArrayList<>();
                        for (int from = 0; from < docValues.length; from += width) {
                            final int to = Math.min(from + width, docValues.length);
                            final DocIdSetIterator matches = reader.matchTerm(new BytesRef(probe));
                            for (int doc = matches.advance(from); doc < to; doc = matches.nextDoc()) {
                                if (doc == DocIdSetIterator.NO_MORE_DOCS) {
                                    break;
                                }
                                found.add(doc);
                            }
                        }
                        assertEquals("term [" + probe + "] over " + slices + " slices", expected(docValues, probe, true), found);
                    }
                }
            }
        );
    }

    /** Advancing past every match, the shape a range holding none of them sees. */
    public void testAdvancePastAllMatches() throws IOException {
        final BytesRef[] docValues = sorted(repeated(between(400, 2000)));
        for (int d = 0; d < docValues.length; d += 5) {
            docValues[d] = null;
        }
        withColumn(
            docValues,
            randomValidBlockSize(),
            randomChunkCodec(),
            randomTargetChunkBytes(),
            DictionaryPolicy.NONE,
            (metadata, reader) -> {
                for (String probe : TERMS) {
                    final List<Integer> all = expected(docValues, probe, true);
                    if (all.isEmpty()) {
                        continue;
                    }
                    final int beyond = all.get(all.size() - 1) + 1;
                    assertEquals(
                        "term [" + probe + "] past its last match",
                        DocIdSetIterator.NO_MORE_DOCS,
                        reader.matchTerm(new BytesRef(probe)).advance(beyond)
                    );
                }
            }
        );
    }

    /**
     * Whatever shape the column has, a match either answers exactly or hands back a check a caller can run
     * itself. Driving the plain iterator over a range of documents costs the rest of the column when the
     * range holds no match, because it looks for one until it finds it, so a caller working a range at a
     * time has to take the check and stop where its range does. This holds every path to that.
     */
    public void testEveryPathIsExactOrVerifiable() throws IOException {
        record Shape(String name, BytesRef[] values, DictionaryPolicy policy) {}
        final BytesRef[] sortedDense = sorted(repeated(between(400, 2000)));
        final BytesRef[] sortedSparse = sorted(repeated(between(400, 2000)));
        for (int d = 0; d < sortedSparse.length; d += 7) {
            sortedSparse[d] = null;
        }
        final List<Shape> shapes = List.of(
            new Shape("sorted dense", sortedDense, DictionaryPolicy.NONE),
            new Shape("sorted sparse", sortedSparse, DictionaryPolicy.NONE),
            new Shape("dictionary", repeated(between(400, 2000)), ROOMY),
            new Shape("unsorted plain", repeated(between(400, 2000)), DictionaryPolicy.NONE)
        );
        for (Shape shape : shapes) {
            withColumn(
                shape.values(),
                randomValidBlockSize(),
                randomChunkCodec(),
                randomTargetChunkBytes(),
                shape.policy(),
                (metadata, reader) -> {
                    for (String probe : new String[] { "alpha", "nothing-here" }) {
                        final DocIdSetIterator matches = reader.matchTerm(new BytesRef(probe));
                        if (TwoPhaseIterator.unwrap(matches) != null) {
                            continue;
                        }
                        // Answering in one phase is only sound where the match already is a range of
                        // documents: a column ranked by document id whose values are in order, or nothing
                        // matching at all.
                        assertTrue(
                            shape.name() + " term [" + probe + "] answers in one phase without knowing its documents",
                            matches.cost() == 0 || (metadata.iterator().isDense() && reader.valuesSorted())
                        );
                        // Such an iterator lands on or past the target without looking at a value to do it.
                        final int target = shape.values().length / 2;
                        final int landed = matches.advance(target);
                        assertTrue(
                            shape.name() + " term [" + probe + "] advanced to " + landed,
                            landed >= target || landed == DocIdSetIterator.NO_MORE_DOCS
                        );
                    }
                }
            );
        }
    }

    /**
     * A value of no bytes begins where the value stored after it does, so telling a repeat from a new value
     * by where its bytes are has to account for how many there are. Most of the column empty is the shape
     * that catches it: the value following an empty one would otherwise be taken for a repeat of it.
     */
    public void testMostlyEmptyValues() throws IOException {
        final BytesRef[] docValues = new BytesRef[between(2000, 5000)];
        for (int d = 0; d < docValues.length; d++) {
            docValues[d] = random().nextDouble() < 0.88 ? new BytesRef("") : new BytesRef("phrase " + random().nextInt(200));
        }
        for (DictionaryPolicy policy : List.of(DictionaryPolicy.NONE, ROOMY)) {
            withColumn(docValues, randomValidBlockSize(), randomChunkCodec(), randomTargetChunkBytes(), policy, (metadata, reader) -> {
                assertEquals("empty term", expected(docValues, "", true), matched(reader.matchTerm(new BytesRef(""))));
                for (String probe : new String[] { "phrase 1", "phrase 42", "phrase 199" }) {
                    assertEquals("term [" + probe + "]", expected(docValues, probe, true), matched(reader.matchTerm(new BytesRef(probe))));
                }
                // The same values read back a page at a time have to rebuild as themselves.
                final int[] docs = new int[docValues.length];
                for (int d = 0; d < docs.length; d++) {
                    docs[d] = d;
                }
                final List<String> rebuilt = new ArrayList<>();
                final int page = 512;
                for (int from = 0; from < docs.length; from += page) {
                    final int count = Math.min(page, docs.length - from);
                    final int at = from;
                    assertTrue("expected a page", reader.readBlock(docs, from, count, new StringBlockSink() {
                        @Override
                        public void appendOrdinals(int[] ordinals, int n, BytesRef[] dictionary, int dictionarySize) {
                            for (int i = 0; i < n; i++) {
                                rebuilt.add(dictionary[ordinals[i]].utf8ToString());
                            }
                        }

                        @Override
                        public void appendValues(BytesRef[] values, int n) {
                            for (int i = 0; i < n; i++) {
                                rebuilt.add(values[i].utf8ToString());
                            }
                        }
                    }));
                }
                final List<String> want = new ArrayList<>();
                for (BytesRef v : docValues) {
                    want.add(v.utf8ToString());
                }
                assertEquals("values rebuilt from pages", want, rebuilt);
            });
        }
    }

    /**
     * Values in order with empty ones among them, which sort first. Bisecting has to find their run like any
     * other, and a value of no bytes begins where the value stored after it does, so the run a page reports
     * has to tell the two apart.
     */
    public void testSortedWithEmptyValues() throws IOException {
        final BytesRef[] docValues = new BytesRef[between(600, 2000)];
        for (int d = 0; d < docValues.length; d++) {
            docValues[d] = random().nextDouble() < 0.3 ? new BytesRef("") : new BytesRef(TERMS[d % (TERMS.length - 1)]);
        }
        assertMatches(sorted(docValues), DictionaryPolicy.NONE, Path.BISECT);
    }

    /** The same, on a column that names its values with ordinals. */
    public void testSortedWithEmptyValuesAndADictionary() throws IOException {
        final BytesRef[] docValues = new BytesRef[between(600, 2000)];
        for (int d = 0; d < docValues.length; d++) {
            docValues[d] = random().nextDouble() < 0.3 ? new BytesRef("") : new BytesRef(TERMS[d % (TERMS.length - 1)]);
        }
        assertMatches(sorted(docValues), ROOMY, Path.BISECT);
    }

    /**
     * Collecting a window at a time has to find the same documents as asking one at a time. A dictionary
     * column confirms a whole block of ordinals in one pass, so this is the path that pass is on, and the
     * two ways of asking are compared against each other over every window boundary that matters.
     */
    public void testWindowedCollectionAgreesWithPerDocument() throws IOException {
        for (DictionaryPolicy policy : List.of(ROOMY, DictionaryPolicy.NONE)) {
            final BytesRef[] docValues = repeated(between(600, 2000));
            withColumn(docValues, randomValidBlockSize(), randomChunkCodec(), randomTargetChunkBytes(), policy, (metadata, reader) -> {
                for (String probe : TERMS) {
                    assertWindowedAgrees("term [" + probe + "]", docValues.length, () -> reader.matchTerm(new BytesRef(probe)));
                    assertWindowedAgrees("contains [" + probe + "]", docValues.length, () -> reader.matchContains(new BytesRef(probe)));
                }
            });
        }
    }

    /**
     * A window collected from a column that let values escape has to agree with asking one document at a
     * time, for a term the dictionary holds and for one only an escaped value carries.
     *
     * <p>An ordinal is enough to decide a value the dictionary named, and a block of them is tested at once.
     * An escaped value has no ordinal but the marker, which says only that its bytes are elsewhere, so a
     * column holding any of them cannot be answered from ordinals alone however the documents are asked for.
     */
    public void testWindowedCollectionWithEscapes() throws IOException {
        final BytesRef[] docValues = new BytesRef[between(600, 2000)];
        for (int d = 0; d < docValues.length; d++) {
            // Rare enough to escape a dictionary built from the terms the column repeats.
            docValues[d] = d % 37 == 5 ? new BytesRef("escaped-" + d) : new BytesRef(TERMS[d % (TERMS.length - 1)]);
        }
        withColumn(docValues, randomValidBlockSize(), randomChunkCodec(), randomTargetChunkBytes(), ROOMY, (metadata, reader) -> {
            assertTrue("expected a dictionary", reader.hasDictionary());
            assertTrue("expected values to have escaped it", reader.escapeCount() > 0);
            final List<String> probes = new ArrayList<>(Arrays.asList(TERMS));
            // Carried by an escaped value and by nothing the dictionary names.
            probes.add("escaped-5");
            for (String probe : probes) {
                assertWindowedAgrees("term [" + probe + "]", docValues.length, () -> reader.matchTerm(new BytesRef(probe)));
                assertWindowedAgrees("contains [" + probe + "]", docValues.length, () -> reader.matchContains(new BytesRef(probe)));
            }
        });
    }

    /**
     * A column ranked by document id whose values are in order answers with the range of documents itself.
     * Bisecting the values leaves the ends of one run and nothing about a document is left to check, so the
     * iterator carries no second phase for a caller to run.
     */
    public void testSortedDenseColumnAnswersInOnePhase() throws IOException {
        final BytesRef[] docValues = sorted(repeated(between(400, 2000)));
        withColumn(
            docValues,
            randomValidBlockSize(),
            randomChunkCodec(),
            randomTargetChunkBytes(),
            DictionaryPolicy.NONE,
            (metadata, reader) -> {
                assertTrue("expected a column every document has a value in", metadata.iterator().isDense());
                assertTrue("expected values in term order", reader.valuesSorted());
                final DocIdSetIterator matches = reader.matchTerm(new BytesRef("alpha"));
                assertNull("a sorted dense column has nothing left to check", TwoPhaseIterator.unwrap(matches));
                assertTrue("expected the term to match something", matches.cost() > 0);
                assertEquals(
                    "the one phase answers what a check would",
                    matched(reader.matchTerm(new BytesRef("alpha"))),
                    matched(matches)
                );
            }
        );
    }

    /**
     * A value contains a term or it does not, whatever order the column is in and whichever layout it took.
     * The shapes that matter are the ones where a column decides some values from its dictionary and the
     * rest from their own bytes.
     */
    public void testContains() throws IOException {
        record Shape(String name, BytesRef[] values, DictionaryPolicy policy) {}
        final BytesRef[] plain = repeated(between(400, 1500));
        final BytesRef[] sorted = sorted(repeated(between(400, 1500)));
        final BytesRef[] withEscapes = new BytesRef[between(600, 1500)];
        for (int d = 0; d < withEscapes.length; d++) {
            withEscapes[d] = d % 40 == 3 ? new BytesRef("rare-alpine-" + d) : new BytesRef(TERMS[d % (TERMS.length - 1)]);
        }
        final BytesRef[] sparse = repeated(between(400, 1500));
        for (int d = 0; d < sparse.length; d += 5) {
            sparse[d] = null;
        }
        // Empty values beside others, so a value that begins where the next one does is searched as itself.
        final BytesRef[] mostlyEmpty = new BytesRef[between(600, 1500)];
        for (int d = 0; d < mostlyEmpty.length; d++) {
            mostlyEmpty[d] = random().nextDouble() < 0.88 ? new BytesRef("") : new BytesRef("phrase " + random().nextInt(200));
        }
        final List<Shape> shapes = List.of(
            new Shape("plain", plain, DictionaryPolicy.NONE),
            new Shape("sorted plain", sorted, DictionaryPolicy.NONE),
            new Shape("dictionary", plain, ROOMY),
            new Shape("dictionary with escapes", withEscapes, ROOMY),
            new Shape("sparse", sparse, DictionaryPolicy.NONE),
            new Shape("mostly empty", mostlyEmpty, DictionaryPolicy.NONE),
            new Shape("mostly empty with a dictionary", mostlyEmpty, ROOMY)
        );
        // Inside a term, at its start, at its end, spanning nothing, and absent.
        final String[] probes = { "lph", "alpha", "pha", "a", "", "zzz", "rare", "alpine" };
        for (Shape shape : shapes) {
            withColumn(
                shape.values(),
                randomValidBlockSize(),
                randomChunkCodec(),
                randomTargetChunkBytes(),
                shape.policy(),
                (metadata, reader) -> {
                    for (String probe : probes) {
                        assertEquals(
                            shape.name() + " contains [" + probe + "]",
                            containing(shape.values(), probe),
                            matched(reader.matchContains(new BytesRef(probe)))
                        );
                    }
                }
            );
        }
    }

    /**
     * A test the column cannot narrow, which is what a pattern comes to once it is an automaton. Nothing about
     * the order of the values helps, so every distinct value has to be offered it and the column's shape only
     * decides how many that is. Whatever it decides, the documents have to be the ones a test of every value
     * would find.
     */
    public void testMatchPredicate() throws IOException {
        for (Shape shape : shapes()) {
            withColumn(
                shape.values(),
                randomValidBlockSize(),
                randomChunkCodec(),
                randomTargetChunkBytes(),
                shape.policy(),
                (metadata, reader) -> {
                    for (String pattern : PATTERNS) {
                        final ByteRunAutomaton automaton = byteRunAutomaton(pattern);
                        assertEquals(
                            shape.name() + " matching [" + pattern + "]",
                            accepted(shape.values(), automaton),
                            matched(reader.match(run(automaton)))
                        );
                    }
                }
            );
        }
    }

    /**
     * The point of naming values with ordinals: a term the dictionary holds is tested once however many
     * documents name it, so what a match costs follows the number of distinct values rather than the number of
     * documents. Only a value that escaped the dictionary is tested on its own.
     */
    public void testADictionaryTestsATermOnce() throws IOException {
        final BytesRef[] docValues = new BytesRef[between(2000, 4000)];
        for (int d = 0; d < docValues.length; d++) {
            docValues[d] = d % 41 == 3 ? new BytesRef("escaped-" + d) : new BytesRef(TERMS[d % (TERMS.length - 1)]);
        }
        withColumn(docValues, randomValidBlockSize(), randomChunkCodec(), randomTargetChunkBytes(), ROOMY, (metadata, reader) -> {
            assertTrue("expected a dictionary", reader.hasDictionary());
            assertTrue("expected values to have escaped it", reader.escapeCount() > 0);
            final ByteRunAutomaton automaton = byteRunAutomaton("al.*");
            final AtomicInteger tests = new AtomicInteger();
            final DocIdSetIterator matches = reader.match(value -> {
                tests.incrementAndGet();
                return automaton.run(value.bytes, value.offset, value.length);
            });
            assertEquals("documents", accepted(docValues, automaton), matched(matches));
            assertTrue(
                "expected at most one test a distinct value, got " + tests.get() + " over " + docValues.length + " documents",
                tests.get() <= reader.dictionarySize() + reader.escapeCount()
            );
            assertTrue("expected fewer tests than documents", tests.get() < docValues.length);
        });
    }

    /** Collecting a window of an opaque test has to find the same documents as asking one at a time. */
    public void testWindowedPredicateAgreesWithPerDocument() throws IOException {
        for (Shape shape : shapes()) {
            withColumn(
                shape.values(),
                randomValidBlockSize(),
                randomChunkCodec(),
                randomTargetChunkBytes(),
                shape.policy(),
                (metadata, reader) -> {
                    for (String pattern : PATTERNS) {
                        final ByteRunAutomaton automaton = byteRunAutomaton(pattern);
                        assertWindowedAgrees(
                            shape.name() + " matching [" + pattern + "]",
                            shape.values().length,
                            () -> reader.match(run(automaton))
                        );
                    }
                }
            );
        }
    }

    /** A column no document has a value in answers nothing, whatever it is asked. */
    public void testMatchPredicateOnEmptyColumn() throws IOException {
        final BytesRef[] docValues = new BytesRef[between(400, 1500)];
        withColumn(docValues, randomValidBlockSize(), randomChunkCodec(), randomTargetChunkBytes(), ROOMY, (metadata, reader) -> {
            assertEquals(List.of(), matched(reader.match(value -> true)));
        });
    }

    /** A pattern anchored on nothing, so the answer is every document that has a value at all. */
    public void testMatchPredicateAcceptingEverything() throws IOException {
        for (Shape shape : shapes()) {
            withColumn(
                shape.values(),
                randomValidBlockSize(),
                randomChunkCodec(),
                randomTargetChunkBytes(),
                shape.policy(),
                (metadata, reader) -> {
                    final List<Integer> withAValue = new ArrayList<>();
                    for (int d = 0; d < shape.values().length; d++) {
                        if (shape.values()[d] != null) {
                            withAValue.add(d);
                        }
                    }
                    assertEquals(shape.name() + " accepting everything", withAValue, matched(reader.match(value -> true)));
                }
            );
        }
    }

    /** The pattern as a test of bytes, determinized the way a query would determinize it. */
    private static ByteRunAutomaton byteRunAutomaton(String pattern) {
        return new ByteRunAutomaton(Operations.determinize(new RegExp(pattern).toAutomaton(), Operations.DEFAULT_DETERMINIZE_WORK_LIMIT));
    }

    private static Predicate<BytesRef> run(ByteRunAutomaton automaton) {
        return value -> automaton.run(value.bytes, value.offset, value.length);
    }

    /** Inside a term, anchored at either end, spanning nothing, and matching nothing the column holds. */
    private static final String[] PATTERNS = { "al.*", ".*pha", "alpha", "[bc].*", ".*", "(alpha)?", "zzz.*", "escaped-.*" };

    /** The documents a test of every value would find. */
    private static List<Integer> accepted(BytesRef[] docValues, ByteRunAutomaton automaton) {
        final List<Integer> docs = new ArrayList<>();
        for (int d = 0; d < docValues.length; d++) {
            final BytesRef value = docValues[d];
            if (value != null && automaton.run(value.bytes, value.offset, value.length)) {
                docs.add(d);
            }
        }
        return docs;
    }

    /** The shape of a column, and the values that give it that shape. */
    private record Shape(String name, BytesRef[] values, DictionaryPolicy policy) {}

    /** Every way a column answers a test of its values, including the two that mix decided and undecided ones. */
    private List<Shape> shapes() {
        final BytesRef[] plain = repeated(between(400, 1500));
        final BytesRef[] withEscapes = new BytesRef[between(600, 1500)];
        for (int d = 0; d < withEscapes.length; d++) {
            withEscapes[d] = d % 40 == 3 ? new BytesRef("escaped-" + d) : new BytesRef(TERMS[d % (TERMS.length - 1)]);
        }
        final BytesRef[] sparse = repeated(between(400, 1500));
        for (int d = 0; d < sparse.length; d += 5) {
            sparse[d] = null;
        }
        final BytesRef[] mostlyEmpty = new BytesRef[between(600, 1500)];
        for (int d = 0; d < mostlyEmpty.length; d++) {
            mostlyEmpty[d] = random().nextDouble() < 0.88 ? new BytesRef("") : new BytesRef("phrase " + random().nextInt(200));
        }
        return List.of(
            new Shape("plain", plain, DictionaryPolicy.NONE),
            new Shape("sorted plain", sorted(repeated(between(400, 1500))), DictionaryPolicy.NONE),
            new Shape("dictionary", plain, ROOMY),
            new Shape("dictionary with escapes", withEscapes, ROOMY),
            new Shape("sparse", sparse, DictionaryPolicy.NONE),
            new Shape("mostly empty", mostlyEmpty, DictionaryPolicy.NONE),
            new Shape("mostly empty with a dictionary", mostlyEmpty, ROOMY)
        );
    }

    /** The documents a search of every value would find. */
    private static List<Integer> containing(BytesRef[] docValues, String probe) {
        final List<Integer> docs = new ArrayList<>();
        for (int d = 0; d < docValues.length; d++) {
            if (docValues[d] != null && docValues[d].utf8ToString().contains(probe)) {
                docs.add(d);
            }
        }
        return docs;
    }

    /**
     * Documents holding several slots, some of them null, matched under both layouts. A document matches on
     * any one of its slots, and a null on none of them — including the empty term, which it stores the same
     * no bytes as. That is what the two layouts keep apart differently: a dictionary names a null with an
     * ordinal below every term's, while a plain column has only its null-slot table to go on.
     */
    public void testMultiValuedWithNullSlots() throws IOException {
        final BytesRef[][] docSlots = new BytesRef[between(600, 2000)][];
        for (int d = 0; d < docSlots.length; d++) {
            final List<BytesRef> slots = new ArrayList<>();
            // At least one value, so the document is one the mapper would have written a field for.
            slots.add(new BytesRef(TERMS[d % (TERMS.length - 1)]));
            for (int extra = between(0, 2); extra > 0; extra--) {
                slots.add(randomBoolean() ? null : new BytesRef(TERMS[randomInt(TERMS.length - 1)]));
            }
            java.util.Collections.shuffle(slots, random());
            docSlots[d] = slots.toArray(new BytesRef[0]);
        }
        for (DictionaryPolicy policy : List.of(DictionaryPolicy.NONE, ROOMY)) {
            withColumn(docSlots, randomValidBlockSize(), randomChunkCodec(), randomTargetChunkBytes(), policy, (metadata, reader) -> {
                assertTrue("expected a multi-valued column", metadata.multiValued());
                assertTrue("expected null slots", metadata.hasNullSlots());
                for (String probe : TERMS) {
                    assertEquals(
                        "term [" + probe + "]",
                        expectedOfSlots(docSlots, probe, true),
                        matched(reader.matchTerm(new BytesRef(probe)))
                    );
                }
                for (String probe : new String[] { "al", "b", "zzz", "" }) {
                    assertEquals(
                        "prefix [" + probe + "]",
                        expectedOfSlots(docSlots, probe, false),
                        matched(reader.matchPrefix(new BytesRef(probe)))
                    );
                }
            });
        }
    }

    /**
     * A window collected from a multi-valued column has to agree with asking one document at a time, as it
     * does on a single-valued one. A dictionary column with nothing escaped confirms a whole block of
     * ordinals in one pass without resolving a value, and a document there matches on any one of its slots,
     * so that pass has to walk the slots rather than take a document's rank for the address of its value.
     */
    public void testWindowedCollectionOnAMultiValuedDictionary() throws IOException {
        final BytesRef[][] docSlots = new BytesRef[between(600, 2000)][];
        for (int d = 0; d < docSlots.length; d++) {
            final BytesRef[] slots = new BytesRef[between(1, 3)];
            for (int s = 0; s < slots.length; s++) {
                // Sparse enough that the terms still cover the column well enough to be worth a dictionary.
                slots[s] = random().nextDouble() < 0.15 ? null : new BytesRef(TERMS[randomInt(TERMS.length - 1)]);
            }
            docSlots[d] = slots;
        }
        for (DictionaryPolicy policy : List.of(ROOMY, DictionaryPolicy.NONE)) {
            withColumn(docSlots, randomValidBlockSize(), randomChunkCodec(), randomTargetChunkBytes(), policy, (metadata, reader) -> {
                assertTrue("expected a multi-valued column", metadata.multiValued());
                assertTrue("expected null slots", metadata.hasNullSlots());
                if (policy == ROOMY) {
                    assertTrue("expected a dictionary", reader.hasDictionary());
                    assertEquals("expected nothing to escape", 0, reader.escapeCount());
                }
                for (String probe : TERMS) {
                    assertWindowedAgrees("term [" + probe + "]", docSlots.length, () -> reader.matchTerm(new BytesRef(probe)));
                    assertWindowedAgrees("contains [" + probe + "]", docSlots.length, () -> reader.matchContains(new BytesRef(probe)));
                }
            });
        }
    }

    /**
     * A multi-valued dictionary that let values escape. An escape has no ordinal but the marker, so the
     * window cannot be filled from the ordinals and every document is decided on its own — and finding an
     * escaped value's bytes means counting the escapes before its address, which on a multi-valued column is
     * not the same number as the escapes before its document.
     */
    public void testMultiValuedDictionaryWithEscapes() throws IOException {
        final BytesRef[][] docSlots = new BytesRef[between(600, 2000)][];
        for (int d = 0; d < docSlots.length; d++) {
            final BytesRef[] slots = new BytesRef[between(1, 3)];
            for (int s = 0; s < slots.length; s++) {
                if (random().nextDouble() < 0.1) {
                    slots[s] = null;
                } else if ((d + s) % 37 == 5) {
                    // Rare enough to escape a dictionary built from the terms the column repeats.
                    slots[s] = new BytesRef("escaped-" + d + "-" + s);
                } else {
                    slots[s] = new BytesRef(TERMS[randomInt(TERMS.length - 1)]);
                }
            }
            docSlots[d] = slots;
        }
        withColumn(docSlots, randomValidBlockSize(), randomChunkCodec(), randomTargetChunkBytes(), ROOMY, (metadata, reader) -> {
            assertTrue("expected a dictionary", reader.hasDictionary());
            assertTrue("expected a multi-valued column", metadata.multiValued());
            assertTrue("expected values to have escaped it", reader.escapeCount() > 0);
            final List<String> probes = new ArrayList<>(Arrays.asList(TERMS));
            probes.add("escaped-5-0");
            probes.add("escaped-42-1");
            for (String probe : probes) {
                assertEquals(
                    "term [" + probe + "]",
                    expectedOfSlots(docSlots, probe, true),
                    matched(reader.matchTerm(new BytesRef(probe)))
                );
                assertWindowedAgrees("term [" + probe + "]", docSlots.length, () -> reader.matchTerm(new BytesRef(probe)));
                assertWindowedAgrees("contains [" + probe + "]", docSlots.length, () -> reader.matchContains(new BytesRef(probe)));
            }
        });
    }

    /**
     * A multi-valued column whose slots happen to arrive in term order. The writer decides sortedness over
     * the flat run of slots, so this really can be sorted, but a rank is not a value address here and the
     * bisection is over ranks — so the match has to fall through to comparing the slots instead. This pins
     * that it does, and that the answer is the same either way.
     */
    public void testSortedMultiValuedFallsBackToScanning() throws IOException {
        final List<BytesRef> flat = new ArrayList<>();
        for (int i = 0; i < between(600, 2000); i++) {
            flat.add(new BytesRef(TERMS[randomInt(TERMS.length - 1)]));
        }
        flat.sort(BytesRef::compareTo);
        final List<BytesRef[]> rows = new ArrayList<>();
        for (int at = 0; at < flat.size();) {
            final int width = Math.min(between(1, 3), flat.size() - at);
            rows.add(flat.subList(at, at + width).toArray(new BytesRef[0]));
            at += width;
        }
        final BytesRef[][] docSlots = rows.toArray(new BytesRef[0][]);
        for (DictionaryPolicy policy : List.of(DictionaryPolicy.NONE, ROOMY)) {
            withColumn(docSlots, randomValidBlockSize(), randomChunkCodec(), randomTargetChunkBytes(), policy, (metadata, reader) -> {
                assertTrue("expected a multi-valued column", metadata.multiValued());
                assertTrue("expected the slots to be in term order", reader.valuesSorted());
                for (String probe : TERMS) {
                    assertEquals(
                        "term [" + probe + "]",
                        expectedOfSlots(docSlots, probe, true),
                        matched(reader.matchTerm(new BytesRef(probe)))
                    );
                    assertEquals(
                        "prefix [" + probe + "]",
                        expectedOfSlots(docSlots, probe, false),
                        matched(reader.matchPrefix(new BytesRef(probe)))
                    );
                }
            });
        }
    }

    /**
     * A document whose only slot is null. The mapper writes a payload for an all-null array, so this is a
     * shape the codec sees, and it leaves the slots and the documents in step — the column is not
     * multi-valued and a document's rank is still its address. A null is stored as no bytes, so every path
     * that decides a lone slot has to ask whether it is one before comparing it with the empty term.
     */
    public void testLoneNullSlots() throws IOException {
        final BytesRef[][] docSlots = new BytesRef[between(600, 2000)][];
        for (int d = 0; d < docSlots.length; d++) {
            // A quarter of the column null, the rest one value each, so the slots stay in step with the
            // documents and nothing widens the column into a multi-valued one.
            docSlots[d] = new BytesRef[] { random().nextDouble() < 0.25 ? null : new BytesRef(TERMS[d % TERMS.length]) };
        }
        for (DictionaryPolicy policy : List.of(DictionaryPolicy.NONE, ROOMY)) {
            withColumn(docSlots, randomValidBlockSize(), randomChunkCodec(), randomTargetChunkBytes(), policy, (metadata, reader) -> {
                assertFalse("expected the slots to stay in step with the documents", metadata.multiValued());
                assertTrue("expected null slots", metadata.hasNullSlots());
                for (String probe : TERMS) {
                    assertEquals(
                        "term [" + probe + "]",
                        expectedOfSlots(docSlots, probe, true),
                        matched(reader.matchTerm(new BytesRef(probe)))
                    );
                    assertWindowedAgrees("term [" + probe + "]", docSlots.length, () -> reader.matchTerm(new BytesRef(probe)));
                }
                // The empty term is the one a null would answer for if its bytes were all that separated them.
                assertEquals("empty term", expectedOfSlots(docSlots, "", true), matched(reader.matchTerm(new BytesRef(""))));
                assertEquals("empty prefix", expectedOfSlots(docSlots, "", false), matched(reader.matchPrefix(new BytesRef(""))));
                assertEquals(
                    "matcher accepting no bytes",
                    expectedOfSlots(docSlots, "", true),
                    matched(reader.match(value -> value.length == 0))
                );
                // A page has no shape for a slot holding nothing, so the column declines to serve one.
                final int[] docs = new int[Math.min(256, docSlots.length)];
                for (int i = 0; i < docs.length; i++) {
                    docs[i] = i;
                }
                assertFalse("a column with null slots serves no page", reader.readBlock(docs, 0, docs.length, new StringBlockSink() {
                    @Override
                    public void appendOrdinals(int[] ordinals, int n, BytesRef[] dictionary, int dictionarySize) {
                        fail("no page expected");
                    }

                    @Override
                    public void appendValues(BytesRef[] values, int n) {
                        fail("no page expected");
                    }
                }));
                assertFalse("nor column-wide ordinals", reader.readOrdinals(docs, 0, docs.length, new int[docs.length]));
            });
        }
    }

    /**
     * A document holding no slot at all — an empty array — on a column whose values are in term order. Fewer
     * slots than documents leaves the column not multi-valued while a rank has still stopped being its own
     * value address, so a bisection over ranks would answer with ranks rather than with documents. This is
     * the shape that tells the two questions apart.
     */
    public void testEmptyArrayOnASortedColumn() throws IOException {
        final BytesRef[][] docSlots = new BytesRef[between(200, 1200)][];
        for (int d = 0; d < docSlots.length; d++) {
            docSlots[d] = d % 5 == 0 ? new BytesRef[0] : new BytesRef[] { new BytesRef(TERMS[(d * (TERMS.length - 1)) / docSlots.length]) };
        }
        for (DictionaryPolicy policy : List.of(DictionaryPolicy.NONE, ROOMY)) {
            withColumn(docSlots, randomValidBlockSize(), randomChunkCodec(), randomTargetChunkBytes(), policy, (metadata, reader) -> {
                assertFalse("fewer slots than documents is not multi-valued", metadata.multiValued());
                assertTrue("but a rank is no longer its own address", metadata.hasValueAddresses());
                assertTrue("expected the values to be in term order", reader.valuesSorted());
                for (String probe : TERMS) {
                    assertEquals(
                        "term [" + probe + "]",
                        expectedOfSlots(docSlots, probe, true),
                        matched(reader.matchTerm(new BytesRef(probe)))
                    );
                    assertEquals(
                        "prefix [" + probe + "]",
                        expectedOfSlots(docSlots, probe, false),
                        matched(reader.matchPrefix(new BytesRef(probe)))
                    );
                }
                // A page is addressed by rank too, so it has to decline this column for the same reason.
                final int[] docs = new int[Math.min(256, docSlots.length)];
                for (int i = 0; i < docs.length; i++) {
                    docs[i] = i;
                }
                assertFalse("a column with empty arrays serves no page", reader.readBlock(docs, 0, docs.length, new StringBlockSink() {
                    @Override
                    public void appendOrdinals(int[] ordinals, int n, BytesRef[] dictionary, int dictionarySize) {
                        fail("no page expected");
                    }

                    @Override
                    public void appendValues(BytesRef[] values, int n) {
                        fail("no page expected");
                    }
                }));
                assertFalse("nor column-wide ordinals", reader.readOrdinals(docs, 0, docs.length, new int[docs.length]));
            });
        }
    }

    /** The documents a scan over the slots themselves would find, a null being no value to compare. */
    private static List<Integer> expectedOfSlots(BytesRef[][] docSlots, String probe, boolean exact) {
        final List<Integer> docs = new ArrayList<>();
        for (int d = 0; d < docSlots.length; d++) {
            for (BytesRef slot : docSlots[d]) {
                if (slot == null) {
                    continue;
                }
                final String value = slot.utf8ToString();
                if (exact ? value.equals(probe) : value.startsWith(probe)) {
                    docs.add(d);
                    break;
                }
            }
        }
        return docs;
    }

    private static final String[] TERMS = { "alpha", "alpine", "bravo", "charlie", "delta", "" };

    private BytesRef[] repeated(int count) {
        final BytesRef[] values = new BytesRef[count];
        for (int d = 0; d < count; d++) {
            values[d] = new BytesRef(TERMS[d % (TERMS.length - 1)]);
        }
        return values;
    }

    private static BytesRef[] sorted(BytesRef[] values) {
        final BytesRef[] copy = values.clone();
        Arrays.sort(copy, (a, b) -> a == null ? (b == null ? 0 : -1) : (b == null ? 1 : a.compareTo(b)));
        return copy;
    }

    /** Which of the three ways of answering a match the column's shape should reach. */
    private enum Path {
        BISECT,
        ORDINALS,
        SCAN
    }

    private void assertMatches(BytesRef[] docValues, DictionaryPolicy policy, Path path) throws IOException {
        withColumn(docValues, randomValidBlockSize(), randomChunkCodec(), randomTargetChunkBytes(), policy, (metadata, reader) -> {
            switch (path) {
                case BISECT -> assertTrue("expected to bisect", reader.valuesSorted() && metadata.multiValued() == false);
                case ORDINALS -> {
                    assertFalse("expected ordinals, not bisection", reader.valuesSorted());
                    assertTrue("expected a dictionary", reader.hasDictionary());
                }
                case SCAN -> {
                    assertFalse("expected a scan, not bisection", reader.valuesSorted());
                    assertFalse("expected a scan, not ordinals", reader.hasDictionary());
                }
            }
            for (String probe : TERMS) {
                assertEquals("term [" + probe + "]", expected(docValues, probe, true), matched(reader.matchTerm(new BytesRef(probe))));
            }
            // A term the column does not hold matches nothing.
            assertEquals("absent term", List.of(), matched(reader.matchTerm(new BytesRef("nothing-here"))));
            // "az" sorts between alpine and bravo, so bisecting lands on a value the prefix does not name
            // rather than past the end. A lower bound that was off by one would answer with bravo's run.
            for (String probe : new String[] { "al", "alp", "b", "d", "az", "zzz", "" }) {
                assertEquals("prefix [" + probe + "]", expected(docValues, probe, false), matched(reader.matchPrefix(new BytesRef(probe))));
            }
        });
    }

    /** The documents a scan over the values themselves would find. */
    private static List<Integer> expected(BytesRef[] docValues, String probe, boolean exact) {
        final List<Integer> docs = new ArrayList<>();
        for (int d = 0; d < docValues.length; d++) {
            if (docValues[d] == null) {
                continue;
            }
            final String value = docValues[d].utf8ToString();
            if (exact ? value.equals(probe) : value.startsWith(probe)) {
                docs.add(d);
            }
        }
        return docs;
    }

    /** A match to run again, since collecting a window consumes the iterator it is collected from. */
    @FunctionalInterface
    private interface Match {
        DocIdSetIterator get() throws IOException;
    }

    /**
     * Collecting a match in windows of any size has to agree with asking one document at a time. A window is
     * what a scorer fills, so this is the shape a search reads a filter through, and a filter that answers a
     * block of documents at once has to leave the same answer as one that is asked about each of them.
     */
    private void assertWindowedAgrees(String label, int docCount, Match match) throws IOException {
        final List<Integer> oneAtATime = matched(match.get());
        for (int window : new int[] { 1, 7, 64, 128, 512, docCount + 1 }) {
            final TwoPhaseIterator twoPhase = TwoPhaseIterator.unwrap(match.get());
            if (twoPhase == null) {
                continue; // answers exactly, so there is no window to collect
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
}
