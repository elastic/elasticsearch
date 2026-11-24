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
import org.apache.lucene.util.StringHelper;
import org.elasticsearch.columnar.numeric.MultiLevelSkipIndexCodec;
import org.elasticsearch.columnar.numeric.NumericColumnMetadata;
import org.elasticsearch.columnar.numeric.SkipIndexCodec;
import org.elasticsearch.core.CheckedFunction;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.NavigableSet;
import java.util.TreeSet;
import java.util.function.Predicate;

import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class DictionaryStringColumnSkipperTests extends ColumnarStringTestCase {

    private static final int INTERVAL = MultiLevelSkipIndexCodec.INTERVAL_SIZE;

    private static final DictionaryPolicy FORCED = new DictionaryPolicy(512 * 1024, 0.0, Double.MAX_VALUE);

    public void testAColumnWithoutEscapesGetsAnIndex() throws IOException {
        final int count = INTERVAL * between(8, 12);
        final BytesRef[][] docSlots = randomBoolean() ? clustered(count, between(2, 3), between(20, 80)) : shuffled(count, between(20, 80));
        withColumn(docSlots, options(), (metadata, reader) -> {
            assertFalse(((StringColumnMetadata.Dictionary) metadata).hasEscapes());
            final NumericColumnMetadata.Skipper skipper = skipperOf(metadata);
            assertNotNull(skipper);
            assertEquals(SkipIndexCodec.MULTI_LEVEL_ID, skipper.codecId());
            assertEquals(count, skipper.docCount());
            assertEquals(count - 1, skipper.maxDocId());
        });
    }

    public void testAColumnWithEscapesGetsNoIndex() throws IOException {
        final int count = INTERVAL * between(8, 12);
        final BytesRef[][] docSlots = randomBoolean() ? clustered(count, 2, between(20, 80)) : shuffled(count, between(20, 80));
        withColumn(withEscapes(docSlots, 700), options(), (metadata, reader) -> {
            assertTrue(((StringColumnMetadata.Dictionary) metadata).hasEscapes());
            assertNull(skipperOf(metadata));
        });
    }

    public void testAColumnWithTheSkipIndexDisabledGetsNoIndex() throws IOException {
        final int count = INTERVAL * between(8, 12);
        withColumn(
            clustered(count, between(2, 3), between(20, 80)),
            options().withoutSkipIndex(),
            (metadata, reader) -> assertNull(skipperOf(metadata))
        );
    }

    public void testAColumnReadInOrderGetsNoIndex() throws IOException {
        final int count = INTERVAL * between(2, 6);
        withColumn(clustered(count, 1, between(20, 80)), options(), (metadata, reader) -> {
            assertTrue(metadata.valuesSorted());
            assertNull(skipperOf(metadata));
        });
    }

    public void testRangeMatchesWhatAScanFinds() throws IOException {
        assertRangesMatchAScan(sparseClusteredDocSlots(), true);
    }

    public void testTermPrefixAndAnyOfMatchWhatAScanFinds() throws IOException {
        final BytesRef[][] docSlots = sparseClusteredDocSlots();
        final List<BytesRef> pool = values(docSlots);
        withColumn(docSlots, options(), (metadata, reader) -> {
            assertNotNull(skipperOf(metadata));
            for (int query = 0; query < 20; query++) {
                final BytesRef term = rarely() ? new BytesRef(randomAlphaOfLength(between(1, 6))) : pool.get(between(0, pool.size() - 1));
                assertMatches(docSlots, value -> value.equals(term), reader.matchTerm(term));
                final BytesRef prefix = new BytesRef(term.utf8ToString().substring(0, between(0, term.length)));
                assertMatches(docSlots, value -> StringHelper.startsWith(value, prefix), reader.matchPrefix(prefix));
                final NavigableSet<BytesRef> terms = new TreeSet<>();
                for (int i = between(1, 4); i > 0; i--) {
                    terms.add(pool.get(between(0, pool.size() - 1)));
                }
                assertMatches(docSlots, terms::contains, reader.matchAnyOf(terms, terms));
            }
        });
    }

    public void testRangeMatchesWhatAScanFindsWithEscapes() throws IOException {
        final int count = between(256, INTERVAL * 6);
        assertRangesMatchAScan(randomDocSlots(count, between(2, 64), between(1, 4), true), false);
    }

    private static BytesRef[][] sparseClusteredDocSlots() {
        final int count = INTERVAL * between(3, 6);
        final BytesRef[][] docSlots = randomDocSlots(count, between(2, 32), between(2, 4), false);
        for (int doc = count - between(1, 5); doc < count; doc++) {
            docSlots[doc] = randomBoolean() ? null : new BytesRef[0];
        }
        return docSlots;
    }

    private static void assertMatches(BytesRef[][] docSlots, Predicate<BytesRef> matches, DocIdSetIterator iterator) throws IOException {
        final List<Integer> expected = expectedWhere(docSlots, matches);
        assertEquals(expected, matched(iterator));
    }

    private void assertRangesMatchAScan(BytesRef[][] docSlots, boolean indexed) throws IOException {
        final int count = docSlots.length;
        final List<BytesRef> pool = values(docSlots);
        withColumn(docSlots, options(), (metadata, reader) -> {
            assertTrue("a dictionary column", metadata instanceof StringColumnMetadata.Dictionary);
            if (indexed) {
                assertFalse(((StringColumnMetadata.Dictionary) metadata).hasEscapes());
                assertNotNull(skipperOf(metadata));
            } else {
                final boolean readInOrder = metadata.valuesSorted() && metadata.hasValueAddresses() == false;
                final boolean hasEscapes = ((StringColumnMetadata.Dictionary) metadata).hasEscapes();
                assertEquals(hasEscapes || readInOrder, skipperOf(metadata) == null);
            }
            for (int query = 0; query < 20; query++) {
                final BytesRef lower = randomBound(pool);
                final BytesRef upper = randomBound(pool);
                final boolean includeLower = randomBoolean();
                final boolean includeUpper = randomBoolean();
                final List<Integer> expected = expectedInRange(docSlots, lower, includeLower, upper, includeUpper);
                final String label = "range " + lower + " " + includeLower + " " + upper + " " + includeUpper;
                assertEquals(label, expected, matched(reader.matchRange(lower, includeLower, upper, includeUpper)));
                assertEquals(
                    label + " in windows",
                    expected,
                    inWindows(reader.matchRange(lower, includeLower, upper, includeUpper), count)
                );
                if (expected.isEmpty() == false) {
                    final int target = between(0, count - 1);
                    final int firstAtOrAfter = expected.stream()
                        .filter(doc -> doc >= target)
                        .findFirst()
                        .orElse(DocIdSetIterator.NO_MORE_DOCS);
                    assertEquals(
                        label + " advance to " + target,
                        firstAtOrAfter,
                        reader.matchRange(lower, includeLower, upper, includeUpper).advance(target)
                    );
                }
            }
        });
    }

    public void testRangesOnASparseColumnFollowDocumentIds() throws IOException {
        final int valued = 3 * INTERVAL + 3;
        final BytesRef[][] docSlots = new BytesRef[2 * valued + 3][];
        for (int rank = 0; rank < valued; rank++) {
            final int group = Math.min(rank / INTERVAL, 3);
            final BytesRef value = switch (group) {
                case 0 -> term(rank % 2);
                case 1 -> term(10 + rank % 2);
                case 2 -> term(20 + rank % 2);
                case 3 -> term(new int[] { 0, 21, 0 }[rank - 3 * INTERVAL]);
                default -> throw new AssertionError(group);
            };
            docSlots[2 * rank + 3] = new BytesRef[] { value };
        }
        withColumn(docSlots, options(), (metadata, reader) -> {
            assertFalse(((StringColumnMetadata.Dictionary) metadata).hasEscapes());
            assertNotNull(skipperOf(metadata));
            for (int[] bounds : new int[][] { { 10, 11 }, { 0, 0 }, { 21, 21 }, { 1, 10 } }) {
                final BytesRef lower = term(bounds[0]);
                final BytesRef upper = term(bounds[1]);
                final List<Integer> expected = expectedInRange(docSlots, lower, true, upper, true);
                assertFalse(expected.isEmpty());
                assertEquals(expected, matched(reader.matchRange(lower, true, upper, true)));
                assertEquals(expected, inWindows(reader.matchRange(lower, true, upper, true), docSlots.length));
            }
        });
    }

    public void testTermPrefixAndAnyOfDecodeOnlyTheOrdinalBlocksOfTheIntervalsTheyCanMatch() throws IOException {
        final int count = INTERVAL * between(12, 16);
        final BytesRef[][] docSlots = clustered(count, 2, 40);
        final int term = between(5, 30);
        final BytesRef target = term(term);
        final BytesRef prefix = new BytesRef(target.utf8ToString().substring(0, 9));
        final NavigableSet<BytesRef> terms = new TreeSet<>(List.of(term(term), term(term + 1)));
        final long wholeColumn = ordinalBlocksDecoded(docSlots, term(0), term(39));
        assertThat(
            "blocks decoded for one term",
            ordinalBlocksDecoded(docSlots, reader -> reader.matchTerm(target), value -> value.equals(target)) * 2,
            lessThanOrEqualTo(wholeColumn)
        );
        assertThat(
            "blocks decoded for a prefix",
            ordinalBlocksDecoded(docSlots, reader -> reader.matchPrefix(prefix), value -> StringHelper.startsWith(value, prefix)) * 2,
            lessThanOrEqualTo(wholeColumn)
        );
        assertThat(
            "blocks decoded for two terms",
            ordinalBlocksDecoded(docSlots, reader -> reader.matchAnyOf(terms, terms), terms::contains) * 2,
            lessThanOrEqualTo(wholeColumn)
        );
    }

    public void testAColumnWithSeveralSlotsInTermOrderStillGetsAnIndex() throws IOException {
        final int count = INTERVAL * between(8, 12);
        final BytesRef[][] docSlots = new BytesRef[count][];
        for (int doc = 0; doc < count; doc++) {
            final BytesRef term = term(doc * 40 / count);
            docSlots[doc] = new BytesRef[] { term, term };
        }
        withColumn(docSlots, options(), (metadata, reader) -> {
            assertTrue("in term order", metadata.valuesSorted());
            assertTrue("with several slots a document", metadata.hasValueAddresses());
            assertNotNull("the reader does not bisect it", skipperOf(metadata));
            for (int query = 0; query < 20; query++) {
                final BytesRef lower = randomBound(values(docSlots));
                final BytesRef upper = randomBound(values(docSlots));
                final boolean includeLower = randomBoolean();
                final boolean includeUpper = randomBoolean();
                assertEquals(
                    "range " + lower + " " + includeLower + " " + upper + " " + includeUpper,
                    expectedInRange(docSlots, lower, includeLower, upper, includeUpper),
                    matched(reader.matchRange(lower, includeLower, upper, includeUpper))
                );
            }
        });
    }

    public void testARangeDecodesOnlyTheOrdinalBlocksOfTheIntervalsItCanMatch() throws IOException {
        final int count = INTERVAL * between(12, 16);
        final BytesRef[][] docSlots = clustered(count, 2, 40);
        final int term = between(5, 30);
        final long narrow = ordinalBlocksDecoded(docSlots, term(term), term(term));
        final long wholeColumn = ordinalBlocksDecoded(docSlots, term(0), term(39));
        assertThat("blocks decoded for one term, for the whole column " + wholeColumn, narrow * 2, lessThanOrEqualTo(wholeColumn));
    }

    private long ordinalBlocksDecoded(BytesRef[][] docSlots, BytesRef lower, BytesRef upper) throws IOException {
        return ordinalBlocksDecoded(
            docSlots,
            reader -> reader.matchRange(lower, true, upper, true),
            value -> inRange(value, lower, true, upper, true)
        );
    }

    private long ordinalBlocksDecoded(
        BytesRef[][] docSlots,
        CheckedFunction<StringColumnReader, DocIdSetIterator, IOException> query,
        Predicate<BytesRef> matching
    ) throws IOException {
        final Counter seeks = new Counter();
        final long[] decoded = new long[1];
        withColumnAndSkipIndex(docSlots, options(), directory -> new SeekCountingDirectory(directory, seeks), (metadata, reader, skip) -> {
            final long before = seeks.get();
            final List<Integer> matches = matched(query.apply(reader));
            assertEquals(expectedWhere(docSlots, matching), matches);
            assertFalse(matches.isEmpty());
            decoded[0] = seeks.get() - before;
        });
        return decoded[0];
    }

    private static NumericColumnMetadata.Skipper skipperOf(StringColumnMetadata metadata) {
        assertTrue("a dictionary column", metadata instanceof StringColumnMetadata.Dictionary);
        return ((StringColumnMetadata.Dictionary) metadata).ordinals().skipper();
    }

    private static StringColumnOptions options() {
        return StringColumnOptions.DEFAULT.withPolicies(FORCED, StringColumnOptions.DEFAULT_SUMMARY);
    }

    private static BytesRef term(int ordinal) {
        return new BytesRef(String.format(Locale.ROOT, "term-%05d", ordinal));
    }

    private static BytesRef[][] clustered(int count, int runs, int terms) {
        final BytesRef[][] docSlots = new BytesRef[count][];
        final int perRun = (count + runs - 1) / runs;
        for (int doc = 0; doc < count; doc++) {
            docSlots[doc] = new BytesRef[] { term((doc % perRun) * terms / perRun) };
        }
        return docSlots;
    }

    private static BytesRef[][] shuffled(int count, int terms) {
        final BytesRef[][] docSlots = new BytesRef[count][];
        for (int doc = 0; doc < count; doc++) {
            docSlots[doc] = new BytesRef[] { term(between(0, terms - 1)) };
        }
        return docSlots;
    }

    private static BytesRef[][] withEscapes(BytesRef[][] docSlots, int every) {
        for (int doc = every / 2; doc < docSlots.length; doc += every) {
            docSlots[doc] = new BytesRef[] { new BytesRef(docSlots[doc][0].utf8ToString() + "-escaped-" + doc) };
        }
        return docSlots;
    }

    private static BytesRef[][] randomDocSlots(int count, int terms, int runs, boolean escapes) {
        final BytesRef[][] clustered = clustered(count, runs, terms);
        final BytesRef[][] docSlots = new BytesRef[count][];
        for (int doc = 0; doc < count; doc++) {
            if (rarely()) {
                continue;
            }
            final int slots = rarely() ? between(0, 3) : 1;
            docSlots[doc] = new BytesRef[slots];
            for (int slot = 0; slot < slots; slot++) {
                if (rarely()) {
                    continue;
                }
                final BytesRef base = slot == 0 ? clustered[doc][0] : term(between(0, terms - 1));
                docSlots[doc][slot] = escapes && rarely() ? new BytesRef(base.utf8ToString() + "-escaped-" + doc) : base;
            }
        }
        return docSlots;
    }

    private static List<BytesRef> values(BytesRef[][] docSlots) {
        final List<BytesRef> values = new ArrayList<>();
        for (BytesRef[] slots : docSlots) {
            if (slots != null) {
                for (BytesRef slot : slots) {
                    if (slot != null) {
                        values.add(slot);
                    }
                }
            }
        }
        return values;
    }

    private static BytesRef randomBound(List<BytesRef> pool) {
        if (rarely() || pool.isEmpty()) {
            return null;
        }
        return rarely() ? new BytesRef(randomAlphaOfLength(between(0, 6))) : pool.get(between(0, pool.size() - 1));
    }

    private static List<Integer> expectedInRange(
        BytesRef[][] docSlots,
        BytesRef lower,
        boolean includeLower,
        BytesRef upper,
        boolean includeUpper
    ) {
        return expectedWhere(docSlots, value -> inRange(value, lower, includeLower, upper, includeUpper));
    }

    private static List<Integer> expectedWhere(BytesRef[][] docSlots, Predicate<BytesRef> matches) {
        final List<Integer> docs = new ArrayList<>();
        for (int doc = 0; doc < docSlots.length; doc++) {
            if (docSlots[doc] == null) {
                continue;
            }
            for (BytesRef value : docSlots[doc]) {
                if (value != null && matches.test(value)) {
                    docs.add(doc);
                    break;
                }
            }
        }
        return docs;
    }

    private static boolean inRange(BytesRef value, BytesRef lower, boolean includeLower, BytesRef upper, boolean includeUpper) {
        if (lower != null) {
            final int cmp = value.compareTo(lower);
            if (cmp < 0 || (cmp == 0 && includeLower == false)) {
                return false;
            }
        }
        if (upper != null) {
            final int cmp = value.compareTo(upper);
            return cmp < 0 || (cmp == 0 && includeUpper);
        }
        return true;
    }

    private static List<Integer> matched(DocIdSetIterator iterator) throws IOException {
        final List<Integer> docs = new ArrayList<>();
        for (int doc = iterator.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = iterator.nextDoc()) {
            docs.add(doc);
        }
        return docs;
    }

    private static List<Integer> inWindows(DocIdSetIterator iterator, int docCount) throws IOException {
        final TwoPhaseIterator twoPhase = TwoPhaseIterator.unwrap(iterator);
        if (twoPhase == null) {
            return matched(iterator);
        }
        final int window = randomFrom(1, 7, 64, 128, 512, docCount + 1);
        final FixedBitSet bits = new FixedBitSet(docCount);
        final DocIdSetIterator approximation = twoPhase.approximation();
        approximation.nextDoc();
        for (int upTo = window; approximation.docID() != DocIdSetIterator.NO_MORE_DOCS; upTo += window) {
            twoPhase.intoBitSet(Math.min(upTo, docCount), bits, 0);
            if (upTo >= docCount) {
                break;
            }
        }
        final List<Integer> docs = new ArrayList<>();
        for (int doc = bits.nextSetBit(0); doc != DocIdSetIterator.NO_MORE_DOCS; doc = doc + 1 < bits.length()
            ? bits.nextSetBit(doc + 1)
            : DocIdSetIterator.NO_MORE_DOCS) {
            docs.add(doc);
        }
        return docs;
    }
}
