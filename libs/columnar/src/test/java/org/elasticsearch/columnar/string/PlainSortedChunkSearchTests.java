/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar.string;

import org.apache.lucene.util.BytesRef;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.TreeSet;

import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.lessThan;

public class PlainSortedChunkSearchTests extends ColumnarStringTestCase {

    private static final int VALUES_PER_BLOCK = 128;

    private static StringColumnOptions.Sizes smallChunks() {
        return PlainStringColumnSkipperTests.sizes(VALUES_PER_BLOCK, randomFrom(256, 512, 1024), randomFrom(128, 256, 1024));
    }

    /** One value for each document in term order, each value repeated {@code repeat} times. */
    private static BytesRef[][] ascending(BytesRef[] pool, int repeat) {
        final BytesRef[][] docSlots = new BytesRef[pool.length * repeat][];
        for (int doc = 0; doc < docSlots.length; doc++) {
            docSlots[doc] = new BytesRef[] { pool[doc / repeat] };
        }
        return docSlots;
    }

    private static int[] entryStarts(BytesRef[][] docSlots, StringColumnOptions.Sizes sizes) {
        final List<BytesRef> slots = new ArrayList<>();
        for (BytesRef[] slotsOfDoc : docSlots) {
            if (slotsOfDoc != null) {
                slots.addAll(Arrays.asList(slotsOfDoc));
            }
        }
        final int[] owners = PlainChunkModel.owners(slots, sizes.plainChunks(), sizes.valuesPerBlock(), sizes.lengthBlockSize());
        final List<Integer> starts = new ArrayList<>();
        for (int slot = 0; slot < owners.length; slot++) {
            if (slot == 0 || owners[slot] != owners[slot - 1]) {
                starts.add(slot);
            }
        }
        return starts.stream().mapToInt(Integer::intValue).toArray();
    }

    /** The values around the edges of some chunks, and just above and below each, which is where a bound can disagree with a separator. */
    private static List<BytesRef> boundsAround(BytesRef[][] docSlots, int[] starts) {
        final List<BytesRef> slots = new ArrayList<>();
        for (BytesRef[] slotsOfDoc : docSlots) {
            if (slotsOfDoc != null) {
                slots.addAll(Arrays.asList(slotsOfDoc));
            }
        }
        final TreeSet<BytesRef> bounds = new TreeSet<>();
        final List<Integer> edges = new ArrayList<>();
        edges.add(0);
        edges.add(slots.size() - 1);
        for (int i = 0; i < 6; i++) {
            final int start = starts[between(0, starts.length - 1)];
            edges.add(start);
            edges.add(Math.max(0, start - 1));
        }
        for (int edge : edges) {
            final BytesRef value = slots.get(edge);
            bounds.add(BytesRef.deepCopyOf(value));
            bounds.add(new BytesRef(Arrays.copyOf(value.bytes, value.offset + value.length + 1), value.offset, value.length + 1));
            if (value.length > 0) {
                bounds.add(new BytesRef(Arrays.copyOfRange(value.bytes, value.offset, value.offset + value.length - 1)));
            }
        }
        return new ArrayList<>(bounds);
    }

    public void testBoundsAtTheEdgesOfChunksAreExact() throws IOException {
        for (int round = 0; round < 5; round++) {
            final BytesRef[] pool = PlainSkipTestValues.sortedPool(between(40, 400));
            final BytesRef[][] docSlots = ascending(pool, between(1, 4));
            final StringColumnOptions.Sizes sizes = smallChunks();
            final int[] starts = entryStarts(docSlots, sizes);
            final List<BytesRef> bounds = boundsAround(docSlots, starts);
            bounds.add(null);
            withColumn(docSlots, PlainStringColumnSkipperTests.plain(true, sizes), (metadata, reader) -> {
                assertNotNull(((StringColumnMetadata.Plain) metadata).skipper());
                for (BytesRef lower : bounds) {
                    for (BytesRef upper : bounds) {
                        for (boolean includeLower : new boolean[] { true, false }) {
                            for (boolean includeUpper : new boolean[] { true, false }) {
                                StringMatchOracle.assertRange(reader, docSlots, lower, includeLower, upper, includeUpper);
                            }
                        }
                    }
                }
                for (BytesRef term : bounds) {
                    if (term != null) {
                        StringMatchOracle.assertMatches(docSlots, term::equals, () -> reader.matchTerm(term));
                        StringMatchOracle.assertMatches(
                            docSlots,
                            value -> StringMatchOracle.startsWith(value, term),
                            () -> reader.matchPrefix(term)
                        );
                    }
                }
            });
        }
    }

    public void testRunsOfOneValueSpanningManyChunksAreExact() throws IOException {
        for (int round = 0; round < 5; round++) {
            final BytesRef[] pool = PlainSkipTestValues.sortedPool(between(1, 5));
            final BytesRef[][] docSlots = ascending(pool, between(300, 1500));
            final StringColumnOptions.Sizes sizes = smallChunks();
            final List<BytesRef> bounds = new ArrayList<>(Arrays.asList(pool));
            bounds.add(null);
            bounds.add(new BytesRef());
            withColumn(docSlots, PlainStringColumnSkipperTests.plain(true, sizes), (metadata, reader) -> {
                for (BytesRef lower : bounds) {
                    for (BytesRef upper : bounds) {
                        StringMatchOracle.assertRange(reader, docSlots, lower, true, upper, true);
                        StringMatchOracle.assertRange(reader, docSlots, lower, false, upper, true);
                        StringMatchOracle.assertRange(reader, docSlots, lower, true, upper, false);
                        StringMatchOracle.assertRange(reader, docSlots, lower, false, upper, false);
                    }
                    if (lower != null) {
                        StringMatchOracle.assertMatches(docSlots, lower::equals, () -> reader.matchTerm(lower));
                    }
                }
            });
        }
    }

    public void testQueriesOutsideTheColumnMatchNothing() throws IOException {
        final BytesRef[] pool = PlainSkipTestValues.sortedPool(between(30, 100));
        final BytesRef[][] docSlots = ascending(pool, 3);
        final BytesRef below = new BytesRef();
        final BytesRef above = new BytesRef(new byte[] { (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, 1 });
        withColumn(docSlots, PlainStringColumnSkipperTests.plain(true, smallChunks()), (metadata, reader) -> {
            StringMatchOracle.assertRange(reader, docSlots, above, true, null, true);
            StringMatchOracle.assertRange(reader, docSlots, null, true, below, false);
            StringMatchOracle.assertRange(reader, docSlots, above, true, above, true);
            StringMatchOracle.assertRange(reader, docSlots, pool[pool.length - 1], false, null, true);
            StringMatchOracle.assertRange(reader, docSlots, null, true, pool[0], false);
            StringMatchOracle.assertRange(reader, docSlots, pool[pool.length - 1], true, pool[0], true);
            StringMatchOracle.assertMatches(docSlots, above::equals, () -> reader.matchTerm(above));
            StringMatchOracle.assertMatches(docSlots, value -> StringMatchOracle.startsWith(value, above), () -> reader.matchPrefix(above));
        });
    }

    public void testOneChunkAndOneValueAreExact() throws IOException {
        for (BytesRef[] pool : new BytesRef[][] { PlainSkipTestValues.sortedPool(1), PlainSkipTestValues.sortedPool(2) }) {
            final BytesRef[][] docSlots = ascending(pool, 1);
            withColumn(docSlots, PlainStringColumnSkipperTests.plain(true, smallChunks()), (metadata, reader) -> {
                StringMatchOracle.checkRandomQueries(reader, docSlots, pool, 40);
                for (BytesRef value : pool) {
                    StringMatchOracle.assertRange(reader, docSlots, value, true, value, true);
                    StringMatchOracle.assertRange(reader, docSlots, value, false, value, true);
                    StringMatchOracle.assertRange(reader, docSlots, value, true, value, false);
                }
            });
        }
    }

    public void testDocumentsWithoutAValueMoveRanksAwayFromDocumentIds() throws IOException {
        for (int round = 0; round < 5; round++) {
            final BytesRef[] pool = PlainSkipTestValues.sortedPool(between(20, 200));
            final BytesRef[][] dense = ascending(pool, between(1, 5));
            final BytesRef[][] docSlots = new BytesRef[dense.length * 2][];
            for (int doc = 0; doc < dense.length; doc++) {
                docSlots[doc * 2 + (randomBoolean() ? 1 : 0)] = dense[doc];
            }
            withColumn(docSlots, PlainStringColumnSkipperTests.plain(true, smallChunks()), (metadata, reader) -> {
                StringMatchOracle.checkRandomQueries(reader, docSlots, pool, 60);
            });
        }
    }

    public void testRandomColumnsAndQueriesAgreeWithTheValues() throws IOException {
        for (int round = 0; round < 10; round++) {
            final BytesRef[] pool = PlainSkipTestValues.sortedPool(between(1, 600));
            final BytesRef[][] docSlots = ascending(pool, between(1, 6));
            final StringColumnOptions.Sizes sizes = PlainStringColumnSkipperTests.sizes(
                randomFrom(128, 256, 512),
                randomFrom(64, 256, 1024, 4096),
                randomFrom(128, 1000, 16384)
            );
            withColumn(docSlots, PlainStringColumnSkipperTests.plain(true, sizes), (metadata, reader) -> {
                StringMatchOracle.checkRandomQueries(reader, docSlots, pool, 80);
            });
            withColumn(docSlots, PlainStringColumnSkipperTests.plain(false, sizes), (metadata, reader) -> {
                StringMatchOracle.checkRandomQueries(reader, docSlots, pool, 20);
            });
        }
    }

    public void testTheWindowsOfAQueryAreOneChunkAtEachEnd() throws IOException {
        for (int round = 0; round < 5; round++) {
            final BytesRef[] pool = PlainSkipTestValues.sortedPool(between(100, 500));
            final BytesRef[][] docSlots = ascending(pool, between(1, 3));
            final StringColumnOptions.Sizes sizes = smallChunks();
            final int[] starts = entryStarts(docSlots, sizes);
            final List<BytesRef> bounds = boundsAround(docSlots, starts);
            bounds.add(null);
            final BytesRef[] slots = Arrays.stream(docSlots).map(slotsOfDoc -> slotsOfDoc[0]).toArray(BytesRef[]::new);
            withColumn(docSlots, PlainStringColumnSkipperTests.plain(true, sizes), (metadata, reader) -> {
                assertThat(reader, instanceOf(PlainStringColumnReader.class));
                final PlainStringColumnReader plain = (PlainStringColumnReader) reader;
                for (BytesRef lower : bounds) {
                    for (BytesRef upper : bounds) {
                        if (lower != null && upper != null && lower.compareTo(upper) > 0) {
                            continue;
                        }
                        for (boolean includeLower : new boolean[] { true, false }) {
                            for (boolean includeUpper : new boolean[] { true, false }) {
                                final StringColumnReader.SortedWindows windows = plain.sortedWindows(
                                    PlainSkipQuery.range(lower, includeLower, upper, includeUpper)
                                );
                                if (windows.isNone()) {
                                    StringMatchOracle.assertRange(reader, docSlots, lower, includeLower, upper, includeUpper);
                                    continue;
                                }
                                int from = 0;
                                while (from < slots.length && below(slots[from], lower, includeLower)) {
                                    from++;
                                }
                                int to = 0;
                                while (to < slots.length && above(slots[to], upper, includeUpper) == false) {
                                    to++;
                                }
                                assertTrue(
                                    windows.startLow() + " <= " + from + " <= " + windows.startHigh(),
                                    inside(from, windows.startLow(), windows.startHigh())
                                );
                                assertTrue(
                                    windows.endLow() + " <= " + to + " <= " + windows.endHigh(),
                                    inside(to, windows.endLow(), windows.endHigh())
                                );
                                assertTrue(
                                    "the start window is one chunk",
                                    oneChunk(starts, slots.length, windows.startLow(), windows.startHigh())
                                );
                                assertTrue(
                                    "the end window is one chunk",
                                    oneChunk(starts, slots.length, windows.endLow(), windows.endHigh())
                                );
                            }
                        }
                    }
                }
            });
        }
    }

    public void testTheBisectionReadsFewerChunksThanWithoutTheRegion() throws IOException {
        final int docs = 20_000;
        final BytesRef[][] docSlots = new BytesRef[docs][];
        for (int doc = 0; doc < docs; doc++) {
            docSlots[doc] = new BytesRef[] { new BytesRef(String.format(Locale.ROOT, "value-%08d", doc)) };
        }
        final BytesRef lower = new BytesRef(String.format(Locale.ROOT, "value-%08d", docs / 2));
        final BytesRef upper = new BytesRef(String.format(Locale.ROOT, "value-%08d", docs / 2 + 300));
        final long[] seeks = new long[2];
        for (int arm = 0; arm < 2; arm++) {
            final Counter counter = new Counter();
            final int index = arm;
            withColumnAndSkipIndex(
                docSlots,
                PlainStringColumnSkipperTests.plain(arm == 0, PlainStringColumnSkipperTests.sizes(128, 1024)),
                directory -> new SeekCountingDirectory(directory, counter),
                (metadata, reader, skip) -> {
                    final long before = counter.get();
                    StringMatchOracle.assertRange(reader, docSlots, lower, true, upper, false);
                    seeks[index] = counter.get() - before;
                }
            );
        }
        assertThat("seeks with the region " + seeks[0] + ", without " + seeks[1], seeks[0], lessThan(seeks[1]));
    }

    private static boolean inside(int rank, int low, int high) {
        return low <= rank && rank <= high;
    }

    private static boolean below(BytesRef value, BytesRef lower, boolean includeLower) {
        if (lower == null) {
            return false;
        }
        final int comparison = value.compareTo(lower);
        return comparison < 0 || (comparison == 0 && includeLower == false);
    }

    private static boolean above(BytesRef value, BytesRef upper, boolean includeUpper) {
        if (upper == null) {
            return false;
        }
        final int comparison = value.compareTo(upper);
        return comparison > 0 || (comparison == 0 && includeUpper == false);
    }

    /** Whether {@code [low, high]} is the slots of one chunk, whose end is the first slot of the next. */
    private static boolean oneChunk(int[] starts, int numSlots, int low, int high) {
        for (int entry = 0; entry < starts.length; entry++) {
            final int end = entry + 1 < starts.length ? starts[entry + 1] : numSlots;
            if (starts[entry] == low && end == high) {
                return true;
            }
        }
        return false;
    }
}
