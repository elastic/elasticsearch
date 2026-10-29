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
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.columnar.numeric.MultiLevelSkipIndexCodec;
import org.elasticsearch.columnar.substrate.BytesSink;
import org.elasticsearch.columnar.substrate.ChunkBounds;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.NavigableSet;
import java.util.TreeSet;

import static org.elasticsearch.columnar.ColumnarTestUtils.randomValidBlockSize;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.instanceOf;

public class PlainStringColumnSkipperTests extends ColumnarStringTestCase {

    private static final int INTERVAL = MultiLevelSkipIndexCodec.INTERVAL_SIZE;

    private static final DictionaryPolicy FORCED = new DictionaryPolicy(512 * 1024, 0.0, Double.MAX_VALUE);

    private enum Shape {
        DESCENDING,
        CLUSTERED,
        SHUFFLED
    }

    public void testTheOptionDoesNotChangeTheLayoutOrTheValuesWritten() throws IOException {
        for (int round = 0; round < 6; round++) {
            final BytesRef[] pool = round % 2 == 0
                ? PlainSkipTestValues.sortedPool(between(3, 12))
                : PlainSkipTestValues.sortedPool(between(300, 600));
            final BytesRef[][] docSlots = column(randomFrom(Shape.values()), between(INTERVAL + 1, 3 * INTERVAL), pool);
            final StringColumnOptions defaults = StringColumnOptions.DEFAULT;
            final List<String> shapes = new ArrayList<>();
            for (boolean enabled : new boolean[] { true, false }) {
                withColumn(docSlots, defaults.withPlainSkipIndex(enabled), (metadata, reader) -> {
                    final String dictionarySize = metadata instanceof StringColumnMetadata.Dictionary d ? "/" + d.dictionarySize() : "";
                    shapes.add(
                        metadata.layout()
                            + dictionarySize
                            + "/"
                            + metadata.numValues()
                            + "/"
                            + metadata.numNullSlots()
                            + "/"
                            + metadata.valueBytes()
                    );
                });
            }
            assertEquals(shapes.get(0), shapes.get(1));
        }
    }

    public void testBoundsAreWrittenOnlyWhenEnabledWhateverTheOrderOfTheValues() throws IOException {
        final int count = 3 * INTERVAL + 5;
        final BytesRef[] pool = PlainSkipTestValues.sortedPool(20);
        final BytesRef[][] ascending = new BytesRef[count][];
        final BytesRef[][] descending = new BytesRef[count][];
        final BytesRef[][] ascendingMultiValued = new BytesRef[count][];
        for (int doc = 0; doc < count; doc++) {
            final int up = (int) ((long) doc * pool.length / count);
            ascending[doc] = new BytesRef[] { pool[up] };
            descending[doc] = new BytesRef[] { pool[pool.length - 1 - up] };
            ascendingMultiValued[doc] = new BytesRef[] { pool[up], pool[up] };
        }
        final StringColumnOptions.Sizes sizes = sizes(128, 4096);
        for (BytesRef[][] docSlots : new BytesRef[][][] { ascending, descending, ascendingMultiValued }) {
            final boolean bisected = docSlots == ascending;
            final long without = skipFileLength(docSlots, plain(false, sizes));
            final long with = skipFileLength(docSlots, plain(true, sizes));
            withColumn(docSlots, plain(true, sizes), (metadata, reader) -> {
                assertThat(metadata, instanceOf(StringColumnMetadata.Plain.class));
                final StringColumnMetadata.Plain column = (StringColumnMetadata.Plain) metadata;
                assertEquals(bisected, column.valuesSorted() && column.hasValueAddresses() == false);
                assertNotNull(column.skipper());
                final List<BytesRef> slots = new ArrayList<>();
                Arrays.stream(docSlots).forEach(slotsOfDoc -> slots.addAll(Arrays.asList(slotsOfDoc)));
                final int[] owners = PlainChunkModel.owners(slots, sizes.plainChunks(), sizes.valuesPerBlock(), sizes.lengthBlockSize());
                assertEquals(PlainSkipFixture.entriesOf(owners), column.skipper().entryCount());
                assertEquals(with - without, column.skipper().entryCount() * 20L + column.skipper().payloadLength());
            });
            withColumn(
                docSlots,
                plain(false, sizes),
                (metadata, reader) -> { assertNull(((StringColumnMetadata.Plain) metadata).skipper()); }
            );
            assertThat(with, greaterThan(without));
        }
    }

    public void testQueriesAgreeWithTheValuesWhateverTheLayoutAndTheBounds() throws IOException {
        for (int round = 0; round < 4; round++) {
            final BytesRef[] pool = PlainSkipTestValues.sortedPool(between(3, 40));
            final BytesRef[][] docSlots = column(randomFrom(Shape.values()), between(INTERVAL + 1, 4 * INTERVAL), pool);
            final StringColumnOptions.Sizes sizes = sizes(randomValidBlockSize(), randomFrom(4096, 65536, 512 * 1024));
            withColumn(docSlots, plain(true, sizes), (metadata, reader) -> {
                assertThat(metadata, instanceOf(StringColumnMetadata.Plain.class));
                final StringColumnMetadata.Plain column = (StringColumnMetadata.Plain) metadata;
                assertEquals(column.valuesSorted() && column.hasValueAddresses() == false, column.skipper() == null);
                StringMatchOracle.checkRandomQueries(reader, docSlots, pool, 30);
            });
            withColumn(docSlots, plain(false, sizes), (metadata, reader) -> {
                assertThat(metadata, instanceOf(StringColumnMetadata.Plain.class));
                assertNull(((StringColumnMetadata.Plain) metadata).skipper());
                StringMatchOracle.checkRandomQueries(reader, docSlots, pool, 30);
            });
            withColumn(docSlots, dictionary(sizes), (metadata, reader) -> {
                assertThat(metadata, instanceOf(StringColumnMetadata.Dictionary.class));
                StringMatchOracle.checkRandomQueries(reader, docSlots, pool, 30);
            });
        }
    }

    public void testQueriesAtTheEdgesOfAnIntervalAreExact() throws IOException {
        final int bands = 4;
        final BytesRef[][] docSlots = new BytesRef[bands * INTERVAL][];
        for (int doc = 0; doc < docSlots.length; doc++) {
            final int band = bands - 1 - doc / INTERVAL;
            docSlots[doc] = new BytesRef[] { new BytesRef("band" + band + "-" + (char) ('a' + doc % 3)) };
        }
        final List<BytesRef> edges = new ArrayList<>();
        for (String edge : new String[] { "band1-a", "band1-c", "band1-b", "band1-", "band0-c", "band2-a", "band1-d", "band1" }) {
            edges.add(new BytesRef(edge));
            edges.add(new BytesRef(edge + "\0"));
        }
        edges.add(null);
        withColumn(docSlots, plain(true, sizes(128, 4096)), (metadata, reader) -> {
            assertNotNull(((StringColumnMetadata.Plain) metadata).skipper());
            for (BytesRef lower : edges) {
                for (BytesRef upper : edges) {
                    for (boolean includeLower : new boolean[] { true, false }) {
                        for (boolean includeUpper : new boolean[] { true, false }) {
                            StringMatchOracle.assertRange(reader, docSlots, lower, includeLower, upper, includeUpper);
                        }
                    }
                }
            }
            for (BytesRef term : edges) {
                if (term != null) {
                    assertEquals(StringMatchOracle.expected(docSlots, term::equals), StringMatchOracle.matched(reader.matchTerm(term)));
                }
            }
        });
    }

    public void testValuesOfHighBytesAndLongValuesAreFound() throws IOException {
        final byte[] allFf = new byte[300];
        Arrays.fill(allFf, (byte) 0xFF);
        final BytesRef[][] docSlots = new BytesRef[3 * INTERVAL][];
        for (int doc = 0; doc < docSlots.length; doc++) {
            docSlots[doc] = new BytesRef[] {
                doc / INTERVAL == 1 ? new BytesRef(Arrays.copyOf(allFf, allFf.length - doc % 5)) : new BytesRef("a" + doc % 7) };
        }
        withColumn(docSlots, plain(true, sizes(128, 4096)), (metadata, reader) -> {
            assertNotNull(((StringColumnMetadata.Plain) metadata).skipper());
            final BytesRef high = new BytesRef(Arrays.copyOf(allFf, 40));
            StringMatchOracle.assertRange(reader, docSlots, high, true, null, true);
            StringMatchOracle.assertRange(reader, docSlots, high, false, null, true);
            StringMatchOracle.assertRange(reader, docSlots, new BytesRef(allFf), true, new BytesRef(allFf), true);
            StringMatchOracle.assertRange(reader, docSlots, new BytesRef("b"), true, null, true);
            assertEquals(
                StringMatchOracle.expected(docSlots, value -> StringMatchOracle.startsWith(value, high)),
                StringMatchOracle.matched(reader.matchPrefix(high))
            );
        });
    }

    public void testNullsAndEmptyArraysNeverMatchAndMultiValuedDocumentsAreBoundedByEveryValue() throws IOException {
        final BytesRef[][] docSlots = new BytesRef[3 * INTERVAL + 7][];
        for (int doc = 0; doc < docSlots.length; doc++) {
            final int interval = doc / INTERVAL;
            docSlots[doc] = switch (doc % 5) {
                case 0 -> new BytesRef[] { new BytesRef("a"), new BytesRef("m" + interval), new BytesRef("z") };
                case 1 -> new BytesRef[] { null };
                case 2 -> new BytesRef[0];
                case 3 -> null;
                case 4 -> new BytesRef[] { null, new BytesRef("m" + interval) };
                default -> throw new AssertionError(doc);
            };
        }
        withColumn(docSlots, plain(true, sizes(128, 4096)), (metadata, reader) -> {
            assertNotNull(((StringColumnMetadata.Plain) metadata).skipper());
            final List<BytesRef> probes = List.of(
                new BytesRef("m0"),
                new BytesRef("m1"),
                new BytesRef("m2"),
                new BytesRef("m3"),
                new BytesRef(),
                new BytesRef("a"),
                new BytesRef("z")
            );
            for (BytesRef probe : probes) {
                assertEquals(StringMatchOracle.expected(docSlots, probe::equals), StringMatchOracle.matched(reader.matchTerm(probe)));
                StringMatchOracle.assertRange(reader, docSlots, probe, true, probe, true);
                StringMatchOracle.assertRange(reader, docSlots, probe, false, null, true);
                StringMatchOracle.assertRange(reader, docSlots, null, true, probe, false);
            }
            StringMatchOracle.assertRange(reader, docSlots, null, true, null, true);
        });
    }

    public void testExcludedEntriesAreNotReadFromTheDataFile() throws IOException {
        final int bands = 12;
        final BytesRef[][] docSlots = new BytesRef[bands * INTERVAL][];
        for (int doc = 0; doc < docSlots.length; doc++) {
            final int band = bands - 1 - doc / INTERVAL;
            docSlots[doc] = new BytesRef[] { new BytesRef(String.format(Locale.ROOT, "band-%02d-%08d", band, between(0, 99_999_999))) };
        }
        final BytesRef inBand = docSlots[5 * INTERVAL + 17][0];
        final BytesRef bandStart = new BytesRef("band-06-");
        final BytesRef bandEnd = new BytesRef("band-06.");
        final List<Query> queries = List.of(
            reader -> reader.matchTerm(inBand),
            reader -> reader.matchRange(bandStart, true, bandEnd, false),
            reader -> reader.matchPrefix(bandStart)
        );
        for (Query query : queries) {
            final long[] decoded = new long[2];
            for (int arm = 0; arm < 2; arm++) {
                final Counter seeks = new Counter();
                final int index = arm;
                withColumnAndSkipIndex(
                    docSlots,
                    plain(arm == 0, sizes(128, 4096)),
                    directory -> new SeekCountingDirectory(directory, seeks),
                    (metadata, reader, skip) -> {
                        assertEquals(index == 0, ((StringColumnMetadata.Plain) metadata).skipper() != null);
                        final long before = seeks.get();
                        final List<Integer> matches = StringMatchOracle.matched(query.of(reader));
                        decoded[index] = seeks.get() - before;
                        assertFalse(matches.isEmpty());
                    }
                );
            }
            assertThat(decoded[0], greaterThan(0L));
            assertThat("seeks with the bounds " + decoded[0] + ", without " + decoded[1], decoded[1], greaterThan(decoded[0] * 6));
        }
    }

    public void testAnIneligibleFieldKeepsTheStreamingPathAndAnEligibleFieldHoldsEachValue() throws IOException {
        final BytesRef[] pool = PlainSkipTestValues.sortedPool(10);
        final BytesRef[][] docSlots = column(Shape.DESCENDING, 2 * INTERVAL, pool);
        final StringColumnOptions.Sizes sizes = sizes(128, 4096);
        for (boolean eligible : new boolean[] { false, true }) {
            final Counter held = new Counter();
            withColumn(docSlots, () -> new HoldingProbe(cursor(docSlots), held), plain(eligible, sizes), (metadata, reader) -> {
                assertEquals(eligible, ((StringColumnMetadata.Plain) metadata).skipper() != null);
                StringMatchOracle.checkRandomQueries(reader, docSlots, pool, 10);
            });
            if (eligible) {
                assertThat(held.get(), greaterThan(0L));
            } else {
                assertEquals(0, held.get());
            }
        }
    }

    public void testAnyOfDecodesTheSameBlocksWithAndWithoutTheBounds() throws IOException {
        final int bands = 12;
        final BytesRef[][] docSlots = new BytesRef[bands * INTERVAL][];
        for (int doc = 0; doc < docSlots.length; doc++) {
            final int band = bands - 1 - doc / INTERVAL;
            docSlots[doc] = new BytesRef[] { new BytesRef(String.format(Locale.ROOT, "band-%02d-%08d", band, between(0, 99_999_999))) };
        }
        final NavigableSet<BytesRef> terms = new TreeSet<>(List.of(docSlots[5 * INTERVAL + 17][0], docSlots[5 * INTERVAL + 400][0]));
        final long[] decoded = new long[2];
        final List<List<Integer>> matches = new ArrayList<>();
        for (int arm = 0; arm < 2; arm++) {
            final Counter seeks = new Counter();
            final int index = arm;
            withColumnAndSkipIndex(
                docSlots,
                plain(arm == 0, sizes(128, 4096)),
                directory -> new SeekCountingDirectory(directory, seeks),
                (metadata, reader, skip) -> {
                    final long before = seeks.get();
                    matches.add(StringMatchOracle.matched(reader.matchAnyOf(terms, terms)));
                    decoded[index] = seeks.get() - before;
                }
            );
        }
        assertEquals(matches.get(0), matches.get(1));
        assertEquals(decoded[0], decoded[1]);
    }

    public void testIteratorsOfOneReaderCanBeInterleaved() throws IOException {
        final BytesRef[] pool = PlainSkipTestValues.sortedPool(30);
        final BytesRef[][] docSlots = column(Shape.DESCENDING, 3 * INTERVAL, pool);
        withColumn(docSlots, plain(true, sizes(128, 4096)), (metadata, reader) -> {
            assertNotNull(((StringColumnMetadata.Plain) metadata).skipper());
            for (int round = 0; round < 10; round++) {
                final BytesRef lowerOne = randomFrom(pool);
                final BytesRef upperOne = randomFrom(pool);
                final BytesRef lowerTwo = randomFrom(pool);
                final DocIdSetIterator one = reader.matchRange(lowerOne, true, upperOne, true);
                final DocIdSetIterator two = reader.matchRange(lowerTwo, false, null, true);
                final List<Integer> foundOne = new ArrayList<>();
                final List<Integer> foundTwo = new ArrayList<>();
                int docOne = one.nextDoc();
                int docTwo = two.nextDoc();
                while (docOne != DocIdSetIterator.NO_MORE_DOCS || docTwo != DocIdSetIterator.NO_MORE_DOCS) {
                    if (docOne != DocIdSetIterator.NO_MORE_DOCS) {
                        foundOne.add(docOne);
                        docOne = one.nextDoc();
                    }
                    if (docTwo != DocIdSetIterator.NO_MORE_DOCS) {
                        foundTwo.add(docTwo);
                        docTwo = two.advance(docTwo + between(1, 50));
                    }
                }
                assertEquals(
                    StringMatchOracle.expected(docSlots, v -> StringMatchOracle.inRange(v, lowerOne, true, upperOne, true)),
                    foundOne
                );
                final List<Integer> expectedTwo = StringMatchOracle.expected(
                    docSlots,
                    v -> StringMatchOracle.inRange(v, lowerTwo, false, null, true)
                );
                assertTrue(expectedTwo.containsAll(foundTwo));
            }
        });
    }

    public void testTheSerializedBoundsOfARealColumnAreTheExactBytesOfItsValues() throws IOException {
        for (int round = 0; round < 6; round++) {
            final StringColumnOptions.Sizes sizes = sizes(randomValidBlockSize(), randomFrom(64, 256, 4096), randomFrom(128, 1000, 16384));
            final BytesRef[] pool = PlainSkipTestValues.sortedPool(between(3, 30));
            final BytesRef[][] docSlots = column(randomFrom(Shape.values()), between(INTERVAL, 2 * INTERVAL), pool);
            docSlots[between(0, docSlots.length - 1)] = new BytesRef[] { null, randomFrom(pool) };
            assertSerializedBoundsAreExact(docSlots, sizes);
        }
    }

    public void testAChunkOfNullsHasNoBoundsAndLaterChunksStayAligned() throws IOException {
        final BytesRef[][] docSlots = new BytesRef[1000][];
        for (int doc = 0; doc < docSlots.length; doc++) {
            docSlots[doc] = new BytesRef[] { doc < 300 ? new BytesRef("a") : doc < 700 ? null : new BytesRef("z") };
        }
        final StringColumnOptions.Sizes sizes = sizes(128, 4096, 128);
        assertSerializedBoundsAreExact(docSlots, sizes);
        withColumn(docSlots, plain(true, sizes), (metadata, reader) -> {
            assertNotNull(((StringColumnMetadata.Plain) metadata).skipper());
            for (String term : new String[] { "a", "z", "m" }) {
                final BytesRef value = new BytesRef(term);
                assertEquals(StringMatchOracle.expected(docSlots, value::equals), StringMatchOracle.matched(reader.matchTerm(value)));
            }
            StringMatchOracle.assertRange(reader, docSlots, null, true, null, true);
            StringMatchOracle.assertRange(reader, docSlots, new BytesRef("b"), true, null, true);
        });
    }

    private void assertSerializedBoundsAreExact(BytesRef[][] docSlots, StringColumnOptions.Sizes sizes) throws IOException {
        withColumnAndSkipIndex(docSlots, plain(true, sizes), (metadata, reader, skip) -> {
            final StringColumnMetadata.Plain.Skipper skipper = ((StringColumnMetadata.Plain) metadata).skipper();
            assertNotNull(skipper);
            final List<PlainSkipFixture.Parsed> parsed = PlainSkipFixture.parse(skip, skipper);
            final List<BytesRef> slots = new ArrayList<>();
            final List<Integer> slotDocs = new ArrayList<>();
            for (int doc = 0; doc < docSlots.length; doc++) {
                if (docSlots[doc] != null) {
                    for (BytesRef value : docSlots[doc]) {
                        slots.add(value);
                        slotDocs.add(doc);
                    }
                }
            }
            final int[] owners = PlainChunkModel.owners(slots, sizes.plainChunks(), sizes.valuesPerBlock(), sizes.lengthBlockSize());
            int entry = -1;
            for (int first = 0; first < owners.length; first++) {
                if (first > 0 && owners[first] == owners[first - 1]) {
                    continue;
                }
                entry++;
                int end = first + 1;
                while (end < owners.length && owners[end] == owners[first]) {
                    end++;
                }
                BytesRef min = null;
                BytesRef max = null;
                for (int address = first; address < end; address++) {
                    final BytesRef value = slots.get(address);
                    if (value != null) {
                        min = min == null || value.compareTo(min) < 0 ? value : min;
                        max = max == null || value.compareTo(max) > 0 ? value : max;
                    }
                }
                assertThat("entries written", parsed.size(), greaterThan(entry));
                assertEquals(first, parsed.get(entry).firstSlot());
                assertEquals(slotDocs.get(first).intValue(), parsed.get(entry).firstDoc());
                assertEquals(min, parsed.get(entry).min());
                assertEquals(max, parsed.get(entry).max());
            }
            assertEquals(entry + 1, parsed.size());
        });
    }

    public void testAColumnWithoutValuesWritesNoBounds() throws IOException {
        withColumn(new BytesRef[10][], plain(true, sizes(128, 4096)), (metadata, reader) -> {
            assertThat(metadata, instanceOf(StringColumnMetadata.Plain.class));
            assertNull(((StringColumnMetadata.Plain) metadata).skipper());
            assertEquals(DocIdSetIterator.NO_MORE_DOCS, reader.matchTerm(new BytesRef("a")).nextDoc());
        });
    }

    private interface Query {
        DocIdSetIterator of(StringColumnReader reader) throws IOException;
    }

    private long skipFileLength(BytesRef[][] docSlots, StringColumnOptions options) throws IOException {
        final long[] length = new long[1];
        withColumnAndSkipIndex(docSlots, options, (metadata, reader, skip) -> length[0] = skip.length());
        return length[0];
    }

    private static BytesRef[][] column(Shape shape, int count, BytesRef[] pool) {
        final BytesRef[][] docSlots = new BytesRef[count][];
        int run = 0;
        int runValue = 0;
        for (int doc = 0; doc < count; doc++) {
            final int index = switch (shape) {
                case DESCENDING -> Math.max(
                    0,
                    Math.min(pool.length - 1, pool.length - 1 - (int) ((long) doc * pool.length / count) + between(-1, 1))
                );
                case CLUSTERED -> {
                    if (run == 0) {
                        run = between(300, 3000);
                        runValue = between(0, pool.length - 1);
                    }
                    run--;
                    yield runValue;
                }
                case SHUFFLED -> between(0, pool.length - 1);
            };
            final int kind = between(0, 59);
            if (kind < 5) {
                continue;
            }
            if (kind == 5) {
                docSlots[doc] = new BytesRef[0];
            } else if (kind < 9) {
                final BytesRef[] slots = new BytesRef[between(2, 4)];
                for (int slot = 0; slot < slots.length; slot++) {
                    slots[slot] = slot == 0 ? pool[index] : randomBoolean() ? randomFrom(pool) : null;
                }
                docSlots[doc] = slots;
            } else {
                docSlots[doc] = new BytesRef[] { pool[index] };
            }
        }
        return docSlots;
    }

    static StringColumnOptions.Sizes sizes(int valuesPerBlock, int chunkBytes) {
        return sizes(valuesPerBlock, chunkBytes, StringColumnOptions.DEFAULT_MAX_VALUES_PER_CHUNK);
    }

    static StringColumnOptions.Sizes sizes(int valuesPerBlock, int chunkBytes, int maxValuesPerChunk) {
        final StringColumnOptions.Sizes defaults = StringColumnOptions.DEFAULT_SIZES;
        return new StringColumnOptions.Sizes(
            valuesPerBlock,
            new ChunkBounds(chunkBytes, maxValuesPerChunk),
            defaults.escapeChunks(),
            defaults.packedOrdinalBlockSize(),
            defaults.compressedOrdinalBlockSize(),
            defaults.escapeRankBlockSize(),
            defaults.slotCountsBlockSize(),
            randomLengthBlockSize(valuesPerBlock)
        );
    }

    static StringColumnOptions plain(boolean bounded, StringColumnOptions.Sizes sizes) {
        return StringColumnOptions.DEFAULT.withPolicies(DictionaryPolicy.NONE, SummaryPolicy.NONE)
            .withSizes(sizes)
            .withPlainSkipIndex(bounded);
    }

    static StringColumnOptions dictionary(StringColumnOptions.Sizes sizes) {
        return StringColumnOptions.DEFAULT.withPolicies(FORCED, StringColumnOptions.DEFAULT_SUMMARY)
            .withSizes(sizes)
            .withPlainSkipIndex(true);
    }

    private static final class HoldingProbe extends StringColumnValues {
        private final StringColumnValues delegate;
        private final Counter held;

        HoldingProbe(StringColumnValues delegate, Counter held) {
            this.delegate = delegate;
            this.held = held;
        }

        @Override
        public int valueCount() {
            return delegate.valueCount();
        }

        @Override
        public int nullCount() throws IOException {
            return delegate.nullCount();
        }

        @Override
        public void nextValue() throws IOException {
            delegate.nextValue();
        }

        @Override
        public BytesRef value() throws IOException {
            held.increment();
            return delegate.value();
        }

        @Override
        public int valueLength() throws IOException {
            final BytesRef value = delegate.value();
            return value == null ? -1 : value.length;
        }

        @Override
        public void transferValue(BytesSink sink) throws IOException {
            final BytesRef value = delegate.value();
            sink.append(value.bytes, value.offset, value.length);
        }

        @Override
        public boolean streamsValues() {
            return true;
        }

        @Override
        public boolean mayBeSorted() {
            return false;
        }

        @Override
        public int docID() {
            return delegate.docID();
        }

        @Override
        public int nextDoc() throws IOException {
            return delegate.nextDoc();
        }

        @Override
        public int advance(int target) throws IOException {
            return delegate.advance(target);
        }

        @Override
        public long cost() {
            return delegate.cost();
        }
    }
}
