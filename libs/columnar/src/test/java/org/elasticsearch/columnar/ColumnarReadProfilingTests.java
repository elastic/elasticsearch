/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LogDocMergePolicy;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.elasticsearch.codec.profiler.ProfileCollector;
import org.elasticsearch.codec.profiler.Scopes;
import org.elasticsearch.codec.profiler.Scopes.Binding;
import org.elasticsearch.codec.profiler.TestCollectors;
import org.elasticsearch.columnar.numeric.NumericBinaryPayload;
import org.elasticsearch.columnar.numeric.NumericPipeline;
import org.elasticsearch.columnar.string.DictionaryPolicy;
import org.elasticsearch.columnar.string.StringBlockSink;
import org.elasticsearch.columnar.string.StringColumnOptions;
import org.elasticsearch.columnar.string.StringColumnOptionsSelector;
import org.elasticsearch.columnar.string.StringColumnSource;
import org.elasticsearch.columnar.string.SummaryPolicy;
import org.elasticsearch.columnar.substrate.ChunkBounds;
import org.elasticsearch.test.ESTestCase;
import org.junit.After;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.columnar.ColumnarTestUtils.columnarBinaryFieldType;
import static org.elasticsearch.columnar.ColumnarTestUtils.columnarCodec;
import static org.elasticsearch.columnar.ColumnarTestUtils.randomValidBlockSize;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class ColumnarReadProfilingTests extends ESTestCase {

    private static final String FIELD = "field";
    private static final int WIDTH = 16;
    private static final int CHUNK_BYTES = 256;
    private static final Set<String> NUMERIC_METRICS = Set.of(
        "ColumNAR.readers.opened",
        "ColumNAR.readers.open_time_nanos",
        "ColumNAR.readers.plain",
        "ColumNAR.readers.dictionary",
        "ColumNAR.readers.numeric",
        "ColumNAR.blocks.fetched",
        "ColumNAR.blocks.bytes_fetched",
        "ColumNAR.blocks.fetch_time_nanos",
        "ColumNAR.blocks.decompressed",
        "ColumNAR.blocks.bytes_inflated",
        "ColumNAR.blocks.decompress_time_nanos",
        "ColumNAR.blocks.decode_time_nanos",
        "ColumNAR.blocks.values_decoded"
    );

    @After
    public void assertNothingIsLeftBound() {
        assertFalse("a test left a scope bound on its thread", TestCollectors.isBound());
    }

    public void testAPlainColumnReportsEveryChunkItInflates() throws IOException {
        final int docs = between(400, 1500);
        try (Directory dir = newDirectory()) {
            index(dir, plainZstd(), fixedWidth(docs));
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                final LeafReader leaf = reader.leaves().get(0).reader();
                final ProfileCollector stats = TestCollectors.elapsed();
                final BinaryDocValues values;
                try (Binding ignored = Scopes.bind(TestCollectors.eager(stats))) {
                    values = leaf.getBinaryDocValues(FIELD);
                }
                assertThat(values, instanceOf(StringColumnSource.class));
                assertFalse(((StringColumnSource) values).reader().hasDictionary());
                scan(values);
                final long total = (long) docs * WIDTH;
                final Map<String, Long> counted = TestCollectors.toKeyedValues(stats.snapshot());
                assertThat(counted.get("ColumNAR.readers.opened"), equalTo(1L));
                assertThat(counted.get("ColumNAR.chunks.decompressed"), equalTo((total + CHUNK_BYTES - 1) / CHUNK_BYTES));
                assertThat(counted.get("ColumNAR.chunks.bytes_inflated"), equalTo(total));
                assertThat(counted.get("ColumNAR.chunks.bytes_compressed"), greaterThan(0L));
            }
        }
    }

    public void testADictionaryColumnHasNoChunkMetricsBecauseItsTermsAreStoredVerbatim() throws IOException {
        final List<String> values = new ArrayList<>();
        final int docs = between(2000, 4000);
        for (int d = 0; d < docs; d++) {
            values.add("term-" + (d % 20));
        }
        try (Directory dir = newDirectory()) {
            index(dir, dictionary(), values);
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                final LeafReader leaf = reader.leaves().get(0).reader();
                final ProfileCollector stats = TestCollectors.elapsed();
                final BinaryDocValues column;
                try (Binding ignored = Scopes.bind(TestCollectors.eager(stats))) {
                    column = leaf.getBinaryDocValues(FIELD);
                }
                assertTrue(((StringColumnSource) column).reader().hasDictionary());
                scan(column);
                final Map<String, Long> counted = TestCollectors.toKeyedValues(stats.snapshot());
                assertThat(counted.get("ColumNAR.readers.opened"), equalTo(1L));
                assertThat(
                    "a dictionary with no escapes stores its terms verbatim, so no chunk is ever inflated",
                    counted.keySet().stream().filter(name -> name.startsWith("ColumNAR.chunks.")).toList(),
                    equalTo(List.of())
                );
            }
        }
    }

    public void testProfilingDoesNotChangeWhatIsRead() throws IOException {
        final List<String> values = new ArrayList<>();
        final int docs = between(500, 2500);
        for (int d = 0; d < docs; d++) {
            values.add(randomBoolean() && d % 9 == 0 ? null : randomFrom("alpha", "beta", "gamma", "d".repeat(between(1, 40)), "v" + d));
        }
        for (Codec codec : List.of(plainZstd(), dictionary())) {
            try (Directory dir = newDirectory()) {
                index(dir, codec, values);
                try (DirectoryReader reader = DirectoryReader.open(dir)) {
                    final LeafReader leaf = reader.leaves().get(0).reader();
                    final BinaryDocValues plain = leaf.getBinaryDocValues(FIELD);
                    final BinaryDocValues profiled;
                    try (Binding ignored = Scopes.bind(TestCollectors.eager(TestCollectors.elapsed()))) {
                        profiled = leaf.getBinaryDocValues(FIELD);
                    }
                    assertThat("the fast path survives profiling", profiled, instanceOf(StringColumnSource.class));
                    assertThat(profiled.getClass(), equalTo(plain.getClass()));
                    assertThat(scan(profiled), equalTo(scan(plain)));
                }
            }
        }
    }

    public void testAReaderOpenedOutsideAnyScopeCountsNothing() throws IOException {
        try (Directory dir = newDirectory()) {
            index(dir, plainZstd(), fixedWidth(500));
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                final LeafReader leaf = reader.leaves().get(0).reader();
                final BinaryDocValues earlier = leaf.getBinaryDocValues(FIELD);
                final ProfileCollector stats = TestCollectors.elapsed();
                try (Binding ignored = Scopes.bind(TestCollectors.eager(stats))) {
                    scan(earlier);
                }
                assertTrue("a reader opened before the scope reports nothing to it", stats.snapshot().isEmpty());
            }
        }
    }

    public void testNestedScopesCountTheirOwnReadersOnly() throws IOException {
        try (Directory dir = newDirectory()) {
            index(dir, plainZstd(), fixedWidth(800));
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                final LeafReader leaf = reader.leaves().get(0).reader();
                final ProfileCollector outer = TestCollectors.elapsed();
                final ProfileCollector inner = TestCollectors.elapsed();
                final BinaryDocValues outerReader;
                final BinaryDocValues innerReader;
                final BinaryDocValues outerAgain;
                try (Binding ignoredOuter = Scopes.bind(TestCollectors.eager(outer))) {
                    outerReader = leaf.getBinaryDocValues(FIELD);
                    try (Binding ignoredInner = Scopes.bind(TestCollectors.eager(inner))) {
                        innerReader = leaf.getBinaryDocValues(FIELD);
                    }
                    outerAgain = leaf.getBinaryDocValues(FIELD);
                }
                final long chunksPerScan = (800L * WIDTH + CHUNK_BYTES - 1) / CHUNK_BYTES;
                scan(innerReader);
                assertThat(TestCollectors.toKeyedValues(inner.snapshot()).get("ColumNAR.readers.opened"), equalTo(1L));
                assertThat(TestCollectors.toKeyedValues(inner.snapshot()).get("ColumNAR.chunks.decompressed"), equalTo(chunksPerScan));
                assertThat(TestCollectors.toKeyedValues(outer.snapshot()).get("ColumNAR.readers.opened"), equalTo(2L));
                assertThat(
                    "the outer scope saw none of the inner reader's work",
                    TestCollectors.toKeyedValues(outer.snapshot()).get("ColumNAR.chunks.decompressed"),
                    equalTo(0L)
                );
                scan(outerReader);
                scan(outerAgain);
                assertThat(TestCollectors.toKeyedValues(outer.snapshot()).get("ColumNAR.chunks.decompressed"), equalTo(2 * chunksPerScan));
                assertThat(
                    "the outer readers did not feed the inner collector",
                    TestCollectors.toKeyedValues(inner.snapshot()).get("ColumNAR.chunks.decompressed"),
                    equalTo(chunksPerScan)
                );
            }
        }
    }

    public void testTwoReadersOnOneLeafKeepSeparateCounts() throws IOException {
        final int docs = between(500, 1200);
        final List<String> repeating = new ArrayList<>();
        final long[] longs = new long[docs];
        for (int d = 0; d < docs; d++) {
            repeating.add("term-" + (d % 20));
            longs[d] = randomLong();
        }
        // NOTE: A reader cache at any level would make the two calls below share a reader, and with it the scope that
        // opened it first, so every reader kind the producer builds is checked.
        for (int layout = 0; layout < 3; layout++) {
            try (Directory dir = newDirectory()) {
                switch (layout) {
                    case 0 -> index(dir, plainZstd(), fixedWidth(docs));
                    case 1 -> index(dir, dictionary(), repeating);
                    case 2 -> indexNumeric(dir, randomValidBlockSize(), longs);
                    default -> throw new AssertionError("unknown layout [" + layout + "]");
                }
                try (DirectoryReader reader = DirectoryReader.open(dir)) {
                    final LeafReader leaf = reader.leaves().get(0).reader();
                    final ProfileCollector first = TestCollectors.countersOnly();
                    final ProfileCollector second = TestCollectors.countersOnly();
                    final BinaryDocValues firstReader;
                    final BinaryDocValues secondReader;
                    try (Binding ignored = Scopes.bind(TestCollectors.eager(first))) {
                        firstReader = leaf.getBinaryDocValues(FIELD);
                    }
                    try (Binding ignored = Scopes.bind(TestCollectors.eager(second))) {
                        secondReader = leaf.getBinaryDocValues(FIELD);
                    }
                    assertNotSame("a request for a column gets a reader of its own, layout " + layout, firstReader, secondReader);
                    final Map<String, Long> firstAtOpen = TestCollectors.toKeyedValues(first.snapshot());
                    consume(secondReader, layout, docs);
                    assertThat(
                        "the second reader's work never reaches the first scope",
                        TestCollectors.toKeyedValues(first.snapshot()),
                        equalTo(firstAtOpen)
                    );
                    consume(firstReader, layout, docs);
                    assertThat(TestCollectors.toKeyedValues(first.snapshot()), equalTo(TestCollectors.toKeyedValues(second.snapshot())));
                }
            }
        }
    }

    private static void consume(BinaryDocValues column, int layout, int docs) throws IOException {
        if (layout == 2) {
            readLongs(column, docs);
        } else {
            scan(column);
        }
    }

    public void testWorkDoneOnAnotherThreadIsAttributedToTheScopeThatOpenedTheReader() throws Exception {
        try (Directory dir = newDirectory()) {
            index(dir, plainZstd(), fixedWidth(900));
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                final LeafReader leaf = reader.leaves().get(0).reader();
                final ProfileCollector stats = TestCollectors.elapsed();
                final BinaryDocValues values;
                try (Binding ignored = Scopes.bind(TestCollectors.eager(stats))) {
                    values = leaf.getBinaryDocValues(FIELD);
                }
                final AtomicReference<Throwable> failure = new AtomicReference<>();
                final Thread other = new Thread(() -> {
                    try {
                        scan(values);
                        assertFalse(TestCollectors.isBound());
                    } catch (Throwable e) {
                        failure.set(e);
                    }
                });
                other.start();
                other.join();
                assertNull(failure.get());
                assertThat(TestCollectors.toKeyedValues(stats.snapshot()).get("ColumNAR.chunks.decompressed"), greaterThan(0L));
            }
        }
    }

    public void testASingleReaderIsNotAffectedByAScopeBoundLater() throws IOException {
        try (Directory dir = newDirectory()) {
            index(dir, plainZstd(), fixedWidth(600));
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                final LeafReader leaf = reader.leaves().get(0).reader();
                final ProfileCollector stats = TestCollectors.elapsed();
                final BinaryDocValues values;
                try (Binding ignored = Scopes.bind(TestCollectors.eager(stats))) {
                    values = leaf.getBinaryDocValues(FIELD);
                }
                final ProfileCollector later = TestCollectors.elapsed();
                try (Binding ignored = Scopes.bind(TestCollectors.eager(later))) {
                    scan(values);
                }
                assertTrue("the scope bound while iterating gets nothing", later.snapshot().isEmpty());
                assertThat(TestCollectors.toKeyedValues(stats.snapshot()), hasKey("ColumNAR.chunks.decompressed"));
            }
        }
    }

    public void testAReaderOpenedAfterTheBindingEndedDoesNotReportToIt() throws IOException {
        try (Directory dir = newDirectory()) {
            index(dir, plainZstd(), fixedWidth(300));
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                final LeafReader leaf = reader.leaves().get(0).reader();
                final ProfileCollector stats = TestCollectors.elapsed();
                try (Binding ignored = Scopes.bind(TestCollectors.eager(stats))) {
                    assertNotNull(leaf.getBinaryDocValues(FIELD));
                }
                final Map<String, Long> afterTheScopedOpen = TestCollectors.toKeyedValues(stats.snapshot());
                scan(leaf.getBinaryDocValues(FIELD));
                assertThat(TestCollectors.toKeyedValues(stats.snapshot()), equalTo(afterTheScopedOpen));
            }
        }
    }

    public void testADictionaryColumnReportsItsOrdinalBlocksAndItsLayout() throws IOException {
        final List<String> values = new ArrayList<>();
        final int docs = between(2000, 6000);
        for (int d = 0; d < docs; d++) {
            values.add("term-" + (d % 20));
        }
        try (Directory dir = newDirectory()) {
            index(dir, dictionary(), values);
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                final ProfileCollector stats = TestCollectors.elapsed();
                scan((BinaryDocValues) openUnder(stats, reader.leaves().get(0).reader()));
                final Map<String, Long> counted = TestCollectors.toKeyedValues(stats.snapshot());
                assertThat(counted.get("ColumNAR.readers.dictionary"), equalTo(1L));
                assertThat(counted.get("ColumNAR.readers.plain"), equalTo(0L));
                final long packedBlocks = (docs + StringColumnOptions.DEFAULT_PACKED_ORDINAL_BLOCK_SIZE - 1)
                    / StringColumnOptions.DEFAULT_PACKED_ORDINAL_BLOCK_SIZE;
                final long compressedBlocks = (docs + StringColumnOptions.DEFAULT_COMPRESSED_ORDINAL_BLOCK_SIZE - 1)
                    / StringColumnOptions.DEFAULT_COMPRESSED_ORDINAL_BLOCK_SIZE;
                assertThat(
                    "a forward scan fetches each block of ordinals once, in the block size the column chose",
                    counted.get("ColumNAR.blocks.fetched"),
                    anyOf(equalTo(packedBlocks), equalTo(compressedBlocks))
                );
                assertThat(counted.get("ColumNAR.blocks.bytes_fetched"), greaterThan(0L));
                final long inflatedBlocks = counted.getOrDefault("ColumNAR.blocks.decompressed", 0L);
                assertThat(inflatedBlocks, lessThanOrEqualTo(counted.get("ColumNAR.blocks.fetched")));
                assertThat(
                    "bytes are inflated exactly when blocks are",
                    counted.getOrDefault("ColumNAR.blocks.bytes_inflated", 0L) > 0,
                    equalTo(inflatedBlocks > 0)
                );
            }
        }
    }

    public void testAPlainColumnWithValuesOfDifferentLengthsReportsItsLengthBlocks() throws IOException {
        final List<String> values = new ArrayList<>();
        final int docs = between(1000, 3000);
        for (int d = 0; d < docs; d++) {
            values.add("v" + "x".repeat(d % 37) + d);
        }
        try (Directory dir = newDirectory()) {
            index(dir, plainZstd(), values);
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                final ProfileCollector stats = TestCollectors.elapsed();
                scan((BinaryDocValues) openUnder(stats, reader.leaves().get(0).reader()));
                final Map<String, Long> counted = TestCollectors.toKeyedValues(stats.snapshot());
                assertThat(counted.get("ColumNAR.readers.plain"), equalTo(1L));
                assertThat(counted.get("ColumNAR.readers.dictionary"), equalTo(0L));
                assertThat(
                    "a forward scan fetches each block of length codes once",
                    counted.get("ColumNAR.blocks.fetched"),
                    equalTo(
                        (long) ((docs + StringColumnOptions.DEFAULT_LENGTH_BLOCK_SIZE - 1) / StringColumnOptions.DEFAULT_LENGTH_BLOCK_SIZE)
                    )
                );
            }
        }
    }

    public void testANumericScanReportsExactlyTheNumericMetricKeys() throws IOException {
        final int blockSize = randomValidBlockSize();
        final int docs = blockSize * between(2, 4) + between(1, blockSize - 1);
        final long[] values = new long[docs];
        for (int d = 0; d < docs; d++) {
            values[d] = randomLong();
        }
        try (Directory dir = newDirectory()) {
            indexNumeric(dir, blockSize, values);
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                final ProfileCollector stats = TestCollectors.elapsed();
                final BinaryDocValues column;
                try (Binding ignored = Scopes.bind(TestCollectors.eager(stats))) {
                    column = reader.leaves().get(0).reader().getBinaryDocValues(FIELD);
                }
                readLongs(column, docs);
                // NOTE: Metrics register when their reader is built, so this key set depends on what was built, not
                // on what the reads touched. The inflation metrics register inside ZstdBlockBytesCodec, which this
                // column does not build. A refactoring that moves a registration must keep the set.
                assertThat(
                    TestCollectors.toKeyedValues(stats.snapshot()).keySet(),
                    equalTo(
                        Set.of(
                            "ColumNAR.readers.opened",
                            "ColumNAR.readers.open_time_nanos",
                            "ColumNAR.readers.plain",
                            "ColumNAR.readers.dictionary",
                            "ColumNAR.readers.numeric",
                            "ColumNAR.blocks.fetched",
                            "ColumNAR.blocks.bytes_fetched",
                            "ColumNAR.blocks.fetch_time_nanos",
                            "ColumNAR.blocks.decode_time_nanos",
                            "ColumNAR.blocks.values_decoded"
                        )
                    )
                );
            }
        }
    }

    public void testAPlainStringScanReportsExactlyThePlainLayoutMetricKeys() throws IOException {
        final List<String> values = new ArrayList<>();
        final int docs = between(1000, 3000);
        for (int d = 0; d < docs; d++) {
            values.add("v" + "x".repeat(d % 37) + d);
        }
        try (Directory dir = newDirectory()) {
            index(dir, plainZstd(), values);
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                final ProfileCollector stats = TestCollectors.elapsed();
                scan((BinaryDocValues) openUnder(stats, reader.leaves().get(0).reader()));
                // NOTE: Metrics register when their reader is built, so this key set depends on what was built, not
                // on what the reads touched. The inflation metrics register inside ZstdBlockBytesCodec, which this
                // column does not build. A refactoring that moves a registration must keep the set.
                assertThat(
                    TestCollectors.toKeyedValues(stats.snapshot()).keySet(),
                    equalTo(
                        Set.of(
                            "ColumNAR.readers.opened",
                            "ColumNAR.readers.open_time_nanos",
                            "ColumNAR.readers.plain",
                            "ColumNAR.readers.dictionary",
                            "ColumNAR.readers.numeric",
                            "ColumNAR.pages.call_time_nanos",
                            "ColumNAR.pages.byte_lengths_call_time_nanos",
                            "ColumNAR.pages.as_ordinals",
                            "ColumNAR.pages.as_values",
                            "ColumNAR.pages.values_in_ordinal_pages",
                            "ColumNAR.pages.values_in_value_pages",
                            "ColumNAR.chunks.decompressed",
                            "ColumNAR.chunks.bytes_compressed",
                            "ColumNAR.chunks.bytes_inflated",
                            "ColumNAR.chunks.decompress_time_nanos",
                            "ColumNAR.chunks.cache_hits",
                            "ColumNAR.blocks.fetched",
                            "ColumNAR.blocks.bytes_fetched",
                            "ColumNAR.blocks.fetch_time_nanos",
                            "ColumNAR.blocks.decode_time_nanos",
                            "ColumNAR.blocks.values_decoded"
                        )
                    )
                );
            }
        }
    }

    public void testEveryLayoutOfAnOpenedReaderRegistersItsBlockMetrics() throws IOException {
        for (Codec codec : List.of(plainZstd(), dictionary())) {
            try (Directory dir = newDirectory()) {
                index(dir, codec, fixedWidth(300));
                try (DirectoryReader reader = DirectoryReader.open(dir)) {
                    final ProfileCollector stats = TestCollectors.elapsed();
                    openUnder(stats, reader.leaves().get(0).reader());
                    assertThat(
                        "every layout counter is registered so a zero says that layout was not opened",
                        TestCollectors.toKeyedValues(stats.snapshot())
                            .keySet()
                            .containsAll(List.of("ColumNAR.readers.plain", "ColumNAR.readers.dictionary", "ColumNAR.readers.numeric")),
                        equalTo(true)
                    );
                }
            }
        }
    }

    public void testEveryOpenCountsInExactlyOneLayout() throws IOException {
        final int opens = between(1, 5);
        final long[] numbers = new long[between(100, 500)];
        for (int i = 0; i < numbers.length; i++) {
            numbers[i] = randomLong();
        }
        try (Directory dir = newDirectory()) {
            indexNumeric(dir, randomValidBlockSize(), numbers);
            assertLayoutCounts(dir, opens, Map.of("ColumNAR.readers.numeric", (long) opens));
        }
        final List<String> values = new ArrayList<>();
        for (int d = 0; d < 600; d++) {
            values.add("term-" + (d % 7));
        }
        for (boolean withDictionary : List.of(false, true)) {
            try (Directory dir = newDirectory()) {
                index(dir, withDictionary ? dictionary() : plainZstd(), values);
                try (DirectoryReader reader = DirectoryReader.open(dir)) {
                    assertThat(
                        openUnder(TestCollectors.elapsed(), reader.leaves().get(0).reader()).reader().hasDictionary(),
                        equalTo(withDictionary)
                    );
                }
                assertLayoutCounts(
                    dir,
                    opens,
                    Map.of(withDictionary ? "ColumNAR.readers.dictionary" : "ColumNAR.readers.plain", (long) opens)
                );
            }
        }
    }

    private static void assertLayoutCounts(Directory dir, int opens, Map<String, Long> expected) throws IOException {
        try (DirectoryReader reader = DirectoryReader.open(dir)) {
            final ProfileCollector stats = TestCollectors.elapsed();
            for (int i = 0; i < opens; i++) {
                try (Binding ignored = Scopes.bind(TestCollectors.eager(stats))) {
                    assertNotNull(reader.leaves().get(0).reader().getBinaryDocValues(FIELD));
                }
            }
            final Map<String, Long> counted = TestCollectors.toKeyedValues(stats.snapshot());
            final Map<String, Long> layouts = new TreeMap<>();
            for (String layout : List.of("ColumNAR.readers.plain", "ColumNAR.readers.dictionary", "ColumNAR.readers.numeric")) {
                layouts.put(layout, expected.getOrDefault(layout, 0L));
            }
            for (Map.Entry<String, Long> layout : layouts.entrySet()) {
                assertThat(layout.getKey(), counted.get(layout.getKey()), equalTo(layout.getValue()));
            }
            assertThat(counted.get("ColumNAR.readers.opened"), equalTo((long) opens));
        }
    }

    public void testASequentialPageScanOfADictionaryWithoutEscapesDecodesEachOrdinalOnce() throws IOException {
        final int docs = between(2000, 6000);
        final List<String> values = new ArrayList<>(docs);
        for (int d = 0; d < docs; d++) {
            values.add("term-" + (d % 20));
        }
        try (Directory dir = newDirectory()) {
            index(dir, dictionary(), values);
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                final ProfileCollector stats = TestCollectors.elapsed();
                final StringColumnSource column = openUnder(stats, reader.leaves().get(0).reader());
                assertTrue(column.reader().hasDictionary());
                assertThat(column.reader().escapeCount(), equalTo(0L));
                final RecordingSink sink = readInPages(column, docs, between(1, 1500));
                assertThat(sink.delivered, equalTo(values));
                assertThat(
                    "every ordinal block is fetched and decoded once when nothing escaped",
                    TestCollectors.toKeyedValues(stats.snapshot()).get("ColumNAR.blocks.values_decoded"),
                    equalTo((long) docs)
                );
            }
        }
    }

    public void testEscapedValuesMakeAPageScanFetchOrdinalBlocksAgain() throws IOException {
        final int docs = between(4000, 8000);
        final List<String> values = new ArrayList<>(docs);
        for (int d = 0; d < docs; d++) {
            values.add(d % 10 == 7 ? "rare-" + d : "term-" + (d % 10));
        }
        // NOTE: Room for every term, so the only values that escape are the ones no other document holds.
        final Codec escaping = codecOf(
            StringColumnOptions.DEFAULT.withPolicies(new DictionaryPolicy(8 << 20, 0.5, 0.9), SummaryPolicy.NONE)
        );
        try (Directory dir = newDirectory()) {
            index(dir, escaping, values);
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                final ProfileCollector stats = TestCollectors.elapsed();
                final StringColumnSource column = openUnder(stats, reader.leaves().get(0).reader());
                assertTrue(column.reader().hasDictionary());
                assertThat("some values escaped the dictionary", column.reader().escapeCount(), greaterThan(0L));
                final RecordingSink sink = readInPages(column, docs, between(1000, 2000));
                assertThat("refetched blocks still deliver the right values", sink.delivered, equalTo(values));
                // NOTE: Resolving where an escaped value is stored walks the ordinals from an earlier position through the
                // reader's one-block cache, so the blocks of the page are fetched again afterwards. This pins that cost;
                // a reader that kept the walk off the page's cache would decode each ordinal once and update this test.
                assertThat(TestCollectors.toKeyedValues(stats.snapshot()).get("ColumNAR.blocks.values_decoded"), greaterThan((long) docs));
            }
        }
    }

    public void testPageRefetchesFollowTheOrdinalBlocksHoldingEscapesNotTheirNumber() throws IOException {
        final int block = 128;
        final int blocksPerPage = 4;
        final int pages = between(3, 8);
        final int docs = pages * blocksPerPage * block;
        final int escapesPerPage = 6;
        final long clustered = fetchedWithEscapesIn(docs, blocksPerPage * block, block, escapesPerPage, 1);
        final long spread = fetchedWithEscapesIn(docs, blocksPerPage * block, block, escapesPerPage, 3);
        final long onePass = docs / block;
        // NOTE: Each escape is resolved by a walk inside the ordinal block that holds it, in ascending order, after the
        // page's blocks were read and its last block is cached. The last block of a page holds no escape here, so each
        // block of a page that holds escapes is fetched once more, however many escapes it holds.
        assertThat("escapes in one block of each page", clustered, equalTo(onePass + pages));
        assertThat("as many escapes, in three blocks of each page", spread, equalTo(onePass + 3L * pages));
    }

    private static long fetchedWithEscapesIn(int docs, int pageSize, int block, int escapesPerPage, int blocksWithEscapes)
        throws IOException {
        final List<String> values = new ArrayList<>(docs);
        for (int d = 0; d < docs; d++) {
            final int inPage = d % pageSize;
            final int blockInPage = inPage / block;
            final int perBlock = escapesPerPage / blocksWithEscapes;
            final boolean escaped = blockInPage < blocksWithEscapes && inPage % block >= 10 && inPage % block < 10 + perBlock;
            values.add(escaped ? "rare-" + d : "term-" + (d % 10));
        }
        final StringColumnOptions.Sizes defaults = StringColumnOptions.DEFAULT_SIZES;
        final StringColumnOptions.Sizes sizes = new StringColumnOptions.Sizes(
            defaults.valuesPerBlock(),
            defaults.plainChunks(),
            defaults.escapeChunks(),
            block,
            block,
            block,
            defaults.slotCountsBlockSize(),
            defaults.lengthBlockSize()
        );
        // NOTE: Room for every term, so the only values that escape are the ones no other document holds.
        final Codec escaping = codecOf(
            StringColumnOptions.DEFAULT.withPolicies(new DictionaryPolicy(8 << 20, 0.5, 0.9), SummaryPolicy.NONE).withSizes(sizes)
        );
        try (Directory dir = newDirectory()) {
            index(dir, escaping, values);
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                final ProfileCollector stats = TestCollectors.elapsed();
                final StringColumnSource column = openUnder(stats, reader.leaves().get(0).reader());
                assertTrue(column.reader().hasDictionary());
                assertThat(column.reader().escapeCount(), equalTo((long) escapesPerPage * (docs / pageSize)));
                assertThat(readInPages(column, docs, pageSize).delivered, equalTo(values));
                return TestCollectors.toKeyedValues(stats.snapshot()).get("ColumNAR.blocks.fetched");
            }
        }
    }

    public void testPagesHandedOverAsValuesAreCounted() throws IOException {
        final int docs = between(400, 1500);
        try (Directory dir = newDirectory()) {
            index(dir, plainZstd(), fixedWidth(docs));
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                final ProfileCollector stats = TestCollectors.elapsed();
                final RecordingSink sink = readInPages(openUnder(stats, reader.leaves().get(0).reader()), docs, between(50, 300));
                assertThat("unique values repeat too little for ordinals", sink.ordinalPages, equalTo(0));
                assertCountsMatchWhatTheSinkSaw(stats, sink);
                assertThat(TestCollectors.toKeyedValues(stats.snapshot()).get("ColumNAR.pages.as_values"), equalTo((long) sink.calls));
            }
        }
    }

    public void testPagesHandedOverAsOrdinalsAreCounted() throws IOException {
        final List<String> values = new ArrayList<>();
        final int docs = between(1000, 3000);
        for (int d = 0; d < docs; d++) {
            values.add("term-" + (d % 20));
        }
        try (Directory dir = newDirectory()) {
            index(dir, dictionary(), values);
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                final ProfileCollector stats = TestCollectors.elapsed();
                final RecordingSink sink = readInPages(openUnder(stats, reader.leaves().get(0).reader()), docs, between(100, 400));
                assertThat("a few terms over many documents are worth ordinals", sink.ordinalPages, greaterThan(0));
                assertCountsMatchWhatTheSinkSaw(stats, sink);
                assertThat(
                    "every page went out in one of the two forms",
                    TestCollectors.toKeyedValues(stats.snapshot()).get("ColumNAR.pages.as_ordinals") + TestCollectors.toKeyedValues(
                        stats.snapshot()
                    ).get("ColumNAR.pages.as_values"),
                    equalTo((long) sink.calls)
                );
            }
        }
    }

    public void testEveryNonEmptyPageIsCountedInTheFormItWasDeliveredIn() throws IOException {
        final List<String> values = new ArrayList<>();
        final int docs = between(500, 2500);
        for (int d = 0; d < docs; d++) {
            values.add(d % 11 == 3 ? null : randomFrom("alpha", "beta", "gamma", "v" + d, "d".repeat(between(1, 40))));
        }
        for (Codec codec : List.of(plainZstd(), dictionary())) {
            try (Directory dir = newDirectory()) {
                index(dir, codec, values);
                try (DirectoryReader reader = DirectoryReader.open(dir)) {
                    final ProfileCollector stats = TestCollectors.elapsed();
                    final RecordingSink sink = readInPages(openUnder(stats, reader.leaves().get(0).reader()), docs, between(30, 400));
                    assertCountsMatchWhatTheSinkSaw(stats, sink);
                    final Map<String, Long> counted = TestCollectors.toKeyedValues(stats.snapshot());
                    assertThat(
                        "every page that was read was delivered in exactly one of the two forms",
                        counted.get("ColumNAR.pages.as_ordinals") + counted.get("ColumNAR.pages.as_values"),
                        equalTo((long) sink.calls)
                    );
                }
            }
        }
    }

    public void testAnEmptyPageIsNotCounted() throws IOException {
        try (Directory dir = newDirectory()) {
            index(dir, plainZstd(), fixedWidth(300));
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                final ProfileCollector stats = TestCollectors.elapsed();
                final StringColumnSource column = openUnder(stats, reader.leaves().get(0).reader());
                assertTrue(column.reader().readBlock(new int[0], 0, 0, new RecordingSink()));
                final Map<String, Long> counted = TestCollectors.toKeyedValues(stats.snapshot());
                assertThat(counted.get("ColumNAR.pages.as_values"), equalTo(0L));
                assertThat(counted.get("ColumNAR.pages.values_in_value_pages"), equalTo(0L));
                assertThat(counted.get("ColumNAR.pages.as_ordinals"), equalTo(0L));
            }
        }
    }

    public void testProfilingDoesNotChangeThePagesThatAreDelivered() throws IOException {
        final List<String> values = new ArrayList<>();
        final int docs = between(500, 2000);
        for (int d = 0; d < docs; d++) {
            values.add(d % 13 == 5 ? null : randomFrom("alpha", "beta", "v" + d));
        }
        for (Codec codec : List.of(plainZstd(), dictionary())) {
            try (Directory dir = newDirectory()) {
                index(dir, codec, values);
                try (DirectoryReader reader = DirectoryReader.open(dir)) {
                    final LeafReader leaf = reader.leaves().get(0).reader();
                    final int pageSize = between(40, 300);
                    final RecordingSink plain = readInPages((StringColumnSource) leaf.getBinaryDocValues(FIELD), docs, pageSize);
                    final RecordingSink profiled = readInPages(openUnder(TestCollectors.elapsed(), leaf), docs, pageSize);
                    assertThat(profiled.delivered, equalTo(plain.delivered));
                    assertThat(profiled.ordinalPages, equalTo(plain.ordinalPages));
                    assertThat(profiled.valuePages, equalTo(plain.valuePages));
                }
            }
        }
    }

    public void testANumericColumnReportsItsReaderAndTheBlocksItReadsAndNothingElse() throws IOException {
        final int blockSize = randomValidBlockSize();
        final int docs = blockSize * between(2, 6) + between(1, blockSize - 1);
        final long[] values = new long[docs];
        for (int d = 0; d < docs; d++) {
            values[d] = randomBoolean() ? randomLong() : between(0, 1000);
        }
        try (Directory dir = newDirectory()) {
            indexNumeric(dir, blockSize, values);
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                final LeafReader leaf = reader.leaves().get(0).reader();
                final ProfileCollector stats = TestCollectors.elapsed();
                final BinaryDocValues column;
                try (Binding ignored = Scopes.bind(TestCollectors.eager(stats))) {
                    column = leaf.getBinaryDocValues(FIELD);
                }
                assertThat("profiling does not change the values", readLongs(column, docs), equalTo(values));
                final Map<String, Long> counted = TestCollectors.toKeyedValues(stats.snapshot());
                assertThat(counted.get("ColumNAR.readers.opened"), equalTo(1L));
                assertThat(
                    "a sequential scan fetches every block once",
                    counted.get("ColumNAR.blocks.fetched"),
                    equalTo((long) ((docs + blockSize - 1) / blockSize))
                );
                assertThat(counted.get("ColumNAR.blocks.bytes_fetched"), greaterThan(0L));
                assertThat(
                    counted.getOrDefault("ColumNAR.blocks.decompressed", 0L),
                    lessThanOrEqualTo(counted.get("ColumNAR.blocks.fetched"))
                );
                assertThat(
                    "a scan decodes each block once, the last one holding the remainder",
                    counted.get("ColumNAR.blocks.values_decoded"),
                    equalTo((long) docs)
                );
                assertTrue(
                    "a numeric column has no pages, chunks or skip metrics, but reported " + counted.keySet(),
                    NUMERIC_METRICS.containsAll(counted.keySet())
                );
            }
        }
    }

    public void testANumericReaderOpenedOutsideAnyScopeCountsNothing() throws IOException {
        final int blockSize = randomValidBlockSize();
        final long[] values = new long[blockSize * between(2, 4)];
        for (int d = 0; d < values.length; d++) {
            values[d] = randomLong();
        }
        try (Directory dir = newDirectory()) {
            indexNumeric(dir, blockSize, values);
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                final LeafReader leaf = reader.leaves().get(0).reader();
                final BinaryDocValues earlier = leaf.getBinaryDocValues(FIELD);
                final ProfileCollector stats = TestCollectors.elapsed();
                try (Binding ignored = Scopes.bind(TestCollectors.eager(stats))) {
                    assertThat(readLongs(earlier, values.length), equalTo(values));
                }
                assertTrue("a reader opened before the scope reports nothing to it", stats.snapshot().isEmpty());
            }
        }
    }

    public void testANumericBlockDecodeIsTimedAfterTheFetch() throws IOException {
        final int blockSize = randomValidBlockSize();
        final int docs = blockSize * between(2, 6) + between(1, blockSize - 1);
        final long[] values = new long[docs];
        for (int d = 0; d < docs; d++) {
            values[d] = randomLong();
        }
        try (Directory dir = newDirectory()) {
            indexNumeric(dir, blockSize, values);
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                final LeafReader leaf = reader.leaves().get(0).reader();
                final long step = between(2, 50);
                final long[] now = { 0 };
                final ProfileCollector stats = TestCollectors.withClock(() -> now[0] += step);
                final BinaryDocValues column;
                try (Binding ignored = Scopes.bind(TestCollectors.eager(stats))) {
                    column = leaf.getBinaryDocValues(FIELD);
                }
                assertThat(readLongs(column, docs), equalTo(values));
                final Map<String, Long> counted = TestCollectors.toKeyedValues(stats.snapshot());
                assertThat(
                    "each fetched block is decoded once, in a window with no nested clock reads",
                    counted.get("ColumNAR.blocks.decode_time_nanos"),
                    equalTo(step * counted.get("ColumNAR.blocks.fetched"))
                );
                assertThat(counted.get("ColumNAR.blocks.values_decoded"), equalTo((long) docs));
            }
        }
    }

    public void testANumericBlockDecodeCpuTimeIsMeasuredWhenAskedForAndAbsentOtherwise() throws IOException {
        final int blockSize = randomValidBlockSize();
        final int docs = blockSize * between(2, 6) + between(1, blockSize - 1);
        final long[] values = new long[docs];
        for (int d = 0; d < docs; d++) {
            values[d] = randomLong();
        }
        try (Directory dir = newDirectory()) {
            indexNumeric(dir, blockSize, values);
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                final LeafReader leaf = reader.leaves().get(0).reader();
                final long step = between(2, 50);
                final long cpuStep = between(1, (int) step);
                final long[] now = { 0 };
                final long[] cpuNow = { 0 };
                final ProfileCollector stats = TestCollectors.withClocks(() -> now[0] += step, () -> cpuNow[0] += cpuStep);
                final BinaryDocValues column;
                try (Binding ignored = Scopes.bind(TestCollectors.eager(stats))) {
                    column = leaf.getBinaryDocValues(FIELD);
                }
                assertThat(readLongs(column, docs), equalTo(values));
                final Map<String, Long> counted = TestCollectors.toKeyedValues(stats.snapshot());
                assertThat(
                    "the CPU companion is sampled with the elapsed decode timer",
                    counted.get("ColumNAR.blocks.decode_cpu_time_nanos"),
                    equalTo(cpuStep * counted.get("ColumNAR.blocks.fetched"))
                );
                assertThat(counted.get("ColumNAR.blocks.decode_time_nanos"), equalTo(step * counted.get("ColumNAR.blocks.fetched")));
                assertThat(
                    "the CPU mode adds exactly one key to the numeric layout",
                    counted.keySet(),
                    equalTo(
                        Set.of(
                            "ColumNAR.readers.opened",
                            "ColumNAR.readers.open_time_nanos",
                            "ColumNAR.readers.plain",
                            "ColumNAR.readers.dictionary",
                            "ColumNAR.readers.numeric",
                            "ColumNAR.blocks.fetched",
                            "ColumNAR.blocks.bytes_fetched",
                            "ColumNAR.blocks.fetch_time_nanos",
                            "ColumNAR.blocks.decode_time_nanos",
                            "ColumNAR.blocks.decode_cpu_time_nanos",
                            "ColumNAR.blocks.values_decoded"
                        )
                    )
                );
            }
        }
    }

    public void testADictionaryColumnCountsAndTimesItsTermLookups() throws IOException {
        final List<String> values = new ArrayList<>();
        final int docs = between(2000, 4000);
        for (int d = 0; d < docs; d++) {
            values.add("term-" + (d % 20));
        }
        try (Directory dir = newDirectory()) {
            index(dir, dictionary(), values);
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                final ProfileCollector timed = TestCollectors.elapsed();
                final ProfileCollector countersOnly = TestCollectors.countersOnly();
                final LeafReader leaf = reader.leaves().get(0).reader();
                final BinaryDocValues timedColumn;
                final BinaryDocValues countedColumn;
                try (Binding ignored = Scopes.bind(TestCollectors.eager(timed))) {
                    timedColumn = leaf.getBinaryDocValues(FIELD);
                }
                try (Binding ignored = Scopes.bind(TestCollectors.eager(countersOnly))) {
                    countedColumn = leaf.getBinaryDocValues(FIELD);
                }
                scan(timedColumn);
                scan(countedColumn);
                assertThat(
                    "a scan resolves each document's term through the dictionary",
                    TestCollectors.toKeyedValues(countersOnly.snapshot()).get("ColumNAR.dictionary.term_lookups"),
                    equalTo((long) docs)
                );
                assertThat(
                    TestCollectors.toKeyedValues(countersOnly.snapshot()),
                    equalTo(withoutTimes(TestCollectors.toKeyedValues(timed.snapshot())))
                );
                assertThat(TestCollectors.toKeyedValues(timed.snapshot()), hasKey("ColumNAR.dictionary.resolve_time_nanos"));
                assertThat(
                    "the dictionary is stored verbatim, so resolving terms reads it verbatim",
                    TestCollectors.toKeyedValues(timed.snapshot()).get("ColumNAR.uncompressed.reads"),
                    greaterThan(0L)
                );
                assertThat(TestCollectors.toKeyedValues(timed.snapshot()).get("ColumNAR.uncompressed.bytes_read"), greaterThan(0L));
            }
        }
    }

    private static Map<String, Long> withoutTimes(Map<String, Long> counted) {
        final Map<String, Long> counters = new TreeMap<>(counted);
        counters.keySet().removeIf(name -> name.endsWith("_time_nanos"));
        return counters;
    }

    private static StringColumnSource openUnder(ProfileCollector stats, LeafReader leaf) throws IOException {
        try (Binding ignored = Scopes.bind(TestCollectors.eager(stats))) {
            return (StringColumnSource) leaf.getBinaryDocValues(FIELD);
        }
    }

    private static RecordingSink readInPages(StringColumnSource column, int docs, int pageSize) throws IOException {
        final int[] ids = new int[docs];
        for (int d = 0; d < docs; d++) {
            ids[d] = d;
        }
        final RecordingSink sink = new RecordingSink();
        for (int from = 0; from < docs; from += pageSize) {
            final int count = Math.min(pageSize, docs - from);
            assertTrue("a layout declined a page, so a fallback metric is now needed", column.reader().readBlock(ids, from, count, sink));
            sink.calls++;
        }
        return sink;
    }

    private static void assertCountsMatchWhatTheSinkSaw(ProfileCollector stats, RecordingSink sink) {
        final Map<String, Long> counted = TestCollectors.toKeyedValues(stats.snapshot());
        assertThat(counted.get("ColumNAR.pages.as_ordinals"), equalTo((long) sink.ordinalPages));
        assertThat(counted.get("ColumNAR.pages.as_values"), equalTo((long) sink.valuePages));
        assertThat(counted.get("ColumNAR.pages.values_in_ordinal_pages"), equalTo(sink.ordinalValues));
        assertThat(counted.get("ColumNAR.pages.values_in_value_pages"), equalTo(sink.valueValues));
    }

    private static final class RecordingSink implements StringBlockSink {
        final List<String> delivered = new ArrayList<>();
        int calls;
        int ordinalPages;
        int valuePages;
        long ordinalValues;
        long valueValues;

        @Override
        public void appendOrdinals(int[] ordinals, int valueCount, int[] valueCounts, int docCount, BytesRef[] dictionary, int size) {
            ordinalPages++;
            ordinalValues += valueCount;
            for (int i = 0; i < valueCount; i++) {
                delivered.add(dictionary[ordinals[i]].utf8ToString());
            }
        }

        @Override
        public Values values(int valueCount, int[] valueCounts, int docCount) {
            if (docCount > 0) {
                valuePages++;
                valueValues += valueCount;
            }
            return new Values() {
                @Override
                public void append(BytesRef value) {
                    delivered.add(value.utf8ToString());
                }

                @Override
                public void finish() {}

                @Override
                public void close() {}
            };
        }
    }

    private static List<String> fixedWidth(int docs) {
        final List<String> values = new ArrayList<>(docs);
        for (int d = 0; d < docs; d++) {
            values.add(String.format(Locale.ROOT, "%0" + WIDTH + "d", d));
        }
        return values;
    }

    private static List<String> scan(BinaryDocValues values) throws IOException {
        final List<String> read = new ArrayList<>();
        for (int doc = values.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = values.nextDoc()) {
            read.add(doc + "=" + BytesRef.deepCopyOf(values.binaryValue()).utf8ToString());
        }
        return read;
    }

    private static long[] readLongs(BinaryDocValues column, int docs) throws IOException {
        final long[] read = new long[docs];
        final long[][] decoded = { new long[1] };
        int seen = 0;
        for (int doc = column.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = column.nextDoc()) {
            assertThat(NumericBinaryPayload.decode(column.binaryValue(), decoded), equalTo(1));
            read[doc] = decoded[0][0];
            seen++;
        }
        assertThat(seen, equalTo(docs));
        return read;
    }

    private static void indexNumeric(Directory dir, int blockSize, long[] values) throws IOException {
        final Codec codec = columnarCodec(
            new ColumNARDocValuesFormat((fieldName, type) -> NumericPipeline::defaultPipeline, field -> ColumnarFieldType.LONG, blockSize)
        );
        final IndexWriterConfig config = new IndexWriterConfig().setCodec(codec).setMergePolicy(new LogDocMergePolicy());
        final BytesRefBuilder builder = new BytesRefBuilder();
        try (IndexWriter writer = new IndexWriter(dir, config)) {
            for (long value : values) {
                final Document doc = new Document();
                final BytesRef payload = BytesRef.deepCopyOf(NumericBinaryPayload.encode(new long[] { value }, 1, builder));
                doc.add(new Field(FIELD, payload, columnarBinaryFieldType()));
                writer.addDocument(doc);
            }
            writer.forceMerge(1);
        }
    }

    private static void index(Directory dir, Codec codec, List<String> values) throws IOException {
        final IndexWriterConfig config = new IndexWriterConfig().setCodec(codec).setMergePolicy(new LogDocMergePolicy());
        try (IndexWriter writer = new IndexWriter(dir, config)) {
            for (String value : values) {
                final Document doc = new Document();
                if (value != null) {
                    doc.add(new Field(FIELD, new BytesRef(value), columnarBinaryFieldType()));
                }
                writer.addDocument(doc);
            }
            writer.forceMerge(1);
        }
    }

    private static Codec plainZstd() {
        final StringColumnOptions.Sizes defaults = StringColumnOptions.DEFAULT_SIZES;
        final StringColumnOptions.Sizes sizes = new StringColumnOptions.Sizes(
            defaults.valuesPerBlock(),
            new ChunkBounds(CHUNK_BYTES, defaults.plainChunks().maxValues()),
            defaults.escapeChunks(),
            defaults.packedOrdinalBlockSize(),
            defaults.compressedOrdinalBlockSize(),
            defaults.escapeRankBlockSize(),
            defaults.slotCountsBlockSize(),
            defaults.lengthBlockSize()
        );
        return codecOf(StringColumnOptions.DEFAULT.withPolicies(DictionaryPolicy.NONE, SummaryPolicy.NONE).withSizes(sizes));
    }

    private static Codec dictionary() {
        return codecOf(StringColumnOptions.DEFAULT);
    }

    private static Codec codecOf(StringColumnOptions options) {
        return columnarCodec(
            new ColumNARDocValuesFormat(
                (fieldName, type) -> NumericPipeline::defaultPipeline,
                field -> ColumnarFieldType.STRING,
                ColumNARDocValuesFormat.DEFAULT_BLOCK_SIZE,
                StringColumnOptionsSelector.always(options.withSingleValued(true))
            )
        );
    }
}
