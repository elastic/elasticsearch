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
import org.elasticsearch.codec.profiler.ProfileCollector;
import org.elasticsearch.codec.profiler.Scopes;
import org.elasticsearch.codec.profiler.Scopes.Binding;
import org.elasticsearch.codec.profiler.TestCollectors;
import org.elasticsearch.columnar.numeric.NumericPipeline;
import org.elasticsearch.columnar.string.DictionaryPolicy;
import org.elasticsearch.columnar.string.PageBudget;
import org.elasticsearch.columnar.string.StringBinaryPayload;
import org.elasticsearch.columnar.string.StringBlockSink;
import org.elasticsearch.columnar.string.StringColumnOptions;
import org.elasticsearch.columnar.string.StringColumnOptionsSelector;
import org.elasticsearch.columnar.string.StringColumnSource;
import org.elasticsearch.columnar.string.SummaryPolicy;
import org.elasticsearch.columnar.substrate.ChunkBounds;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.LongSupplier;

import static org.elasticsearch.columnar.ColumnarTestUtils.columnarBinaryFieldType;
import static org.elasticsearch.columnar.ColumnarTestUtils.columnarCodec;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.sameInstance;

public class ColumnarReadTimingTests extends ESTestCase {

    private static final String FIELD = "field";
    private static final int WIDTH = 16;
    private static final int CHUNK_BYTES = 256;

    // NOTE: The clock moves a fixed step on every read. A measured call reads it once to start and once to stop, so it
    // lasts exactly one step, and the expected time of a number of calls is independent of the code under test.
    private static final class SteppingClock implements LongSupplier {
        private final long step;
        private long now = randomBoolean() ? 0 : 1_000_000;
        long reads;

        SteppingClock(long step) {
            this.step = step;
        }

        @Override
        public long getAsLong() {
            reads++;
            final long value = now;
            now += step;
            return value;
        }
    }

    public void testTheReaderOpenTimeCoversTheOpenAndNotTheReadsAfterIt() throws IOException {
        try (Directory dir = newDirectory()) {
            index(dir, plainZstd(), fixedWidth(between(1500, 3000)));
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                final LeafReader leaf = reader.leaves().get(0).reader();
                final long step = between(2, 90);
                final ProfileCollector stats = TestCollectors.withClock(new SteppingClock(step));
                final int opens = between(1, 5);
                final List<BinaryDocValues> columns = new ArrayList<>();
                for (int i = 0; i < opens; i++) {
                    columns.add(openUnder(stats, leaf));
                }
                final long afterOpening = TestCollectors.toKeyedValues(stats.snapshot()).get("ColumNAR.readers.open_time_nanos");
                assertThat("each open is one call of one step", afterOpening, equalTo(step * opens));
                for (BinaryDocValues column : columns) {
                    scan(column);
                }
                assertThat(
                    "reading does not add to the open time",
                    TestCollectors.toKeyedValues(stats.snapshot()).get("ColumNAR.readers.open_time_nanos"),
                    equalTo(afterOpening)
                );
                assertThat(TestCollectors.toKeyedValues(stats.snapshot()).get("ColumNAR.readers.opened"), equalTo((long) opens));
            }
        }
    }

    public void testEachCollectorGetsTheTimeOfTheReadersItOpenedWhateverTheInterleaving() throws IOException {
        try (Directory dir = newDirectory()) {
            index(dir, plainZstd(), fixedWidth(between(1500, 3000)));
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                final LeafReader leaf = reader.leaves().get(0).reader();
                final long stepA = 100;
                final long stepB = 7;
                final ProfileCollector a = TestCollectors.withClock(new SteppingClock(stepA));
                final ProfileCollector b = TestCollectors.withClock(new SteppingClock(stepB));
                final BinaryDocValues columnA = openUnder(a, leaf);
                final BinaryDocValues columnB = openUnder(b, leaf);
                try (Binding ignored = Scopes.bind(TestCollectors.eager(TestCollectors.elapsed()))) {
                    for (int step = 0; step < 300; step++) {
                        columnA.nextDoc();
                        columnA.binaryValue();
                        columnB.nextDoc();
                        columnB.binaryValue();
                    }
                    scan(columnA);
                    scan(columnB);
                }
                assertThat(
                    TestCollectors.toKeyedValues(a.snapshot()).get("ColumNAR.chunks.decompress_time_nanos"),
                    equalTo(stepA * TestCollectors.toKeyedValues(a.snapshot()).get("ColumNAR.chunks.decompressed"))
                );
                assertThat(
                    TestCollectors.toKeyedValues(b.snapshot()).get("ColumNAR.chunks.decompress_time_nanos"),
                    equalTo(stepB * TestCollectors.toKeyedValues(b.snapshot()).get("ColumNAR.chunks.decompressed"))
                );
                assertThat(TestCollectors.toKeyedValues(a.snapshot()).get("ColumNAR.chunks.decompressed"), greaterThan(1L));
                assertThat(TestCollectors.toKeyedValues(a.snapshot()).get("ColumNAR.readers.open_time_nanos"), equalTo(stepA));
                assertThat(TestCollectors.toKeyedValues(b.snapshot()).get("ColumNAR.readers.open_time_nanos"), equalTo(stepB));
            }
        }
    }

    public void testTwoReadersOfOneCollectorAddUpInOneAccumulator() throws IOException {
        try (Directory dir = newDirectory()) {
            index(dir, plainZstd(), fixedWidth(between(1500, 3000)));
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                final LeafReader leaf = reader.leaves().get(0).reader();
                final long step = between(2, 50);
                final ProfileCollector stats = TestCollectors.withClock(new SteppingClock(step));
                final BinaryDocValues first = openUnder(stats, leaf);
                final BinaryDocValues second = openUnder(stats, leaf);
                scan(first);
                scan(second);
                final Map<String, Long> counted = TestCollectors.toKeyedValues(stats.snapshot());
                assertThat(counted.get("ColumNAR.readers.open_time_nanos"), equalTo(2 * step));
                assertThat(
                    counted.get("ColumNAR.chunks.decompress_time_nanos"),
                    equalTo(step * counted.get("ColumNAR.chunks.decompressed"))
                );
                assertThat("both readers inflated every chunk", counted.get("ColumNAR.chunks.decompressed") % 2, equalTo(0L));
            }
        }
    }

    public void testTimeStaysWithTheCollectorOfTheReaderWhenAnotherThreadReadsIt() throws Exception {
        try (Directory dir = newDirectory()) {
            index(dir, plainZstd(), fixedWidth(between(1500, 3000)));
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                final long step = between(2, 50);
                final ProfileCollector owner = TestCollectors.withClock(new SteppingClock(step));
                final BinaryDocValues column = openUnder(owner, reader.leaves().get(0).reader());
                final ExecutorService executor = Executors.newSingleThreadExecutor();
                try {
                    final ProfileCollector bystander = TestCollectors.withClock(new SteppingClock(3));
                    executor.submit(() -> {
                        try (Binding ignored = Scopes.bind(TestCollectors.eager(bystander))) {
                            scan(column);
                        }
                        return null;
                    }).get();
                    assertThat(TestCollectors.toKeyedValues(bystander.snapshot()), equalTo(Map.of()));
                } finally {
                    terminate(executor);
                }
                assertThat(TestCollectors.toKeyedValues(owner.snapshot()).get("ColumNAR.chunks.decompressed"), greaterThan(0L));
                assertThat(
                    TestCollectors.toKeyedValues(owner.snapshot()).get("ColumNAR.chunks.decompress_time_nanos"),
                    equalTo(step * TestCollectors.toKeyedValues(owner.snapshot()).get("ColumNAR.chunks.decompressed"))
                );
            }
        }
    }

    public void testACollectorWithoutTimingCountsTheSameWorkAndReportsNoTimeMetric() throws IOException {
        try (Directory dir = newDirectory()) {
            index(dir, plainZstd(), fixedWidth(between(1500, 3000)));
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                final LeafReader leaf = reader.leaves().get(0).reader();
                final ProfileCollector timed = TestCollectors.withClock(new SteppingClock(5));
                final ProfileCollector countersOnly = TestCollectors.countersOnly();
                scan(openUnder(timed, leaf));
                scan(openUnder(countersOnly, leaf));
                for (String key : TestCollectors.toKeyedValues(countersOnly.snapshot()).keySet()) {
                    assertThat(key, key.endsWith("_time_nanos"), equalTo(false));
                    assertThat(
                        key,
                        TestCollectors.toKeyedValues(countersOnly.snapshot()).get(key),
                        equalTo(TestCollectors.toKeyedValues(timed.snapshot()).get(key))
                    );
                }
                assertThat(TestCollectors.toKeyedValues(timed.snapshot()), hasKey("ColumNAR.chunks.decompress_time_nanos"));
                assertThat(TestCollectors.toKeyedValues(countersOnly.snapshot()), not(hasKey("ColumNAR.chunks.decompress_time_nanos")));
                assertThat(TestCollectors.toKeyedValues(countersOnly.snapshot()), not(hasKey("ColumNAR.readers.open_time_nanos")));
            }
        }
    }

    public void testADictionaryColumnTimesItsBlockReadsAndTheInflationInsideThem() throws IOException {
        final List<String> values = new ArrayList<>();
        final int docs = between(2000, 6000);
        for (int d = 0; d < docs; d++) {
            values.add("term-" + (d % 20));
        }
        try (Directory dir = newDirectory()) {
            index(dir, codecOf(StringColumnOptions.DEFAULT), values);
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                final long step = between(2, 50);
                final SteppingClock clock = new SteppingClock(step);
                final ProfileCollector stats = TestCollectors.withClock(clock);
                scan(openUnder(stats, reader.leaves().get(0).reader()));
                final Map<String, Long> counted = TestCollectors.toKeyedValues(stats.snapshot());
                final long blocks = counted.get("ColumNAR.blocks.fetched");
                final long inflated = counted.getOrDefault("ColumNAR.blocks.decompressed", 0L);
                assertThat(blocks, greaterThan(0L));
                assertThat(
                    "an exact timer measures each inflation as one step",
                    counted.getOrDefault("ColumNAR.blocks.decompress_time_nanos", 0L),
                    equalTo(step * inflated)
                );
                assertThat(
                    "a fetch lasts a step, and one more for each inflation it contains",
                    counted.get("ColumNAR.blocks.fetch_time_nanos"),
                    equalTo(step * (blocks + 2 * inflated))
                );
                assertThat(
                    counted.get("ColumNAR.blocks.fetch_time_nanos"),
                    greaterThanOrEqualTo(counted.getOrDefault("ColumNAR.blocks.decompress_time_nanos", 0L))
                );
                assertThat(
                    "each fetched block is decoded once, after the fetch, in a window with no nested clock reads",
                    counted.get("ColumNAR.blocks.decode_time_nanos"),
                    equalTo(step * blocks)
                );
            }
        }
    }

    public void testAMultiValuedDictionaryColumnDecodesOrdinalAndSlotCountBlocksUnderOneTimer() throws IOException {
        final int docs = 3000;
        final String[][] values = new String[docs][];
        long totalValues = 0;
        for (int d = 0; d < docs; d++) {
            values[d] = new String[1 + d % 3];
            for (int s = 0; s < values[d].length; s++) {
                values[d][s] = "term-" + ((d + s) % 20);
            }
            totalValues += values[d].length;
        }
        try (Directory dir = newDirectory()) {
            indexMulti(dir, multiValuedCodecOf(StringColumnOptions.DEFAULT), values);
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                final long step = between(2, 50);
                final SteppingClock clock = new SteppingClock(step);
                final ProfileCollector stats = TestCollectors.withClock(clock);
                scan(openUnder(stats, reader.leaves().get(0).reader()));
                final Map<String, Long> counted = TestCollectors.toKeyedValues(stats.snapshot());
                assertThat("the cyclic terms take the dictionary layout", counted, hasKey("ColumNAR.dictionary.term_lookups"));
                assertThat(counted.get("ColumNAR.blocks.fetched"), greaterThan(1L));
                assertThat(
                    "ordinal blocks and slot count blocks are decoded by different readers under the one timer",
                    counted.get("ColumNAR.blocks.decode_time_nanos"),
                    equalTo(step * counted.get("ColumNAR.blocks.fetched"))
                );
                assertThat(
                    "a scan decodes one ordinal per value and one slot count per document",
                    counted.get("ColumNAR.blocks.values_decoded"),
                    equalTo(totalValues + docs)
                );
            }
        }
    }

    public void testAPageLoadIsTimedAroundTheWholeCallIncludingTheSinkAndTheChunksInsideIt() throws IOException {
        final int docs = between(1500, 3000);
        try (Directory dir = newDirectory()) {
            index(dir, plainZstd(), fixedWidth(docs));
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                final long[] now = { 0 };
                final ProfileCollector stats = TestCollectors.withClock(() -> now[0]);
                final StringColumnSource column = (StringColumnSource) openUnder(stats, reader.leaves().get(0).reader());
                final StringBlockSink slowSink = new CollectingSink(new ArrayList<>(), () -> now[0] += 500);

                assertTrue(column.reader().readBlock(allDocs(docs), 0, docs, slowSink));

                assertThat(
                    "the sink's 500 is inside the page",
                    TestCollectors.toKeyedValues(stats.snapshot()).get("ColumNAR.pages.call_time_nanos"),
                    equalTo(500L)
                );
                final long chunks = TestCollectors.toKeyedValues(stats.snapshot()).get("ColumNAR.chunks.decompressed");
                assertThat(chunks, greaterThan(1L));
                assertThat(
                    "the sink is outside every chunk call, each of which took no time",
                    TestCollectors.toKeyedValues(stats.snapshot()).get("ColumNAR.chunks.decompress_time_nanos"),
                    equalTo(chunks)
                );
            }
        }
    }

    public void testASinkThatThrowsEndsThePageTimerAndLeavesTheReaderUsable() throws IOException {
        final int docs = between(1500, 3000);
        final List<String> values = fixedWidth(docs);
        try (Directory dir = newDirectory()) {
            index(dir, plainZstd(), values);
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                final long step = between(2, 40);
                final SteppingClock clock = new SteppingClock(step);
                final ProfileCollector stats = TestCollectors.withClock(clock);
                final StringColumnSource column = (StringColumnSource) openUnder(stats, reader.leaves().get(0).reader());
                final int[] ids = allDocs(docs);
                final IllegalStateException failure = new IllegalStateException("the consumer failed");

                final long readsBefore = clock.reads;
                final StringBlockSink failing = new CollectingSink(new ArrayList<>(), () -> { throw failure; });
                assertThat(
                    expectThrows(IllegalStateException.class, () -> column.reader().readBlock(ids, 0, 100, failing)),
                    sameInstance(failure)
                );
                final long readsDuring = clock.reads - readsBefore;
                assertThat(
                    "the failed call is timed from the first clock read to the last, whatever ran inside it",
                    TestCollectors.toKeyedValues(stats.snapshot()).get("ColumNAR.pages.call_time_nanos"),
                    equalTo(step * (readsDuring - 1))
                );
                assertThat(
                    "the page was counted when it began",
                    TestCollectors.toKeyedValues(stats.snapshot()).get("ColumNAR.pages.as_values"),
                    equalTo(1L)
                );

                final List<String> page = new ArrayList<>();
                assertTrue(
                    "the same reader serves the next page, so no timer is left active",
                    column.reader().readBlock(ids, 100, 50, new CollectingSink(page, () -> {}))
                );
                assertThat(page, equalTo(values.subList(100, 150)));
                assertThat(TestCollectors.toKeyedValues(stats.snapshot()).get("ColumNAR.pages.as_values"), equalTo(2L));
                assertThat(
                    "the second page was timed too",
                    TestCollectors.toKeyedValues(stats.snapshot()).get("ColumNAR.pages.call_time_nanos"),
                    greaterThan(step * (readsDuring - 1))
                );
            }
        }
    }

    public void testTheMetricsOfPlainAndDictionaryPageReadsFitOneCollectorWithRoomToSpare() throws IOException {
        final List<String> repeated = new ArrayList<>();
        for (int d = 0; d < 4000; d++) {
            repeated.add("term-" + (d % 20));
        }
        try (Directory plainDir = newDirectory(); Directory dictionaryDir = newDirectory()) {
            index(plainDir, plainZstd(), fixedWidth(3000));
            index(dictionaryDir, codecOf(StringColumnOptions.DEFAULT), repeated);
            try (DirectoryReader plain = DirectoryReader.open(plainDir); DirectoryReader dictionary = DirectoryReader.open(dictionaryDir)) {
                final ProfileCollector stats = TestCollectors.elapsed();
                final int[] ids = allDocs(3000);
                final StringColumnSource plainColumn = (StringColumnSource) openUnder(stats, plain.leaves().get(0).reader());
                plainColumn.reader().readBlock(ids, 0, 3000, new CollectingSink(new ArrayList<>(), () -> {}));
                final StringColumnSource dictionaryColumn = (StringColumnSource) openUnder(stats, dictionary.leaves().get(0).reader());
                dictionaryColumn.reader().readBlock(allDocs(4000), 0, 4000, new CollectingSink(new ArrayList<>(), () -> {}));
                final long timeMetrics = TestCollectors.toKeyedValues(stats.snapshot())
                    .keySet()
                    .stream()
                    .filter(name -> name.endsWith("_time_nanos"))
                    .count();
                assertThat(
                    TestCollectors.toKeyedValues(stats.snapshot())
                        .keySet()
                        .stream()
                        .filter(name -> name.endsWith("_time_nanos"))
                        .sorted()
                        .toList(),
                    equalTo(
                        List.of(
                            "ColumNAR.blocks.decode_time_nanos",
                            "ColumNAR.blocks.fetch_time_nanos",
                            "ColumNAR.chunks.decompress_time_nanos",
                            "ColumNAR.dictionary.resolve_time_nanos",
                            "ColumNAR.ordinal_skip.navigation_time_nanos",
                            "ColumNAR.pages.byte_lengths_call_time_nanos",
                            "ColumNAR.pages.call_time_nanos",
                            "ColumNAR.readers.open_time_nanos",
                            "ColumNAR.uncompressed.read_time_nanos"
                        )
                    )
                );
                assertThat(timeMetrics, equalTo(9L));
                assertThat(
                    "room for the features still to come",
                    TestCollectors.toKeyedValues(stats.snapshot()).size(),
                    lessThanOrEqualTo(TestCollectors.MAX_METRICS - 8)
                );
            }
        }
    }

    public void testReadingByteLengthsIsTimedWhereThePageTimerDoesNotSeeIt() throws IOException {
        final int docs = between(1500, 3000);
        try (Directory dir = newDirectory()) {
            index(dir, plainZstd(), fixedWidth(docs));
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                final long step = between(2, 40);
                final SteppingClock clock = new SteppingClock(step);
                final ProfileCollector stats = TestCollectors.withClock(clock);
                final StringColumnSource column = (StringColumnSource) openUnder(stats, reader.leaves().get(0).reader());
                final int[] counts = new int[docs];
                final int[] lengths = new int[docs];

                final long readsBefore = clock.reads;
                column.reader().readByteLengths(allDocs(docs), 0, docs, counts, lengths, PageBudget.UNLIMITED);
                final long readsDuring = clock.reads - readsBefore;

                for (int d = 0; d < docs; d++) {
                    assertThat(counts[d], equalTo(1));
                    assertThat("a value of " + WIDTH + " bytes", lengths[d], equalTo(WIDTH));
                }
                assertThat(
                    "timed from the first clock read of the call to the last, whatever ran inside it",
                    TestCollectors.toKeyedValues(stats.snapshot()).get("ColumNAR.pages.byte_lengths_call_time_nanos"),
                    equalTo(step * (readsDuring - 1))
                );
                assertThat(
                    "no page was handed over, so the page timer saw nothing",
                    TestCollectors.toKeyedValues(stats.snapshot()).get("ColumNAR.pages.call_time_nanos"),
                    equalTo(0L)
                );
                assertThat(TestCollectors.toKeyedValues(stats.snapshot()).get("ColumNAR.pages.as_values"), equalTo(0L));
            }
        }
    }

    public void testAByteLengthsReadThatFailsIsStillTimedAndTheReaderStaysUsable() throws IOException {
        final int docs = between(1500, 3000);
        try (Directory dir = newDirectory()) {
            index(dir, plainZstd(), fixedWidth(docs));
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                final long step = between(2, 40);
                final SteppingClock clock = new SteppingClock(step);
                final ProfileCollector stats = TestCollectors.withClock(clock);
                final StringColumnSource column = (StringColumnSource) openUnder(stats, reader.leaves().get(0).reader());
                final int[] counts = new int[docs];
                final int[] lengths = new int[docs];
                final boolean[] fail = { true };
                final IllegalStateException failure = new IllegalStateException("over budget");
                final PageBudget budget = bytes -> {
                    if (fail[0]) {
                        throw failure;
                    }
                };

                final long readsBefore = clock.reads;
                assertThat(
                    expectThrows(
                        IllegalStateException.class,
                        () -> column.reader().readByteLengths(allDocs(docs), 0, docs, counts, lengths, budget)
                    ),
                    sameInstance(failure)
                );
                final long readsDuring = clock.reads - readsBefore;
                assertThat(
                    TestCollectors.toKeyedValues(stats.snapshot()).get("ColumNAR.pages.byte_lengths_call_time_nanos"),
                    equalTo(step * (readsDuring - 1))
                );

                fail[0] = false;
                column.reader().readByteLengths(allDocs(docs), 0, docs, counts, lengths, budget);
                assertThat("no timer was left active", lengths[docs - 1], equalTo(WIDTH));
                assertThat(
                    TestCollectors.toKeyedValues(stats.snapshot()).get("ColumNAR.pages.byte_lengths_call_time_nanos"),
                    greaterThan(step * (readsDuring - 1))
                );
            }
        }
    }

    public void testAByteLengthsReadOfACollectorWithoutTimingReportsNoTime() throws IOException {
        final int docs = between(1500, 3000);
        try (Directory dir = newDirectory()) {
            index(dir, plainZstd(), fixedWidth(docs));
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                final ProfileCollector stats = TestCollectors.countersOnly();
                final StringColumnSource column = (StringColumnSource) openUnder(stats, reader.leaves().get(0).reader());
                final int[] lengths = new int[docs];
                column.reader().readByteLengths(allDocs(docs), 0, docs, new int[docs], lengths, PageBudget.UNLIMITED);
                assertThat(TestCollectors.toKeyedValues(stats.snapshot()), not(hasKey("ColumNAR.pages.byte_lengths_call_time_nanos")));
                assertThat("the lengths are the same without timing", lengths[docs / 2], equalTo(WIDTH));
                assertThat(
                    "the reader was still counted",
                    TestCollectors.toKeyedValues(stats.snapshot()).get("ColumNAR.readers.opened"),
                    equalTo(1L)
                );
            }
        }
    }

    private static int[] allDocs(int docs) {
        final int[] ids = new int[docs];
        for (int d = 0; d < docs; d++) {
            ids[d] = d;
        }
        return ids;
    }

    private static final class CollectingSink implements StringBlockSink {
        private final List<String> page;
        private final Runnable onValues;

        CollectingSink(List<String> page, Runnable onValues) {
            this.page = page;
            this.onValues = onValues;
        }

        @Override
        public void appendOrdinals(int[] ordinals, int valueCount, int[] valueCounts, int docCount, BytesRef[] dictionary, int size) {
            for (int i = 0; i < valueCount; i++) {
                page.add(dictionary[ordinals[i]].utf8ToString());
            }
        }

        @Override
        public Values values(int valueCount, int[] valueCounts, int docCount) {
            onValues.run();
            return new Values() {
                @Override
                public void append(BytesRef value) {
                    page.add(value.utf8ToString());
                }

                @Override
                public void finish() {}

                @Override
                public void close() {}
            };
        }
    }

    private static BinaryDocValues openUnder(ProfileCollector stats, LeafReader leaf) throws IOException {
        try (Binding ignored = Scopes.bind(TestCollectors.eager(stats))) {
            return leaf.getBinaryDocValues(FIELD);
        }
    }

    private static void scan(BinaryDocValues column) throws IOException {
        while (column.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
            column.binaryValue();
        }
    }

    private static List<String> fixedWidth(int docs) {
        final List<String> values = new ArrayList<>(docs);
        for (int d = 0; d < docs; d++) {
            values.add(String.format(Locale.ROOT, "%0" + WIDTH + "d", d));
        }
        return values;
    }

    private static void index(Directory dir, Codec codec, List<String> values) throws IOException {
        final IndexWriterConfig config = new IndexWriterConfig().setCodec(codec).setMergePolicy(new LogDocMergePolicy());
        try (IndexWriter writer = new IndexWriter(dir, config)) {
            for (String value : values) {
                final Document doc = new Document();
                doc.add(new Field(FIELD, new BytesRef(value), columnarBinaryFieldType()));
                writer.addDocument(doc);
            }
            writer.forceMerge(1);
        }
    }

    private static void indexMulti(Directory dir, Codec codec, String[][] values) throws IOException {
        final IndexWriterConfig config = new IndexWriterConfig().setCodec(codec).setMergePolicy(new LogDocMergePolicy());
        try (IndexWriter writer = new IndexWriter(dir, config)) {
            for (String[] slots : values) {
                final List<BytesRef> refs = new ArrayList<>(slots.length);
                for (String slot : slots) {
                    refs.add(new BytesRef(slot));
                }
                final Document doc = new Document();
                doc.add(new Field(FIELD, BytesRef.deepCopyOf(new StringBinaryPayload.Builder().encode(refs)), columnarBinaryFieldType()));
                writer.addDocument(doc);
            }
            writer.forceMerge(1);
        }
    }

    private static Codec multiValuedCodecOf(StringColumnOptions options) {
        return columnarCodec(
            new ColumNARDocValuesFormat(
                (fieldName, type) -> NumericPipeline::defaultPipeline,
                field -> ColumnarFieldType.STRING,
                ColumNARDocValuesFormat.DEFAULT_BLOCK_SIZE,
                StringColumnOptionsSelector.always(options)
            )
        );
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
