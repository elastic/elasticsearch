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
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.LogDocMergePolicy;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.SerialMergeScheduler;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.FilterIndexInput;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.RandomAccessInput;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.codec.profiler.ProfileCollector;
import org.elasticsearch.codec.profiler.Scopes;
import org.elasticsearch.codec.profiler.Scopes.Binding;
import org.elasticsearch.codec.profiler.TestCollectors;
import org.elasticsearch.columnar.numeric.NumericPipeline;
import org.elasticsearch.columnar.string.ColumnarStringBinaryDocValues;
import org.elasticsearch.columnar.string.DictionaryPolicy;
import org.elasticsearch.columnar.string.StringBlockSink;
import org.elasticsearch.columnar.string.StringColumnOptions;
import org.elasticsearch.columnar.string.StringColumnOptionsSelector;
import org.elasticsearch.columnar.string.StringColumnReader;
import org.elasticsearch.columnar.string.StringColumnSource;
import org.elasticsearch.columnar.string.SummaryPolicy;
import org.elasticsearch.test.ESTestCase;
import org.junit.After;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

import static org.elasticsearch.columnar.ColumnarTestUtils.columnarBinaryFieldType;
import static org.elasticsearch.columnar.ColumnarTestUtils.columnarCodec;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.instanceOf;

public class ColumnarProfilingInvariantTests extends ESTestCase {

    private static final String FIELD = "field";

    private enum Layout {
        PLAIN,
        DICTIONARY
    }

    @After
    public void assertNothingIsLeftBound() {
        assertFalse("a test left a scope bound on its thread", TestCollectors.isBound());
    }

    public void testAReaderKeepsItsCollectorWhileAnotherScopeIsActiveDuringConsumption() throws IOException {
        for (Layout layout : Layout.values()) {
            final List<String> values = values(layout, between(1500, 3000), false);
            try (Directory dir = newDirectory()) {
                write(dir, layout, values, values.size());
                try (DirectoryReader reader = DirectoryReader.open(dir)) {
                    final LeafReader leaf = reader.leaves().get(0).reader();
                    final ProfileCollector expected = TestCollectors.countersOnly();
                    scan(openUnder(expected, leaf));

                    final ProfileCollector owner = TestCollectors.countersOnly();
                    final BinaryDocValues column = openUnder(owner, leaf);
                    final ProfileCollector bystander = TestCollectors.countersOnly();
                    try (Binding ignored = Scopes.bind(TestCollectors.eager(bystander))) {
                        scan(column);
                    }
                    assertThat(
                        layout.name(),
                        TestCollectors.toKeyedValues(owner.snapshot()),
                        equalTo(TestCollectors.toKeyedValues(expected.snapshot()))
                    );
                    assertThat(layout.name(), TestCollectors.toKeyedValues(bystander.snapshot()), equalTo(Map.of()));
                }
            }
        }
    }

    public void testInterleavedNestingGivesEveryReaderToTheScopeThatOpenedIt() throws IOException {
        final List<String> values = values(Layout.PLAIN, 2000, false);
        try (Directory dir = newDirectory()) {
            write(dir, Layout.PLAIN, values, values.size());
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                final LeafReader leaf = reader.leaves().get(0).reader();
                final ProfileCollector a = TestCollectors.countersOnly();
                final ProfileCollector b = TestCollectors.countersOnly();
                final BinaryDocValues a1;
                final BinaryDocValues b1;
                final BinaryDocValues a2;
                try (Binding outer = Scopes.bind(TestCollectors.eager(a))) {
                    a1 = leaf.getBinaryDocValues(FIELD);
                    try (Binding inner = Scopes.bind(TestCollectors.eager(b))) {
                        b1 = leaf.getBinaryDocValues(FIELD);
                    }
                    a2 = leaf.getBinaryDocValues(FIELD);
                }
                scan(b1);
                scan(a2);
                scan(a1);
                final ProfileCollector single = TestCollectors.countersOnly();
                scan(openUnder(single, leaf));
                assertThat(
                    "b opened one reader and read it once",
                    TestCollectors.toKeyedValues(b.snapshot()),
                    equalTo(TestCollectors.toKeyedValues(single.snapshot()))
                );
                assertThat(TestCollectors.toKeyedValues(a.snapshot()).get("ColumNAR.readers.opened"), equalTo(2L));
                assertThat(
                    "a read two readers, each as much as the single one",
                    TestCollectors.toKeyedValues(a.snapshot()).get("ColumNAR.chunks.bytes_inflated"),
                    equalTo(2 * TestCollectors.toKeyedValues(single.snapshot()).get("ColumNAR.chunks.bytes_inflated"))
                );
            }
        }
    }

    public void testAFailureWhileAReaderIsBeingOpenedLeavesNothingBound() throws IOException {
        final List<String> values = values(Layout.DICTIONARY, 1500, false);
        try (Directory dir = newDirectory()) {
            write(dir, Layout.DICTIONARY, values, values.size());
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                final LeafReader leaf = reader.leaves().get(0).reader();
                final ProfileCollector outer = TestCollectors.elapsed();
                final ProfileCollector inner = TestCollectors.elapsed();
                expectThrows(IllegalStateException.class, () -> {
                    try (Binding a = Scopes.bind(TestCollectors.eager(outer))) {
                        try (Binding b = Scopes.bind(TestCollectors.eager(inner))) {
                            leaf.getBinaryDocValues(FIELD);
                            throw new IllegalStateException("the caller fails after the reader was opened");
                        }
                    }
                });
                assertFalse(TestCollectors.isBound());
                assertThat(TestCollectors.toKeyedValues(inner.snapshot()).get("ColumNAR.readers.opened"), equalTo(1L));
                assertThat(TestCollectors.toKeyedValues(outer.snapshot()), equalTo(Map.of()));
            }
        }
    }

    public void testReadersOfDifferentLayoutsOpenedInDifferentScopesKeepTheirOwnCountsWhenInterleaved() throws IOException {
        final List<String> plainValues = values(Layout.PLAIN, 2000, false);
        final List<String> dictionaryValues = values(Layout.DICTIONARY, 2000, false);
        try (Directory plainDir = newDirectory(); Directory dictionaryDir = newDirectory()) {
            write(plainDir, Layout.PLAIN, plainValues, plainValues.size());
            write(dictionaryDir, Layout.DICTIONARY, dictionaryValues, dictionaryValues.size());
            try (
                DirectoryReader plainReader = DirectoryReader.open(plainDir);
                DirectoryReader dictionaryReader = DirectoryReader.open(dictionaryDir)
            ) {
                final ProfileCollector plain = TestCollectors.elapsed();
                final ProfileCollector dictionary = TestCollectors.elapsed();
                final BinaryDocValues plainColumn = openUnder(plain, plainReader.leaves().get(0).reader());
                final BinaryDocValues dictionaryColumn = openUnder(dictionary, dictionaryReader.leaves().get(0).reader());
                for (int step = 0; step < 50; step++) {
                    plainColumn.nextDoc();
                    plainColumn.binaryValue();
                    dictionaryColumn.nextDoc();
                    dictionaryColumn.binaryValue();
                }
                scan(plainColumn);
                scan(dictionaryColumn);
                assertThat(TestCollectors.toKeyedValues(plain.snapshot()).get("ColumNAR.readers.plain"), equalTo(1L));
                assertThat(TestCollectors.toKeyedValues(plain.snapshot()).get("ColumNAR.readers.dictionary"), equalTo(0L));
                assertThat(TestCollectors.toKeyedValues(dictionary.snapshot()).get("ColumNAR.readers.dictionary"), equalTo(1L));
                assertThat(TestCollectors.toKeyedValues(dictionary.snapshot()).get("ColumNAR.readers.plain"), equalTo(0L));
                assertThat(TestCollectors.toKeyedValues(plain.snapshot()).get("ColumNAR.chunks.decompressed"), greaterThan(0L));
            }
        }
    }

    public void testConcurrentScopesReadingOneSegmentKeepTheirOwnCounts() throws IOException {
        final Layout layout = randomFrom(Layout.values());
        final List<String> values = values(layout, between(1500, 3000), false);
        try (Directory dir = newDirectory()) {
            write(dir, layout, values, values.size());
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                final LeafReader leaf = reader.leaves().get(0).reader();
                final ProfileCollector single = TestCollectors.countersOnly();
                scan(openUnder(single, leaf));
                final Map<String, Long> expected = TestCollectors.toKeyedValues(single.snapshot());
                assertThat(expected.get("ColumNAR.readers.opened"), equalTo(1L));

                // NOTE: The last task opens its reader with no scope bound, so profiled and unprofiled readers of the same
                // segment and field are read at the same time.
                final int profiled = between(2, 4);
                final List<ProfileCollector> collectors = new ArrayList<>();
                for (int i = 0; i < profiled; i++) {
                    collectors.add(TestCollectors.countersOnly());
                }
                final CyclicBarrier opened = new CyclicBarrier(profiled + 1);
                startInParallel(profiled + 1, task -> {
                    try {
                        final BinaryDocValues column = task < profiled
                            ? openUnder(collectors.get(task), leaf)
                            : leaf.getBinaryDocValues(FIELD);
                        safeAwait(opened);
                        scan(column);
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                });
                for (int i = 0; i < profiled; i++) {
                    assertThat(
                        "scope " + i + " holds the work of its own reader and nothing else",
                        TestCollectors.toKeyedValues(collectors.get(i).snapshot()),
                        equalTo(expected)
                    );
                }
            }
        }
    }

    public void testProfiledAndUnprofiledReadsAndMatchesAgreeOnEveryLayout() throws IOException {
        for (int round = 0; round < 6; round++) {
            final Layout layout = randomFrom(Layout.values());
            final List<String> values = values(layout, between(1200, 4000), true);
            try (Directory dir = newDirectory()) {
                write(dir, layout, values, between(400, 2000));
                try (DirectoryReader reader = DirectoryReader.open(dir)) {
                    for (LeafReaderContext context : reader.leaves()) {
                        assertSameBehaviour(layout, context.reader(), values, TestCollectors::elapsed);
                        assertSameBehaviour(layout, context.reader(), values, TestCollectors::countersOnly);
                    }
                }
            }
        }
    }

    public void testProfilingReadsExactlyTheBytesAnUnprofiledReadDoes() throws IOException {
        for (Layout layout : Layout.values()) {
            final List<String> values = values(layout, between(2000, 4000), true);
            try (Directory base = newDirectory()) {
                write(base, layout, values, between(700, 1500));
                final long[] bytes = new long[2];
                for (int profiled = 0; profiled < 2; profiled++) {
                    final CountingDirectory counting = new CountingDirectory(base);
                    try (DirectoryReader reader = DirectoryReader.open(counting)) {
                        final java.util.Random sequence = new java.util.Random(7);
                        for (LeafReaderContext context : reader.leaves()) {
                            final LeafReader leaf = context.reader();
                            final BinaryDocValues column = profiled == 1
                                ? openUnder(TestCollectors.elapsed(), leaf)
                                : leaf.getBinaryDocValues(FIELD);
                            if (column == null) {
                                continue;
                            }
                            for (int hop = 0; hop < 200; hop++) {
                                final int target = Math.min(leaf.maxDoc() - 1, sequence.nextInt(leaf.maxDoc()));
                                if (column.docID() < target) {
                                    if (column.advance(target) != DocIdSetIterator.NO_MORE_DOCS) {
                                        column.binaryValue();
                                    }
                                }
                            }
                        }
                    }
                    bytes[profiled] = counting.bytes.get();
                }
                assertThat(layout + ": the files are read the same with and without profiling", bytes[1], equalTo(bytes[0]));
                assertThat(bytes[0], greaterThan(0L));
            }
        }
    }

    public void testNoSupportedLayoutDeclinesAPageAndEveryNonEmptyPageIsCountedOnce() throws IOException {
        for (int round = 0; round < 6; round++) {
            final Layout layout = randomFrom(Layout.values());
            final List<String> values = values(layout, between(1500, 4000), false);
            try (Directory dir = newDirectory()) {
                write(dir, layout, values, values.size());
                try (DirectoryReader reader = DirectoryReader.open(dir)) {
                    final ProfileCollector stats = TestCollectors.elapsed();
                    final StringColumnSource column = (StringColumnSource) openUnder(stats, reader.leaves().get(0).reader());
                    final int[] docs = new int[values.size()];
                    for (int d = 0; d < docs.length; d++) {
                        docs[d] = d;
                    }
                    final int pageSize = between(1, 1500);
                    int pages = 0;
                    for (int from = 0; from < docs.length; from += pageSize) {
                        final int count = Math.min(pageSize, docs.length - from);
                        assertTrue(
                            layout + " declined a page, so a fallback metric is needed",
                            column.reader().readBlock(docs, from, count, new DiscardingSink())
                        );
                        pages++;
                    }
                    final Map<String, Long> counted = TestCollectors.toKeyedValues(stats.snapshot());
                    assertThat(
                        layout.name(),
                        counted.get("ColumNAR.pages.as_ordinals") + counted.get("ColumNAR.pages.as_values"),
                        equalTo((long) pages)
                    );
                    assertThat(
                        layout.name(),
                        counted.get("ColumNAR.pages.values_in_ordinal_pages") + counted.get("ColumNAR.pages.values_in_value_pages"),
                        equalTo((long) values.size())
                    );
                }
            }
        }
    }

    public void testAMergeNeverAttachesItsReadersToTheScopeBoundOnTheMergingThread() throws IOException {
        for (Layout layout : Layout.values()) {
            final List<String> values = values(layout, between(2000, 4000), true);
            try (Directory dir = newDirectory()) {
                write(dir, layout, values, between(500, 1000));
                final ProfileCollector scope = TestCollectors.elapsed();
                final IndexWriterConfig config = new IndexWriterConfig().setCodec(codecFor(layout))
                    .setMergeScheduler(new SerialMergeScheduler())
                    .setMergePolicy(new LogDocMergePolicy());
                try (IndexWriter writer = new IndexWriter(dir, config)) {
                    try (Binding ignored = Scopes.bind(TestCollectors.eager(scope))) {
                        writer.forceMerge(1);
                    }
                }
                assertThat(
                    layout + ": the merge read its inputs without reporting to a query",
                    TestCollectors.toKeyedValues(scope.snapshot()),
                    equalTo(Map.of())
                );
                try (DirectoryReader reader = DirectoryReader.open(dir)) {
                    assertThat(reader.leaves().size(), equalTo(1));
                    final BinaryDocValues merged = reader.leaves().get(0).reader().getBinaryDocValues(FIELD);
                    final List<String> kept = new ArrayList<>();
                    for (String value : values) {
                        if (value != null) {
                            kept.add(value);
                        }
                    }
                    final List<String> read = new ArrayList<>();
                    for (int doc = merged.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = merged.nextDoc()) {
                        read.add(merged.binaryValue().utf8ToString());
                    }
                    assertThat("the merge kept every value in order", read, equalTo(kept));
                }
            }
        }
    }

    private static Codec codecFor(Layout layout) {
        final StringColumnOptions options = layout == Layout.PLAIN
            ? StringColumnOptions.DEFAULT.withPolicies(DictionaryPolicy.NONE, SummaryPolicy.NONE)
            : StringColumnOptions.DEFAULT;
        return columnarCodec(
            new ColumNARDocValuesFormat(
                (fieldName, type) -> NumericPipeline::defaultPipeline,
                field -> ColumnarFieldType.STRING,
                ColumNARDocValuesFormat.DEFAULT_BLOCK_SIZE,
                StringColumnOptionsSelector.always(options.withSingleValued(true))
            )
        );
    }

    private static void assertSameBehaviour(Layout layout, LeafReader leaf, List<String> allValues, Supplier<ProfileCollector> collectors)
        throws IOException {
        final BinaryDocValues plain = leaf.getBinaryDocValues(FIELD);
        final ProfileCollector stats = collectors.get();
        final BinaryDocValues profiled = openUnder(stats, leaf);
        if (plain == null) {
            assertNull(profiled);
            return;
        }
        assertThat("the fast path survives profiling", profiled, instanceOf(StringColumnSource.class));
        assertThat(profiled, instanceOf(ColumnarStringBinaryDocValues.class));
        for (int doc = plain.nextDoc();; doc = plain.nextDoc()) {
            assertThat(profiled.nextDoc(), equalTo(doc));
            if (doc == DocIdSetIterator.NO_MORE_DOCS) {
                break;
            }
            assertThat(profiled.binaryValue(), equalTo(plain.binaryValue()));
        }
        assertThat(profiled.docID(), equalTo(plain.docID()));

        final BinaryDocValues randomPlain = leaf.getBinaryDocValues(FIELD);
        final BinaryDocValues randomProfiled = openUnder(collectors.get(), leaf);
        for (int hop = 0; hop < 100; hop++) {
            final int target = randomPlain.docID() + 1 + between(0, 40);
            if (target >= leaf.maxDoc()) {
                break;
            }
            final int expected = randomPlain.advance(target);
            assertThat(randomProfiled.advance(target), equalTo(expected));
            if (expected == DocIdSetIterator.NO_MORE_DOCS) {
                break;
            }
            assertThat(randomProfiled.binaryValue(), equalTo(randomPlain.binaryValue()));
        }

        final StringColumnReader plainReader = ((ColumnarStringBinaryDocValues) leaf.getBinaryDocValues(FIELD)).reader();
        final StringColumnReader profiledReader = ((ColumnarStringBinaryDocValues) openUnder(collectors.get(), leaf)).reader();
        final List<BytesRef> pool = new ArrayList<>();
        for (String value : allValues) {
            if (value != null) {
                pool.add(new BytesRef(value));
            }
        }
        for (int query = 0; query < 8; query++) {
            final BytesRef term = pool.get(between(0, pool.size() - 1));
            assertThat(matches(profiledReader.matchTerm(term)), equalTo(matches(plainReader.matchTerm(term))));
            final BytesRef prefix = new BytesRef(term.utf8ToString().substring(0, Math.min(term.length, between(1, 6))));
            assertThat(matches(profiledReader.matchPrefix(prefix)), equalTo(matches(plainReader.matchPrefix(prefix))));
            BytesRef low = pool.get(between(0, pool.size() - 1));
            BytesRef high = pool.get(between(0, pool.size() - 1));
            if (low.compareTo(high) > 0) {
                final BytesRef swap = low;
                low = high;
                high = swap;
            }
            final boolean includeLow = randomBoolean();
            final boolean includeHigh = randomBoolean();
            assertThat(
                matches(profiledReader.matchRange(low, includeLow, high, includeHigh)),
                equalTo(matches(plainReader.matchRange(low, includeLow, high, includeHigh)))
            );
            final TreeSet<BytesRef> anyOf = new TreeSet<>();
            for (int i = 0; i < between(1, 5); i++) {
                anyOf.add(pool.get(between(0, pool.size() - 1)));
            }
            final Set<BytesRef> membership = new HashSet<>(anyOf);
            assertThat(matches(profiledReader.matchAnyOf(anyOf, membership)), equalTo(matches(plainReader.matchAnyOf(anyOf, membership))));
        }
    }

    private static List<Integer> matches(DocIdSetIterator iterator) throws IOException {
        final List<Integer> docs = new ArrayList<>();
        for (int doc = iterator.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = iterator.nextDoc()) {
            docs.add(doc);
        }
        assertThat(iterator.docID(), equalTo(DocIdSetIterator.NO_MORE_DOCS));
        return docs;
    }

    private static BinaryDocValues openUnder(ProfileCollector stats, LeafReader leaf) throws IOException {
        try (Binding ignored = Scopes.bind(TestCollectors.eager(stats))) {
            return leaf.getBinaryDocValues(FIELD);
        }
    }

    private static void scan(BinaryDocValues column) throws IOException {
        if (column == null) {
            return;
        }
        while (column.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
            column.binaryValue();
        }
    }

    private List<String> values(Layout layout, int count, boolean withNulls) {
        final List<String> values = new ArrayList<>(count);
        final int vocabulary = layout == Layout.DICTIONARY ? between(5, 40) : Integer.MAX_VALUE;
        for (int d = 0; d < count; d++) {
            if (withNulls && randomInt(9) == 0) {
                values.add(null);
            } else if (layout == Layout.DICTIONARY) {
                values.add(String.format(Locale.ROOT, "term-%04d", randomInt(vocabulary - 1)));
            } else {
                values.add(String.format(Locale.ROOT, "value-%08d-%s", d, "x".repeat(between(0, 40))));
            }
        }
        return values;
    }

    private static void write(Directory dir, Layout layout, List<String> values, int segmentSize) throws IOException {
        final Codec codec = codecFor(layout);
        try (IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig().setCodec(codec).setMergePolicy(NoMergePolicy.INSTANCE))) {
            for (int i = 0; i < values.size(); i++) {
                final Document doc = new Document();
                if (values.get(i) != null) {
                    doc.add(new Field(FIELD, new BytesRef(values.get(i)), columnarBinaryFieldType()));
                }
                writer.addDocument(doc);
                if ((i + 1) % segmentSize == 0) {
                    writer.commit();
                }
            }
            writer.commit();
        }
    }

    private static final class DiscardingSink implements StringBlockSink {
        @Override
        public void appendOrdinals(int[] ordinals, int valueCount, int[] valueCounts, int docCount, BytesRef[] dictionary, int size) {}

        @Override
        public Values values(int valueCount, int[] valueCounts, int docCount) {
            return new Values() {
                @Override
                public void append(BytesRef value) {}

                @Override
                public void finish() {}

                @Override
                public void close() {}
            };
        }
    }

    private static final class CountingDirectory extends FilterDirectory {
        final AtomicLong bytes = new AtomicLong();

        CountingDirectory(Directory in) {
            super(in);
        }

        @Override
        public IndexInput openInput(String name, IOContext context) throws IOException {
            return new CountingInput(name, super.openInput(name, context), bytes);
        }
    }

    private static final class CountingInput extends FilterIndexInput {
        private final AtomicLong bytes;

        CountingInput(String description, IndexInput in, AtomicLong bytes) {
            super(description, in);
            this.bytes = bytes;
        }

        @Override
        public byte readByte() throws IOException {
            bytes.incrementAndGet();
            return in.readByte();
        }

        @Override
        public void readBytes(byte[] b, int offset, int len) throws IOException {
            bytes.addAndGet(len);
            in.readBytes(b, offset, len);
        }

        @Override
        public short readShort() throws IOException {
            bytes.addAndGet(2);
            return in.readShort();
        }

        @Override
        public int readInt() throws IOException {
            bytes.addAndGet(4);
            return in.readInt();
        }

        @Override
        public long readLong() throws IOException {
            bytes.addAndGet(8);
            return in.readLong();
        }

        @Override
        public IndexInput clone() {
            return new CountingInput(toString(), in.clone(), bytes);
        }

        @Override
        public IndexInput slice(String sliceDescription, long offset, long length) throws IOException {
            return new CountingInput(sliceDescription, in.slice(sliceDescription, offset, length), bytes);
        }

        @Override
        public RandomAccessInput randomAccessSlice(long offset, long length) throws IOException {
            final RandomAccessInput delegate = in.randomAccessSlice(offset, length);
            return new RandomAccessInput() {
                @Override
                public long length() {
                    return delegate.length();
                }

                @Override
                public byte readByte(long pos) throws IOException {
                    bytes.incrementAndGet();
                    return delegate.readByte(pos);
                }

                @Override
                public void readBytes(long pos, byte[] b, int offset, int len) throws IOException {
                    bytes.addAndGet(len);
                    delegate.readBytes(pos, b, offset, len);
                }

                @Override
                public short readShort(long pos) throws IOException {
                    bytes.addAndGet(2);
                    return delegate.readShort(pos);
                }

                @Override
                public int readInt(long pos) throws IOException {
                    bytes.addAndGet(4);
                    return delegate.readInt(pos);
                }

                @Override
                public long readLong(long pos) throws IOException {
                    bytes.addAndGet(8);
                    return delegate.readLong(pos);
                }

                @Override
                public void prefetch(long offset, long length) throws IOException {
                    delegate.prefetch(offset, length);
                }
            };
        }
    }
}
