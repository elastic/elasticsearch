/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.parquet.dv;

import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.DocValuesSkipper;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.index.mapper.BlockLoader;
import org.elasticsearch.index.mapper.Uid;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.parquet.ParquetSampleFiles;
import org.elasticsearch.xpack.parquet.ParquetTestFiles;
import org.elasticsearch.xpack.parquet.engine.ExternalParquetCodecReader;
import org.elasticsearch.xpack.parquet.engine.ParquetAttachCodec;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

/**
 * Attaches a Parquet file as a stub segment with {@code IndexWriter#addIndexes}, opens the segment through Lucene SPI, and reads its
 * doc values the ways that Lucene consumers do: a scan with {@code nextDoc}, leaps with {@code advance}, and random access with
 * {@code advanceExact} in any order. The file is larger than one window of the producer.
 */
public class ParquetDocValuesTests extends ESTestCase {

    private static final String LOCATION = "memory://flat.parquet";
    private static final int ROWS = 10_000;

    private interface LeafCheck {
        void check(LeafReader leaf) throws IOException;
    }

    public void testScanWithNextDoc() throws IOException {
        withSegment(0, ROWS, leaf -> {
            NumericDocValues ids = leaf.getNumericDocValues("id");
            NumericDocValues ns = leaf.getNumericDocValues("n");
            BinaryDocValues names = leaf.getBinaryDocValues("name");
            for (int doc = 0; doc < ROWS; doc++) {
                assertThat(ids.nextDoc(), equalTo(doc));
                assertThat(ids.longValue(), equalTo(doc * 7L));
            }
            assertThat(ids.nextDoc(), equalTo(DocIdSetIterator.NO_MORE_DOCS));
            int withValue = 0;
            for (int doc = ns.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = ns.nextDoc()) {
                assertThat(doc % 3 != 0, equalTo(true));
                assertThat(ns.longValue(), equalTo((long) doc));
                withValue++;
            }
            assertThat(withValue, equalTo(ROWS - (ROWS + 2) / 3));
            int count = 0;
            for (int doc = names.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = names.nextDoc()) {
                assertThat(doc % 5 != 0, equalTo(true));
                assertThat(names.binaryValue().utf8ToString(), equalTo("name-" + doc));
                count++;
            }
            assertThat(count, equalTo(ROWS - ROWS / 5));
        });
    }

    public void testRandomAccessWithAdvanceExactInAnyOrder() throws IOException {
        withSegment(0, ROWS, leaf -> {
            List<Integer> docs = new ArrayList<>();
            for (int i = 0; i < ROWS; i++) {
                docs.add(i);
            }
            Collections.shuffle(docs, random());
            // a fresh iterator for each kind, because advanceExact may move backward only on a fresh position per call in Lucene's
            // contract for sparse iterators; the ES|QL and search code paths create one iterator per segment and thread
            NumericDocValues ids = leaf.getNumericDocValues("id");
            NumericDocValues ns = leaf.getNumericDocValues("n");
            BinaryDocValues names = leaf.getBinaryDocValues("name");
            for (int doc : docs) {
                assertTrue(ids.advanceExact(doc));
                assertThat(ids.longValue(), equalTo(doc * 7L));
                assertThat(ns.advanceExact(doc), equalTo(doc % 3 != 0));
                if (doc % 3 != 0) {
                    assertThat(ns.longValue(), equalTo((long) doc));
                }
                assertThat(names.advanceExact(doc), equalTo(doc % 5 != 0));
                if (doc % 5 != 0) {
                    assertThat(names.binaryValue().utf8ToString(), equalTo("name-" + doc));
                }
            }
        });
    }

    public void testAdvanceSkipsDocumentsWithoutValue() throws IOException {
        withSegment(0, ROWS, leaf -> {
            NumericDocValues ns = leaf.getNumericDocValues("n");
            assertThat(ns.advance(3), equalTo(4));
            assertThat(ns.advance(4096 * 2), equalTo(4096 * 2 % 3 == 0 ? 4096 * 2 + 1 : 4096 * 2));
            assertThat(ns.advance(ROWS), equalTo(DocIdSetIterator.NO_MORE_DOCS));
        });
    }

    public void testSegmentThatServesPartOfTheFile() throws IOException {
        int firstRow = 4321;
        int rowCount = 5000;
        withSegment(firstRow, rowCount, leaf -> {
            assertThat(leaf.maxDoc(), equalTo(rowCount));
            NumericDocValues ids = leaf.getNumericDocValues("id");
            for (int doc : new int[] { 0, 1, 4095, 4096, rowCount - 1 }) {
                assertTrue(ids.advanceExact(doc));
                assertThat(ids.longValue(), equalTo((firstRow + doc) * 7L));
            }
            BinaryDocValues names = leaf.getBinaryDocValues("name");
            assertTrue(names.advanceExact(1));
            assertThat(names.binaryValue().utf8ToString(), equalTo("name-" + (firstRow + 1)));
        });
    }

    private record Interval(int minDoc, int maxDoc, long min, long max, int docCount) {}

    private static List<Interval> intervals(DocValuesSkipper skipper) throws IOException {
        List<Interval> intervals = new ArrayList<>();
        skipper.advance(0);
        while (skipper.minDocID(0) != NO_MORE_DOCS) {
            intervals.add(
                new Interval(skipper.minDocID(0), skipper.maxDocID(0), skipper.minValue(0), skipper.maxValue(0), skipper.docCount(0))
            );
            skipper.advance(skipper.maxDocID(0) + 1);
        }
        return intervals;
    }

    /** A segment over the whole file, with the statistics of its footer, as the engine attaches it. */
    private static ExternalFilePiece pieceWithStatistics(byte[] file, List<ColumnBinding> bindings) throws IOException {
        ParquetFileInfo info = ParquetFileInfo.read(LOCATION, ParquetTestFiles.storageObject(file));
        return ExternalFilePiece.of(info, 0, info.rowGroups().size(), 0, bindings);
    }

    public void testSkipperServesTheBoundsAndCountsOfTheRowGroups() throws IOException {
        byte[] file = ParquetTestFiles.threeColumnFile(ROWS);
        try (Directory dir = attach(file, bindings(), pieceWithStatistics(file, bindings()))) {
            readWith(dir, file, leaf -> {
                // a required column, whose value is 7 times the row, and an optional one, which is missing when the row is a multiple of 3
                List<Interval> ids = intervals(leaf.getDocValuesSkipper("id"));
                assertTrue("the file has several row groups", ids.size() > 3);
                int next = 0;
                for (Interval interval : ids) {
                    assertThat(interval.minDoc(), equalTo(next));
                    next = interval.maxDoc() + 1;
                    assertThat(interval.min(), equalTo(interval.minDoc() * 7L));
                    assertThat(interval.max(), equalTo(interval.maxDoc() * 7L));
                    assertThat(interval.docCount(), equalTo(interval.maxDoc() - interval.minDoc() + 1));
                }
                assertThat(next, equalTo(ROWS));
                DocValuesSkipper idSkipper = leaf.getDocValuesSkipper("id");
                assertThat(idSkipper.minValue(), equalTo(0L));
                assertThat(idSkipper.maxValue(), equalTo((ROWS - 1) * 7L));
                assertThat(idSkipper.docCount(), equalTo(ROWS));

                DocValuesSkipper nSkipper = leaf.getDocValuesSkipper("n");
                int withValue = 0;
                for (Interval interval : intervals(nSkipper)) {
                    int first = interval.minDoc();
                    while (first % 3 == 0) {
                        first++;
                    }
                    int last = interval.maxDoc();
                    while (last % 3 == 0) {
                        last--;
                    }
                    int count = 0;
                    for (int doc = interval.minDoc(); doc <= interval.maxDoc(); doc++) {
                        count += doc % 3 == 0 ? 0 : 1;
                    }
                    assertThat(interval.min(), equalTo((long) first));
                    assertThat(interval.max(), equalTo((long) last));
                    assertThat(interval.docCount(), equalTo(count));
                    withValue += count;
                }
                assertThat(nSkipper.docCount(), equalTo(withValue));
                assertThat(withValue, equalTo(ROWS - (ROWS + 2) / 3));
                // a string field has no skipper
                assertNull(leaf.getDocValuesSkipper("name"));
            });
        }
    }

    /** Wrong bounds or counts would give wrong counts here, because a range query takes its blocks from the skipper. */
    public void testRangeQueriesWithTheSkipperAgreeWithABruteForceCount() throws IOException {
        byte[] file = ParquetTestFiles.threeColumnFile(ROWS);
        try (Directory dir = attach(file, bindings(), pieceWithStatistics(file, bindings()))) {
            readWith(dir, file, leaf -> {
                IndexSearcher searcher = new IndexSearcher(leaf);
                for (int i = 0; i < 60; i++) {
                    long low = randomLongBetween(-10, ROWS * 7L + 10);
                    long high = low + randomLongBetween(0, i % 2 == 0 ? 50 : ROWS * 3L);
                    int expectedIds = 0;
                    for (int doc = 0; doc < ROWS; doc++) {
                        long value = doc * 7L;
                        expectedIds += value >= low && value <= high ? 1 : 0;
                    }
                    assertThat(
                        "id in [" + low + ", " + high + "]",
                        searcher.count(NumericDocValuesField.newSlowRangeQuery("id", low, high)),
                        equalTo(expectedIds)
                    );
                    int expectedN = 0;
                    for (int doc = 0; doc < ROWS; doc++) {
                        expectedN += doc % 3 != 0 && doc >= low && doc <= high ? 1 : 0;
                    }
                    assertThat(
                        "n in [" + low + ", " + high + "]",
                        searcher.count(NumericDocValuesField.newSlowRangeQuery("n", low, high)),
                        equalTo(expectedN)
                    );
                }
            });
        }
    }

    public void testSkipperBoundsFollowTheConversionAndTheReaderScale() throws IOException {
        byte[] file = ParquetSampleFiles.writeTypes(50);
        List<ColumnBinding> bindings = List.of(
            new ColumnBinding("day", 0, "day", DocValuesType.NUMERIC),
            new ColumnBinding("ts_millis", 1, "ts_millis", DocValuesType.NUMERIC),
            new ColumnBinding("ts_micros", 2, "ts_micros", DocValuesType.NUMERIC),
            new ColumnBinding("small", 3, "small", DocValuesType.NUMERIC),
            new ColumnBinding("seconds", 4, "small", DocValuesType.NUMERIC, Conversion.SECONDS_TO_MILLIS)
        );
        try (Directory dir = attach(file, bindings, pieceWithStatistics(file, bindings))) {
            readWith(dir, file, leaf -> {
                // a DATE is epoch milliseconds when the reader returns it, and a microsecond timestamp is epoch nanoseconds
                assertThat(leaf.getDocValuesSkipper("day").minValue(), equalTo(19_000L * 86_400_000L));
                assertThat(leaf.getDocValuesSkipper("day").maxValue(), equalTo(19_049L * 86_400_000L));
                assertThat(leaf.getDocValuesSkipper("ts_millis").minValue(), equalTo(1_700_000_000_000L));
                assertThat(leaf.getDocValuesSkipper("ts_micros").minValue(), equalTo(1_700_000_000_000_000L * 1_000L));
                assertThat(leaf.getDocValuesSkipper("ts_micros").maxValue(), equalTo((1_700_000_000_000_000L + 49) * 1_000L));
                assertThat(leaf.getDocValuesSkipper("small").maxValue(), equalTo(49L));
                // the conversion of a field applies to the bounds, as it applies to the values
                assertThat(leaf.getDocValuesSkipper("seconds").minValue(), equalTo(0L));
                assertThat(leaf.getDocValuesSkipper("seconds").maxValue(), equalTo(49_000L));
                // and the bounds hold for the values that the iterator returns
                NumericDocValues values = leaf.getNumericDocValues("ts_micros");
                for (int doc = 0; doc < 50; doc++) {
                    assertTrue(values.advanceExact(doc));
                    assertThat(values.longValue(), greaterThanOrEqualTo(leaf.getDocValuesSkipper("ts_micros").minValue()));
                    assertThat(values.longValue(), lessThanOrEqualTo(leaf.getDocValuesSkipper("ts_micros").maxValue()));
                }
            });
        }
    }

    public void testFooterWithoutStatisticsGivesNoUsableBounds() throws IOException {
        byte[] file = ParquetTestFiles.threeColumnFile(ROWS, false);
        try (Directory dir = attach(file, bindings(), pieceWithStatistics(file, bindings()))) {
            readWith(dir, file, leaf -> {
                // a required column has no missing value, so the count is known, and the bounds are the widest: nothing is skipped
                DocValuesSkipper id = leaf.getDocValuesSkipper("id");
                assertThat(id.docCount(), equalTo(ROWS));
                for (Interval interval : intervals(id)) {
                    assertThat(interval.min(), equalTo(Long.MIN_VALUE));
                    assertThat(interval.max(), equalTo(Long.MAX_VALUE));
                }
                // an optional one has no count either, so it has no skipper
                assertNull(leaf.getDocValuesSkipper("n"));
                IndexSearcher searcher = new IndexSearcher(leaf);
                assertThat(searcher.count(NumericDocValuesField.newSlowRangeQuery("id", 0, 700)), equalTo(101));
            });
        }
    }

    /**
     * A query reads a column through several live iterators on one thread (the filter and the loader). They must not make each other
     * restart their scans, because a restart decodes the row group from its first page again, and they must not each decode every page:
     * the first iterator that gets to a page decodes it and the others take it from the cache.
     */
    public void testSeveralLiveIteratorsOfOneColumnShareTheirScansAndDoNotReopenThem() throws IOException {
        byte[] file = ParquetTestFiles.threeColumnFile(ROWS);
        ParquetFileInfo info = ParquetFileInfo.read(LOCATION, ParquetTestFiles.storageObject(file));
        ExternalFilePiece piece = ExternalFilePiece.of(info, 0, info.rowGroups().size(), 0, bindings());
        // a small window gives the file many pages, so that every row group needs many loads
        ParquetDocValuesProducer producer = new ParquetDocValuesProducer(
            bindings(),
            piece,
            (location, length) -> ParquetTestFiles.storageObject(file),
            256
        );
        producer.resolveStorage();
        FieldInfo field = new ExternalParquetCodecReader(bindings(), piece).getFieldInfos().fieldInfo("name");
        int iterators = 5;
        List<BinaryDocValues> values = new ArrayList<>();
        for (int i = 0; i < iterators; i++) {
            values.add(producer.getBinary(field));
        }
        for (int doc = 0; doc < ROWS; doc++) {
            for (BinaryDocValues iterator : values) {
                assertThat(iterator.advanceExact(doc), equalTo(doc % 5 != 0));
                if (doc % 5 != 0) {
                    assertThat(iterator.binaryValue().utf8ToString(), equalTo("name-" + doc));
                }
            }
        }
        assertThat(
            "each iterator opens a scan once for each row group at most",
            producer.scansOpened(),
            lessThanOrEqualTo((long) iterators * info.rowGroups().size())
        );
        assertThat("the iterators did scan", producer.scansOpened(), greaterThanOrEqualTo((long) iterators));
        producer.close();
    }

    public void testSegmentOpenedOutsideAnEngineCannotReadValues() throws IOException {
        byte[] file = ParquetTestFiles.threeColumnFile(100);
        try (Directory dir = attach(file, bindings(), new ExternalFilePiece(LOCATION, file.length, 0, 100))) {
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                LeafReader leaf = reader.leaves().get(0).reader();
                IllegalStateException e = expectThrows(IllegalStateException.class, () -> leaf.getNumericDocValues("id"));
                assertThat(e.getMessage(), containsString("outside a Parquet engine"));
            }
        }
    }

    public void testFileWithFewerRowsThanTheSegmentNeedsFails() throws IOException {
        byte[] file = ParquetTestFiles.threeColumnFile(100);
        try (Directory dir = attach(file, bindings(), new ExternalFilePiece(LOCATION, file.length, 50, 100))) {
            IOException e = expectThrows(IOException.class, () -> readWith(dir, file, leaf -> leaf.getNumericDocValues("id")));
            assertThat(e.getMessage(), containsString("fewer than the [150]"));
        }
    }

    public void testConversionAndSynthesizedIdWithAnIndexRowBase() throws IOException {
        byte[] file = ParquetTestFiles.threeColumnFile(300);
        List<ColumnBinding> bindings = List.of(
            new ColumnBinding("when", 0, "id", DocValuesType.NUMERIC, Conversion.SECONDS_TO_MILLIS),
            ColumnBinding.synthesizedId(1)
        );
        // the segment serves rows 100 to 199 of the file and follows 5000 rows of other segments in its index
        try (Directory dir = attach(file, bindings, new ExternalFilePiece(LOCATION, file.length, 100, 100, 5000))) {
            readWith(dir, file, leaf -> {
                NumericDocValues when = leaf.getNumericDocValues("when");
                BinaryDocValues ids = leaf.getBinaryDocValues("_id");
                for (int doc : new int[] { 0, 7, 99 }) {
                    assertTrue(when.advanceExact(doc));
                    assertThat(when.longValue(), equalTo((100 + doc) * 7L * 1_000L));
                    assertTrue(ids.advanceExact(doc));
                    BytesRef id = ids.binaryValue();
                    assertThat(Uid.decodeId(id.bytes, id.offset, id.length), equalTo(Long.toString(5000 + doc)));
                }
                assertThat(ids.advanceExact(100), equalTo(false));
            });
        }
    }

    /** The {@code _id} of a document is its row number in the index, so an id query is a lookup and not a scan. */
    public void testIdQueryFindsTheRowWithoutAScan() throws IOException {
        byte[] file = ParquetTestFiles.threeColumnFile(300);
        List<ColumnBinding> bindings = List.of(new ColumnBinding("id", 0, "id", DocValuesType.NUMERIC), ColumnBinding.synthesizedId(1));
        // the segment holds the rows 5000 to 5099 of its index
        try (Directory dir = attach(file, bindings, new ExternalFilePiece(LOCATION, file.length, 0, 100, 5000))) {
            readWith(dir, file, leaf -> {
                BlockLoader.OptionalColumnAtATimeReader ids = (BlockLoader.OptionalColumnAtATimeReader) leaf.getBinaryDocValues("_id");
                assertThat(docs(ids, "5000"), equalTo(List.of(0)));
                assertThat(docs(ids, "5099"), equalTo(List.of(99)));
                // before, after, and not a canonical number or not a number at all
                for (String id : new String[] { "4999", "5100", "05000", "+5000", "5000.0", "abc" }) {
                    assertThat(id, docs(ids, id), equalTo(List.of()));
                }
                // a term that is not the encoding of any id
                assertThat(ids.tryTermEqualIterator(new BytesRef(new byte[] { (byte) 0xff, 0x01 })).nextDoc(), equalTo(NO_MORE_DOCS));
            });
        }
    }

    private static List<Integer> docs(BlockLoader.OptionalColumnAtATimeReader ids, String id) throws IOException {
        List<Integer> docs = new ArrayList<>();
        DocIdSetIterator iterator = ids.tryTermEqualIterator(Uid.encodeId(id));
        for (int doc = iterator.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = iterator.nextDoc()) {
            docs.add(doc);
        }
        return docs;
    }

    /**
     * The mapping defaults rely on the reader returning epoch milliseconds for a DATE and epoch nanoseconds for a microsecond
     * timestamp.
     */
    public void testTemporalColumnsComeAsTheReaderConvertsThem() throws IOException {
        byte[] file = ParquetSampleFiles.writeTypes(50);
        List<ColumnBinding> bindings = List.of(
            new ColumnBinding("day", 0, "day", DocValuesType.NUMERIC),
            new ColumnBinding("ts_millis", 1, "ts_millis", DocValuesType.NUMERIC),
            new ColumnBinding("ts_micros", 2, "ts_micros", DocValuesType.NUMERIC),
            new ColumnBinding("small", 3, "small", DocValuesType.NUMERIC)
        );
        try (Directory dir = attach(file, bindings, new ExternalFilePiece(LOCATION, file.length, 0, 50))) {
            readWith(dir, file, leaf -> {
                for (int doc : new int[] { 0, 1, 49 }) {
                    for (String field : new String[] { "day", "ts_millis", "ts_micros", "small" }) {
                        assertTrue(leaf.getNumericDocValues(field).advanceExact(doc));
                    }
                    NumericDocValues day = leaf.getNumericDocValues("day");
                    day.advanceExact(doc);
                    assertThat(day.longValue(), equalTo((19_000L + doc) * 86_400_000L));
                    NumericDocValues millis = leaf.getNumericDocValues("ts_millis");
                    millis.advanceExact(doc);
                    assertThat(millis.longValue(), equalTo(1_700_000_000_000L + doc));
                    NumericDocValues micros = leaf.getNumericDocValues("ts_micros");
                    micros.advanceExact(doc);
                    assertThat(micros.longValue(), equalTo((1_700_000_000_000_000L + doc) * 1_000L));
                    NumericDocValues small = leaf.getNumericDocValues("small");
                    small.advanceExact(doc);
                    assertThat(small.longValue(), equalTo((long) doc));
                }
            });
        }
    }

    private static List<ColumnBinding> bindings() {
        return List.of(
            new ColumnBinding("id", 0, "id", DocValuesType.NUMERIC),
            new ColumnBinding("n", 1, "n", DocValuesType.NUMERIC),
            new ColumnBinding("name", 2, "name", DocValuesType.BINARY)
        );
    }

    private void withSegment(int firstRow, int rowCount, LeafCheck check) throws IOException {
        byte[] file = ParquetTestFiles.threeColumnFile(ROWS);
        try (Directory dir = attach(file, bindings(), new ExternalFilePiece(LOCATION, file.length, firstRow, rowCount))) {
            readWith(dir, file, check);
        }
    }

    /** Writes the stub segment: the only thing that is stored in the directory is the reference to the file. */
    private static Directory attach(byte[] file, List<ColumnBinding> bindings, ExternalFilePiece piece) throws IOException {
        Directory dir = new ByteBuffersDirectory();
        IndexWriterConfig config = new IndexWriterConfig().setCodec(new ParquetAttachCodec()).setMergePolicy(NoMergePolicy.INSTANCE);
        try (IndexWriter writer = new IndexWriter(dir, config)) {
            writer.addIndexes(new ExternalParquetCodecReader(bindings, piece));
            writer.commit();
        }
        return dir;
    }

    /** Opens the segment in the context of an engine that resolves the file, and runs the check. */
    private static void readWith(Directory dir, byte[] file, LeafCheck check) throws IOException {
        ParquetReadContext.Resolver resolver = (location, length) -> {
            assertThat(location, equalTo(LOCATION));
            assertThat(length, equalTo((long) file.length));
            return ParquetTestFiles.storageObject(file);
        };
        ParquetReadContext.with(resolver, () -> {
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                assertThat(reader.leaves().size(), equalTo(1));
                check.check(reader.leaves().get(0).reader());
            }
            return null;
        });
    }
}
