/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.parquet.dv;

import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.util.BytesRef;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.lucene.read.DelegatingBlockLoaderFactory;
import org.elasticsearch.index.mapper.BlockLoader;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.parquet.ParquetSampleFiles;
import org.elasticsearch.xpack.parquet.ParquetTestFiles;
import org.elasticsearch.xpack.parquet.engine.ExternalParquetCodecReader;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.notNullValue;

/**
 * Reading a page of documents into a block for ES|QL ({@code tryRead}), and the dictionary pages that let a block of strings keep its
 * values as ordinals into the distinct ones.
 */
public class ParquetTryReadTests extends ESTestCase {

    private static final String LOCATION = "memory://f.parquet";
    private static final int ROWS = 10_000;

    /**
     * The block factory that the compute engine uses to build the blocks of a block loader is package-private, but its base class is
     * not and leaves one method, which the engine implements with a block of nulls.
     */
    private final BlockLoader.BlockFactory factory = new DelegatingBlockLoaderFactory(ParquetBlocks.factory()) {
        @Override
        public BlockLoader.Block constantNulls(int count) {
            return ParquetBlocks.factory().newConstantNullBlock(count);
        }
    };

    private record Docs(int[] docs) implements BlockLoader.Docs {
        @Override
        public int count() {
            return docs.length;
        }

        @Override
        public int get(int i) {
            return docs[i];
        }

        @Override
        public boolean mayContainDuplicates() {
            return false;
        }
    }

    private static Docs range(int from, int to) {
        int[] docs = new int[to - from];
        for (int i = 0; i < docs.length; i++) {
            docs[i] = from + i;
        }
        return new Docs(docs);
    }

    /** A producer over the whole file, which the tests read through without a Lucene directory. */
    private static final class Segment {
        final ParquetDocValuesProducer producer;
        final org.apache.lucene.index.FieldInfos fieldInfos;

        Segment(byte[] file, List<ColumnBinding> bindings) throws IOException {
            ParquetFileInfo info = ParquetFileInfo.read(LOCATION, ParquetTestFiles.storageObject(file));
            ExternalFilePiece piece = ExternalFilePiece.of(info, 0, info.rowGroups().size(), 0, bindings);
            // a small window, so that a read crosses many pages
            producer = new ParquetDocValuesProducer(bindings, piece, (location, length) -> ParquetTestFiles.storageObject(file), 512);
            producer.resolveStorage();
            fieldInfos = new ExternalParquetCodecReader(bindings, piece).getFieldInfos();
        }

        NumericDocValues numeric(String field) throws IOException {
            return producer.getNumeric(fieldInfos.fieldInfo(field));
        }

        BinaryDocValues binary(String field) throws IOException {
            return producer.getBinary(fieldInfos.fieldInfo(field));
        }
    }

    private static List<ColumnBinding> threeColumns() {
        return List.of(
            new ColumnBinding("id", 0, "id", DocValuesType.NUMERIC),
            new ColumnBinding("n", 1, "n", DocValuesType.NUMERIC),
            new ColumnBinding("name", 2, "name", DocValuesType.BINARY)
        );
    }

    private static BlockLoader.OptionalColumnAtATimeReader reader(Object docValues) {
        return (BlockLoader.OptionalColumnAtATimeReader) docValues;
    }

    /**
     * Reads the first documents one by one, as ES|QL does. The first load of an iterator is by position and gives plain values, and the
     * loads after it continue by a scan of the row group, which is the path that returns dictionary pages.
     */
    private static BinaryDocValues warmedUp(BinaryDocValues values, int upTo) throws IOException {
        for (int doc = 0; doc < upTo; doc++) {
            values.advanceExact(doc);
        }
        return values;
    }

    public void testNumericDocumentsThatAllHaveAValueAreOneBlock() throws IOException {
        Segment segment = new Segment(ParquetTestFiles.threeColumnFile(ROWS), threeColumns());
        try (Block block = (Block) reader(segment.numeric("id")).tryRead(factory, range(100, 4100), 0, true, null, false, false)) {
            LongBlock longs = (LongBlock) block;
            assertThat(longs.getPositionCount(), equalTo(4000));
            for (int i = 0; i < 4000; i++) {
                assertThat(longs.getLong(longs.getFirstValueIndex(i)), equalTo((100 + i) * 7L));
            }
        }
    }

    public void testNumericOffsetAndSparseDocuments() throws IOException {
        Segment segment = new Segment(ParquetTestFiles.threeColumnFile(ROWS), threeColumns());
        int[] docs = { 3, 9, 700, 701, 4_000, 4_001, 9_999 };
        // the first two documents belong to an earlier read, which the offset skips
        try (Block block = (Block) reader(segment.numeric("id")).tryRead(factory, new Docs(docs), 2, true, null, false, false)) {
            LongBlock longs = (LongBlock) block;
            assertThat(longs.getPositionCount(), equalTo(5));
            for (int i = 0; i < 5; i++) {
                assertThat(longs.getLong(longs.getFirstValueIndex(i)), equalTo(docs[2 + i] * 7L));
            }
        }
    }

    public void testNumericMissingValuesAreNullsAndConversionsApply() throws IOException {
        Segment segment = new Segment(ParquetTestFiles.threeColumnFile(ROWS), threeColumns());
        Docs docs = range(0, 1_500);
        // the column n has no value for the rows that are multiples of 3, and the row for the others
        try (Block block = (Block) reader(segment.numeric("n")).tryRead(factory, docs, 0, false, null, false, false)) {
            LongBlock longs = (LongBlock) block;
            for (int i = 0; i < 1_500; i++) {
                assertThat(longs.isNull(i), equalTo(i % 3 == 0));
                if (i % 3 != 0) {
                    assertThat(longs.getLong(longs.getFirstValueIndex(i)), equalTo((long) i));
                }
            }
        }
        try (Block block = (Block) reader(segment.numeric("n")).tryRead(factory, docs, 0, false, null, true, false)) {
            IntBlock ints = (IntBlock) block;
            for (int i = 0; i < 1_500; i++) {
                assertThat(ints.isNull(i), equalTo(i % 3 == 0));
                if (i % 3 != 0) {
                    assertThat(ints.getInt(ints.getFirstValueIndex(i)), equalTo(i));
                }
            }
        }
        try (Block block = (Block) reader(segment.numeric("n")).tryRead(factory, docs, 0, false, v -> v * 0.5, false, false)) {
            DoubleBlock doubles = (DoubleBlock) block;
            for (int i = 0; i < 1_500; i++) {
                assertThat(doubles.isNull(i), equalTo(i % 3 == 0));
                if (i % 3 != 0) {
                    assertThat(doubles.getDouble(doubles.getFirstValueIndex(i)), equalTo(i * 0.5));
                }
            }
        }
    }

    public void testNumericTryReadOfDocumentsThatAllHaveAValueAsIntsAndDoubles() throws IOException {
        Segment segment = new Segment(ParquetTestFiles.threeColumnFile(ROWS), threeColumns());
        try (Block block = (Block) reader(segment.numeric("id")).tryRead(factory, range(0, 50), 0, true, v -> v / 7.0, false, false)) {
            DoubleBlock doubles = (DoubleBlock) block;
            for (int i = 0; i < 50; i++) {
                assertThat(doubles.getDouble(doubles.getFirstValueIndex(i)), equalTo((double) i));
            }
        }
    }

    public void testBinaryValuesAndNulls() throws IOException {
        Segment segment = new Segment(ParquetTestFiles.threeColumnFile(ROWS), threeColumns());
        try (Block block = (Block) reader(segment.binary("name")).tryRead(factory, range(2_000, 4_000), 0, false, null, false, false)) {
            assertNames((BytesRefBlock) block, 2_000, 4_000);
        }
    }

    private static void assertNames(BytesRefBlock strings, int from, int to) {
        assertThat(strings.getPositionCount(), equalTo(to - from));
        BytesRef scratch = new BytesRef();
        for (int i = 0; i < to - from; i++) {
            int row = from + i;
            assertThat("row " + row, strings.isNull(i), equalTo(row % 5 == 0));
            if (row % 5 != 0) {
                assertThat(strings.getBytesRef(strings.getFirstValueIndex(i), scratch).utf8ToString(), equalTo("name-" + row));
            }
        }
    }

    private static byte[] repetitiveStrings(int rows, int distinct, boolean optionalNulls, ParquetSampleFiles.Options options)
        throws IOException {
        List<String> required = new ArrayList<>();
        List<String> optional = new ArrayList<>();
        for (int i = 0; i < rows; i++) {
            required.add("value-" + (i * 7 % distinct));
            optional.add(optionalNulls && i % 4 == 0 ? null : "kind-" + (i % 3));
        }
        return ParquetSampleFiles.writeStrings(required, optional, options);
    }

    private static List<ColumnBinding> stringColumns() {
        return List.of(new ColumnBinding("req", 0, "req", DocValuesType.BINARY), new ColumnBinding("opt", 1, "opt", DocValuesType.BINARY));
    }

    public void testRepeatedStringsAreDictionaryPagesAndOrdinalBlocks() throws IOException {
        Segment segment = new Segment(repetitiveStrings(ROWS, 5, true, ParquetSampleFiles.Options.DEFAULT), stringColumns());
        ParquetBinaryDocValues required = (ParquetBinaryDocValues) warmedUp(segment.binary("req"), 1_100);
        BytesWindow.BytesPage page = required.window().bytesPage();
        assertTrue("the reader returns ordinals for a dictionary encoded page, and the page keeps them", page.isDictionary());
        assertThat("only the values that the page uses", page.dictionarySize(), lessThanOrEqualTo(5));

        try (Block block = (Block) reader(required).tryRead(factory, range(1_100, 3_100), 0, true, null, false, false)) {
            BytesRefBlock strings = (BytesRefBlock) block;
            assertThat("a block of repeated values is ordinals into its distinct values", strings.asOrdinals(), notNullValue());
            assertThat(strings.asOrdinals().getDictionaryVector().getPositionCount(), lessThanOrEqualTo(5));
            BytesRef scratch = new BytesRef();
            for (int i = 0; i < 2_000; i++) {
                assertThat(
                    strings.getBytesRef(strings.getFirstValueIndex(i), scratch).utf8ToString(),
                    equalTo("value-" + ((1_100 + i) * 7 % 5))
                );
            }
        }
        // a column with missing values: the nulls are in the block, and the other values are ordinals
        BinaryDocValues optional = warmedUp(segment.binary("opt"), 1_100);
        try (Block block = (Block) reader(optional).tryRead(factory, range(1_100, 3_100), 0, false, null, false, false)) {
            BytesRefBlock strings = (BytesRefBlock) block;
            BytesRef scratch = new BytesRef();
            for (int i = 0; i < 2_000; i++) {
                int row = 1_100 + i;
                assertThat(strings.isNull(i), equalTo(row % 4 == 0));
                if (row % 4 != 0) {
                    assertThat(strings.getBytesRef(strings.getFirstValueIndex(i), scratch).utf8ToString(), equalTo("kind-" + (row % 3)));
                }
            }
        }
    }

    public void testAColumnOfOneValueIsAConstantBlock() throws IOException {
        List<String> same = new ArrayList<>();
        List<String> kinds = new ArrayList<>();
        for (int i = 0; i < 3_000; i++) {
            same.add("always");
            kinds.add(null);
        }
        Segment segment = new Segment(ParquetSampleFiles.writeStrings(same, kinds, ParquetSampleFiles.Options.DEFAULT), stringColumns());
        try (
            Block block = (Block) reader(warmedUp(segment.binary("req"), 1_100)).tryRead(
                factory,
                range(1_100, 2_000),
                0,
                true,
                null,
                false,
                false
            )
        ) {
            BytesRefBlock strings = (BytesRefBlock) block;
            assertThat(strings.getPositionCount(), equalTo(900));
            assertTrue(
                "was " + strings.getClass() + " vector " + strings.asVector(),
                strings.asVector() != null && strings.asVector().isConstant()
            );
            assertThat(strings.getBytesRef(0, new BytesRef()).utf8ToString(), equalTo("always"));
        }
        // and a column that has no value at all is a block of nulls
        try (
            Block block = (Block) reader(warmedUp(segment.binary("opt"), 1_100)).tryRead(
                factory,
                range(1_100, 2_000),
                0,
                false,
                null,
                false,
                false
            )
        ) {
            assertTrue(block.areAllValuesNull());
            assertThat(block.getPositionCount(), equalTo(900));
        }
    }

    /** A dictionary that fills up makes the writer continue with plain pages, so a block can cross both kinds. */
    public void testBlocksAcrossDictionaryAndPlainPagesHaveTheRightValues() throws IOException {
        // a dictionary of at most 256 bytes: the first values go into it, and the writer falls back to plain pages when it is full
        ParquetSampleFiles.Options smallDictionary = new ParquetSampleFiles.Options(true, 1024, 256, CompressionCodecName.UNCOMPRESSED);
        int rows = 6_000;
        List<String> required = new ArrayList<>();
        List<String> optional = new ArrayList<>();
        for (int i = 0; i < rows; i++) {
            required.add("value-" + (i < 2_000 ? i % 4 : i));
            optional.add(null);
        }
        Segment segment = new Segment(ParquetSampleFiles.writeStrings(required, optional, smallDictionary), stringColumns());
        try (Block block = (Block) reader(segment.binary("req")).tryRead(factory, range(0, rows), 0, true, null, false, false)) {
            BytesRefBlock strings = (BytesRefBlock) block;
            assertThat(strings.getPositionCount(), equalTo(rows));
            BytesRef scratch = new BytesRef();
            for (int i = 0; i < rows; i++) {
                assertThat(strings.getBytesRef(strings.getFirstValueIndex(i), scratch).utf8ToString(), equalTo(required.get(i)));
            }
        }
        // and the iterator, which reads the same pages, agrees
        BinaryDocValues values = segment.binary("req");
        for (int i = 0; i < rows; i += 7) {
            assertTrue(values.advanceExact(i));
            assertThat(values.binaryValue().utf8ToString(), equalTo(required.get(i)));
        }
    }

    public void testTheMultiValuedFramingIsNotServed() throws IOException {
        Segment segment = new Segment(ParquetTestFiles.threeColumnFile(ROWS), threeColumns());
        assertNull(reader(segment.binary("name")).tryRead(factory, range(0, 10), 0, false, null, false, true));
        assertNull(reader(segment.numeric("id")).tryRead(factory, range(0, 10), 0, true, null, false, true));
    }

    public void testFieldInfoIsNotNeededToReadBlocks() throws IOException {
        // the doc values of a field read the same through the iterator and through a block
        Segment segment = new Segment(ParquetTestFiles.threeColumnFile(ROWS), threeColumns());
        FieldInfo field = segment.fieldInfos.fieldInfo("id");
        assertThat(field.getDocValuesType(), equalTo(DocValuesType.NUMERIC));
        NumericDocValues iterator = segment.numeric("id");
        try (Block block = (Block) reader(segment.numeric("id")).tryRead(factory, range(500, 700), 0, true, null, false, false)) {
            LongBlock longs = (LongBlock) block;
            for (int i = 0; i < 200; i++) {
                assertTrue(iterator.advanceExact(500 + i));
                assertThat(longs.getLong(longs.getFirstValueIndex(i)), equalTo(iterator.longValue()));
            }
        }
    }
}
