/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.parquet.docvalues;

import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.parquet.format.ConvertedType;
import org.apache.parquet.format.Type;
import org.elasticsearch.common.hash.MessageDigests;
import org.elasticsearch.core.PathUtils;
import org.elasticsearch.test.ESTestCase;

import java.io.EOFException;
import java.io.IOException;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.List;

/**
 * Reads the footer of a real dataset, the ClickBench {@code hits.parquet} file of the acceptance target in the plan (A0). It only runs
 * when the file is given, for example {@code -Dtests.jvm.argline=-Dtests.parquet.hits=/path/to/hits.parquet}, because the file is
 * 14 GB. The expected values are the footer facts that we dumped with pyarrow.
 */
// The test reads a file outside of the test directories. That is an access of a developer's machine for a manual check, and
// production code never opens a local path: it reads through the BlobContainer of a registered repository.
@ESTestCase.WithoutEntitlements
public class ParquetFooterReaderRealFileTests extends ESTestCase {

    private static ParquetObject open(FileChannel channel) {
        return new ParquetObject() {
            @Override
            public String location() {
                return "repository://clickbench/hits.parquet";
            }

            @Override
            public long length() throws IOException {
                return channel.size();
            }

            @Override
            public byte[] readRange(long position, int length) throws IOException {
                ByteBuffer buffer = ByteBuffer.allocate(length);
                while (buffer.hasRemaining()) {
                    if (channel.read(buffer, position + buffer.position()) < 0) {
                        throw new EOFException();
                    }
                }
                return buffer.array();
            }
        };
    }

    private static String hitsPath() {
        String file = System.getProperty("tests.parquet.hits");
        assumeTrue("set tests.parquet.hits to the path of hits.parquet", file != null && file.isEmpty() == false);
        return file;
    }

    /**
     * Decodes columns of the first three row groups of the real file, which are SNAPPY compressed with dictionary pages that fall
     * back to plain pages, and compares sums and bounds with what pyarrow reads from the same file (1,406,798 rows).
     */
    public void testClickBenchHitsValues() throws IOException {
        String file = hitsPath();
        try (FileChannel channel = FileChannel.open(PathUtils.get(file), StandardOpenOption.READ)) {
            ParquetObject object = open(channel);
            ParquetFileInfo footer = ParquetFooterReader.read(object);
            List<String> names = List.of("CounterID", "EventTime", "EventDate", "UserID", "WatchID", "ClientIP");
            List<ColumnBinding> bindings = new ArrayList<>();
            for (int i = 0; i < names.size(); i++) {
                bindings.add(new ColumnBinding("hits." + names.get(i), i, names.get(i), DocValuesType.NUMERIC));
            }
            ExternalFilePiece piece = footer.piece(0, 3, 0, names);
            assertEquals(1_406_798, piece.maxDoc());

            try (Directory dir = ParquetTestFiles.newStubDirectory()) {
                IndexWriterConfig config = new IndexWriterConfig().setCodec(new ParquetAttachCodec())
                    .setMergePolicy(NoMergePolicy.INSTANCE);
                try (IndexWriter writer = new IndexWriter(dir, config)) {
                    writer.addIndexes(new ExternalParquetCodecReader(bindings, List.of(piece)));
                    writer.commit();
                }
                ParquetReadContext.with((location, length) -> object, () -> {
                    try (DirectoryReader reader = DirectoryReader.open(dir)) {
                        LeafReader leaf = reader.leaves().get(0).reader();
                        assertColumn(leaf, "CounterID", new long[] { 17, 17, 17, 17, 17 }, 786, 17, 786, "196902436");
                        assertColumn(
                            leaf,
                            "EventTime",
                            new long[] { 1373834327L, 1373913230L, 1373914595L, 1373914712L, 1373833834L },
                            1373841325L,
                            1373832000L,
                            1373918399L,
                            "1932764402156484"
                        );
                        assertColumn(
                            leaf,
                            "EventDate",
                            new long[] { 15901, 15901, 15901, 15901, 15901 },
                            15901,
                            15901,
                            15901,
                            "22369494998"
                        );
                        assertColumn(
                            leaf,
                            "UserID",
                            new long[] {
                                -2461439046089301801L,
                                -2461439046089301801L,
                                -2461439046089301801L,
                                -2461439046089301801L,
                                376160620089546609L },
                            4350581375518433907L,
                            -9219796849203399214L,
                            9223258955477571344L,
                            "3485531592801426163580153"
                        );
                        assertColumn(
                            leaf,
                            "WatchID",
                            new long[] {
                                9110818468285196899L,
                                8156744413230856864L,
                                5206346422301499756L,
                                6308646140879811077L,
                                8740403056911509777L },
                            7710997808886322873L,
                            4611687214012840539L,
                            9223371478009085789L,
                            "9730873324327542069526468"
                        );
                        assertColumn(
                            leaf,
                            "ClientIP",
                            new long[] { -1216690514L, -1216690514L, -1216690514L, -1216690514L, 1615432634L },
                            -1356475887L,
                            -2147461176L,
                            2147482601L,
                            "795834220000605"
                        );
                    }
                    return null;
                });
            }
        }
    }

    private void assertColumn(LeafReader leaf, String column, long[] first, long last, long min, long max, String sum) throws IOException {
        long start = System.nanoTime();
        NumericDocValues values = leaf.getNumericDocValues("hits." + column);
        BigInteger total = BigInteger.ZERO;
        long lowest = Long.MAX_VALUE;
        long highest = Long.MIN_VALUE;
        long lastValue = 0;
        int rows = 0;
        for (int doc = values.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = values.nextDoc()) {
            long value = values.longValue();
            if (rows < first.length) {
                assertEquals(column + " row " + rows, first[rows], value);
            }
            total = total.add(BigInteger.valueOf(value));
            lowest = Math.min(lowest, value);
            highest = Math.max(highest, value);
            lastValue = value;
            rows++;
        }
        logger.info("--> decoded [{}] rows of [{}] in [{}] ms", rows, column, (System.nanoTime() - start) / 1_000_000);
        assertEquals(1_406_798, rows);
        assertEquals(column + " last", last, lastValue);
        assertEquals(column + " min", min, lowest);
        assertEquals(column + " max", max, highest);
        assertEquals(column + " sum", new BigInteger(sum), total);
    }

    /**
     * Decodes string columns of the first three row groups of the real file: big chunks (up to 14 MB of URLs), dictionary pages with
     * plain fallback, and many empty strings. The count, the bytes, the empty strings and an MD5 over all the values with a zero byte
     * after each one are compared with what pyarrow reads.
     */
    public void testClickBenchHitsStrings() throws IOException {
        String file = hitsPath();
        try (FileChannel channel = FileChannel.open(PathUtils.get(file), StandardOpenOption.READ)) {
            ParquetObject object = open(channel);
            ParquetFileInfo footer = ParquetFooterReader.read(object);
            List<String> names = List.of("URL", "Title", "SearchPhrase", "Referer", "HitColor", "UserAgentMinor");
            List<ColumnBinding> bindings = new ArrayList<>();
            for (int i = 0; i < names.size(); i++) {
                bindings.add(new ColumnBinding("hits." + names.get(i), i, names.get(i), DocValuesType.BINARY));
            }
            ExternalFilePiece piece = footer.piece(0, 3, 0, names);

            try (Directory dir = ParquetTestFiles.newStubDirectory()) {
                IndexWriterConfig config = new IndexWriterConfig().setCodec(new ParquetAttachCodec())
                    .setMergePolicy(NoMergePolicy.INSTANCE);
                try (IndexWriter writer = new IndexWriter(dir, config)) {
                    writer.addIndexes(new ExternalParquetCodecReader(bindings, List.of(piece)));
                    writer.commit();
                }
                ParquetReadContext.with((location, length) -> object, () -> {
                    try (DirectoryReader reader = DirectoryReader.open(dir)) {
                        LeafReader leaf = reader.leaves().get(0).reader();
                        assertStrings(leaf, "URL", 139298758L, 268, "32e0fabcf758185b279bd67d6b3e4c39");
                        assertStrings(leaf, "Title", 149863037L, 275652, "68046a9d891ab5cab81d58e935bf0333");
                        assertStrings(leaf, "SearchPhrase", 3698996L, 1332549, "f572717045248987a10281ce8efd32f6");
                        assertStrings(leaf, "Referer", 116035864L, 117086, "1ff810411ffcdab6df8055eaa07131f5");
                        assertStrings(leaf, "HitColor", 1406798L, 0, "c0936e7b8e432effed6cf8c2d667e706");
                        assertStrings(leaf, "UserAgentMinor", 5260234L, 0, "884bca9a851510ce52d60d76146eaaf6");
                    }
                    return null;
                });
            }
        }
    }

    private void assertStrings(LeafReader leaf, String column, long bytes, int empty, String md5) throws IOException {
        long start = System.nanoTime();
        BinaryDocValues values = leaf.getBinaryDocValues("hits." + column);
        MessageDigest digest = MessageDigests.md5();
        long total = 0;
        int emptyValues = 0;
        int rows = 0;
        for (int doc = values.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = values.nextDoc()) {
            BytesRef value = values.binaryValue();
            digest.update(value.bytes, value.offset, value.length);
            digest.update((byte) 0);
            total += value.length;
            if (value.length == 0) {
                emptyValues++;
            }
            rows++;
        }
        logger.info("--> decoded [{}] rows of [{}], [{}] bytes, in [{}] ms", rows, column, total, (System.nanoTime() - start) / 1_000_000);
        assertEquals(1_406_798, rows);
        assertEquals(column + " bytes", bytes, total);
        assertEquals(column + " empty values", empty, emptyValues);
        assertEquals(column + " md5", md5, MessageDigests.toHexString(digest.digest()));
    }

    public void testClickBenchHitsFooter() throws IOException {
        String file = hitsPath();

        try (FileChannel channel = FileChannel.open(PathUtils.get(file), StandardOpenOption.READ)) {
            ParquetObject object = open(channel);
            long start = System.nanoTime();
            ParquetFileInfo info = ParquetFooterReader.read(object);
            logger.info("--> read the footer of [{}] in [{}] ms", file, (System.nanoTime() - start) / 1_000_000);

            assertEquals(99_997_497L, info.rowCount());
            assertEquals(226, info.rowGroups().size());
            assertEquals(105, info.columns().size());
            assertTrue(info.columns().stream().allMatch(ParquetFileInfo.LeafColumn::required));
            assertTrue(info.columns().stream().allMatch(ParquetFileInfo.LeafColumn::flat));
            assertEquals(151_669, info.rowGroups().stream().mapToInt(ParquetFileInfo.RowGroup::numRows).min().orElseThrow());
            assertEquals(819_200, info.rowGroups().stream().mapToInt(ParquetFileInfo.RowGroup::numRows).max().orElseThrow());
            assertEquals("parquet-cpp version 1.5.1-SNAPSHOT", info.createdBy());

            // the types that we found in the footer of this file
            assertEquals(Type.INT64.getValue(), info.column("EventTime").physicalType());
            assertEquals("the timestamps carry no annotation", -1, info.column("EventTime").convertedType());
            assertEquals(Type.INT32.getValue(), info.column("EventDate").physicalType());
            assertEquals(ConvertedType.UINT_16.getValue(), info.column("EventDate").convertedType());
            assertEquals(Type.INT32.getValue(), info.column("CounterID").physicalType());
            assertEquals(Type.BYTE_ARRAY.getValue(), info.column("URL").physicalType());
            assertEquals(ConvertedType.UTF8.getValue(), info.column("URL").convertedType());

            // a piece of three row groups with the snapshot of one numeric and one string column
            ExternalFilePiece piece = info.piece(0, 3, 0, java.util.List.of("CounterID", "URL"));
            assertEquals(
                info.rowGroups().get(0).numRows() + info.rowGroups().get(1).numRows() + info.rowGroups().get(2).numRows(),
                piece.maxDoc()
            );
            assertEquals(2, piece.columns().size());
        }
    }
}
