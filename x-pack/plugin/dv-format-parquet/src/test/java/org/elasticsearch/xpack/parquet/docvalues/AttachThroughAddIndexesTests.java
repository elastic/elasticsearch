/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.parquet.docvalues;

import org.apache.lucene.codecs.perfield.PerFieldDocValuesFormat;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

/**
 * Attach spike, candidate 1 of the plan: a stub segment is created by handing a custom {@code CodecReader} to
 * {@code IndexWriter#addIndexes}. The tests only need to see that the segment opens and resolves our format by name.
 */
public class AttachThroughAddIndexesTests extends ESTestCase {

    private static final String FIELD = "parquet.status";

    private static IndexWriterConfig config() {
        return new IndexWriterConfig().setCodec(new ParquetAttachCodec()).setMergePolicy(NoMergePolicy.INSTANCE);
    }

    private static ExternalParquetCodecReader reader(ExternalFilePiece... pieces) throws IOException {
        return new ExternalParquetCodecReader(List.of(new ColumnBinding(FIELD, 0, "id", DocValuesType.NUMERIC)), Arrays.asList(pieces));
    }

    public void testAttachedSegmentOpensAndServesTheField() throws IOException {
        BytesParquetObject file = new BytesParquetObject(
            "repository://lake/events/part-0001.parquet",
            ParquetTestFiles.writeFlat(3_500, 1_000)
        );
        ExternalFilePiece piece = ParquetFooterReader.read(file).piece(0, 4, 0, List.of("id"));
        try (Directory dir = ParquetTestFiles.newStubDirectory()) {
            try (IndexWriter writer = new IndexWriter(dir, config())) {
                writer.addIndexes(reader(piece));
                writer.commit();
            }
            ParquetReadContext.with((location, length) -> file, () -> {
                try (DirectoryReader directoryReader = DirectoryReader.open(dir)) {
                    assertEquals(1, directoryReader.leaves().size());
                    assertEquals(3_500, directoryReader.maxDoc());
                    LeafReader leaf = directoryReader.leaves().get(0).reader();
                    assertEquals(DocValuesType.NUMERIC, leaf.getFieldInfos().fieldInfo(FIELD).getDocValuesType());
                    NumericDocValues values = leaf.getNumericDocValues(FIELD);
                    assertNotNull(values);
                    assertTrue(values.advanceExact(3_499));
                    assertEquals(3_499L, values.longValue());
                }
                return null;
            });
            logger.info("--> segment files: {}", Arrays.toString(dir.listAll()));
        }
    }

    /**
     * Measures what the parts of the segment that carry no data cost for {@code maxDoc} rows. The row count defaults to a small number
     * so that the test is cheap, and {@code -Dtests.parquet.attach.rows=100000000} runs the size of a large file.
     */
    public void testAttachCostForManyRows() throws IOException {
        int rows = Integer.getInteger("tests.parquet.attach.rows", 1_000_000);
        int rowGroups = 200;
        int[] rowGroupRows = new int[rowGroups];
        Arrays.fill(rowGroupRows, rows / rowGroups);
        ExternalFilePiece piece = new ExternalFilePiece("repository://lake/big.parquet", 1L << 33, 0L, 0, rowGroupRows, List.of());
        try (Directory dir = ParquetTestFiles.newStubDirectory()) {
            long start = System.nanoTime();
            try (IndexWriter writer = new IndexWriter(dir, config())) {
                writer.addIndexes(reader(piece));
                writer.commit();
            }
            long millis = (System.nanoTime() - start) / 1_000_000;
            long bytes = 0;
            for (String file : dir.listAll()) {
                bytes += dir.fileLength(file);
                logger.info("--> [{}] {} bytes", file, dir.fileLength(file));
            }
            logger.info("--> attached [{}] rows in [{}] ms, [{}] bytes on disk", rowGroups * (rows / rowGroups), millis, bytes);
            try (DirectoryReader directoryReader = DirectoryReader.open(dir)) {
                assertEquals(rowGroups * (rows / rowGroups), directoryReader.maxDoc());
            }
        }
    }

    /**
     * The footer snapshot survives the trip through the meta file: the piece that we read back from the stub segment is the piece
     * that we attached, with the chunks of its columns.
     */
    public void testMetaFileKeepsTheFooterSnapshot() throws IOException {
        BytesParquetObject file = new BytesParquetObject("repository://lake/flat.parquet", ParquetTestFiles.writeFlat(1_000, 300));
        ParquetFileInfo footer = ParquetFooterReader.read(file);
        ExternalFilePiece piece = footer.piece(1, 3, 300, List.of("id", "maybe", "name"));
        List<ColumnBinding> bindings = List.of(
            new ColumnBinding("parquet.id", 0, "id", DocValuesType.NUMERIC),
            new ColumnBinding("parquet.maybe", 1, "maybe", DocValuesType.NUMERIC)
        );
        try (Directory dir = ParquetTestFiles.newStubDirectory()) {
            try (IndexWriter writer = new IndexWriter(dir, config())) {
                writer.addIndexes(new ExternalParquetCodecReader(bindings, List.of(piece)));
                writer.commit();
            }
            SegmentInfos infos = SegmentInfos.readLatestCommit(dir);
            SegmentInfo segmentInfo = infos.info(0).info;
            FieldInfos fieldInfos = segmentInfo.getCodec().fieldInfosFormat().read(dir, segmentInfo, "", IOContext.READONCE);
            String suffix = fieldInfos.fieldInfo("parquet.id").getAttribute(PerFieldDocValuesFormat.PER_FIELD_SUFFIX_KEY);
            String format = fieldInfos.fieldInfo("parquet.id").getAttribute(PerFieldDocValuesFormat.PER_FIELD_FORMAT_KEY);
            assertEquals(ParquetDocValuesFormat.NAME, format);
            String segmentSuffix = format + "_" + suffix;
            try (
                ParquetDocValuesProducer producer = (ParquetDocValuesProducer) new ParquetDocValuesFormat().fieldsProducer(
                    new SegmentReadState(dir, segmentInfo, fieldInfos, IOContext.DEFAULT, segmentSuffix)
                )
            ) {
                assertEquals(List.of(piece), producer.pieces());
                assertEquals("row groups 1 to 3 hold 300 + 300 + 100 rows", 700, producer.maxDoc());
                assertEquals(
                    List.of("parquet.id", "parquet.maybe"),
                    producer.bindings().stream().map(ColumnBinding::fieldName).sorted().toList()
                );
            }
        }
    }
}
