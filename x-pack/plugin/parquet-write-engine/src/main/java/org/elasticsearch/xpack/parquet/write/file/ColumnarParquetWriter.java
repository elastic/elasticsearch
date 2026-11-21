/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.parquet.write.file;

import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.ColumnWriteStore;
import org.apache.parquet.column.ColumnWriter;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.compression.CompressionCodecFactory;
import org.apache.parquet.hadoop.ColumnChunkPageWriteStore;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.MessageType;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * The output file: column writers that buffer a row group, and the cut of a row group when the buffered data reaches the size. It is
 * what the parquet-mr record writer does between its record consumer and the file, without the record consumer.
 *
 * <p>There are two ways to fill it. A caller that has rows (the merge) writes the values of one row to every column and calls
 * {@link #endRecord()}, which cuts a row group when the buffered data is large enough. A caller that has columns (a bulk) writes all the
 * values of one column, then the next, and calls {@link #rowsWrittenByColumns} once: the file is then one row group with one page for
 * each column. Parquet counts the rows of a column by its repetition levels (a value with level 0 starts a row), so it does not need
 * the order of the values across columns.
 */
final class ColumnarParquetWriter implements AutoCloseable {
    /** The bounds of the number of records between two checks of the buffered size, as in the parquet-mr record writer. */
    private static final long MIN_RECORDS_BETWEEN_CHECKS = 100;
    private static final long MAX_RECORDS_BETWEEN_CHECKS = 10_000;

    private final MessageType schema;
    private final List<ColumnDescriptor> columns;
    private final long rowGroupBytes;
    private final ParquetProperties properties;
    private final CompressionCodecFactory codecs = new ParquetCodecs();
    private final CompressionCodecFactory.BytesInputCompressor compressor;
    private final ParquetFileWriter fileWriter;
    private ColumnChunkPageWriteStore pageStore;
    private ColumnWriteStore store;
    private ColumnWriter[] writers;
    private long recordCount;
    private long recordCountForNextCheck = MIN_RECORDS_BETWEEN_CHECKS;

    /** The properties of a writer that is filled row by row: the pages are cut by the size that parquet-mr uses by default. */
    static ParquetProperties rowProperties() {
        return ParquetProperties.builder().withWriterVersion(ParquetProperties.WriterVersion.PARQUET_1_0).build();
    }

    /**
     * The properties of a writer that is filled column by column: no page is ever cut, so a column of the row group is one page. The
     * store decides about a page when a record ends, and it counts the rows of a page from the records that ended, which does not fit a
     * column that has all its values before the first record ends. We did not see it go wrong (the default properties also gave one page
     * for each column in a test with 12 MB columns), but that is a property of the internals of parquet-mr, so the store is never given
     * the chance to decide. A page is at most the column of one bulk, which is at most 5,000 documents.
     */
    static ParquetProperties columnProperties() {
        return ParquetProperties.builder()
            .withWriterVersion(ParquetProperties.WriterVersion.PARQUET_1_0)
            .withPageSize(Integer.MAX_VALUE)
            .withPageRowCountLimit(Integer.MAX_VALUE)
            .build();
    }

    ColumnarParquetWriter(
        BatchParquetWriter.IndexOutputFile file,
        MessageType schema,
        CompressionCodecName codec,
        long rowGroupBytes,
        ParquetProperties properties
    ) throws IOException {
        this.properties = properties;
        this.schema = schema;
        this.columns = schema.getColumns();
        this.rowGroupBytes = rowGroupBytes;
        this.compressor = codecs.getCompressor(codec);
        this.fileWriter = new ParquetFileWriter(file, schema, ParquetFileWriter.Mode.CREATE, rowGroupBytes, 0, null, properties);
        fileWriter.start();
        startRowGroup();
    }

    private void startRowGroup() {
        pageStore = new ColumnChunkPageWriteStore(
            compressor,
            schema,
            properties.getAllocator(),
            properties.getColumnIndexTruncateLength(),
            properties.getPageWriteChecksumEnabled()
        );
        store = properties.newColumnWriteStore(schema, pageStore);
        writers = new ColumnWriter[columns.size()];
        for (int i = 0; i < writers.length; i++) {
            writers[i] = store.getColumnWriter(columns.get(i));
        }
        recordCount = 0;
        recordCountForNextCheck = MIN_RECORDS_BETWEEN_CHECKS;
    }

    ColumnWriter columnWriter(int index) {
        return writers[index];
    }

    void endRecord() throws IOException {
        store.endRecord();
        recordCount++;
        if (recordCount >= recordCountForNextCheck) {
            final long buffered = store.getBufferedSize();
            final long recordSize = Math.max(1, buffered / recordCount);
            if (buffered > rowGroupBytes - 2 * recordSize) {
                endRowGroup();
                startRowGroup();
            } else {
                recordCountForNextCheck = Math.min(
                    Math.max(MIN_RECORDS_BETWEEN_CHECKS, (recordCount + (long) (rowGroupBytes / (double) recordSize)) / 2),
                    recordCount + MAX_RECORDS_BETWEEN_CHECKS
                );
            }
        }
    }

    /**
     * For a caller that wrote each column of all the rows on its own, with {@link #columnProperties()}: the row group has {@code rows}
     * more rows. The records end now, because the store writes the last page of a column only when it has counted the rows of it. No row
     * group is cut: the caller makes one row group.
     */
    void rowsWrittenByColumns(long rows) {
        for (long i = 0; i < rows; i++) {
            store.endRecord();
        }
        recordCount += rows;
    }

    private void endRowGroup() throws IOException {
        if (recordCount > 0) {
            fileWriter.startBlock(recordCount);
            store.flush();
            pageStore.flushToFileWriter(fileWriter);
            fileWriter.endBlock();
        }
        store.close();
        pageStore.close();
        store = null;
        pageStore = null;
    }

    /** Writes the last row group and the footer, which closes the output. */
    void finish() throws IOException {
        endRowGroup();
        fileWriter.end(Map.of());
    }

    @Override
    public void close() {
        // after finish() the stores are gone; after a failure they are still open
        if (store != null) {
            store.close();
            pageStore.close();
        }
        codecs.release();
    }
}
