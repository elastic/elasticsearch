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
import org.apache.parquet.schema.Type;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
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
 *
 * <p>The two ways have their own encoding, and the factories {@link #forRows} and {@link #forColumns} are the only way to make a writer,
 * so that no file gets the encoding of the other use:
 * <ul>
 *   <li>The file of a merge ({@code forRows}) stays in the object store for a long time, so its storage efficiency matters: dictionary
 *       encoding, and ZSTD at {@link ParquetCodecs#DEFAULT_ZSTD_LEVEL}. The merge of a flush, of a compaction and of an adoption all write
 *       this file.</li>
 *   <li>The local file of a bulk ({@code forColumns}), and the staged copy of it, is intermediate: it is written once and read once, by
 *       the merge, which encodes the rows again. It gets the cheapest encoding: no dictionary (the hashing of the values was 45% of the
 *       CPU of the write of a bulk in a profile), and ZSTD at {@link #LOCAL_ZSTD_LEVEL}.</li>
 * </ul>
 *
 * <p>The columns can be split into groups, each with its own page store, column store and column writers, so that several threads can
 * encode different columns of the same row group (see {@code ParquetMerger}). A row group is still one block of the file: the groups
 * are flushed into it one after the other in the order of the columns, so the file is the same as one that a single group writes. With
 * one group, which is what every other use has, the writer is what it was before the groups existed.
 */

final class ColumnarParquetWriter implements AutoCloseable {
    /** The ZSTD level of the local files of the bulks, and so of their staged copies. */
    static final int LOCAL_ZSTD_LEVEL = 1;

    /** The bounds of the number of records between two checks of the buffered size, as in the parquet-mr record writer. */
    private static final long MIN_RECORDS_BETWEEN_CHECKS = 100;
    private static final long MAX_RECORDS_BETWEEN_CHECKS = 10_000;

    /**
     * The page store, the column store and the column writers of a range of the columns of the output (all of them, or the columns that
     * one thread of a parallel merge encodes). A group is used by one thread at a time.
     */
    private static final class Group {
        final MessageType schema;
        final List<ColumnDescriptor> columns;
        final CompressionCodecFactory.BytesInputCompressor compressor;
        ColumnChunkPageWriteStore pageStore;
        ColumnWriteStore store;
        ColumnWriter[] writers;

        Group(MessageType schema, CompressionCodecFactory.BytesInputCompressor compressor) {
            this.schema = schema;
            this.columns = schema.getColumns();
            this.compressor = compressor;
        }

        void start(ParquetProperties properties) {
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
        }

        /** Writes the columns of the group into the block that the file writer has started, and releases the group's buffers. */
        void flushTo(ParquetFileWriter fileWriter) throws IOException {
            store.flush();
            pageStore.flushToFileWriter(fileWriter);
        }

        void release() {
            if (store != null) {
                store.close();
                pageStore.close();
                store = null;
                pageStore = null;
            }
        }
    }

    private final long rowGroupBytes;
    private final ParquetProperties properties;
    private final CompressionCodecFactory codecs;
    private final ParquetFileWriter fileWriter;
    private final List<Group> groups = new ArrayList<>();
    /** The writers of all the groups, in the order of the columns of the schema. */
    private ColumnWriter[] writers;
    private long recordCount;
    private long recordCountForNextCheck = MIN_RECORDS_BETWEEN_CHECKS;

    /**
     * A writer that is filled row by row, for the file of a merge. The pages are cut by the size that parquet-mr uses by default, the
     * values have a dictionary where it pays, and ZSTD is at {@link ParquetCodecs#DEFAULT_ZSTD_LEVEL}.
     */
    static ColumnarParquetWriter forRows(
        BatchParquetWriter.IndexOutputFile file,
        MessageType schema,
        CompressionCodecName codec,
        long rowGroupBytes
    ) throws IOException {
        return forRowGroups(file, schema, codec, rowGroupBytes, List.of(schema));
    }

    /**
     * The same writer as {@link #forRows}, with the columns in groups: {@code groupSchemas} are the schemas of the groups, which together
     * have the fields of {@code schema} in the same order. The caller fills a group with {@link #columnWriter} and {@link #endRecord(int)},
     * one thread for each group, and calls {@link #inputDone} when all the groups have the rows of one input.
     */
    static ColumnarParquetWriter forRowGroups(
        BatchParquetWriter.IndexOutputFile file,
        MessageType schema,
        CompressionCodecName codec,
        long rowGroupBytes,
        List<MessageType> groupSchemas
    ) throws IOException {
        final ParquetProperties properties = ParquetProperties.builder()
            .withWriterVersion(ParquetProperties.WriterVersion.PARQUET_1_0)
            .build();
        return new ColumnarParquetWriter(file, schema, codec, rowGroupBytes, properties, new ParquetCodecs(), groupSchemas);
    }

    /**
     * A writer that is filled column by column, for the local file of a bulk. It writes the cheapest file: no dictionary, and ZSTD at
     * {@link #LOCAL_ZSTD_LEVEL}.
     *
     * <p>No page is ever cut, so a column of the row group is one page. The store decides about a page when a record ends, and it counts
     * the rows of a page from the records that ended, which does not fit a column that has all its values before the first record ends.
     * We did not see it go wrong (the default properties also gave one page for each column in a test with 12 MB columns), but that is a
     * property of the internals of parquet-mr, so the store is never given the chance to decide. A page is at most the column of one
     * bulk, which is at most 5,000 documents.
     */
    static ColumnarParquetWriter forColumns(BatchParquetWriter.IndexOutputFile file, MessageType schema, CompressionCodecName codec)
        throws IOException {
        final ParquetProperties properties = ParquetProperties.builder()
            .withWriterVersion(ParquetProperties.WriterVersion.PARQUET_1_0)
            .withDictionaryEncoding(false)
            .withPageSize(Integer.MAX_VALUE)
            .withPageRowCountLimit(Integer.MAX_VALUE)
            .build();
        return new ColumnarParquetWriter(
            file,
            schema,
            codec,
            Long.MAX_VALUE,
            properties,
            new ParquetCodecs(LOCAL_ZSTD_LEVEL),
            List.of(schema)
        );
    }

    private ColumnarParquetWriter(
        BatchParquetWriter.IndexOutputFile file,
        MessageType schema,
        CompressionCodecName codec,
        long rowGroupBytes,
        ParquetProperties properties,
        CompressionCodecFactory codecs,
        List<MessageType> groupSchemas
    ) throws IOException {
        this.properties = properties;
        this.codecs = codecs;
        this.rowGroupBytes = rowGroupBytes;
        int fields = 0;
        for (MessageType groupSchema : groupSchemas) {
            // a compressor for each group: the groups are used by different threads
            groups.add(new Group(groupSchema, codecs.getCompressor(codec)));
            for (Type field : groupSchema.getFields()) {
                if (field.equals(schema.getType(fields++)) == false) {
                    throw new IllegalArgumentException("the groups do not have the fields of the schema in its order: [" + field + "]");
                }
            }
        }
        if (fields != schema.getFieldCount()) {
            throw new IllegalArgumentException("the groups have [" + fields + "] fields and the schema [" + schema.getFieldCount() + "]");
        }
        this.fileWriter = new ParquetFileWriter(file, schema, ParquetFileWriter.Mode.CREATE, rowGroupBytes, 0, null, properties);
        fileWriter.start();
        startRowGroup();
    }

    private void startRowGroup() {
        final List<ColumnWriter> all = new ArrayList<>();
        for (Group group : groups) {
            group.start(properties);
            all.addAll(Arrays.asList(group.writers));
        }
        writers = all.toArray(new ColumnWriter[0]);
        recordCount = 0;
        recordCountForNextCheck = MIN_RECORDS_BETWEEN_CHECKS;
    }

    /** The writer of a column, by its place among the columns of the whole schema. */
    ColumnWriter columnWriter(int index) {
        return writers[index];
    }

    /** The writer of a column of a group, by its place among the columns of the group. */
    ColumnWriter columnWriter(int group, int index) {
        return groups.get(group).writers[index];
    }

    /** Ends a record, for a writer with one group: it cuts a row group when the buffered data is large enough. */
    void endRecord() throws IOException {
        assert groups.size() == 1 : "a writer with groups ends the records of a group";
        groups.get(0).store.endRecord();
        recordCount++;
        if (recordCount >= recordCountForNextCheck) {
            final long buffered = groups.get(0).store.getBufferedSize();
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

    /** Ends a record of one group. Different threads may do it for different groups. It never cuts a row group (see {@link #inputDone}). */
    void endRecord(int group) {
        groups.get(group).store.endRecord();
    }

    /**
     * For a writer with groups: every group has written the {@code rows} rows of one input. This is the only place where a row group is
     * cut, because the groups run on their own threads and the buffered size of all of them is only known when none is running. The
     * rule is the one of {@link #endRecord()}, applied at the end of an input: a row group is cut when the buffered data of all the groups
     * is within two records of the target, so it can be at most one input larger.
     */
    void inputDone(long rows) throws IOException {
        recordCount += rows;
        long buffered = 0;
        for (Group group : groups) {
            buffered += group.store.getBufferedSize();
        }
        final long recordSize = Math.max(1, buffered / Math.max(1, recordCount));
        if (buffered > rowGroupBytes - 2 * recordSize) {
            endRowGroup();
            startRowGroup();
        }
    }

    /**
     * For a caller that wrote each column of all the rows on its own, with {@link #forColumns}: the row group has {@code rows}
     * more rows. The records end now, because the store writes the last page of a column only when it has counted the rows of it. No row
     * group is cut: the caller makes one row group.
     */
    void rowsWrittenByColumns(long rows) {
        for (long i = 0; i < rows; i++) {
            groups.get(0).store.endRecord();
        }
        recordCount += rows;
    }

    private void endRowGroup() throws IOException {
        if (recordCount > 0) {
            fileWriter.startBlock(recordCount);
            for (Group group : groups) {
                group.flushTo(fileWriter); // in the order of the columns
            }
            fileWriter.endBlock();
        }
        for (Group group : groups) {
            group.release();
        }
    }

    /** Writes the last row group and the footer, which closes the output. */
    void finish() throws IOException {
        endRowGroup();
        fileWriter.end(Map.of());
    }

    @Override
    public void close() {
        // after finish() the stores are gone; after a failure they are still open
        for (Group group : groups) {
            group.release();
        }
        codecs.release();
    }
}
