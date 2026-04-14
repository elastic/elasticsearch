/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.parquet;

import org.apache.lucene.util.BytesRef;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.ColumnReader;
import org.apache.parquet.column.Dictionary;
import org.apache.parquet.column.impl.ColumnReadStoreImpl;
import org.apache.parquet.column.page.DictionaryPage;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.column.page.PageReader;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.internal.column.columnindex.ColumnIndex;
import org.apache.parquet.internal.column.columnindex.OffsetIndex;
import org.apache.parquet.io.api.Converter;
import org.apache.parquet.io.api.GroupConverter;
import org.apache.parquet.io.api.PrimitiveConverter;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.CloseableIterator;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.xpack.esql.core.QlIllegalArgumentException;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReader;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * Optimized Parquet column iterator behind the {@code optimized_reader} feature flag.
 *
 * <p>Stage 0: functionally identical to the baseline {@code ParquetColumnIterator} but structured
 * for progressive enhancement. Uses {@link PreloadedRowGroupMetadata} for preloaded column/offset
 * indexes and dictionary pages.
 *
 * <p>Stage 1: dictionary-aware row group pruning. When a row group has dictionary-encoded predicate
 * columns, evaluates the pushed predicate against dictionary values. Row groups where no dictionary
 * value matches are skipped entirely.
 *
 * <p>Stage 2: page-level skipping via ColumnIndex. Uses preloaded ColumnIndex min/max per data page
 * to evaluate pushed predicates and skip pages that cannot contain matching rows. Produces
 * {@link RowRanges} that represent selected row ranges within a row group. Anti-fragmentation
 * discards the ranges when the selection is too dense or fragmented to benefit from skipping.
 *
 * <p>Stage 3: batch column reader. Flat (non-list) columns are decoded via {@link BatchColumnReader}
 * which uses tight loops with a non-nullable fast path (no def-level check when maxDef == 0) and
 * builds null {@link java.util.BitSet}s directly instead of going through a {@code boolean[]}
 * intermediary. List columns remain row-at-a-time since they require stateful multi-value handling.
 *
 * <p>The existing baseline {@code ParquetColumnIterator} is never modified — it remains as the
 * stable fallback when {@code optimized_reader=false}.
 */
final class OptimizedParquetColumnIterator implements CloseableIterator<Page> {

    private static final Logger logger = LogManager.getLogger(OptimizedParquetColumnIterator.class);

    private static final char[] HEX = "0123456789abcdef".toCharArray();

    private final ParquetFileReader reader;
    private final MessageType projectedSchema;
    private final List<Attribute> attributes;
    private final int batchSize;
    private final BlockFactory blockFactory;
    private final String createdBy;
    private final String fileLocation;
    private final ColumnInfo[] columnInfos;
    private final PreloadedRowGroupMetadata preloadedMetadata;
    private final ParquetPushedExpressions pushedExpressions;
    private final boolean pageLevelReader;
    private int rowBudget;

    private PageReadStore rowGroup;
    private ColumnReader[] columnReaders;
    private PageColumnReader[] pageColumnReaders;
    private long rowsRemainingInGroup;
    private boolean exhausted = false;
    private int rowGroupOrdinal = -1;
    private int pageBatchIndexInRowGroup = 0;
    private long rowGroupsSkippedByDictionary = 0;
    private long pagesSkippedByColumnIndex = 0;
    private long pagesEvaluatedByColumnIndex = 0;

    OptimizedParquetColumnIterator(
        ParquetFileReader reader,
        MessageType projectedSchema,
        List<Attribute> attributes,
        int batchSize,
        BlockFactory blockFactory,
        int rowLimit,
        String createdBy,
        String fileLocation,
        ColumnInfo[] columnInfos,
        PreloadedRowGroupMetadata preloadedMetadata,
        ParquetPushedExpressions pushedExpressions,
        boolean pageLevelReader
    ) {
        this.reader = reader;
        this.projectedSchema = projectedSchema;
        this.attributes = attributes;
        this.batchSize = batchSize;
        this.blockFactory = blockFactory;
        this.rowBudget = rowLimit;
        this.createdBy = createdBy != null ? createdBy : "";
        this.fileLocation = fileLocation;
        this.columnInfos = columnInfos;
        this.preloadedMetadata = preloadedMetadata;
        this.pushedExpressions = pushedExpressions;
        this.pageLevelReader = pageLevelReader;

        reader.setRequestedSchema(projectedSchema);
    }

    @Override
    public boolean hasNext() {
        if (exhausted) {
            return false;
        }
        if (rowBudget != FormatReader.NO_LIMIT && rowBudget <= 0) {
            exhausted = true;
            return false;
        }
        if (rowsRemainingInGroup > 0) {
            return true;
        }
        try {
            return advanceRowGroup();
        } catch (IOException e) {
            throw new ElasticsearchException(
                "Failed to read Parquet row group [" + (rowGroupOrdinal + 1) + "] in file [" + fileLocation + "]: " + e.getMessage(),
                e
            );
        }
    }

    private boolean advanceRowGroup() throws IOException {
        while (true) {
            if (rowGroup != null) {
                rowGroup.close();
                rowGroup = null;
            }
            rowGroup = reader.readNextFilteredRowGroup();
            if (rowGroup == null) {
                exhausted = true;
                if (rowGroupsSkippedByDictionary > 0) {
                    logger.debug("Dictionary pruning skipped [{}] row groups in [{}]", rowGroupsSkippedByDictionary, fileLocation);
                }
                if (pagesEvaluatedByColumnIndex > 0) {
                    logger.debug(
                        "Page-level skipping: evaluated [{}] pages, skipped [{}] in [{}]",
                        pagesEvaluatedByColumnIndex,
                        pagesSkippedByColumnIndex,
                        fileLocation
                    );
                }
                return false;
            }
            rowGroupOrdinal++;
            pageBatchIndexInRowGroup = 0;
            rowsRemainingInGroup = rowGroup.getRowCount();

            if (shouldSkipByDictionary()) {
                rowGroupsSkippedByDictionary++;
                continue;
            }

            RowRanges pageRanges = computePageRowRanges();
            if (pageRanges != null && pageRanges.isEmpty()) {
                logger.trace("Page-level skipping: all pages pruned for row group [{}] in [{}]", rowGroupOrdinal, fileLocation);
                continue;
            }

            if (pageLevelReader) {
                pageColumnReaders = new PageColumnReader[columnInfos.length];
                columnReaders = null;
                for (int i = 0; i < columnInfos.length; i++) {
                    if (columnInfos[i] != null && columnInfos[i].maxRepLevel() == 0) {
                        ColumnDescriptor desc = columnInfos[i].descriptor();
                        PageReader pr = rowGroup.getPageReader(desc);
                        pageColumnReaders[i] = new PageColumnReader(pr, desc, columnInfos[i], pageRanges);
                    }
                }
                // List columns still need ColumnReadStoreImpl
                boolean hasListColumns = false;
                for (int i = 0; i < columnInfos.length; i++) {
                    if (columnInfos[i] != null && columnInfos[i].maxRepLevel() > 0) {
                        hasListColumns = true;
                        break;
                    }
                }
                if (hasListColumns) {
                    ColumnReadStoreImpl store = new ColumnReadStoreImpl(
                        rowGroup,
                        new NoOpGroupConverter(projectedSchema),
                        projectedSchema,
                        createdBy
                    );
                    columnReaders = new ColumnReader[columnInfos.length];
                    for (int i = 0; i < columnInfos.length; i++) {
                        if (columnInfos[i] != null && columnInfos[i].maxRepLevel() > 0) {
                            columnReaders[i] = store.getColumnReader(columnInfos[i].descriptor());
                        }
                    }
                }
            } else {
                pageColumnReaders = null;
                ColumnReadStoreImpl store = new ColumnReadStoreImpl(
                    rowGroup,
                    new NoOpGroupConverter(projectedSchema),
                    projectedSchema,
                    createdBy
                );
                columnReaders = new ColumnReader[columnInfos.length];
                for (int i = 0; i < columnInfos.length; i++) {
                    if (columnInfos[i] != null) {
                        columnReaders[i] = store.getColumnReader(columnInfos[i].descriptor());
                    }
                }
            }
            return rowsRemainingInGroup > 0;
        }
    }

    /**
     * Evaluates pushed predicates against dictionary values for the current row group.
     * If any predicate column is dictionary-encoded and no dictionary value matches,
     * the entire row group can be skipped.
     *
     * @return true if the row group should be skipped (no dictionary values match any predicate)
     */
    private boolean shouldSkipByDictionary() {
        if (pushedExpressions == null) {
            return false;
        }
        List<BlockMetaData> rowGroups = reader.getRowGroups();
        if (rowGroupOrdinal >= rowGroups.size()) {
            return false;
        }
        BlockMetaData block = rowGroups.get(rowGroupOrdinal);

        for (ColumnChunkMetaData col : block.getColumns()) {
            if (col.hasDictionaryPage() == false) {
                continue;
            }
            String columnPath = col.getPath().toDotString();
            if (pushedExpressions.referencesColumn(columnPath) == false) {
                continue;
            }

            DictionaryPage dictPage = readDictionaryPage(col);
            if (dictPage == null) {
                continue;
            }

            try {
                Dictionary dictionary = dictPage.decode(projectedSchema.getColumnDescription(col.getPath().toArray()));
                if (dictionary == null || dictionary.getMaxId() < 0) {
                    continue;
                }

                if (pushedExpressions.allDictionaryValuesRejected(columnPath, dictionary, col.getPrimitiveType())) {
                    logger.trace(
                        "Dictionary pruning: skipping row group [{}] — no dictionary values match for column [{}] in [{}]",
                        rowGroupOrdinal,
                        columnPath,
                        fileLocation
                    );
                    return true;
                }
            } catch (Exception e) {
                logger.debug(
                    "Dictionary evaluation failed for column [{}] in row group [{}] of [{}]: {}",
                    columnPath,
                    rowGroupOrdinal,
                    fileLocation,
                    e.getMessage()
                );
            }
        }
        return false;
    }

    /**
     * Reads the dictionary page for a column chunk from the current row group's page store.
     */
    private DictionaryPage readDictionaryPage(ColumnChunkMetaData col) {
        try {
            PageReader pageReader = rowGroup.getPageReader(projectedSchema.getColumnDescription(col.getPath().toArray()));
            if (pageReader != null) {
                return pageReader.readDictionaryPage();
            }
        } catch (Exception e) {
            logger.debug("Failed to read dictionary page for [{}] in row group [{}]: {}", col.getPath(), rowGroupOrdinal, e.getMessage());
        }
        return null;
    }

    /**
     * Computes page-level row ranges by evaluating pushed predicates against ColumnIndex min/max
     * for each data page. Returns null if page-level skipping is not applicable (no predicates,
     * no column indexes, or anti-fragmentation triggered). Returns an empty RowRanges if all
     * pages are pruned (row group can be skipped entirely).
     *
     * <p>For each predicate column with a ColumnIndex and OffsetIndex, evaluates each page's
     * min/max against the pushed expressions. Matching pages are converted to row ranges using
     * the OffsetIndex's firstRowIndex. Ranges from different predicate columns are intersected
     * (a row must satisfy ALL predicates).
     */
    private RowRanges computePageRowRanges() {
        if (pushedExpressions == null || preloadedMetadata.hasColumnIndexes() == false || preloadedMetadata.hasOffsetIndexes() == false) {
            return null;
        }

        List<BlockMetaData> rowGroups = reader.getRowGroups();
        if (rowGroupOrdinal >= rowGroups.size()) {
            return null;
        }
        BlockMetaData block = rowGroups.get(rowGroupOrdinal);
        long rowGroupRowCount = block.getRowCount();

        RowRanges combined = null;

        for (ColumnChunkMetaData col : block.getColumns()) {
            String columnPath = col.getPath().toDotString();
            if (pushedExpressions.referencesColumn(columnPath) == false) {
                continue;
            }

            ColumnIndex columnIndex = preloadedMetadata.getColumnIndex(rowGroupOrdinal, columnPath);
            OffsetIndex offsetIndex = preloadedMetadata.getOffsetIndex(rowGroupOrdinal, columnPath);
            if (columnIndex == null || offsetIndex == null) {
                continue;
            }

            int pageCount = offsetIndex.getPageCount();
            if (pageCount == 0) {
                continue;
            }

            List<long[]> matchingRanges = new ArrayList<>();
            for (int pageIdx = 0; pageIdx < pageCount; pageIdx++) {
                pagesEvaluatedByColumnIndex++;
                boolean canMatch = pushedExpressions.pageCanMatch(columnPath, columnIndex, pageIdx, col.getPrimitiveType());
                if (canMatch) {
                    long pageStart = offsetIndex.getFirstRowIndex(pageIdx);
                    long pageEnd = (pageIdx + 1 < pageCount) ? offsetIndex.getFirstRowIndex(pageIdx + 1) : rowGroupRowCount;
                    matchingRanges.add(new long[] { pageStart, pageEnd });
                } else {
                    pagesSkippedByColumnIndex++;
                }
            }

            RowRanges columnRanges;
            if (matchingRanges.isEmpty()) {
                columnRanges = RowRanges.of(0, 0, rowGroupRowCount);
            } else if (matchingRanges.size() == pageCount) {
                columnRanges = RowRanges.all(rowGroupRowCount);
            } else {
                columnRanges = RowRanges.fromUnsorted(matchingRanges, rowGroupRowCount);
            }

            combined = (combined == null) ? columnRanges : combined.intersect(columnRanges);

            if (combined.isEmpty()) {
                return combined;
            }
        }

        if (combined == null) {
            return null;
        }

        if (combined.shouldDiscard()) {
            logger.trace(
                "Page-level skipping: anti-fragmentation triggered for row group [{}] in [{}] "
                    + "(density={}, transitions={}, selected={}/{})",
                rowGroupOrdinal,
                fileLocation,
                combined.density(),
                combined.transitionCount(),
                combined.selectedRowCount(),
                rowGroupRowCount
            );
            return null;
        }

        return combined;
    }

    @Override
    public Page next() {
        if (hasNext() == false) {
            throw new NoSuchElementException();
        }
        int effectiveBatch = batchSize;
        if (rowBudget != FormatReader.NO_LIMIT) {
            effectiveBatch = Math.min(effectiveBatch, rowBudget);
        }
        int rowsToRead = (int) Math.min(effectiveBatch, rowsRemainingInGroup);

        Block[] blocks = new Block[attributes.size()];
        try {
            for (int col = 0; col < columnInfos.length; col++) {
                ColumnInfo info = columnInfos[col];
                if (info == null) {
                    blocks[col] = blockFactory.newConstantNullBlock(rowsToRead);
                } else {
                    try {
                        blocks[col] = readColumnBlock(col, info, rowsToRead);
                    } catch (Exception e) {
                        Releasables.closeExpectNoException(blocks);
                        Attribute attr = attributes.get(col);
                        throw new ElasticsearchException(
                            "Failed to read Parquet column ["
                                + attr.name()
                                + "] (type "
                                + attr.dataType()
                                + ") at row group ["
                                + (rowGroupOrdinal + 1)
                                + "] page batch ["
                                + pageBatchIndexInRowGroup
                                + "] in file ["
                                + fileLocation
                                + "]: "
                                + e.getMessage(),
                            e
                        );
                    }
                }
            }
        } catch (ElasticsearchException e) {
            throw e;
        } catch (Exception e) {
            Releasables.closeExpectNoException(blocks);
            throw new ElasticsearchException(
                "Failed to create Page batch at row group ["
                    + (rowGroupOrdinal + 1)
                    + "] page batch ["
                    + pageBatchIndexInRowGroup
                    + "] in file ["
                    + fileLocation
                    + "]: "
                    + e.getMessage(),
                e
            );
        }

        pageBatchIndexInRowGroup++;
        rowsRemainingInGroup -= rowsToRead;
        if (rowBudget != FormatReader.NO_LIMIT) {
            rowBudget -= rowsToRead;
        }
        return new Page(blocks);
    }

    private Block readColumnBlock(int colIndex, ColumnInfo info, int rowsToRead) {
        if (pageColumnReaders != null && pageColumnReaders[colIndex] != null) {
            return pageColumnReaders[colIndex].readBatch(rowsToRead, blockFactory);
        }
        ColumnReader cr = columnReaders != null ? columnReaders[colIndex] : null;
        if (cr == null) {
            return blockFactory.newConstantNullBlock(rowsToRead);
        }
        Block batch = BatchColumnReader.readBatch(cr, info, rowsToRead, blockFactory);
        if (batch != null) {
            return batch;
        }
        if (info.maxRepLevel() > 0) {
            return readListColumn(cr, info, rowsToRead);
        }
        skipValues(cr, rowsToRead);
        return blockFactory.newConstantNullBlock(rowsToRead);
    }

    // --- List column readers (not batch-optimized: require stateful multi-value handling) ---

    private Block readListColumn(ColumnReader cr, ColumnInfo info, int rows) {
        DataType elementType = info.esqlType();
        int maxDef = info.maxDefLevel();
        return switch (elementType) {
            case INTEGER -> readListIntColumn(cr, maxDef, rows);
            case LONG -> readListLongColumn(cr, maxDef, rows);
            case DOUBLE -> readListDoubleColumn(cr, maxDef, rows);
            case BOOLEAN -> readListBooleanColumn(cr, maxDef, rows);
            case KEYWORD, TEXT -> readListBytesRefColumn(cr, maxDef, rows);
            case DATETIME -> readListDatetimeColumn(cr, info, rows);
            default -> {
                skipListValues(cr, maxDef, rows);
                yield blockFactory.newConstantNullBlock(rows);
            }
        };
    }

    private static void skipListValues(ColumnReader cr, int maxDef, int rows) {
        for (int row = 0; row < rows; row++) {
            cr.consume();
            while (cr.getCurrentRepetitionLevel() > 0) {
                cr.consume();
            }
        }
    }

    private Block readListIntColumn(ColumnReader cr, int maxDef, int rows) {
        try (var builder = blockFactory.newIntBlockBuilder(rows)) {
            for (int row = 0; row < rows; row++) {
                readListRow(cr, maxDef, builder, () -> builder.appendInt(cr.getInteger()));
            }
            return builder.build();
        }
    }

    private Block readListLongColumn(ColumnReader cr, int maxDef, int rows) {
        try (var builder = blockFactory.newLongBlockBuilder(rows)) {
            for (int row = 0; row < rows; row++) {
                readListRow(cr, maxDef, builder, () -> builder.appendLong(cr.getLong()));
            }
            return builder.build();
        }
    }

    private Block readListDoubleColumn(ColumnReader cr, int maxDef, int rows) {
        try (var builder = blockFactory.newDoubleBlockBuilder(rows)) {
            for (int row = 0; row < rows; row++) {
                readListRow(cr, maxDef, builder, () -> builder.appendDouble(cr.getDouble()));
            }
            return builder.build();
        }
    }

    private Block readListBooleanColumn(ColumnReader cr, int maxDef, int rows) {
        try (var builder = blockFactory.newBooleanBlockBuilder(rows)) {
            for (int row = 0; row < rows; row++) {
                readListRow(cr, maxDef, builder, () -> builder.appendBoolean(cr.getBoolean()));
            }
            return builder.build();
        }
    }

    private Block readListBytesRefColumn(ColumnReader cr, int maxDef, int rows) {
        try (var builder = blockFactory.newBytesRefBlockBuilder(rows)) {
            for (int row = 0; row < rows; row++) {
                readListRow(cr, maxDef, builder, () -> builder.appendBytesRef(new BytesRef(cr.getBinary().getBytes())));
            }
            return builder.build();
        }
    }

    private Block readListDatetimeColumn(ColumnReader cr, ColumnInfo info, int rows) {
        try (var builder = blockFactory.newLongBlockBuilder(rows)) {
            int maxDef = info.maxDefLevel();
            for (int row = 0; row < rows; row++) {
                readListRow(cr, maxDef, builder, () -> builder.appendLong(convertTimestampToMillis(cr.getLong(), info.logicalType())));
            }
            return builder.build();
        }
    }

    @FunctionalInterface
    private interface ValueAppender {
        void append();
    }

    private static void readListRow(ColumnReader cr, int maxDef, Block.Builder builder, ValueAppender appender) {
        int def = cr.getCurrentDefinitionLevel();
        if (def >= maxDef) {
            builder.beginPositionEntry();
            appender.append();
            cr.consume();
            while (cr.getCurrentRepetitionLevel() > 0) {
                if (cr.getCurrentDefinitionLevel() >= maxDef) {
                    appender.append();
                }
                cr.consume();
            }
            builder.endPositionEntry();
        } else {
            cr.consume();
            boolean hasValues = false;
            while (cr.getCurrentRepetitionLevel() > 0) {
                if (cr.getCurrentDefinitionLevel() >= maxDef) {
                    if (hasValues == false) {
                        builder.beginPositionEntry();
                        hasValues = true;
                    }
                    appender.append();
                }
                cr.consume();
            }
            if (hasValues) {
                builder.endPositionEntry();
            } else {
                builder.appendNull();
            }
        }
    }

    // --- Utilities ---

    private static long convertTimestampToMillis(long raw, LogicalTypeAnnotation logicalType) {
        if (logicalType instanceof LogicalTypeAnnotation.TimestampLogicalTypeAnnotation ts) {
            return switch (ts.getUnit()) {
                case MILLIS -> raw;
                case MICROS -> raw / 1_000;
                case NANOS -> raw / 1_000_000;
            };
        }
        return raw;
    }

    private static void skipValues(ColumnReader cr, int rows) {
        for (int i = 0; i < rows; i++) {
            cr.consume();
        }
    }

    static String formatUuid(byte[] bytes) {
        if (bytes == null || bytes.length < 16) {
            throw new QlIllegalArgumentException("UUID requires 16 bytes, got " + (bytes == null ? "null" : bytes.length));
        }
        StringBuilder sb = new StringBuilder(36);
        for (int i = 0; i < 16; i++) {
            sb.append(HEX[(bytes[i] >> 4) & 0xF]);
            sb.append(HEX[bytes[i] & 0xF]);
            if (i == 3 || i == 5 || i == 7 || i == 9) {
                sb.append('-');
            }
        }
        return sb.toString();
    }

    @Override
    public void close() throws IOException {
        try {
            if (rowGroup != null) {
                rowGroup.close();
            }
        } finally {
            reader.close();
        }
    }

    /**
     * Minimal GroupConverter that satisfies {@link ColumnReadStoreImpl}'s constructor.
     */
    private static class NoOpGroupConverter extends GroupConverter {
        private final GroupType schema;

        NoOpGroupConverter(GroupType schema) {
            this.schema = schema;
        }

        @Override
        public Converter getConverter(int fieldIndex) {
            Type field = schema.getType(fieldIndex);
            return field.isPrimitive() ? new NoOpPrimitiveConverter() : new NoOpGroupConverter(field.asGroupType());
        }

        @Override
        public void start() {}

        @Override
        public void end() {}
    }

    private static class NoOpPrimitiveConverter extends PrimitiveConverter {}
}
