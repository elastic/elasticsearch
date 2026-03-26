/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.orc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ListColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.TimestampColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.io.sarg.PredicateLeaf;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument;
import org.apache.orc.ColumnStatistics;
import org.apache.orc.DoubleColumnStatistics;
import org.apache.orc.IntegerColumnStatistics;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.apache.orc.RecordReader;
import org.apache.orc.StringColumnStatistics;
import org.apache.orc.TypeDescription;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.esql.core.QlIllegalArgumentException;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.util.Check;
import org.elasticsearch.xpack.esql.datasources.CloseableIterator;
import org.elasticsearch.xpack.esql.datasources.spi.ColumnBlockConversions;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReadContext;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReader;
import org.elasticsearch.xpack.esql.datasources.spi.SimpleSourceMetadata;
import org.elasticsearch.xpack.esql.datasources.spi.SourceMetadata;
import org.elasticsearch.xpack.esql.datasources.spi.SourceStatistics;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.OptionalLong;

/**
 * {@link FormatReader} implementation for Apache ORC files.
 *
 * <p>Uses ORC's vectorized reader ({@link VectorizedRowBatch}) which maps naturally to
 * ESQL's columnar {@link Block}/{@link Page} model. Each ORC {@link ColumnVector} is
 * converted directly to the corresponding ESQL Block type.
 *
 * <p>Key features:
 * <ul>
 *   <li>Works with any StorageProvider (HTTP, S3, local) via {@link OrcStorageObjectAdapter}</li>
 *   <li>Efficient columnar reading with column projection</li>
 *   <li>Direct conversion from ORC VectorizedRowBatch to ESQL Page</li>
 * </ul>
 */
public class OrcFormatReader implements FormatReader {

    private static final long MILLIS_PER_DAY = Duration.ofDays(1).toMillis();

    private final BlockFactory blockFactory;
    private final SearchArgument pushedFilter;

    public OrcFormatReader(BlockFactory blockFactory) {
        this(blockFactory, null);
    }

    private OrcFormatReader(BlockFactory blockFactory, SearchArgument pushedFilter) {
        this.blockFactory = blockFactory;
        this.pushedFilter = pushedFilter;
    }

    @Override
    public FormatReader withPushedFilter(Object pushedFilter) {
        if (pushedFilter == null) {
            return this;
        }
        return new OrcFormatReader(this.blockFactory, (SearchArgument) pushedFilter);
    }

    @Override
    public SourceMetadata metadata(StorageObject object) throws IOException {
        OrcStorageObjectAdapter fs = new OrcStorageObjectAdapter(object);
        Path path = new Path(object.path().toString());
        OrcFile.ReaderOptions options = orcReaderOptions(fs);
        try (Reader reader = OrcFile.createReader(path, options)) {
            TypeDescription schema = reader.getSchema();
            List<Attribute> attributes = convertOrcSchemaToAttributes(schema);
            SourceStatistics statistics = extractStatistics(reader, schema);
            return new SimpleSourceMetadata(attributes, formatName(), object.path().toString(), statistics, null);
        }
    }

    /**
     * Creates ORC reader options with consistent configuration.
     * <p>
     * Sets {@code useUTCTimestamp(true)} which is required for correct timestamp predicate
     * pushdown. This is a reader-level flag (not per-column) that controls how SargApplier
     * reads timestamp column statistics: {@code true} uses getMinimumUTC/getMaximumUTC,
     * {@code false} uses getMinimum/getMaximum which applies a local-timezone shift. Without
     * it, predicates against TIMESTAMP_INSTANT columns cause false stripe exclusions.
     * <p>
     * This is safe because ESQL maps all date types to DATETIME using UTC epoch millis,
     * and the ORC files use TIMESTAMP_INSTANT (UTC-anchored) columns. If we ever support
     * files with plain TIMESTAMP columns (writer-local timezone), this flag would incorrectly
     * treat their statistics as UTC too — at that point we'd need per-column evaluation by
     * bypassing SearchArgument and reading stripe statistics directly (the Trino approach).
     */
    private static OrcFile.ReaderOptions orcReaderOptions(OrcStorageObjectAdapter fs) {
        return OrcFile.readerOptions(new Configuration(false)).filesystem(fs).useUTCTimestamp(true);
    }

    private static SourceStatistics extractStatistics(Reader reader, TypeDescription schema) {
        long rowCount = reader.getNumberOfRows();
        long sizeInBytes = reader.getContentLength();
        ColumnStatistics[] orcStats = reader.getStatistics();
        List<String> fieldNames = schema.getFieldNames();
        List<TypeDescription> children = schema.getChildren();

        Map<String, SourceStatistics.ColumnStatistics> columnStats = new HashMap<>();
        for (int i = 0; i < fieldNames.size(); i++) {
            String name = fieldNames.get(i);
            int colId = children.get(i).getId();
            if (colId >= orcStats.length) {
                continue;
            }
            ColumnStatistics cs = orcStats[colId];
            long totalValues = cs.getNumberOfValues();
            long nullCount = rowCount - totalValues;
            Object minVal = extractOrcMin(cs);
            Object maxVal = extractOrcMax(cs);

            columnStats.put(name, new SourceStatistics.ColumnStatistics() {
                @Override
                public OptionalLong nullCount() {
                    return OptionalLong.of(nullCount);
                }

                @Override
                public OptionalLong distinctCount() {
                    return OptionalLong.empty();
                }

                @Override
                public Optional<Object> minValue() {
                    return Optional.ofNullable(minVal);
                }

                @Override
                public Optional<Object> maxValue() {
                    return Optional.ofNullable(maxVal);
                }
            });
        }

        return new SourceStatistics() {
            @Override
            public OptionalLong rowCount() {
                return OptionalLong.of(rowCount);
            }

            @Override
            public OptionalLong sizeInBytes() {
                return OptionalLong.of(sizeInBytes);
            }

            @Override
            public Optional<Map<String, SourceStatistics.ColumnStatistics>> columnStatistics() {
                return columnStats.isEmpty() ? Optional.empty() : Optional.of(columnStats);
            }
        };
    }

    private static Object extractOrcMin(ColumnStatistics cs) {
        if (cs instanceof IntegerColumnStatistics intStats) {
            return intStats.getMinimum();
        } else if (cs instanceof DoubleColumnStatistics dblStats) {
            return dblStats.getMinimum();
        } else if (cs instanceof StringColumnStatistics strStats) {
            return strStats.getMinimum();
        }
        return null;
    }

    private static Object extractOrcMax(ColumnStatistics cs) {
        if (cs instanceof IntegerColumnStatistics intStats) {
            return intStats.getMaximum();
        } else if (cs instanceof DoubleColumnStatistics dblStats) {
            return dblStats.getMaximum();
        } else if (cs instanceof StringColumnStatistics strStats) {
            return strStats.getMaximum();
        }
        return null;
    }

    @Override
    public CloseableIterator<Page> read(StorageObject object, FormatReadContext context) throws IOException {
        List<String> projectedColumns = context.projectedColumns();
        int batchSize = context.batchSize();
        int rowLimit = context.rowLimit();

        OrcStorageObjectAdapter fs = new OrcStorageObjectAdapter(object);
        Path path = new Path(object.path().toString());
        OrcFile.ReaderOptions readerOptions = orcReaderOptions(fs);
        Reader reader = OrcFile.createReader(path, readerOptions);

        TypeDescription schema = reader.getSchema();
        List<Attribute> attributes = convertOrcSchemaToAttributes(schema);

        List<Attribute> projectedAttributes;
        boolean[] include = null;
        if (projectedColumns == null || projectedColumns.isEmpty()) {
            projectedAttributes = attributes;
        } else {
            projectedAttributes = new ArrayList<>();
            Map<String, Integer> nameToIndex = new HashMap<>();
            List<String> fieldNames = schema.getFieldNames();
            for (int i = 0; i < fieldNames.size(); i++) {
                nameToIndex.put(fieldNames.get(i), i);
            }
            // ORC include array: index 0 is the root struct, then 1..N for each field
            include = new boolean[schema.getMaximumId() + 1];
            include[0] = true;
            Map<String, Attribute> attributeMap = new HashMap<>();
            for (Attribute attr : attributes) {
                attributeMap.put(attr.name(), attr);
            }
            for (String columnName : projectedColumns) {
                Attribute attr = attributeMap.get(columnName);
                if (attr != null) {
                    Integer idx = nameToIndex.get(columnName);
                    if (idx != null) {
                        TypeDescription child = schema.getChildren().get(idx);
                        includeColumnForType(include, child);
                    }
                } else {
                    attr = new ReferenceAttribute(Source.EMPTY, columnName, DataType.NULL);
                }
                projectedAttributes.add(attr);
            }
        }

        Reader.Options readOptions = reader.options().rowBatchSize(batchSize);
        if (include != null) {
            readOptions.include(include);
        }
        if (pushedFilter != null) {
            List<PredicateLeaf> leaves = pushedFilter.getLeaves();
            LinkedHashSet<String> nameSet = new LinkedHashSet<>(leaves.size());
            for (PredicateLeaf leaf : leaves) {
                nameSet.add(leaf.getColumnName());
            }
            readOptions.searchArgument(pushedFilter, nameSet.toArray(new String[0]));
        }
        RecordReader rows = reader.rows(readOptions);

        CloseableIterator<Page> iter = new OrcPageIterator(reader, rows, schema, projectedAttributes, batchSize, blockFactory);
        return rowLimit != NO_LIMIT ? new RowLimitingIterator(iter, rowLimit) : iter;
    }

    /**
     * Include a column and its children in the ORC read mask.
     * For LIST and MAP types, recurses into children so list/map element data is read.
     * For scalars and other types, only the type itself is included.
     */
    private static void includeColumnForType(boolean[] include, TypeDescription type) {
        include[type.getId()] = true;
        var category = type.getCategory();
        if (category == TypeDescription.Category.LIST || category == TypeDescription.Category.MAP) {
            for (TypeDescription child : type.getChildren()) {
                includeColumnForType(include, child);
            }
        }
    }

    @Override
    public String formatName() {
        return "orc";
    }

    @Override
    public List<String> fileExtensions() {
        return List.of(".orc");
    }

    @Override
    public void close() throws IOException {
        // No resources to close at the reader level
    }

    private static List<Attribute> convertOrcSchemaToAttributes(TypeDescription schema) {
        List<Attribute> attributes = new ArrayList<>();
        List<String> fieldNames = schema.getFieldNames();
        List<TypeDescription> children = schema.getChildren();
        for (int i = 0; i < fieldNames.size(); i++) {
            String name = fieldNames.get(i);
            DataType esqlType = convertOrcTypeToEsql(children.get(i));
            attributes.add(new ReferenceAttribute(Source.EMPTY, name, esqlType));
        }
        return attributes;
    }

    private static DataType convertOrcTypeToEsql(TypeDescription orcType) {
        return switch (orcType.getCategory()) {
            case BOOLEAN -> DataType.BOOLEAN;
            case BYTE, SHORT, INT -> DataType.INTEGER;
            case LONG -> DataType.LONG;
            case FLOAT, DOUBLE -> DataType.DOUBLE;
            case STRING -> DataType.TEXT;
            case VARCHAR, CHAR -> DataType.KEYWORD;
            case BINARY -> DataType.KEYWORD;
            case TIMESTAMP, TIMESTAMP_INSTANT, DATE -> DataType.DATETIME;
            case DECIMAL -> DataType.DOUBLE;
            case LIST -> convertOrcTypeToEsql(orcType.getChildren().get(0));
            default -> DataType.UNSUPPORTED;
        };
    }

    private static class OrcPageIterator implements CloseableIterator<Page> {
        private final Reader reader;
        private final RecordReader rows;
        private final List<Attribute> attributes;
        private final BlockFactory blockFactory;
        private final VectorizedRowBatch batch;
        private boolean exhausted = false;
        private boolean batchReady = false;
        private final Map<String, Integer> fieldNameToIndex;

        OrcPageIterator(
            Reader reader,
            RecordReader rows,
            TypeDescription schema,
            List<Attribute> attributes,
            int batchSize,
            BlockFactory blockFactory
        ) {
            this.reader = reader;
            this.rows = rows;
            this.attributes = attributes;
            this.blockFactory = blockFactory;
            this.batch = schema.createRowBatch(batchSize);

            fieldNameToIndex = new HashMap<>(schema.getFieldNames().size());
            int i = 0;
            for (var fieldName : schema.getFieldNames()) {
                fieldNameToIndex.put(fieldName, i++);
            }
        }

        @Override
        public boolean hasNext() {
            if (exhausted) {
                return false;
            }
            if (batchReady) {
                return true;
            }
            try {
                if (rows.nextBatch(batch)) {
                    batchReady = true;
                    return true;
                } else {
                    exhausted = true;
                    return false;
                }
            } catch (IOException e) {
                throw new RuntimeException("Failed to read ORC batch", e);
            }
        }

        @Override
        public Page next() {
            if (hasNext() == false) {
                throw new NoSuchElementException();
            }
            batchReady = false;
            return convertToPage();
        }

        private Page convertToPage() {
            int rowCount = batch.size;
            Block[] blocks = new Block[attributes.size()];

            for (int col = 0; col < attributes.size(); col++) {
                Attribute attribute = attributes.get(col);
                String fieldName = attribute.name();
                DataType dataType = attribute.dataType();

                try {
                    var fieldIndex = fieldNameToIndex.get(fieldName);
                    if (fieldIndex == null) {
                        blocks[col] = blockFactory.newConstantNullBlock(rowCount);
                    } else {
                        ColumnVector vector = batch.cols[fieldIndex];
                        blocks[col] = createBlock(vector, dataType, rowCount);
                    }
                } catch (Exception e) {
                    Releasables.closeExpectNoException(blocks);
                    throw e;
                }
            }

            return new Page(blocks);
        }

        private Block createBlock(ColumnVector vector, DataType dataType, int rowCount) {
            if (vector instanceof ListColumnVector listCol) {
                return createListBlock(listCol, dataType, rowCount);
            }
            return switch (dataType) {
                case BOOLEAN -> ColumnBlockConversions.booleanColumnFromLongs(
                    blockFactory,
                    ((LongColumnVector) vector).vector,
                    rowCount,
                    vector.noNulls,
                    vector.isRepeating,
                    vector.isNull
                );
                case INTEGER -> ColumnBlockConversions.intColumnFromLongs(
                    blockFactory,
                    ((LongColumnVector) vector).vector,
                    rowCount,
                    vector.noNulls,
                    vector.isRepeating,
                    vector.isNull
                );
                case LONG -> ColumnBlockConversions.longColumn(
                    blockFactory,
                    ((LongColumnVector) vector).vector,
                    rowCount,
                    vector.noNulls,
                    vector.isRepeating,
                    vector.isNull
                );
                case DOUBLE -> createDoubleBlock(vector, rowCount);
                case KEYWORD, TEXT -> createBytesRefBlock(vector, rowCount);
                case DATETIME -> createDatetimeBlock(vector, rowCount);
                default -> blockFactory.newConstantNullBlock(rowCount);
            };
        }

        private Block createListBlock(ListColumnVector listCol, DataType elementType, int rowCount) {
            return switch (elementType) {
                case KEYWORD, TEXT -> createListBytesRefBlock(listCol, rowCount);
                case INTEGER -> createListIntBlock(listCol, rowCount);
                case LONG -> createListLongBlock(listCol, rowCount);
                case DOUBLE -> createListDoubleBlock(listCol, rowCount);
                case BOOLEAN -> createListBooleanBlock(listCol, rowCount);
                case DATETIME -> createListDatetimeBlock(listCol, rowCount);
                default -> blockFactory.newConstantNullBlock(rowCount);
            };
        }

        private Block createListBytesRefBlock(ListColumnVector listCol, int rowCount) {
            BytesColumnVector child = (BytesColumnVector) listCol.child;
            try (var builder = blockFactory.newBytesRefBlockBuilder(rowCount)) {
                for (int i = 0; i < rowCount; i++) {
                    if (listCol.noNulls == false && listCol.isNull[i]) {
                        builder.appendNull();
                    } else {
                        int start = (int) listCol.offsets[i];
                        int len = (int) listCol.lengths[i];
                        builder.beginPositionEntry();
                        for (int j = 0; j < len; j++) {
                            int idx = start + j;
                            if (child.noNulls == false && child.isNull[idx]) {
                                builder.appendBytesRef(new org.apache.lucene.util.BytesRef());
                            } else {
                                builder.appendBytesRef(
                                    new org.apache.lucene.util.BytesRef(child.vector[idx], child.start[idx], child.length[idx])
                                );
                            }
                        }
                        builder.endPositionEntry();
                    }
                }
                return builder.build();
            }
        }

        private Block createListIntBlock(ListColumnVector listCol, int rowCount) {
            LongColumnVector child = (LongColumnVector) listCol.child;
            try (var builder = blockFactory.newIntBlockBuilder(rowCount)) {
                for (int i = 0; i < rowCount; i++) {
                    if (listCol.noNulls == false && listCol.isNull[i]) {
                        builder.appendNull();
                    } else {
                        int start = (int) listCol.offsets[i];
                        int len = (int) listCol.lengths[i];
                        builder.beginPositionEntry();
                        for (int j = 0; j < len; j++) {
                            int idx = start + j;
                            if (child.noNulls == false && child.isNull[idx]) {
                                builder.appendInt(0);
                            } else {
                                builder.appendInt((int) child.vector[idx]);
                            }
                        }
                        builder.endPositionEntry();
                    }
                }
                return builder.build();
            }
        }

        private Block createListLongBlock(ListColumnVector listCol, int rowCount) {
            LongColumnVector child = (LongColumnVector) listCol.child;
            try (var builder = blockFactory.newLongBlockBuilder(rowCount)) {
                for (int i = 0; i < rowCount; i++) {
                    if (listCol.noNulls == false && listCol.isNull[i]) {
                        builder.appendNull();
                    } else {
                        int start = (int) listCol.offsets[i];
                        int len = (int) listCol.lengths[i];
                        builder.beginPositionEntry();
                        for (int j = 0; j < len; j++) {
                            int idx = start + j;
                            if (child.noNulls == false && child.isNull[idx]) {
                                builder.appendLong(0L);
                            } else {
                                builder.appendLong(child.vector[idx]);
                            }
                        }
                        builder.endPositionEntry();
                    }
                }
                return builder.build();
            }
        }

        private Block createListDoubleBlock(ListColumnVector listCol, int rowCount) {
            DoubleColumnVector child = (DoubleColumnVector) listCol.child;
            try (var builder = blockFactory.newDoubleBlockBuilder(rowCount)) {
                for (int i = 0; i < rowCount; i++) {
                    if (listCol.noNulls == false && listCol.isNull[i]) {
                        builder.appendNull();
                    } else {
                        int start = (int) listCol.offsets[i];
                        int len = (int) listCol.lengths[i];
                        builder.beginPositionEntry();
                        for (int j = 0; j < len; j++) {
                            int idx = start + j;
                            if (child.noNulls == false && child.isNull[idx]) {
                                builder.appendDouble(0.0);
                            } else {
                                builder.appendDouble(child.vector[idx]);
                            }
                        }
                        builder.endPositionEntry();
                    }
                }
                return builder.build();
            }
        }

        private Block createListBooleanBlock(ListColumnVector listCol, int rowCount) {
            LongColumnVector child = (LongColumnVector) listCol.child;
            try (var builder = blockFactory.newBooleanBlockBuilder(rowCount)) {
                for (int i = 0; i < rowCount; i++) {
                    if (listCol.noNulls == false && listCol.isNull[i]) {
                        builder.appendNull();
                    } else {
                        int start = (int) listCol.offsets[i];
                        int len = (int) listCol.lengths[i];
                        builder.beginPositionEntry();
                        for (int j = 0; j < len; j++) {
                            int idx = start + j;
                            if (child.noNulls == false && child.isNull[idx]) {
                                builder.appendBoolean(false);
                            } else {
                                builder.appendBoolean(child.vector[idx] != 0);
                            }
                        }
                        builder.endPositionEntry();
                    }
                }
                return builder.build();
            }
        }

        private Block createListDatetimeBlock(ListColumnVector listCol, int rowCount) {
            ColumnVector child = listCol.child;
            try (var builder = blockFactory.newLongBlockBuilder(rowCount)) {
                for (int i = 0; i < rowCount; i++) {
                    if (listCol.noNulls == false && listCol.isNull[i]) {
                        builder.appendNull();
                    } else {
                        int start = (int) listCol.offsets[i];
                        int len = (int) listCol.lengths[i];
                        builder.beginPositionEntry();
                        for (int j = 0; j < len; j++) {
                            int idx = start + j;
                            long millis;
                            if (child instanceof TimestampColumnVector ts) {
                                if (ts.noNulls == false && ts.isNull[idx]) {
                                    millis = 0L;
                                } else {
                                    millis = ts.getTime(idx);
                                }
                            } else if (child instanceof LongColumnVector lv) {
                                if (lv.noNulls == false && lv.isNull[idx]) {
                                    millis = 0L;
                                } else {
                                    millis = lv.vector[idx] * MILLIS_PER_DAY;
                                }
                            } else {
                                millis = 0L;
                            }
                            builder.appendLong(millis);
                        }
                        builder.endPositionEntry();
                    }
                }
                return builder.build();
            }
        }

        private Block createDoubleBlock(ColumnVector vector, int rowCount) {
            if (vector instanceof DoubleColumnVector doubleVector) {
                return ColumnBlockConversions.doubleColumn(
                    blockFactory,
                    doubleVector.vector,
                    rowCount,
                    doubleVector.noNulls,
                    doubleVector.isRepeating,
                    doubleVector.isNull
                );
            } else if (vector instanceof LongColumnVector longVector) {
                return ColumnBlockConversions.doubleColumnFromLongs(
                    blockFactory,
                    longVector.vector,
                    rowCount,
                    longVector.noNulls,
                    longVector.isRepeating,
                    longVector.isNull
                );
            }
            throw new QlIllegalArgumentException("Unsupported column type: " + vector.getClass().getSimpleName());
        }

        private Block createBytesRefBlock(ColumnVector vector, int rowCount) {
            Check.isTrue(vector instanceof BytesColumnVector, "Unsupported column type: " + vector.getClass().getSimpleName());
            BytesColumnVector bytesVector = (BytesColumnVector) vector;
            if (bytesVector.isRepeating) {
                if (bytesVector.noNulls == false && bytesVector.isNull[0]) {
                    return blockFactory.newConstantNullBlock(rowCount);
                }
                return blockFactory.newConstantBytesRefBlockWith(
                    new org.apache.lucene.util.BytesRef(bytesVector.vector[0], bytesVector.start[0], bytesVector.length[0]),
                    rowCount
                );
            }
            try (var builder = blockFactory.newBytesRefBlockBuilder(rowCount)) {
                for (int i = 0; i < rowCount; i++) {
                    if (bytesVector.noNulls == false && bytesVector.isNull[i]) {
                        builder.appendNull();
                    } else {
                        builder.appendBytesRef(
                            new org.apache.lucene.util.BytesRef(bytesVector.vector[i], bytesVector.start[i], bytesVector.length[i])
                        );
                    }
                }
                return builder.build();
            }
        }

        private Block createDatetimeBlock(ColumnVector vector, int rowCount) {
            if (vector instanceof TimestampColumnVector tsVector) {
                if (tsVector.isRepeating) {
                    if (tsVector.noNulls == false && tsVector.isNull[0]) {
                        return blockFactory.newConstantNullBlock(rowCount);
                    }
                    return blockFactory.newConstantLongBlockWith(tsVector.getTime(0), rowCount);
                }
                long[] millis = new long[rowCount];
                for (int i = 0; i < rowCount; i++) {
                    millis[i] = tsVector.getTime(i);
                }
                if (tsVector.noNulls) {
                    return blockFactory.newLongArrayVector(millis, rowCount).asBlock();
                }
                return blockFactory.newLongArrayBlock(
                    millis,
                    rowCount,
                    null,
                    toBitSet(tsVector.isNull, rowCount),
                    Block.MvOrdering.UNORDERED
                );
            } else if (vector instanceof LongColumnVector longVector) {
                if (longVector.isRepeating) {
                    if (longVector.noNulls == false && longVector.isNull[0]) {
                        return blockFactory.newConstantNullBlock(rowCount);
                    }
                    return blockFactory.newConstantLongBlockWith(longVector.vector[0] * MILLIS_PER_DAY, rowCount);
                }
                long[] millis = new long[rowCount];
                for (int i = 0; i < rowCount; i++) {
                    millis[i] = longVector.vector[i] * MILLIS_PER_DAY;
                }
                if (longVector.noNulls) {
                    return blockFactory.newLongArrayVector(millis, rowCount).asBlock();
                }
                return blockFactory.newLongArrayBlock(
                    millis,
                    rowCount,
                    null,
                    toBitSet(longVector.isNull, rowCount),
                    Block.MvOrdering.UNORDERED
                );
            }
            return blockFactory.newConstantNullBlock(rowCount);
        }

        private static BitSet toBitSet(boolean[] isNull, int length) {
            BitSet bits = new BitSet(length);
            for (int i = 0; i < length; i++) {
                if (isNull[i]) {
                    bits.set(i);
                }
            }
            return bits;
        }

        @Override
        public void close() throws IOException {
            try {
                rows.close();
            } finally {
                reader.close();
            }
        }
    }

    private static class RowLimitingIterator implements CloseableIterator<Page> {
        private final CloseableIterator<Page> delegate;
        private int remaining;

        RowLimitingIterator(CloseableIterator<Page> delegate, int rowLimit) {
            if (rowLimit <= 0) {
                throw new IllegalArgumentException("rowLimit must be positive, got: " + rowLimit);
            }
            this.delegate = delegate;
            this.remaining = rowLimit;
        }

        @Override
        public boolean hasNext() {
            if (remaining <= 0) {
                return false;
            }
            return delegate.hasNext();
        }

        @Override
        public Page next() {
            if (hasNext() == false) {
                throw new NoSuchElementException();
            }
            Page page = delegate.next();
            int rows = page.getPositionCount();
            if (rows > remaining) {
                int[] positions = new int[remaining];
                for (int i = 0; i < remaining; i++) {
                    positions[i] = i;
                }
                page = page.filter(false, positions);
                remaining = 0;
                try {
                    delegate.close();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            } else {
                remaining -= rows;
            }
            return page;
        }

        @Override
        public void close() throws IOException {
            delegate.close();
        }
    }

}
