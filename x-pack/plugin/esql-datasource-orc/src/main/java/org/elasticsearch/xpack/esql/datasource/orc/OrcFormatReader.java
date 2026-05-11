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
import org.apache.hadoop.hive.ql.exec.vector.Decimal64ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DecimalColumnVector;
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
import org.apache.orc.StripeInformation;
import org.apache.orc.StripeStatistics;
import org.apache.orc.TypeDescription;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.CloseableIterator;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.esql.core.QlIllegalArgumentException;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.util.Check;
import org.elasticsearch.xpack.esql.datasources.SourceStatisticsSerializer;
import org.elasticsearch.xpack.esql.datasources.spi.AggregatePushdownSupport;
import org.elasticsearch.xpack.esql.datasources.spi.ColumnBlockConversions;
import org.elasticsearch.xpack.esql.datasources.spi.FilterPushdownSupport;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReadContext;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReader;
import org.elasticsearch.xpack.esql.datasources.spi.NoConfigFormatReader;
import org.elasticsearch.xpack.esql.datasources.spi.RangeAwareFormatReader;
import org.elasticsearch.xpack.esql.datasources.spi.RangeAwareFormatReader.SplitRange;
import org.elasticsearch.xpack.esql.datasources.spi.RangeReadContext;
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
 * {@link RangeAwareFormatReader} implementation for Apache ORC files.
 *
 * <p>Uses ORC's vectorized reader ({@link VectorizedRowBatch}) which maps naturally to
 * ESQL's columnar {@link Block}/{@link Page} model. Each ORC {@link ColumnVector} is
 * converted directly to the corresponding ESQL Block type.
 *
 * <p>Supports stripe-level split parallelism: {@link #discoverSplitRanges} exposes
 * per-stripe byte ranges, and {@link #readRange} restricts reading to stripes within
 * a given byte range via ORC's {@code Reader.Options.range()}.
 *
 * <p>Key features:
 * <ul>
 *   <li>Works with any StorageProvider (HTTP, S3, local) via {@link OrcStorageObjectAdapter}</li>
 *   <li>Efficient columnar reading with column projection</li>
 *   <li>Direct conversion from ORC VectorizedRowBatch to ESQL Page</li>
 *   <li>Stripe-level split parallelism for multi-stripe files</li>
 * </ul>
 */
public class OrcFormatReader implements RangeAwareFormatReader, NoConfigFormatReader {

    private static final long MILLIS_PER_DAY = Duration.ofDays(1).toMillis();

    private final BlockFactory blockFactory;
    private final SearchArgument pushedFilter;
    private final OrcPushedExpressions pushedExpressions;

    public OrcFormatReader(BlockFactory blockFactory) {
        this(blockFactory, null, null);
    }

    private OrcFormatReader(BlockFactory blockFactory, SearchArgument pushedFilter, OrcPushedExpressions pushedExpressions) {
        this.blockFactory = blockFactory;
        this.pushedFilter = pushedFilter;
        this.pushedExpressions = pushedExpressions;
    }

    @Override
    public FormatReader withPushedFilter(Object pushedFilter) {
        if (pushedFilter instanceof SearchArgument sarg) {
            return new OrcFormatReader(this.blockFactory, sarg, null);
        }
        if (pushedFilter instanceof OrcPushedExpressions exprs) {
            return new OrcFormatReader(this.blockFactory, null, exprs);
        }
        return this;
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
     * This is safe because the ORC files use TIMESTAMP_INSTANT (UTC-anchored) columns. If we
     * ever support files with plain TIMESTAMP columns (writer-local timezone), this flag would
     * incorrectly treat their statistics as UTC too — at that point we'd need per-column
     * evaluation by bypassing SearchArgument and reading stripe statistics directly (the Trino
     * approach).
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
            long bytesOnDisk = cs.getBytesOnDisk();

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

                @Override
                public OptionalLong sizeInBytes() {
                    return bytesOnDisk > 0 ? OptionalLong.of(bytesOnDisk) : OptionalLong.empty();
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
        Reader reader = OrcFile.createReader(path, orcReaderOptions(fs));
        TypeDescription schema = reader.getSchema();
        List<Attribute> attributes = convertOrcSchemaToAttributes(schema);

        List<Attribute> projectedAttributes = resolveProjection(attributes, projectedColumns);
        boolean[] include = buildIncludeMask(schema, projectedColumns);

        Reader.Options readOptions = configureReadOptions(reader, batchSize, include, schema);
        RecordReader rows = reader.rows(readOptions);

        CloseableIterator<Page> iter = new OrcPageIterator(reader, rows, schema, projectedAttributes, batchSize, blockFactory);
        return rowLimit != NO_LIMIT ? new RowLimitingIterator(iter, rowLimit) : iter;
    }

    @Override
    public List<SplitRange> discoverSplitRanges(StorageObject object) throws IOException {
        OrcStorageObjectAdapter fs = new OrcStorageObjectAdapter(object);
        Path path = new Path(object.path().toString());
        try (Reader reader = OrcFile.createReader(path, orcReaderOptions(fs))) {
            List<StripeInformation> stripes = reader.getStripes();
            if (stripes.isEmpty()) {
                return List.of();
            }
            List<StripeStatistics> stripeStats = reader.getStripeStatistics();
            TypeDescription schema = reader.getSchema();
            if (stripes.size() == 1) {
                StripeInformation stripe = stripes.getFirst();
                Map<String, Object> stats = stripeStats.isEmpty() == false
                    ? buildStripeStats(stripe, stripeStats.getFirst(), schema)
                    : Map.of();
                return List.of(new SplitRange(stripe.getOffset(), stripe.getLength(), stats));
            }
            List<SplitRange> ranges = new ArrayList<>(stripes.size());
            for (int i = 0; i < stripes.size(); i++) {
                StripeInformation stripe = stripes.get(i);
                Map<String, Object> stats = (i < stripeStats.size()) ? buildStripeStats(stripe, stripeStats.get(i), schema) : Map.of();
                ranges.add(new SplitRange(stripe.getOffset(), stripe.getLength(), stats));
            }
            return ranges;
        }
    }

    private static Map<String, Object> buildStripeStats(StripeInformation stripe, StripeStatistics stats, TypeDescription schema) {
        Map<String, Object> map = new HashMap<>();
        map.put(SourceStatisticsSerializer.STATS_ROW_COUNT, stripe.getNumberOfRows());
        map.put(SourceStatisticsSerializer.STATS_SIZE_BYTES, stripe.getLength());
        List<String> fieldNames = schema.getFieldNames();
        List<TypeDescription> children = schema.getChildren();
        ColumnStatistics[] colStats = stats.getColumnStatistics();
        for (int i = 0; i < fieldNames.size(); i++) {
            String colName = fieldNames.get(i);
            int colId = children.get(i).getId();
            if (colId >= colStats.length) {
                continue;
            }
            ColumnStatistics cs = colStats[colId];
            if (cs == null) {
                continue;
            }
            long totalValues = cs.getNumberOfValues();
            long nullCount = stripe.getNumberOfRows() - totalValues;
            map.put(SourceStatisticsSerializer.columnNullCountKey(colName), nullCount);
            if (cs.getBytesOnDisk() > 0) {
                map.put(SourceStatisticsSerializer.columnSizeBytesKey(colName), cs.getBytesOnDisk());
            }
            Object minVal = extractOrcMin(cs);
            Object maxVal = extractOrcMax(cs);
            if (minVal != null) {
                map.put(SourceStatisticsSerializer.columnMinKey(colName), minVal);
            }
            if (maxVal != null) {
                map.put(SourceStatisticsSerializer.columnMaxKey(colName), maxVal);
            }
        }
        return Map.copyOf(map);
    }

    /**
     * Reads only the stripes whose byte range falls within {@code [rangeStart, rangeEnd)}.
     * {@code errorPolicy} is accepted for interface compliance but not applied — ORC errors are
     * structural (corrupt stripe, schema mismatch) rather than row-level.
     */
    @Override
    public CloseableIterator<Page> readRange(StorageObject object, RangeReadContext context) throws IOException {
        long rangeStart = context.rangeStart();
        long rangeEnd = context.rangeEnd();
        List<String> projectedColumns = context.projectedColumns();
        int batchSize = context.batchSize();
        List<Attribute> resolvedAttributes = context.resolvedAttributes();

        if (rangeEnd <= rangeStart) {
            throw new IllegalArgumentException("rangeEnd [" + rangeEnd + "] must be greater than rangeStart [" + rangeStart + "]");
        }
        OrcStorageObjectAdapter fs = new OrcStorageObjectAdapter(object);
        Path path = new Path(object.path().toString());
        Reader reader = OrcFile.createReader(path, orcReaderOptions(fs));
        TypeDescription schema = reader.getSchema();

        final List<Attribute> attributes = resolvedAttributes != null && resolvedAttributes.isEmpty() == false
            ? resolvedAttributes
            : convertOrcSchemaToAttributes(schema);

        List<Attribute> projectedAttributes = resolveProjection(attributes, projectedColumns);
        boolean[] include = buildIncludeMask(schema, projectedColumns);

        Reader.Options readOptions = configureReadOptions(reader, batchSize, include, schema);
        readOptions.range(rangeStart, rangeEnd - rangeStart);
        RecordReader rows = reader.rows(readOptions);

        return new OrcPageIterator(reader, rows, schema, projectedAttributes, batchSize, blockFactory);
    }

    private static List<Attribute> resolveProjection(List<Attribute> attributes, List<String> projectedColumns) {
        if (projectedColumns == null || projectedColumns.isEmpty()) {
            return attributes;
        }
        List<Attribute> projected = new ArrayList<>();
        Map<String, Attribute> attributeMap = new HashMap<>();
        for (Attribute attr : attributes) {
            attributeMap.put(attr.name(), attr);
        }
        for (String columnName : projectedColumns) {
            Attribute attr = attributeMap.get(columnName);
            projected.add(attr != null ? attr : new ReferenceAttribute(Source.EMPTY, columnName, DataType.NULL));
        }
        return projected;
    }

    private static boolean[] buildIncludeMask(TypeDescription schema, List<String> projectedColumns) {
        if (projectedColumns == null || projectedColumns.isEmpty()) {
            return null;
        }
        Map<String, Integer> nameToIndex = new HashMap<>();
        List<String> fieldNames = schema.getFieldNames();
        for (int i = 0; i < fieldNames.size(); i++) {
            nameToIndex.put(fieldNames.get(i), i);
        }
        boolean[] include = new boolean[schema.getMaximumId() + 1];
        include[0] = true;
        for (String columnName : projectedColumns) {
            Integer idx = nameToIndex.get(columnName);
            if (idx != null) {
                TypeDescription child = schema.getChildren().get(idx);
                includeColumnForType(include, child);
            }
        }
        return include;
    }

    private Reader.Options configureReadOptions(Reader reader, int batchSize, boolean[] include, TypeDescription schema) {
        Reader.Options readOptions = reader.options().rowBatchSize(batchSize);
        if (include != null) {
            readOptions.include(include);
        }
        SearchArgument resolvedFilter = resolveSearchArgument(schema);
        if (resolvedFilter != null) {
            List<PredicateLeaf> leaves = resolvedFilter.getLeaves();
            LinkedHashSet<String> nameSet = new LinkedHashSet<>(leaves.size());
            for (PredicateLeaf leaf : leaves) {
                nameSet.add(leaf.getColumnName());
            }
            readOptions.searchArgument(resolvedFilter, nameSet.toArray(new String[0]));
        }
        return readOptions;
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

    /**
     * Resolves the SearchArgument to use for a given file. If deferred expressions are present,
     * builds the SearchArgument using the actual file schema for correct DATE/DECIMAL mapping.
     */
    private SearchArgument resolveSearchArgument(TypeDescription schema) {
        if (pushedFilter != null) {
            return pushedFilter;
        }
        if (pushedExpressions != null) {
            return pushedExpressions.toSearchArgument(schema);
        }
        return null;
    }

    @Override
    public FilterPushdownSupport filterPushdownSupport() {
        return new OrcFilterPushdownSupport();
    }

    @Override
    public AggregatePushdownSupport aggregatePushdownSupport() {
        return new OrcAggregatePushdownSupport();
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
            case STRING, VARCHAR, CHAR -> DataType.KEYWORD;
            case TIMESTAMP, TIMESTAMP_INSTANT -> DataType.DATETIME;
            case DATE -> DataType.DATETIME;
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
            ColumnVector child = listCol.child;
            double d64ScaleFactor = child instanceof Decimal64ColumnVector d64 ? Math.pow(10, d64.scale) : 0;
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
                                builder.appendDouble(readDoubleFrom(child, idx, d64ScaleFactor));
                            }
                        }
                        builder.endPositionEntry();
                    }
                }
                return builder.build();
            }
        }

        private static double readDoubleFrom(ColumnVector vector, int idx, double d64ScaleFactor) {
            if (vector instanceof DoubleColumnVector dv) {
                return dv.vector[idx];
            } else if (vector instanceof DecimalColumnVector decV) {
                return decV.vector[idx].doubleValue();
            } else if (vector instanceof Decimal64ColumnVector d64) {
                return d64.vector[idx] / d64ScaleFactor;
            }
            throw new QlIllegalArgumentException("Unsupported list element type: " + vector.getClass().getSimpleName());
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
                                throw new QlIllegalArgumentException(
                                    "Unsupported list child type for DATETIME: " + child.getClass().getSimpleName()
                                );
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
            } else if (vector instanceof DecimalColumnVector decVector) {
                return createDecimalDoubleBlock(decVector, rowCount);
            } else if (vector instanceof Decimal64ColumnVector dec64Vector) {
                // Decimal64ColumnVector extends LongColumnVector — must check before LongColumnVector
                return createDecimal64DoubleBlock(dec64Vector, rowCount);
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

        /**
         * Converts a {@link DecimalColumnVector} (arbitrary precision) to a double block.
         * Each element is a {@code HiveDecimalWritable} whose {@code doubleValue()} returns the
         * properly scaled value. Precision loss beyond ~15 significant digits is inherent to double.
         */
        private Block createDecimalDoubleBlock(DecimalColumnVector decVector, int rowCount) {
            if (decVector.isRepeating) {
                if (decVector.noNulls == false && decVector.isNull[0]) {
                    return blockFactory.newConstantNullBlock(rowCount);
                }
                return blockFactory.newConstantDoubleBlockWith(decVector.vector[0].doubleValue(), rowCount);
            }
            try (var builder = blockFactory.newDoubleBlockBuilder(rowCount)) {
                for (int i = 0; i < rowCount; i++) {
                    if (decVector.noNulls == false && decVector.isNull[i]) {
                        builder.appendNull();
                    } else {
                        builder.appendDouble(decVector.vector[i].doubleValue());
                    }
                }
                return builder.build();
            }
        }

        /**
         * Converts a {@link Decimal64ColumnVector} (precision &le; 18) to a double block.
         * Values are stored as unscaled longs; dividing by 10^scale recovers the decimal value.
         */
        private Block createDecimal64DoubleBlock(Decimal64ColumnVector dec64Vector, int rowCount) {
            double scaleFactor = Math.pow(10, dec64Vector.scale);
            if (dec64Vector.isRepeating) {
                if (dec64Vector.noNulls == false && dec64Vector.isNull[0]) {
                    return blockFactory.newConstantNullBlock(rowCount);
                }
                return blockFactory.newConstantDoubleBlockWith(dec64Vector.vector[0] / scaleFactor, rowCount);
            }
            try (var builder = blockFactory.newDoubleBlockBuilder(rowCount)) {
                for (int i = 0; i < rowCount; i++) {
                    if (dec64Vector.noNulls == false && dec64Vector.isNull[i]) {
                        builder.appendNull();
                    } else {
                        builder.appendDouble(dec64Vector.vector[i] / scaleFactor);
                    }
                }
                return builder.build();
            }
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
                throw new QlIllegalArgumentException("rowLimit must be positive, got: " + rowLimit);
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
                Page truncated;
                try {
                    truncated = page.slice(0, remaining);
                } finally {
                    page.releaseBlocks();
                }
                page = truncated;
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
