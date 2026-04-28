/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.parquet;

import org.apache.lucene.util.BytesRef;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.CloseableIterator;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.datasources.spi.AggregateScanSpec;
import org.elasticsearch.xpack.esql.datasources.spi.AggregateScanSpec.AggOp;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

/**
 * Iterator over per-row-group intermediate-state {@link Page}s for an
 * {@link AggregateScanSpec}. For each row group it picks one of two paths:
 * <ul>
 *   <li><b>Fast path</b>: every column referenced by the spec has non-empty
 *       {@link Statistics} in this row group — derive the intermediate page from those
 *       stats (rowCount + per-column nullCount/min/max). No row data is decoded.</li>
 *   <li><b>Slow path</b>: at least one referenced column lacks stats — decode the
 *       projected columns into ESQL {@link Block}s and fold values into accumulators,
 *       then emit a single intermediate page from the accumulators.</li>
 * </ul>
 * Both paths produce a {@link Page} matching {@code spec.intermediateAttributes()}.
 * <p>
 * Multi-valued column semantics on the slow path:
 * <ul>
 *   <li>{@code COUNT(field)} counts positions whose value is non-null.</li>
 *   <li>{@code MIN/MAX(field)} folds over all values within each position.</li>
 * </ul>
 * Multi-valued (Parquet maxRepLevel &gt; 0) columns are not supported on the slow path
 * in this implementation; rows for those columns are reported as {@code seen=false}.
 * Column statistics on multi-valued columns are still fast-path-eligible.
 * <p>
 * The iterator owns the supplied {@link ParquetFileReader} and closes it on
 * {@link #close()}.
 */
final class ParquetAggregateScanIterator implements CloseableIterator<Page> {

    private static final int SLOW_PATH_BATCH_SIZE = 16 * 1024;

    private final ParquetFileReader reader;
    private final AggregateScanSpec spec;
    private final BlockFactory blockFactory;
    private final List<BlockMetaData> rowGroups;
    private final Map<String, ColumnInfo> columnInfoByDottedName;
    private final String createdBy;
    private final Set<String> projectedColumns;

    private int idx;
    private boolean closed;

    ParquetAggregateScanIterator(ParquetFileReader reader, MessageType schema, AggregateScanSpec spec, BlockFactory blockFactory) {
        this.reader = reader;
        this.spec = spec;
        this.blockFactory = blockFactory;
        this.rowGroups = reader.getRowGroups();
        this.createdBy = reader.getFileMetaData().getCreatedBy();
        this.projectedColumns = projectedColumnsOf(spec);
        this.columnInfoByDottedName = buildColumnInfoIndex(schema, projectedColumns);
    }

    @Override
    public boolean hasNext() {
        return closed == false && idx < rowGroups.size();
    }

    @Override
    public Page next() {
        if (hasNext() == false) {
            throw new NoSuchElementException();
        }
        BlockMetaData rg = rowGroups.get(idx);
        int rgIndex = idx;
        idx++;
        try {
            if (statsCoverAllProjectedColumns(rg)) {
                return buildPageFromStats(rg);
            }
            return buildPageFromRowData(rg, rgIndex);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void close() throws IOException {
        if (closed) {
            return;
        }
        closed = true;
        reader.close();
    }

    // === fast path ===

    /**
     * Returns true when every column the spec references has non-empty {@link Statistics}
     * in this row group. CountStar contributes no column. Multi-valued (rep level > 0)
     * columns are still eligible for the fast path: stats are scoped to a column chunk
     * regardless of repetition level.
     */
    private boolean statsCoverAllProjectedColumns(BlockMetaData rg) {
        if (projectedColumns.isEmpty()) {
            return true;
        }
        Map<String, Statistics<?>> statsByCol = statsByColumn(rg);
        for (String col : projectedColumns) {
            Statistics<?> s = statsByCol.get(col);
            if (s == null || s.isEmpty()) {
                return false;
            }
        }
        return true;
    }

    private Page buildPageFromStats(BlockMetaData rg) {
        Map<String, Statistics<?>> statsByCol = statsByColumn(rg);
        long rowCount = rg.getRowCount();
        Block[] blocks = new Block[spec.ops().size() * 2];
        try {
            for (int i = 0; i < spec.ops().size(); i++) {
                AggOp op = spec.ops().get(i);
                Attribute valueAttr = spec.intermediateAttributes().get(i * 2);
                Object value = resolveStatsValue(op, rowCount, statsByCol);
                if (value == null) {
                    blocks[i * 2] = blockFactory.newConstantNullBlock(1);
                    blocks[i * 2 + 1] = blockFactory.newConstantBooleanBlockWith(false, 1);
                } else {
                    blocks[i * 2] = constantValueBlock(value, valueAttr.dataType());
                    blocks[i * 2 + 1] = blockFactory.newConstantBooleanBlockWith(true, 1);
                }
            }
            Page p = new Page(blocks);
            blocks = null; // ownership transferred
            return p;
        } finally {
            if (blocks != null) {
                Releasables.closeExpectNoException(blocks);
            }
        }
    }

    private Object resolveStatsValue(AggOp op, long rowCount, Map<String, Statistics<?>> statsByCol) {
        if (op instanceof AggOp.CountStar) {
            return rowCount;
        }
        if (op instanceof AggOp.CountField cf) {
            Statistics<?> s = statsByCol.get(cf.column());
            if (s == null || s.isEmpty()) {
                return null;
            }
            return rowCount - s.getNumNulls();
        }
        if (op instanceof AggOp.MinField mn) {
            Statistics<?> s = statsByCol.get(mn.column());
            if (s == null || s.isEmpty() || s.hasNonNullValue() == false) {
                return null;
            }
            return normalizeStatValue(s.genericGetMin());
        }
        if (op instanceof AggOp.MaxField mx) {
            Statistics<?> s = statsByCol.get(mx.column());
            if (s == null || s.isEmpty() || s.hasNonNullValue() == false) {
                return null;
            }
            return normalizeStatValue(s.genericGetMax());
        }
        return null;
    }

    private Block constantValueBlock(Object value, DataType dataType) {
        return switch (dataType) {
            case INTEGER -> blockFactory.newConstantIntBlockWith(((Number) value).intValue(), 1);
            case LONG, COUNTER_LONG, DATETIME -> blockFactory.newConstantLongBlockWith(((Number) value).longValue(), 1);
            case DOUBLE, COUNTER_DOUBLE -> blockFactory.newConstantDoubleBlockWith(((Number) value).doubleValue(), 1);
            case BOOLEAN -> blockFactory.newConstantBooleanBlockWith(((Boolean) value), 1);
            case KEYWORD, TEXT -> blockFactory.newConstantBytesRefBlockWith(toBytesRef(value), 1);
            default -> {
                if (value instanceof Number n) {
                    yield blockFactory.newConstantLongBlockWith(n.longValue(), 1);
                }
                yield blockFactory.newConstantNullBlock(1);
            }
        };
    }

    private static BytesRef toBytesRef(Object value) {
        if (value instanceof BytesRef br) {
            return br;
        }
        if (value instanceof Binary b) {
            return new BytesRef(b.getBytes());
        }
        return new BytesRef(value.toString());
    }

    private static Object normalizeStatValue(Object value) {
        if (value instanceof Binary binary) {
            return new BytesRef(binary.getBytes());
        }
        return value;
    }

    private static Map<String, Statistics<?>> statsByColumn(BlockMetaData rg) {
        Map<String, Statistics<?>> out = new HashMap<>();
        for (ColumnChunkMetaData ch : rg.getColumns()) {
            out.put(ch.getPath().toDotString(), ch.getStatistics());
        }
        return out;
    }

    // === slow path ===

    private Page buildPageFromRowData(BlockMetaData rg, int rgIndex) throws IOException {
        long rowCount = rg.getRowCount();
        // Per-op accumulators; null means we couldn't compute (e.g. multi-valued column).
        Accumulator[] accs = createAccumulators();
        if (projectedColumns.isEmpty() == false) {
            PageReadStore pages = reader.readRowGroup(rgIndex);
            for (String column : projectedColumns) {
                ColumnInfo ci = columnInfoByDottedName.get(column);
                if (ci == null || ci.maxRepLevel() > 0) {
                    // Unknown column or multi-valued: mark all ops referencing this column as unsupported.
                    markColumnUnsupported(accs, column);
                    continue;
                }
                accumulateColumn(pages, ci, column, accs, rowCount);
            }
        }
        // Apply CountStar (no column dependency).
        for (int i = 0; i < spec.ops().size(); i++) {
            if (spec.ops().get(i) instanceof AggOp.CountStar && accs[i] instanceof CountAccumulator c) {
                c.setRowCount(rowCount);
            }
        }
        return assemblePageFromAccumulators(accs);
    }

    private void accumulateColumn(PageReadStore pages, ColumnInfo ci, String column, Accumulator[] accs, long totalRows) {
        ColumnDescriptor desc = ci.descriptor();
        // PageColumnReader is package-private; we share the same package, so direct use is fine.
        PageColumnReader pcr = new PageColumnReader(pages.getPageReader(desc), desc, ci, RowRanges.all(totalRows));
        long remaining = totalRows;
        while (remaining > 0) {
            int batch = (int) Math.min(remaining, SLOW_PATH_BATCH_SIZE);
            Block block = pcr.readBatch(batch, blockFactory);
            try {
                int positionCount = block.getPositionCount();
                for (int i = 0; i < spec.ops().size(); i++) {
                    AggOp op = spec.ops().get(i);
                    if (referencesColumn(op, column) == false) {
                        continue;
                    }
                    Accumulator a = accs[i];
                    if (a == null) {
                        continue;
                    }
                    a.accumulate(block, positionCount);
                }
            } finally {
                Releasables.closeExpectNoException(block);
            }
            remaining -= batch;
        }
    }

    private void markColumnUnsupported(Accumulator[] accs, String column) {
        for (int i = 0; i < spec.ops().size(); i++) {
            if (referencesColumn(spec.ops().get(i), column)) {
                accs[i] = null;
            }
        }
    }

    private static boolean referencesColumn(AggOp op, String column) {
        return switch (op) {
            case AggOp.CountStar ignored -> false;
            case AggOp.CountField cf -> cf.column().equals(column);
            case AggOp.MinField mn -> mn.column().equals(column);
            case AggOp.MaxField mx -> mx.column().equals(column);
        };
    }

    private Accumulator[] createAccumulators() {
        Accumulator[] accs = new Accumulator[spec.ops().size()];
        for (int i = 0; i < spec.ops().size(); i++) {
            AggOp op = spec.ops().get(i);
            DataType valueType = spec.intermediateAttributes().get(i * 2).dataType();
            accs[i] = switch (op) {
                case AggOp.CountStar ignored -> new CountAccumulator();
                case AggOp.CountField ignored -> new CountAccumulator();
                case AggOp.MinField ignored -> minMaxAccumulator(valueType, true);
                case AggOp.MaxField ignored -> minMaxAccumulator(valueType, false);
            };
        }
        return accs;
    }

    private static Accumulator minMaxAccumulator(DataType valueType, boolean isMin) {
        return switch (valueType) {
            case INTEGER -> new IntMinMaxAccumulator(isMin);
            case LONG, COUNTER_LONG, DATETIME -> new LongMinMaxAccumulator(isMin);
            case DOUBLE, COUNTER_DOUBLE -> new DoubleMinMaxAccumulator(isMin);
            case BOOLEAN -> new BooleanMinMaxAccumulator(isMin);
            case KEYWORD, TEXT -> new BytesRefMinMaxAccumulator(isMin);
            default -> null;
        };
    }

    private Page assemblePageFromAccumulators(Accumulator[] accs) {
        Block[] blocks = new Block[spec.ops().size() * 2];
        try {
            for (int i = 0; i < spec.ops().size(); i++) {
                Accumulator a = accs[i];
                if (a == null || a.seen() == false) {
                    blocks[i * 2] = blockFactory.newConstantNullBlock(1);
                    blocks[i * 2 + 1] = blockFactory.newConstantBooleanBlockWith(false, 1);
                } else {
                    DataType vt = spec.intermediateAttributes().get(i * 2).dataType();
                    blocks[i * 2] = constantValueBlock(a.value(), vt);
                    blocks[i * 2 + 1] = blockFactory.newConstantBooleanBlockWith(true, 1);
                }
            }
            Page p = new Page(blocks);
            blocks = null; // ownership transferred
            return p;
        } finally {
            if (blocks != null) {
                Releasables.closeExpectNoException(blocks);
            }
        }
    }

    // === helpers ===

    private static Set<String> projectedColumnsOf(AggregateScanSpec spec) {
        Set<String> out = new LinkedHashSet<>();
        for (AggOp op : spec.ops()) {
            switch (op) {
                case AggOp.CountStar ignored -> {
                }
                case AggOp.CountField cf -> out.add(cf.column());
                case AggOp.MinField mn -> out.add(mn.column());
                case AggOp.MaxField mx -> out.add(mx.column());
            }
        }
        return out;
    }

    /**
     * Build a dotted-name to {@link ColumnInfo} map for the columns we need. Walks the
     * {@link MessageType} columns; nested types are flattened to dotted names matching the
     * convention used elsewhere in this file (and by Parquet's
     * {@code ColumnPath#toDotString}).
     */
    private static Map<String, ColumnInfo> buildColumnInfoIndex(MessageType schema, Set<String> wanted) {
        Map<String, ColumnInfo> out = new HashMap<>();
        for (ColumnDescriptor desc : schema.getColumns()) {
            String dotted = String.join(".", desc.getPath());
            if (wanted.contains(dotted) == false) {
                continue;
            }
            DataType esqlType = esqlTypeFor(desc);
            LogicalTypeAnnotation logicalType = desc.getPrimitiveType().getLogicalTypeAnnotation();
            ColumnInfo ci = new ColumnInfo(
                desc,
                desc.getPrimitiveType().getPrimitiveTypeName(),
                esqlType,
                desc.getMaxDefinitionLevel(),
                desc.getMaxRepetitionLevel(),
                logicalType
            );
            out.put(dotted, ci);
        }
        return out;
    }

    /**
     * Map a Parquet primitive column to an ESQL {@link DataType}. Mirrors
     * {@code ParquetFormatReader#convertParquetTypeToEsql} for the subset of types we read on
     * the slow path. Anything unsupported maps to {@link DataType#UNSUPPORTED} so the
     * {@link PageColumnReader} dispatches to the constant-null branch.
     */
    private static DataType esqlTypeFor(ColumnDescriptor desc) {
        PrimitiveType pt = desc.getPrimitiveType();
        LogicalTypeAnnotation lt = pt.getLogicalTypeAnnotation();
        return switch (pt.getPrimitiveTypeName()) {
            case BOOLEAN -> DataType.BOOLEAN;
            case INT32 -> {
                if (lt instanceof LogicalTypeAnnotation.DateLogicalTypeAnnotation) yield DataType.DATETIME;
                yield DataType.INTEGER;
            }
            case INT64 -> {
                if (lt instanceof LogicalTypeAnnotation.TimestampLogicalTypeAnnotation) yield DataType.DATETIME;
                yield DataType.LONG;
            }
            case INT96 -> DataType.DATETIME;
            case FLOAT, DOUBLE -> DataType.DOUBLE;
            case BINARY, FIXED_LEN_BYTE_ARRAY -> DataType.KEYWORD;
        };
    }

    // === accumulators ===

    private interface Accumulator {
        void accumulate(Block block, int positionCount);

        boolean seen();

        Object value();
    }

    private static final class CountAccumulator implements Accumulator {
        private long count;

        @Override
        public void accumulate(Block block, int positionCount) {
            // CountField: count positions whose value is non-null. Multi-valued positions count once.
            for (int p = 0; p < positionCount; p++) {
                if (block.isNull(p) == false) {
                    count++;
                }
            }
        }

        void setRowCount(long rowCount) {
            this.count = rowCount;
        }

        @Override
        public boolean seen() {
            return true;
        }

        @Override
        public Object value() {
            return count;
        }
    }

    private abstract static class MinMaxAccumulator implements Accumulator {
        protected final boolean isMin;
        protected boolean seen;

        protected MinMaxAccumulator(boolean isMin) {
            this.isMin = isMin;
        }

        @Override
        public boolean seen() {
            return seen;
        }
    }

    private static final class IntMinMaxAccumulator extends MinMaxAccumulator {
        private int best;

        IntMinMaxAccumulator(boolean isMin) {
            super(isMin);
        }

        @Override
        public void accumulate(Block block, int positionCount) {
            IntBlock ib = (IntBlock) block;
            for (int p = 0; p < positionCount; p++) {
                if (ib.isNull(p)) continue;
                int firstIdx = ib.getFirstValueIndex(p);
                int valueCount = ib.getValueCount(p);
                for (int v = 0; v < valueCount; v++) {
                    int val = ib.getInt(firstIdx + v);
                    if (seen == false) {
                        best = val;
                        seen = true;
                    } else if (isMin ? val < best : val > best) {
                        best = val;
                    }
                }
            }
        }

        @Override
        public Object value() {
            return best;
        }
    }

    private static final class LongMinMaxAccumulator extends MinMaxAccumulator {
        private long best;

        LongMinMaxAccumulator(boolean isMin) {
            super(isMin);
        }

        @Override
        public void accumulate(Block block, int positionCount) {
            LongBlock lb = (LongBlock) block;
            for (int p = 0; p < positionCount; p++) {
                if (lb.isNull(p)) continue;
                int firstIdx = lb.getFirstValueIndex(p);
                int valueCount = lb.getValueCount(p);
                for (int v = 0; v < valueCount; v++) {
                    long val = lb.getLong(firstIdx + v);
                    if (seen == false) {
                        best = val;
                        seen = true;
                    } else if (isMin ? val < best : val > best) {
                        best = val;
                    }
                }
            }
        }

        @Override
        public Object value() {
            return best;
        }
    }

    private static final class DoubleMinMaxAccumulator extends MinMaxAccumulator {
        private double best;

        DoubleMinMaxAccumulator(boolean isMin) {
            super(isMin);
        }

        @Override
        public void accumulate(Block block, int positionCount) {
            DoubleBlock db = (DoubleBlock) block;
            for (int p = 0; p < positionCount; p++) {
                if (db.isNull(p)) continue;
                int firstIdx = db.getFirstValueIndex(p);
                int valueCount = db.getValueCount(p);
                for (int v = 0; v < valueCount; v++) {
                    double val = db.getDouble(firstIdx + v);
                    if (seen == false) {
                        best = val;
                        seen = true;
                    } else if (isMin ? val < best : val > best) {
                        best = val;
                    }
                }
            }
        }

        @Override
        public Object value() {
            return best;
        }
    }

    private static final class BooleanMinMaxAccumulator extends MinMaxAccumulator {
        // For booleans, MIN is "any false", MAX is "any true" — fold over values.
        private boolean best;

        BooleanMinMaxAccumulator(boolean isMin) {
            super(isMin);
        }

        @Override
        public void accumulate(Block block, int positionCount) {
            BooleanBlock bb = (BooleanBlock) block;
            for (int p = 0; p < positionCount; p++) {
                if (bb.isNull(p)) continue;
                int firstIdx = bb.getFirstValueIndex(p);
                int valueCount = bb.getValueCount(p);
                for (int v = 0; v < valueCount; v++) {
                    boolean val = bb.getBoolean(firstIdx + v);
                    if (seen == false) {
                        best = val;
                        seen = true;
                    } else if (isMin ? Boolean.compare(val, best) < 0 : Boolean.compare(val, best) > 0) {
                        best = val;
                    }
                }
            }
        }

        @Override
        public Object value() {
            return best;
        }
    }

    private static final class BytesRefMinMaxAccumulator extends MinMaxAccumulator {
        private BytesRef best;
        private final BytesRef scratch = new BytesRef();

        BytesRefMinMaxAccumulator(boolean isMin) {
            super(isMin);
        }

        @Override
        public void accumulate(Block block, int positionCount) {
            BytesRefBlock bb = (BytesRefBlock) block;
            for (int p = 0; p < positionCount; p++) {
                if (bb.isNull(p)) continue;
                int firstIdx = bb.getFirstValueIndex(p);
                int valueCount = bb.getValueCount(p);
                for (int v = 0; v < valueCount; v++) {
                    BytesRef val = bb.getBytesRef(firstIdx + v, scratch);
                    if (seen == false) {
                        best = BytesRef.deepCopyOf(val);
                        seen = true;
                    } else {
                        int cmp = val.compareTo(best);
                        if (isMin ? cmp < 0 : cmp > 0) {
                            best = BytesRef.deepCopyOf(val);
                        }
                    }
                }
            }
        }

        @Override
        public Object value() {
            return best;
        }
    }

}
