/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefArrayBlock;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.ConstantBooleanVector;
import org.elasticsearch.compute.data.ConstantBytesRefVector;
import org.elasticsearch.compute.data.ConstantDoubleVector;
import org.elasticsearch.compute.data.ConstantFloatVector;
import org.elasticsearch.compute.data.ConstantIntVector;
import org.elasticsearch.compute.data.ConstantLongVector;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.OrdinalBytesRefBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.data.Vector;
import org.elasticsearch.core.ReleasableIterator;
import org.elasticsearch.core.Releasables;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * {@link Block#lookup Looks up} values from one or more {@link Block}s and
 * merges them into each {@link Page}.
 */
public class ColumnLoadOperator extends AbstractPageMappingToIteratorOperator {
    public record Values(String name, Block block) {
        public Values {
            Objects.requireNonNull(name, "name");
            Objects.requireNonNull(block, "block");
        }

        @Override
        public String toString() {
            return name + ":" + block.elementType();
        }
    }

    /**
     * Factory for {@link ColumnLoadOperator}. The {@link Block}s inside {@link Values}
     * are never closed by this factory; each operator instance deep-copies them into its
     * own {@link BlockFactory} on construction.
     */
    public record Factory(List<Values> values, int positionsOrd) implements OperatorFactory {
        public Factory {
            values = List.copyOf(values);
            if (values.isEmpty()) {
                throw new IllegalArgumentException("expected [values] to be non-empty");
            }
        }

        public Factory(Values value, int positionsOrd) {
            this(List.of(value), positionsOrd);
        }

        @Override
        public Operator get(DriverContext driverContext) {
            Values[] copied = new Values[values.size()];
            boolean success = false;
            try {
                for (int i = 0; i < copied.length; i++) {
                    Values v = values.get(i);
                    copied[i] = new Values(v.name, v.block.deepCopy(driverContext.blockFactory()));
                }
                ColumnLoadOperator operator = new ColumnLoadOperator(copied, positionsOrd);
                success = true;
                return operator;
            } finally {
                if (success == false) {
                    closeValues(copied);
                }
            }
        }

        @Override
        public String describe() {
            return "ColumnLoad[values=" + values + ", positions=" + positionsOrd + "]";
        }
    }

    private static final int UNSET_FIXED_LENGTH = -1;
    private static final int VARIABLE_LENGTH = -2;
    private static final long BYTES_REF_BLOCK_BASE_BYTES = RamUsageEstimator.shallowSizeOfInstance(BytesRefArrayBlock.class);

    /**
     * Maximum combined byte size of the blocks materialized per emitted page. Upstream
     * row-size estimation sizes input pages to fit {@link Operator#TARGET_PAGE_SIZE}
     * including these added columns, so ordinary pages pass through whole and only pages
     * whose materialized values outgrow a page (e.g., multivalued aggregates) are split.
     */
    static final ByteSizeValue TARGET_LOADED_BYTES = ByteSizeValue.ofBytes(Operator.TARGET_PAGE_SIZE);
    private static final ByteSizeValue UNBOUNDED_BLOCK_SIZE = ByteSizeValue.ofBytes(Long.MAX_VALUE);

    private final Values[] values;
    private final ColumnScanState[] scanColumns;
    private final boolean hasConstantColumn;
    private final int positionsOrd;

    ColumnLoadOperator(Values value, int positionsOrd) {
        this(new Values[] { value }, positionsOrd);
    }

    /**
     * Takes ownership of the {@link Block}s inside {@code values} and releases them in {@link #close()}.
     */
    ColumnLoadOperator(Values[] values, int positionsOrd) {
        if (values.length < 1) {
            throw new IllegalArgumentException("expected [values] to be non-empty");
        }
        Values[] owned = values.clone();
        for (Values v : owned) {
            Objects.requireNonNull(v, "value");
        }
        this.positionsOrd = positionsOrd;
        this.values = owned;
        if (owned.length > 1) {
            this.scanColumns = new ColumnScanState[owned.length];
            boolean anyConstant = false;
            for (int c = 0; c < owned.length; c++) {
                ColumnScanState state = new ColumnScanState(owned[c].block);
                this.scanColumns[c] = state;
                anyConstant |= state.hasConstantLookup;
            }
            this.hasConstantColumn = anyConstant;
        } else {
            this.scanColumns = null;
            this.hasConstantColumn = false;
        }
    }

    @Override
    protected ReleasableIterator<Page> receive(Page page) {
        IntBlock positions = page.getBlock(positionsOrd);
        if (values.length == 1) {
            return appendBlocks(page, values[0].block.lookup(positions, TARGET_LOADED_BYTES));
        }
        return appendBlockArrays(page, new MultiColumnLookup(scanColumns, hasConstantColumn, positions, TARGET_LOADED_BYTES));
    }

    @Override
    public void close() {
        Releasables.closeExpectNoException(super::close, () -> closeValues(values));
    }

    private static void closeValues(Values[] values) {
        for (Values v : values) {
            if (v != null) {
                Releasables.closeExpectNoException(v.block);
            }
        }
    }

    @Override
    public String toString() {
        return "ColumnLoad[values=" + Arrays.toString(values) + ", positions=" + positionsOrd + "]";
    }

    private static final class MultiColumnLookup implements ReleasableIterator<Block[]> {
        private final ColumnScanState[] columns;
        private final boolean hasConstantColumn;
        private final IntBlock positions;
        private final long targetByteSize;
        private final BytesRef scratch = new BytesRef();
        private int position;
        private boolean closed;

        MultiColumnLookup(ColumnScanState[] columns, boolean hasConstantColumn, IntBlock positions, ByteSizeValue targetByteSize) {
            positions.incRef();
            this.columns = columns;
            this.hasConstantColumn = hasConstantColumn;
            this.positions = positions;
            this.targetByteSize = targetByteSize.getBytes();
        }

        @Override
        public boolean hasNext() {
            return position < positions.getPositionCount();
        }

        @Override
        public Block[] next() {
            int batchStart = position;
            int batchEnd = scanBatchEnd(batchStart);
            Block[] result = new Block[columns.length];
            boolean success = false;
            try (IntBlock batchPositions = positions.slice(batchStart, batchEnd)) {
                for (int c = 0; c < columns.length; c++) {
                    try (ReleasableIterator<? extends Block> it = columns[c].block.lookup(batchPositions, UNBOUNDED_BLOCK_SIZE)) {
                        result[c] = it.next();
                        assert it.hasNext() == false : "unexpected extra block from single-batch lookup";
                    }
                }
                position = batchEnd;
                success = true;
                return result;
            } finally {
                if (success == false) {
                    Releasables.closeExpectNoException(result);
                }
            }
        }

        private int scanBatchEnd(int batchStart) {
            boolean anyNeedsScan = resetColumns(batchStart);
            if (anyNeedsScan == false) {
                return positions.getPositionCount();
            }
            int scanPos = batchStart;
            int count = 0;
            while (scanPos < positions.getPositionCount()) {
                if (scanRow(scanPos, count) == false) {
                    break;
                }
                count++;
                scanPos++;
                if (count > Operator.MIN_TARGET_PAGE_SIZE && combinedOutputBytes(count) >= targetByteSize) {
                    break;
                }
            }
            return scanPos;
        }

        private boolean resetColumns(int batchStart) {
            IntVector positionsVector = hasConstantColumn ? positions.asVector() : null;
            int positionsMin = -1;
            int positionsMax = Integer.MAX_VALUE;
            if (positionsVector != null) {
                if (batchStart == 0) {
                    positionsMin = positionsVector.min();
                    positionsMax = positionsVector.max();
                } else {
                    positionsMin = Integer.MAX_VALUE;
                    positionsMax = Integer.MIN_VALUE;
                    for (int p = batchStart; p < positionsVector.getPositionCount(); p++) {
                        int vp = positionsVector.getInt(p);
                        positionsMin = Math.min(positionsMin, vp);
                        positionsMax = Math.max(positionsMax, vp);
                    }
                }
            }
            boolean anyNeedsScan = false;
            for (ColumnScanState column : columns) {
                anyNeedsScan |= column.reset(positionsVector != null, positionsMin, positionsMax);
            }
            return anyNeedsScan;
        }

        private boolean scanRow(int pos, int count) {
            int pStart = positions.getFirstValueIndex(pos);
            int pEnd = pStart + positions.getValueCount(pos);
            boolean anyNullToNonNull = false;
            for (ColumnScanState column : columns) {
                if (column.stagePosition(positions, pStart, pEnd, count, scratch) == false) {
                    return false;
                }
                anyNullToNonNull |= column.transitionsFromAllNull();
            }
            if (count >= Operator.MIN_TARGET_PAGE_SIZE && anyNullToNonNull && prospectiveOutputBytes(count + 1) >= targetByteSize) {
                return false;
            }
            for (ColumnScanState column : columns) {
                column.commitStagedPosition();
            }
            return true;
        }

        private long prospectiveOutputBytes(int nextCount) {
            long combined = 0;
            for (ColumnScanState column : columns) {
                combined += column.prospectiveOutputBytes(nextCount);
            }
            return combined;
        }

        private long combinedOutputBytes(int count) {
            long combined = 0;
            for (ColumnScanState column : columns) {
                combined += column.outputBytes(count);
            }
            return combined;
        }

        @Override
        public void close() {
            if (closed == false) {
                closed = true;
                positions.close();
            }
        }
    }

    private static final class ColumnScanState {
        final Block block;
        final boolean hasConstantLookup;
        private final BytesRefBlock bytesBlock;
        private final int fixedElementBytes;
        private boolean skipRowScan;
        private long totalValues;
        private long totalSlots;
        private boolean hasMultiValues;
        private long totalValueBytes;
        private int fixedLength;

        private long stagedValues;
        private long stagedSlots;
        private long stagedBytes;
        private int stagedFixedLength;

        ColumnScanState(Block block) {
            this.block = block;
            boolean allNull = block.areAllValuesNull();
            this.hasConstantLookup = allNull == false && hasConstantVectorLookup(block);
            this.bytesBlock = (allNull == false && block instanceof BytesRefBlock b) ? b : null;
            this.fixedElementBytes = allNull ? 0 : fixedElementBytes(block.elementType());
        }

        private static boolean hasConstantVectorLookup(Block block) {
            if (block instanceof OrdinalBytesRefBlock) {
                return false;
            }
            Vector v = block.asVector();
            return v instanceof ConstantBooleanVector
                || v instanceof ConstantIntVector
                || v instanceof ConstantLongVector
                || v instanceof ConstantFloatVector
                || v instanceof ConstantDoubleVector
                || v instanceof ConstantBytesRefVector;
        }

        private static int fixedElementBytes(ElementType type) {
            return switch (type) {
                case BOOLEAN -> Byte.BYTES;
                case INT -> Integer.BYTES;
                case FLOAT -> Float.BYTES;
                case LONG -> Long.BYTES;
                case DOUBLE -> Double.BYTES;
                case NULL, BYTES_REF, DOC, COMPOSITE, UNKNOWN, AGGREGATE_METRIC_DOUBLE, EXPONENTIAL_HISTOGRAM, TDIGEST, LONG_RANGE,
                    DOUBLE_RANGE -> 0;
            };
        }

        boolean reset(boolean densePositions, int positionsMin, int positionsMax) {
            int colPosCount = block.getPositionCount();
            boolean skipConstant = hasConstantLookup
                && densePositions
                && positionsMin >= 0
                && (positionsMin >= colPosCount || positionsMax < colPosCount);
            skipRowScan = skipConstant || (bytesBlock == null && fixedElementBytes == 0);
            totalValues = 0;
            totalSlots = 0;
            hasMultiValues = false;
            totalValueBytes = 0;
            fixedLength = UNSET_FIXED_LENGTH;
            return skipRowScan == false;
        }

        boolean stagePosition(IntBlock positions, int pStart, int pEnd, int count, BytesRef scratch) {
            if (skipRowScan) {
                stagedValues = 0;
                stagedSlots = 0;
                return true;
            }
            int colPosCount = block.getPositionCount();
            long valuesForPos = 0;
            long posBytes = 0;
            int posFixed = fixedLength;
            for (int i = pStart; i < pEnd; i++) {
                int vp = positions.getInt(i);
                if (vp < 0) {
                    throw new IllegalArgumentException("invalid position [" + vp + "]");
                }
                if (vp < colPosCount) {
                    int vCount = block.getValueCount(vp);
                    valuesForPos += vCount;
                    if (valuesForPos > Block.MAX_LOOKUP) {
                        throw new IllegalArgumentException("Found a single entry with " + valuesForPos + " entries");
                    }
                    if (bytesBlock != null) {
                        int vStart = bytesBlock.getFirstValueIndex(vp);
                        for (int v = vStart; v < vStart + vCount; v++) {
                            int len = bytesBlock.getBytesRef(v, scratch).length;
                            posBytes += len;
                            posFixed = mergeFixedLength(posFixed, len);
                        }
                    }
                }
            }
            long slotCount = Math.max(1L, valuesForPos);
            if (count > 0 && totalSlots + slotCount > ArrayUtil.MAX_ARRAY_LENGTH) {
                return false;
            }
            stagedValues = valuesForPos;
            stagedSlots = slotCount;
            stagedBytes = posBytes;
            stagedFixedLength = valuesForPos == 0 ? mergeFixedLength(posFixed, 0) : posFixed;
            return true;
        }

        boolean transitionsFromAllNull() {
            return skipRowScan == false && totalValues == 0 && stagedValues > 0;
        }

        void commitStagedPosition() {
            if (skipRowScan) {
                return;
            }
            totalValues += stagedValues;
            totalSlots += stagedSlots;
            if (stagedValues > 1) {
                hasMultiValues = true;
            }
            if (bytesBlock != null) {
                totalValueBytes += stagedBytes;
                fixedLength = stagedFixedLength;
            }
        }

        long prospectiveOutputBytes(int nextCount) {
            if (skipRowScan) {
                return 0;
            }
            return computeBytes(
                totalValues + stagedValues,
                totalSlots + stagedSlots,
                hasMultiValues || stagedValues > 1,
                totalValueBytes + stagedBytes,
                stagedFixedLength,
                nextCount
            );
        }

        long outputBytes(int count) {
            if (skipRowScan) {
                return 0;
            }
            return computeBytes(totalValues, totalSlots, hasMultiValues, totalValueBytes, fixedLength, count);
        }

        private long computeBytes(long vals, long slots, boolean mv, long valBytes, int fixedLen, int count) {
            if (vals == 0) {
                return 0;
            }
            long bytes;
            if (bytesBlock != null) {
                bytes = BYTES_REF_BLOCK_BASE_BYTES + valBytes;
                if (fixedLen == VARIABLE_LENGTH) {
                    bytes += (slots + 1L) * Integer.BYTES;
                }
            } else {
                bytes = RamUsageEstimator.NUM_BYTES_ARRAY_HEADER + slots * fixedElementBytes;
            }
            if (mv) {
                bytes += RamUsageEstimator.NUM_BYTES_ARRAY_HEADER + (count + 1L) * Integer.BYTES;
            }
            return bytes;
        }

        private static int mergeFixedLength(int current, int length) {
            if (current == UNSET_FIXED_LENGTH || current == length) {
                return length;
            }
            return VARIABLE_LENGTH;
        }
    }
}
