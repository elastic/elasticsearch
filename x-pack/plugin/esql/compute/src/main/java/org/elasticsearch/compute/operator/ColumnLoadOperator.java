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
import org.elasticsearch.compute.data.CompositeBlock;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.ReleasableIterator;
import org.elasticsearch.core.Releasables;

import java.util.Arrays;

/**
 * {@link Block#lookup Looks up} values from one or more {@link Block}s and
 * merges them into each {@link Page}.
 */
public class ColumnLoadOperator extends AbstractPageMappingToIteratorOperator {
    public record Values(String name, Block block) {
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
    public record Factory(Values[] values, int positionsOrd) implements OperatorFactory {
        public Factory {
            if (values.length < 1) {
                throw new IllegalArgumentException("expected [values] to be non-empty");
            }
        }

        public Factory(Values value, int positionsOrd) {
            this(new Values[] { value }, positionsOrd);
        }

        @Override
        public Operator get(DriverContext driverContext) {
            Values[] copied = new Values[values.length];
            boolean success = false;
            try {
                for (int i = 0; i < values.length; i++) {
                    copied[i] = new Values(values[i].name, values[i].block.deepCopy(driverContext.blockFactory()));
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
            return "ColumnLoad[values=" + Arrays.toString(values) + ", positions=" + positionsOrd + "]";
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
    private final int positionsOrd;

    public ColumnLoadOperator(Values value, int positionsOrd) {
        this(new Values[] { value }, positionsOrd);
    }

    public ColumnLoadOperator(Values[] values, int positionsOrd) {
        if (values.length < 1) {
            throw new IllegalArgumentException("expected [values] to be non-empty");
        }
        this.positionsOrd = positionsOrd;
        this.values = values;
    }

    @Override
    protected ReleasableIterator<Page> receive(Page page) {
        IntBlock positions = page.getBlock(positionsOrd);
        if (values.length == 1) {
            return appendBlocks(page, values[0].block.lookup(positions, TARGET_LOADED_BYTES));
        }
        return appendBlockArrays(page, new MultiColumnLookup(values, positions, TARGET_LOADED_BYTES));
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
        private final IntBlock positions;
        private final long targetByteSize;
        private final BytesRef scratch = new BytesRef();
        private int position;
        private boolean closed;

        MultiColumnLookup(Values[] values, IntBlock positions, ByteSizeValue targetByteSize) {
            positions.incRef();
            this.positions = positions;
            this.targetByteSize = targetByteSize.getBytes();
            this.columns = new ColumnScanState[values.length];
            for (int c = 0; c < values.length; c++) {
                this.columns[c] = new ColumnScanState(values[c].block);
            }
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
            for (ColumnScanState column : columns) {
                column.reset();
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

        private boolean scanRow(int pos, int count) {
            int pStart = positions.getFirstValueIndex(pos);
            int pEnd = pStart + positions.getValueCount(pos);
            for (ColumnScanState column : columns) {
                if (column.scanPosition(positions, pStart, pEnd, count, scratch) == false) {
                    return false;
                }
            }
            return true;
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
        private final BytesRefBlock bytesBlock;
        private final int fixedElementBytes;
        private long totalValues;
        private long totalSlots;
        private boolean hasMultiValues;
        private long totalValueBytes;
        private int fixedLength;

        ColumnScanState(Block block) {
            this.block = block;
            boolean allNull = block.areAllValuesNull();
            this.bytesBlock = (allNull == false && block instanceof BytesRefBlock b) ? b : null;
            this.fixedElementBytes = (allNull || bytesBlock != null || block instanceof CompositeBlock) ? 0 : block.valueMaxByteSize();
        }

        void reset() {
            totalValues = 0;
            totalSlots = 0;
            hasMultiValues = false;
            totalValueBytes = 0;
            fixedLength = UNSET_FIXED_LENGTH;
        }

        boolean scanPosition(IntBlock positions, int pStart, int pEnd, int count, BytesRef scratch) {
            if (bytesBlock == null && fixedElementBytes == 0) {
                return true;
            }
            int colPosCount = block.getPositionCount();
            long valuesForPos = 0;
            long posBytes = 0;
            int posFixed = fixedLength;
            for (int i = pStart; i < pEnd; i++) {
                int vp = positions.getInt(i);
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
            return commitPosition(valuesForPos, posBytes, posFixed, count);
        }

        private boolean commitPosition(long valuesForPos, long posBytes, int posFixed, int count) {
            long slotCount = Math.max(1L, valuesForPos);
            if (count > 0 && totalSlots + slotCount > ArrayUtil.MAX_ARRAY_LENGTH) {
                return false;
            }
            totalValues += valuesForPos;
            totalSlots += slotCount;
            if (valuesForPos > 1) {
                hasMultiValues = true;
            }
            if (bytesBlock != null) {
                totalValueBytes += posBytes;
                fixedLength = valuesForPos == 0 ? mergeFixedLength(posFixed, 0) : posFixed;
            }
            return true;
        }

        long outputBytes(int count) {
            if (totalValues == 0) {
                return 0;
            }
            long bytes;
            if (bytesBlock != null) {
                bytes = BYTES_REF_BLOCK_BASE_BYTES + totalValueBytes;
                if (fixedLength == VARIABLE_LENGTH) {
                    bytes += (totalSlots + 1L) * Integer.BYTES;
                }
            } else {
                bytes = RamUsageEstimator.NUM_BYTES_ARRAY_HEADER + totalSlots * fixedElementBytes;
            }
            if (hasMultiValues) {
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
