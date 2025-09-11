/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import org.apache.lucene.util.Accountable;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.ReleasableIterator;
import org.elasticsearch.core.Releasables;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * A {@link Block} containing a {@code Map} from column name to a {@code Block} of data.
 */
public class ColumnsBlock extends AbstractNonThreadSafeRefCounted implements Block {
    private final BlockFactory blockFactory;
    private final int positionCount;

    // NOCOMMIT rendering assumes all sub-types are KEYWORD.
    private final Map<String, RuntimeTypedBlock> columns;

    public ColumnsBlock(BlockFactory blockFactory, int positionCount, Map<String, RuntimeTypedBlock> columns) {
        this.blockFactory = blockFactory;
        this.positionCount = positionCount;
        this.columns = columns;
        for (Map.Entry<String, RuntimeTypedBlock> c : columns.entrySet()) {
            if (c.getValue().block.isReleased()) {
                throw new IllegalStateException(
                    c.getKey() + ": can't build ColumnsBlock out of released blocks but [" + c.getValue() + "] was released"
                );
            }
            if (c.getValue().block.getPositionCount() != positionCount) {
                throw new IllegalStateException(c.getKey() + ": " + c.getValue() + " doesn't have " + positionCount + " positions");
            }
        }
    }

    public Map<String, RuntimeTypedBlock> columns() {
        return columns;
    }

    @Override
    public Vector asVector() {
        return null;
    }

    @Override
    public int getTotalValueCount() {
        return columns.values().stream().mapToInt(b -> b.block.getTotalValueCount()).sum();
    }

    @Override
    public int getPositionCount() {
        return positionCount;
    }

    @Override
    public int getFirstValueIndex(int position) {
        throw new UnsupportedOperationException("ColumnsBlock");
    }

    @Override
    public int getValueCount(int position) {
        throw new UnsupportedOperationException("ColumnsBlock");
    }

    @Override
    public ElementType elementType() {
        return ElementType.COLUMNS;
    }

    @Override
    public BlockFactory blockFactory() {
        return blockFactory;
    }

    @Override
    public void allowPassingToDifferentDriver() {
        for (RuntimeTypedBlock c : columns.values()) {
            c.block.allowPassingToDifferentDriver();
        }
    }

    @Override
    public boolean isNull(int position) {
        return false;
    }

    @Override
    public boolean mayHaveNulls() {
        return false;
    }

    @Override
    public boolean areAllValuesNull() {
        return false;
    }

    @Override
    public boolean mayHaveMultivaluedFields() {
        throw new UnsupportedOperationException("ColumnsBlock");
    }

    @Override
    public boolean doesHaveMultivaluedFields() {
        throw new UnsupportedOperationException("ColumnsBlock");
    }

    @Override
    public Block filter(int... positions) {
        ColumnsBlock result = null;
        Map<String, RuntimeTypedBlock> newColumns = new HashMap<>();
        try {
            for (Map.Entry<String, RuntimeTypedBlock> c : columns.entrySet()) {
                newColumns.put(c.getKey(), new RuntimeTypedBlock(c.getValue().type, c.getValue().block.filter(positions)));
            }
            result = new ColumnsBlock(blockFactory, positions.length, newColumns);
            return result;
        } finally {
            if (result == null) {
                Releasables.close(newColumns.values());
            }
        }
    }

    @Override
    public Block keepMask(BooleanVector mask) {
        ColumnsBlock result = null;
        Map<String, RuntimeTypedBlock> newColumns = new HashMap<>();
        try {
            for (Map.Entry<String, RuntimeTypedBlock> c : columns.entrySet()) {
                newColumns.put(c.getKey(), new RuntimeTypedBlock(c.getValue().type, c.getValue().block.keepMask(mask)));
            }
            result = new ColumnsBlock(blockFactory, mask.getPositionCount(), newColumns);
            return result;
        } finally {
            if (result == null) {
                Releasables.close(newColumns.values());
            }
        }
    }

    @Override
    public ReleasableIterator<? extends Block> lookup(IntBlock positions, ByteSizeValue targetBlockSize) {
        throw new UnsupportedOperationException("ColumnsBlock");
    }

    @Override
    public MvOrdering mvOrdering() {
        return MvOrdering.UNORDERED;
    }

    @Override
    public Block expand() {
        throw new UnsupportedOperationException("ColumnsBlock");
    }

    @Override
    public Block deepCopy(BlockFactory blockFactory) {
        ColumnsBlock result = null;
        Map<String, RuntimeTypedBlock> newColumns = new HashMap<>();
        try {
            for (Map.Entry<String, RuntimeTypedBlock> c : columns.entrySet()) {
                newColumns.put(c.getKey(), new RuntimeTypedBlock(c.getValue().type, c.getValue().block.deepCopy(blockFactory)));
            }
            result = new ColumnsBlock(blockFactory, positionCount, newColumns);
            return result;
        } finally {
            if (result == null) {
                Releasables.close(newColumns.values());
            }
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        throw new UnsupportedOperationException("NOCOMMIT ColumnsBlock");
    }

    @Override
    public long ramBytesUsed() {
        // NOCOMMIT some bytes of my own
        return columns.values().stream().mapToLong(Accountable::ramBytesUsed).sum();
    }

    @Override
    protected void closeInternal() {
        Releasables.close(columns.values());
    }

    public record RuntimeTypedBlock(Object type, Block block) implements Accountable, Releasable {
        @Override
        public long ramBytesUsed() {
            // NOCOMMIT some bytes of my own.
            return block.ramBytesUsed();
        }

        @Override
        public void close() {
            block.close();
        }
    }

}
