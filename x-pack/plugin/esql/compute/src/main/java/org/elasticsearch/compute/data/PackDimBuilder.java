/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.compute.data;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.util.BytesRefHash;
import org.elasticsearch.compute.operator.BreakingBytesRefBuilder;
import org.elasticsearch.compute.operator.topn.TopNEncoder;
import org.elasticsearch.core.Releasables;

/** Builds sparse records using the engine's existing byte and integer builders and breaker accounting. */
public final class PackDimBuilder implements PackDimBlock.Builder {
    private static final long BASE_RAM_BYTES_USED = org.apache.lucene.util.RamUsageEstimator.shallowSizeOfInstance(PackDimBuilder.class);
    private final IntBlock.Builder ordinals;
    private final BytesRefBlock.Builder names;
    private final BytesRefBlock.Builder values;
    private final BlockFactory factory;
    private BytesRefHash seen;
    private BreakingBytesRefBuilder key;
    private int records;

    PackDimBuilder(BlockFactory factory, int estimatedSize) {
        this.factory = factory;
        ordinals = factory.newIntBlockBuilder(estimatedSize);
        BytesRefBlock.Builder namesBuilder = null;
        BytesRefBlock.Builder valuesBuilder = null;
        try {
            namesBuilder = factory.newBytesRefBlockBuilder(estimatedSize);
            valuesBuilder = factory.newBytesRefBlockBuilder(estimatedSize);
        } finally {
            if (valuesBuilder == null) Releasables.closeExpectNoException(ordinals, namesBuilder);
        }
        names = namesBuilder;
        values = valuesBuilder;
    }

    /** Append one record. Fields must be sorted and unique; an empty record is different from a null record. */
    public PackDimBuilder append(BytesRef[] fieldNames, BytesRef[] fieldValues) {
        if (fieldNames.length != fieldValues.length) throw new IllegalArgumentException("different field and value counts");
        for (int i = 0; i < fieldNames.length; i++) {
            if (fieldNames[i] == null || fieldValues[i] == null || (i > 0 && fieldNames[i - 1].compareTo(fieldNames[i]) >= 0)) {
                throw new IllegalArgumentException("attribute fields must be non-null, unique and sorted");
            }
        }
        if (seen == null) seen = new BytesRefHash(1, factory.bigArrays());
        if (key == null) key = new BreakingBytesRefBuilder(factory.breaker(), "packed dimension interning");
        key.clear();
        for (int i = 0; i < fieldNames.length; i++) {
            TopNEncoder.DEFAULT_UNSORTABLE.encodeInt(fieldNames[i].length, key);
            key.append(fieldNames[i]);
            TopNEncoder.DEFAULT_UNSORTABLE.encodeInt(fieldValues[i].length, key);
            key.append(fieldValues[i]);
        }
        long id = seen.add(key.bytesRefView());
        if (id < 0) return appendOrdinal(Math.toIntExact(-1 - id));
        if (fieldNames.length == 0) {
            names.appendNull();
            values.appendNull();
        } else {
            if (fieldNames.length > 1) {
                names.beginPositionEntry();
                values.beginPositionEntry();
            }
            for (int i = 0; i < fieldNames.length; i++) {
                names.appendBytesRef(fieldNames[i]);
                values.appendBytesRef(fieldValues[i]);
            }
            if (fieldNames.length > 1) {
                names.endPositionEntry();
                values.endPositionEntry();
            }
        }
        ordinals.appendInt(records++);
        return this;
    }

    /** Reuses a previously appended record without copying its fields. */
    private PackDimBuilder appendOrdinal(int ordinal) {
        if (ordinal < 0 || ordinal >= records) throw new IllegalArgumentException("unknown record ordinal " + ordinal);
        ordinals.appendInt(ordinal);
        return this;
    }

    @Override
    public PackDimBuilder appendNull() {
        ordinals.appendNull();
        return this;
    }

    @Override
    public PackDimBuilder beginPositionEntry() {
        throw new UnsupportedOperationException("packed dimensions are single-valued");
    }

    @Override
    public PackDimBuilder endPositionEntry() {
        throw new UnsupportedOperationException("packed dimensions are single-valued");
    }

    @Override
    public PackDimBuilder mvOrdering(Block.MvOrdering ordering) {
        return this;
    }

    @Override
    public long estimatedBytes() {
        return BASE_RAM_BYTES_USED + ordinals.estimatedBytes() + names.estimatedBytes() + values.estimatedBytes() + (seen == null
            ? 0
            : seen.ramBytesUsed()) + (key == null ? 0 : key.ramBytesUsed());
    }

    @Override
    public PackDimBuilder appendPackDim(PackDimValue value) {
        BytesRef[] fieldNames = new BytesRef[value.size()];
        BytesRef[] fieldValues = new BytesRef[value.size()];
        for (int i = 0; i < value.size(); i++) {
            fieldNames[i] = value.nameAt(i, new BytesRef());
            fieldValues[i] = value.valueAt(i, new BytesRef());
        }
        return append(fieldNames, fieldValues);
    }

    @Override
    public PackDimBuilder copyFrom(Block block, int begin, int end) {
        PackDimValue scratch = new PackDimValue();
        for (int p = begin; p < end; p++) {
            if (block.isNull(p)) appendNull();
            else appendPackDim(((PackDimBlock) block).getPackDim(block.getFirstValueIndex(p), scratch));
        }
        return this;
    }

    @Override
    public PackDimBlock build() {
        Block[] blocks = Block.Builder.buildAll(ordinals, names, values);
        boolean success = false;
        try {
            OrdinalPackDimBlock result = new OrdinalPackDimBlock(
                (IntBlock) blocks[0],
                (BytesRefBlock) blocks[1],
                (BytesRefBlock) blocks[2]
            );
            success = true;
            return result;
        } finally {
            if (success == false) Releasables.closeExpectNoException(blocks);
        }
    }

    @Override
    public void close() {
        Releasables.close(ordinals, names, values, seen, key);
    }
}
