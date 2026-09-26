/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.compute.data;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.ReleasableIterator;
import org.elasticsearch.core.Releasables;

import java.io.IOException;
import java.util.List;

import static org.apache.lucene.util.RamUsageEstimator.shallowSizeOfInstance;

/**
 * Sparse records whose field names are data, rather than part of a page's schema. Each dictionary record has parallel
 * name and value entries; values are opaque to this container. A codec above the block defines their logical types.
 * Row ordinals allow records to be shared without requiring a union of their field names. Selection only changes these
 * ordinals: consumers must visit referenced records, never infer live rows from dictionary membership.
 */
public final class OrdinalPackDimBlock extends AbstractDelegatingCompoundBlock<PackDimBlock> implements PackDimBlock {
    private static final long BASE_RAM_BYTES_USED = shallowSizeOfInstance(OrdinalPackDimBlock.class);

    private final IntBlock ordinals;
    private final BytesRefBlock names;
    private final BytesRefBlock values;

    /**
     * Takes ownership of the three blocks on success; the caller releases them if construction fails.
     * Names within each record must be unique and sorted by UTF-8 bytes.
     */
    OrdinalPackDimBlock(IntBlock ordinals, BytesRefBlock names, BytesRefBlock values) {
        super(ordinals.getPositionCount(), null);
        this.ordinals = ordinals;
        this.names = names;
        this.values = values;
        if (names.getPositionCount() != values.getPositionCount() || ordinals.mayHaveMultivaluedFields()) {
            throw new IllegalArgumentException("invalid attribute-set layout");
        }
        assert validLayout();
        ordinals.blockFactory().adjustBreaker(BASE_RAM_BYTES_USED);
    }

    private boolean validLayout() {
        for (int d = 0; d < names.getPositionCount(); d++) {
            assert names.getValueCount(d) == values.getValueCount(d);
            BytesRef previous = null;
            for (int i = 0; i < names.getValueCount(d); i++) {
                BytesRef name = names.getBytesRef(names.getFirstValueIndex(d) + i, new BytesRef());
                assert previous == null || previous.compareTo(name) < 0;
                previous = BytesRef.deepCopyOf(name);
            }
        }
        for (int p = 0; p < getPositionCount(); p++) {
            assert isNull(p)
                || ordinals.getInt(ordinals.getFirstValueIndex(p)) >= 0
                    && ordinals.getInt(ordinals.getFirstValueIndex(p)) < names.getPositionCount();
        }
        return true;
    }

    /** Borrowed ordinal block; values address this block's record dictionary, not a query-global identity. */
    public IntBlock getOrdinalsBlock() {
        return ordinals;
    }

    public int getDictionarySize() {
        return names.getPositionCount();
    }

    @Override
    public OrdinalPackDimBlock asOrdinalPackDim() {
        return this;
    }

    @Override
    public PackDimValue getPackDim(int valueIndex, PackDimValue scratch) {
        // The compound base uses row-aligned sub-block positions as value indices.
        assert ordinals.isNull(valueIndex) == false;
        int record = ordinals.getInt(ordinals.getFirstValueIndex(valueIndex));
        return scratch.reset(names, values, record);
    }

    @Override
    protected List<Block> getSubBlocks() {
        return List.of(ordinals);
    }

    @Override
    protected PackDimBlock buildFromSubBlocks(List<Block> blocks, int positions, int[] firstValueIndexes) {
        if (firstValueIndexes != null) throw new IllegalArgumentException("packed dimension rows are single-valued");
        return shareDictionary((IntBlock) blocks.getFirst());
    }

    /** Takes ownership of the supplied local dictionary ordinals on success or failure and shares immutable dictionary storage. */
    public OrdinalPackDimBlock withOrdinals(IntBlock selected) {
        boolean success = false;
        try {
            var result = shareDictionary(selected);
            success = true;
            return result;
        } finally {
            if (success == false) selected.close();
        }
    }

    // Compound-block selection releases selected sub-blocks itself if construction fails.
    private OrdinalPackDimBlock shareDictionary(IntBlock selected) {
        names.incRef();
        values.incRef();
        boolean success = false;
        try {
            OrdinalPackDimBlock result = new OrdinalPackDimBlock(selected, names, values);
            success = true;
            return result;
        } finally {
            if (success == false) Releasables.closeExpectNoException(names, values);
        }
    }

    @Override
    public Vector asVector() {
        return null;
    }

    @Override
    public ElementType elementType() {
        return ElementType.PACK_DIM;
    }

    @Override
    public BlockFactory blockFactory() {
        return ordinals.blockFactory();
    }

    @Override
    public OrdinalPackDimBlock expand() {
        incRef();
        return this;
    }

    @Override
    public int valueMaxByteSize() {
        long max = 0;
        BytesRef scratch = new BytesRef();
        for (int d = 0; d < names.getPositionCount(); d++) {
            long size = 0;
            for (int i = 0; i < names.getValueCount(d); i++) {
                size += names.getBytesRef(names.getFirstValueIndex(d) + i, scratch).length;
                size += values.getBytesRef(values.getFirstValueIndex(d) + i, scratch).length;
            }
            max = Math.max(max, size);
        }
        return Math.toIntExact(max);
    }

    @Override
    public ReleasableIterator<? extends Block> lookup(IntBlock positions, ByteSizeValue targetBlockSize) {
        // Attribute sets are single values; a multi-valued lookup has no record interpretation.
        if (positions.mayHaveMultivaluedFields()) throw new IllegalArgumentException(
            "attribute-set lookup requires single-valued positions"
        );
        var selected = ordinals.lookup(positions, targetBlockSize);
        incRef();
        return new ReleasableIterator<OrdinalPackDimBlock>() {
            @Override
            public boolean hasNext() {
                return selected.hasNext();
            }

            @Override
            public OrdinalPackDimBlock next() {
                return withOrdinals(selected.next());
            }

            @Override
            public void close() {
                Releasables.close(selected, OrdinalPackDimBlock.this);
            }
        };
    }

    @Override
    public OrdinalPackDimBlock deepCopy(BlockFactory factory) {
        IntBlock copiedOrdinals = null;
        BytesRefBlock copiedNames = null;
        BytesRefBlock copiedValues = null;
        boolean success = false;
        try {
            copiedOrdinals = ordinals.deepCopy(factory);
            copiedNames = names.deepCopy(factory);
            copiedValues = values.deepCopy(factory);
            OrdinalPackDimBlock result = new OrdinalPackDimBlock(copiedOrdinals, copiedNames, copiedValues);
            success = true;
            return result;
        } finally {
            if (success == false) Releasables.closeExpectNoException(copiedOrdinals, copiedNames, copiedValues);
        }
    }

    @Override
    public void allowPassingToDifferentDriver() {
        makeRefCountsThreadSafe();
        ordinals.allowPassingToDifferentDriver();
        names.allowPassingToDifferentDriver();
        values.allowPassingToDifferentDriver();
    }

    @Override
    public long ramBytesUsed() {
        return BASE_RAM_BYTES_USED + ordinals.ramBytesUsed() + names.ramBytesUsed() + values.ramBytesUsed();
    }

    @Override
    protected void closeInternal() {
        blockFactory().adjustBreaker(-BASE_RAM_BYTES_USED);
        Releasables.close(ordinals, names, values);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        if (out.getTransportVersion().supports(ESQL_PACK_DIM) == false) {
            throw new IOException("packed dimensions require " + ESQL_PACK_DIM);
        }
        ordinals.writeTo(out);
        names.writeTo(out);
        values.writeTo(out);
    }

    public static OrdinalPackDimBlock readFrom(BlockStreamInput in) throws IOException {
        IntBlock ordinals = null;
        BytesRefBlock names = null;
        BytesRefBlock values = null;
        boolean success = false;
        try {
            ordinals = IntBlock.readFrom(in);
            names = BytesRefBlock.readFrom(in);
            values = BytesRefBlock.readFrom(in);
            OrdinalPackDimBlock result = new OrdinalPackDimBlock(ordinals, names, values);
            success = true;
            return result;
        } finally {
            if (success == false) Releasables.closeExpectNoException(ordinals, names, values);
        }
    }

}
