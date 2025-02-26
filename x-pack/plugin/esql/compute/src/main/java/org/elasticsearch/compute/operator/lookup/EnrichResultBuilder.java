/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.lookup;

import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.Releasable;

/**
 * An abstract class responsible for collecting values for an output block of enrich.
 * The incoming values of the same position are combined and added to a single corresponding position.
 */
abstract class EnrichResultBuilder implements Releasable {
    protected final BlockFactory blockFactory;
    protected final int channel;
    private long usedBytes;

    EnrichResultBuilder(BlockFactory blockFactory, int channel) {
        this.blockFactory = blockFactory;
        this.channel = channel;
    }

    /**
     * Collects the input values from the input page.
     *
     * @param positions the positions vector
     * @param page      the input page. The block located at {@code channel} is the value block
     */
    abstract void addInputPage(IntVector positions, Page page);

    abstract Block build(IntBlock selected);

    final void adjustBreaker(long bytes) {
        blockFactory.breaker().addEstimateBytesAndMaybeBreak(bytes, "<<enrich-result>>");
        usedBytes += bytes;
    }

    @Override
    public void close() {
        blockFactory.breaker().addWithoutBreaking(-usedBytes);
    }

    static EnrichResultBuilder enrichResultBuilder(ElementType elementType, BlockFactory blockFactory, int channel) {
        return switch (elementType) {
            case NULL -> new EnrichResultBuilderForNull(blockFactory, channel);
            case INT -> new EnrichResultBuilderForInt(blockFactory, channel);
            case LONG -> new EnrichResultBuilderForLong(blockFactory, channel);
            case DOUBLE -> new EnrichResultBuilderForDouble(blockFactory, channel);
            case BOOLEAN -> new EnrichResultBuilderForBoolean(blockFactory, channel);
            case BYTES_REF -> new EnrichResultBuilderForBytesRef(blockFactory, channel);
            default -> throw new IllegalArgumentException("no enrich result builder for [" + elementType + "]");
        };
    }

    private static class EnrichResultBuilderForNull extends EnrichResultBuilder {
        EnrichResultBuilderForNull(BlockFactory blockFactory, int channel) {
            super(blockFactory, channel);
        }

        @Override
        void addInputPage(IntVector positions, Page page) {
            assert page.getBlock(channel).areAllValuesNull() : "expected all nulls; but got values";
        }

        @Override
        Block build(IntBlock selected) {
            return blockFactory.newConstantNullBlock(selected.getPositionCount());
        }
    }
}
