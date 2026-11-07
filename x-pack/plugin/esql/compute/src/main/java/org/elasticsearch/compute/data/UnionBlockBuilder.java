/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasables;

import java.util.ArrayList;
import java.util.List;

/**
 * Builds a {@link UnionArrayBlock}. The leg element types are fixed at construction, and every append writes one
 * position to the tag builder and to every leg builder, so the sub-blocks stay positionally aligned.
 */
public final class UnionBlockBuilder extends AbstractDelegatingCompoundBlock.AbstractCompositeBlockBuilder<UnionBlock>
    implements
        UnionBlock.Builder {

    private final List<ElementType> legElementTypes;
    private final IntBlock.Builder tagsBuilder;
    private final List<Block.Builder> legBuilders;
    private final BytesRef scratch = new BytesRef();

    public UnionBlockBuilder(List<ElementType> legElementTypes, int estimatedSize, BlockFactory blockFactory) {
        super(blockFactory);
        if (legElementTypes.isEmpty()) {
            throw new IllegalArgumentException("a union block needs at least one leg");
        }
        for (ElementType legElementType : legElementTypes) {
            if (canHoldLeg(legElementType) == false) {
                throw new IllegalArgumentException("a union leg cannot hold " + legElementType);
            }
        }
        this.legElementTypes = List.copyOf(legElementTypes);
        IntBlock.Builder tags = null;
        List<Block.Builder> legs = new ArrayList<>(this.legElementTypes.size());
        boolean success = false;
        try {
            tags = blockFactory.newIntBlockBuilder(estimatedSize);
            for (ElementType legElementType : this.legElementTypes) {
                legs.add(legElementType.newBlockBuilder(estimatedSize, blockFactory));
            }
            success = true;
        } finally {
            if (success == false) {
                Releasables.close(tags, Releasables.wrap(legs));
            }
        }
        this.tagsBuilder = tags;
        this.legBuilders = legs;
    }

    @Override
    public UnionBlockBuilder appendNull() {
        assert isPositionEntryOpen() == false : "a null is a position of its own, not a value inside one";
        tagsBuilder.appendNull();
        for (Block.Builder legBuilder : legBuilders) {
            legBuilder.appendNull();
        }
        valueAppended();
        return this;
    }

    @Override
    public UnionBlockBuilder appendValue(int leg, Block source, int sourceValueIndex) {
        if (leg < 0 || leg >= legBuilders.size()) {
            throw new IllegalArgumentException("leg " + leg + " out of range for " + legBuilders.size() + " legs");
        }
        if (source.elementType() != legElementTypes.get(leg)) {
            throw new IllegalArgumentException(
                "leg " + leg + " holds " + legElementTypes.get(leg) + " but the value came from a " + source.elementType() + " block"
            );
        }
        if (source.isNull(sourceValueIndex)) {
            // The typed getters read the slot without checking, and a null slot holds a default rather than
            // nothing, so reading it here would turn a missing value into a present, tagged zero that the
            // block's own invariants cannot see.
            throw new IllegalArgumentException("no value at index " + sourceValueIndex + " of the source block; append a null instead");
        }
        tagsBuilder.appendInt(leg);
        for (int i = 0; i < legBuilders.size(); i++) {
            if (i == leg) {
                appendOneValue(legBuilders.get(i), legElementTypes.get(i), source, sourceValueIndex);
            } else {
                legBuilders.get(i).appendNull();
            }
        }
        valueAppended();
        return this;
    }

    private void appendOneValue(Block.Builder builder, ElementType elementType, Block source, int valueIndex) {
        switch (elementType) {
            case BOOLEAN -> ((BooleanBlock.Builder) builder).appendBoolean(((BooleanBlock) source).getBoolean(valueIndex));
            case INT -> ((IntBlock.Builder) builder).appendInt(((IntBlock) source).getInt(valueIndex));
            case LONG -> ((LongBlock.Builder) builder).appendLong(((LongBlock) source).getLong(valueIndex));
            case FLOAT -> ((FloatBlock.Builder) builder).appendFloat(((FloatBlock) source).getFloat(valueIndex));
            case DOUBLE -> ((DoubleBlock.Builder) builder).appendDouble(((DoubleBlock) source).getDouble(valueIndex));
            case BYTES_REF -> ((BytesRefBlock.Builder) builder).appendBytesRef(((BytesRefBlock) source).getBytesRef(valueIndex, scratch));
            case NULL, COMPOSITE, DOC, UNKNOWN, AGGREGATE_METRIC_DOUBLE, EXPONENTIAL_HISTOGRAM, TDIGEST, LONG_RANGE, DOUBLE_RANGE, UNION ->
                throw new UnsupportedOperationException("a union leg cannot hold " + elementType);
        }
    }

    /**
     * Whether a leg can hold values of this element type. The declared leg types are checked against this at construction, so
     * {@link #appendValue} never has to refuse one. The logical layer keeps its own list of the types it will offer as legs, in
     * another module that cannot call this; a test is what holds the two lists together.
     * <p>
     * {@link ElementType#NULL} is not among them even though a leg's own block may be a {@link ConstantNullBlock}: that is a leg
     * which happens to hold no values, whereas a leg declared {@code NULL} could never hold one.
     * </p>
     */
    public static boolean canHoldLeg(ElementType elementType) {
        return switch (elementType) {
            case BOOLEAN, INT, LONG, FLOAT, DOUBLE, BYTES_REF -> true;
            case NULL, COMPOSITE, DOC, UNKNOWN, AGGREGATE_METRIC_DOUBLE, EXPONENTIAL_HISTOGRAM, TDIGEST, LONG_RANGE, DOUBLE_RANGE, UNION ->
                false;
        };
    }

    @Override
    protected void copySubBlockPositions(AbstractDelegatingCompoundBlock<?> block, int startSubBlockPos, int endSubBlockPos) {
        UnionArrayBlock from = (UnionArrayBlock) block;
        if (from.legElementTypes().equals(legElementTypes) == false) {
            throw new IllegalArgumentException("cannot copy a union of " + from.legElementTypes() + " into a union of " + legElementTypes);
        }
        from.copySubBlockPositionsInto(tagsBuilder, legBuilders, startSubBlockPos, endSubBlockPos);
    }

    @Override
    public UnionBlockBuilder mvOrdering(Block.MvOrdering mvOrdering) {
        assert mvOrdering == Block.MvOrdering.UNORDERED : "a union has no natural order, so ordering it doesn't make sense";
        return this;
    }

    @Override
    public long estimatedBytes() {
        long bytes = super.estimatedBytes() + tagsBuilder.estimatedBytes();
        for (Block.Builder legBuilder : legBuilders) {
            bytes += legBuilder.estimatedBytes();
        }
        return bytes;
    }

    @Override
    public UnionBlock doBuild(int positionCount, @Nullable int[] firstValueIndexes) {
        IntBlock tags = null;
        List<Block> legs = new ArrayList<>(legBuilders.size());
        boolean success = false;
        try {
            tags = tagsBuilder.build();
            for (Block.Builder legBuilder : legBuilders) {
                legs.add(legBuilder.build());
            }
            UnionBlock result = new UnionArrayBlock(tags, legs, legElementTypes, positionCount, firstValueIndexes);
            success = true;
            return result;
        } finally {
            if (success == false) {
                Releasables.close(tags, Releasables.wrap(legs));
            }
        }
    }

    @Override
    protected void extraClose() {
        Releasables.closeExpectNoException(tagsBuilder, Releasables.wrap(legBuilders));
    }
}
