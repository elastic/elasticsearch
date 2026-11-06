/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.ReleasableIterator;
import org.elasticsearch.core.Releasables;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * A {@link UnionBlock} made of a tag sub-block and one sub-block per leg.
 */
public final class UnionArrayBlock extends AbstractDelegatingCompoundBlock<UnionBlock> implements UnionBlock {

    private final IntBlock tags;
    private final List<Block> legs;
    /**
     * Declared per leg, not read back off the leg blocks: a leg holding no values at all is a
     * {@link ConstantNullBlock}, whose element type is {@link ElementType#NULL} rather than the leg's.
     */
    private final List<ElementType> legElementTypes;
    /**
     * The tag followed by the legs, built once: the base class reads this on {@link #isNull} and every other
     * path that derives nullity, so building it per call would allocate on the hottest method the block has.
     */
    private final List<Block> subBlocks;

    public UnionArrayBlock(
        IntBlock tags,
        List<Block> legs,
        List<ElementType> legElementTypes,
        int positionCount,
        @Nullable int[] firstValueIndexes
    ) {
        super(positionCount, firstValueIndexes);
        if (legs.size() != legElementTypes.size()) {
            throw new IllegalArgumentException("got " + legs.size() + " legs but " + legElementTypes.size() + " leg element types");
        }
        if (legs.isEmpty()) {
            throw new IllegalArgumentException("a union block needs at least one leg");
        }
        this.tags = tags;
        this.legs = List.copyOf(legs);
        this.legElementTypes = List.copyOf(legElementTypes);
        List<Block> allSubBlocks = new ArrayList<>(this.legs.size() + 1);
        allSubBlocks.add(tags);
        allSubBlocks.addAll(this.legs);
        this.subBlocks = List.copyOf(allSubBlocks);
        assert assertInvariants();
    }

    @Override
    protected boolean assertInvariants() {
        super.assertInvariants();
        int expectedSubBlockPositions = subBlockPositionCount();
        for (Block b : getSubBlocks()) {
            assert b.isReleased() == false;
            assert b.doesHaveMultivaluedFields() == false : "UnionArrayBlock sub-blocks can't have multi-values but [" + b + "] does";
            assert b.getPositionCount() == expectedSubBlockPositions
                : "UnionArrayBlock sub-blocks must have the same position count but ["
                    + b
                    + "] has "
                    + b.getPositionCount()
                    + " instead of "
                    + expectedSubBlockPositions;
        }
        for (int subPos = 0; subPos < expectedSubBlockPositions; subPos++) {
            if (tags.isNull(subPos)) {
                for (Block leg : legs) {
                    assert leg.isNull(subPos) : "UnionArrayBlock leg [" + leg + "] must be null where untagged, at position " + subPos;
                }
                continue;
            }
            int tag = tags.getInt(tags.getFirstValueIndex(subPos));
            assert tag >= 0 && tag < legs.size() : "UnionArrayBlock tag " + tag + " out of range at position " + subPos;
            for (int leg = 0; leg < legs.size(); leg++) {
                assert legs.get(leg).isNull(subPos) == (leg != tag)
                    : "UnionArrayBlock leg " + leg + " has wrong nullity at position " + subPos + " tagged " + tag;
            }
        }
        return true;
    }

    @Override
    protected List<Block> getSubBlocks() {
        return subBlocks;
    }

    @Override
    protected UnionArrayBlock buildFromSubBlocks(List<Block> newSubBlocks, int newPositionCount, @Nullable int[] newFirstValueIndexes) {
        assert newSubBlocks.size() == legs.size() + 1;
        return new UnionArrayBlock(
            (IntBlock) newSubBlocks.getFirst(),
            newSubBlocks.subList(1, newSubBlocks.size()),
            legElementTypes,
            newPositionCount,
            newFirstValueIndexes
        );
    }

    @Override
    public int legCount() {
        return legs.size();
    }

    @Override
    public ElementType legElementType(int leg) {
        return legElementTypes.get(leg);
    }

    @Override
    public List<ElementType> legElementTypes() {
        return legElementTypes;
    }

    @Override
    public int tag(int valueIndex) {
        return tags.isNull(valueIndex) ? NO_TAG : tags.getInt(tags.getFirstValueIndex(valueIndex));
    }

    @Override
    public Block legBlock(int leg) {
        return legs.get(leg);
    }

    IntBlock tagsBlock() {
        return tags;
    }

    @Override
    public Vector asVector() {
        return null;
    }

    @Override
    public ElementType elementType() {
        return ElementType.UNION;
    }

    @Override
    public ReleasableIterator<? extends UnionBlock> lookup(IntBlock positions, ByteSizeValue targetBlockSize) {
        throw new UnsupportedOperationException("Lookup is not supported on UnionArrayBlock");
    }

    @Override
    public Block expand() {
        if (mayHaveMultivaluedFields()) {
            throw new UnsupportedOperationException("Not yet implemented for multi-valued blocks");
        }
        incRef();
        return this;
    }

    /**
     * Copy a range of sub-block positions into the given builders, which must be the tag builder followed by one
     * builder per leg in leg order.
     */
    void copySubBlockPositionsInto(Block.Builder tagsBuilder, List<Block.Builder> legBuilders, int beginInclusive, int endExclusive) {
        assert legBuilders.size() == legs.size();
        tagsBuilder.copyFrom(tags, beginInclusive, endExclusive);
        for (int leg = 0; leg < legs.size(); leg++) {
            legBuilders.get(leg).copyFrom(legs.get(leg), beginInclusive, endExclusive);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        writeMultiValueMetadata(out);
        out.writeVInt(legElementTypes.size());
        for (ElementType legElementType : legElementTypes) {
            legElementType.writeTo(out);
        }
        Block.writeTypedBlock(tags, out);
        for (Block leg : legs) {
            Block.writeTypedBlock(leg, out);
        }
    }

    public static UnionArrayBlock readFrom(BlockStreamInput input) throws IOException {
        return AbstractDelegatingCompoundBlock.readFrom(input, UnionArrayBlock::readSubBlocksFrom);
    }

    private static UnionArrayBlock readSubBlocksFrom(BlockStreamInput in, int positionCount, int[] firstValueIndexes) throws IOException {
        int legCount = in.readVInt();
        List<ElementType> legElementTypes = new ArrayList<>(legCount);
        for (int i = 0; i < legCount; i++) {
            legElementTypes.add(ElementType.readFrom(in));
        }
        IntBlock tags = null;
        List<Block> legs = new ArrayList<>(legCount);
        boolean success = false;
        try {
            tags = (IntBlock) Block.readTypedBlock(in);
            for (int i = 0; i < legCount; i++) {
                legs.add(Block.readTypedBlock(in));
            }
            UnionArrayBlock result = new UnionArrayBlock(
                tags,
                legs,
                legElementTypes,
                firstValueIndexes == null ? tags.getPositionCount() : positionCount,
                firstValueIndexes
            );
            success = true;
            return result;
        } finally {
            if (success == false) {
                Releasables.close(tags, Releasables.wrap(legs));
            }
        }
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof UnionBlock block) {
            return UnionBlock.equals(this, block);
        }
        return false;
    }

    boolean equalsAfterTypeCheck(UnionArrayBlock that) {
        return layoutEquals(that) && legElementTypes.equals(that.legElementTypes) && tags.equals(that.tags) && legs.equals(that.legs);
    }

    @Override
    public int hashCode() {
        return Objects.hash(tags.hashCode(), legElementTypes);
    }

    @Override
    public String toString() {
        return "UnionArrayBlock[positions=" + getPositionCount() + ", legs=" + legElementTypes + "]";
    }
}
