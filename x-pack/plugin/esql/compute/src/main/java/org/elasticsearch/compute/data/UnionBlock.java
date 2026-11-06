/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import java.util.List;

/**
 * A column whose values do not all have the same type. It holds one <em>leg</em> per type, each a conventional typed
 * block spanning the whole column, plus a tag per value saying which leg holds it. A value sits in its own leg at its
 * own position, and the other legs are null there.
 * <p>
 * Legs are physical: {@link ElementType}, not a logical type. This module cannot see {@code DataType}, so the logical
 * type of each leg travels beside the column as the leg order agreed between the plan and the response. Two legs may
 * share an element type while differing logically, so a leg is addressed by index and never by its element type.
 * </p>
 * <p>
 * The tag is a sub-block rather than something inferred from which leg happens to be non-null: it answers in constant
 * time, it answers for an absent value too, and as the first sub-block it is the one
 * {@link AbstractDelegatingCompoundBlock} derives nullity from.
 * </p>
 */
public sealed interface UnionBlock extends Block permits ConstantNullBlock, UnionArrayBlock {

    /** {@link #tag(int)} where the column has no value. */
    int NO_TAG = -1;

    static boolean equals(UnionBlock blockA, UnionBlock blockB) {
        if (blockA == blockB) {
            return true;
        }
        return switch (blockA) {
            case null -> false;
            case ConstantNullBlock a -> a.equals(blockB);
            case UnionArrayBlock a -> switch (blockB) {
                case null -> false;
                case ConstantNullBlock b -> b.equals(a);
                case UnionArrayBlock b -> a.equalsAfterTypeCheck(b);
            };
        };
    }

    /**
     * How many legs this column has. A union column that turned out null everywhere may be a
     * {@link ConstantNullBlock}, which reports zero, so this is not the column's declared leg count and must
     * not be zipped with the leg list the plan carries.
     */
    int legCount();

    ElementType legElementType(int leg);

    List<ElementType> legElementTypes();

    /**
     * The leg holding the value at the given value index, or {@link #NO_TAG} where there is none. A value index is a
     * sub-block position: the block position for a single-valued column, otherwise from
     * {@link Block#getFirstValueIndex(int)}.
     */
    int tag(int valueIndex);

    /** One leg, whole. Ownership stays with this block, so the caller must not close it. */
    Block legBlock(int leg);

    sealed interface Builder extends Block.Builder permits UnionBlockBuilder {

        /**
         * Append the value at {@code sourceValueIndex} of {@code source} as a value of leg {@code leg}, tagging it and
         * leaving every other leg null. {@code source} must have that leg's element type.
         */
        Builder appendValue(int leg, Block source, int sourceValueIndex);

        @Override
        UnionBlock build();
    }
}
