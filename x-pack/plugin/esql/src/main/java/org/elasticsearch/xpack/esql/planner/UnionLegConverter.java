/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.planner;

import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.UnionBlock;
import org.elasticsearch.compute.lucene.read.ValuesSourceReaderOperator;
import org.elasticsearch.compute.operator.DriverContext;

import java.util.List;
import java.util.Objects;

/**
 * Wraps one shard's natively loaded values into a union column, tagging every value with the leg that shard's own
 * mapping puts it in.
 * <p>
 * Nothing is converted, which is the whole difference between this and {@link TypeConverter}: a union holds each value
 * as the type its source gave it, so the shard reads its field with its own loader and this only records which leg the
 * values landed in. Every shard therefore contributes to one leg, and a page spanning shards of different mappings
 * carries values of several.
 * </p>
 */
class UnionLegConverter implements ValuesSourceReaderOperator.ConverterFactory {

    private final List<ElementType> legElementTypes;
    private final int leg;

    UnionLegConverter(List<ElementType> legElementTypes, int leg) {
        if (leg < 0 || leg >= legElementTypes.size()) {
            throw new IllegalArgumentException("leg " + leg + " out of range for " + legElementTypes.size() + " legs");
        }
        this.legElementTypes = legElementTypes;
        this.leg = leg;
    }

    @Override
    public ValuesSourceReaderOperator.ConverterEvaluator build(DriverContext context) {
        return new Evaluator(context.blockFactory(), legElementTypes, leg);
    }

    private record Evaluator(BlockFactory blockFactory, List<ElementType> legElementTypes, int leg)
        implements
            ValuesSourceReaderOperator.ConverterEvaluator {

        @Override
        public Block convert(Block loaded) {
            // The converter owns the block it is handed. TypeConverter's own loader hands the page's block straight
            // through without a ref count and the generated convert evaluators close it, so this must too.
            try (Block block = loaded) {
                int positionCount = block.getPositionCount();
                if (block.areAllValuesNull()) {
                    // A whole page with nothing for this field. The loaded block may be typed rather than a null
                    // block, and its element type would then not be UNION, so a constant null is both cheaper than k
                    // null legs and the shape the renderers already handle for a union with no legs.
                    return blockFactory.newConstantNullBlock(positionCount);
                }
                try (UnionBlock.Builder builder = blockFactory.newUnionBlockBuilder(legElementTypes, positionCount)) {
                    for (int p = 0; p < positionCount; p++) {
                        int valueCount = block.getValueCount(p);
                        if (valueCount == 0) {
                            builder.appendNull();
                            continue;
                        }
                        if (valueCount > 1) {
                            builder.beginPositionEntry();
                        }
                        int first = block.getFirstValueIndex(p);
                        for (int v = 0; v < valueCount; v++) {
                            // By value index, not by position: a multi-valued field is ordinary here, since a shard
                            // may map the field as an array of its own type.
                            builder.appendValue(leg, block, first + v);
                        }
                        if (valueCount > 1) {
                            builder.endPositionEntry();
                        }
                    }
                    return builder.build();
                }
            }
        }

        @Override
        public void close() {}
    }

    @Override
    public String toString() {
        return "UnionLeg[" + leg + " of " + legElementTypes + "]";
    }

    @Override
    public boolean equals(Object o) {
        if (o == this) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        UnionLegConverter that = (UnionLegConverter) o;
        return leg == that.leg && legElementTypes.equals(that.legElementTypes);
    }

    @Override
    public int hashCode() {
        return Objects.hash(legElementTypes, leg);
    }
}
