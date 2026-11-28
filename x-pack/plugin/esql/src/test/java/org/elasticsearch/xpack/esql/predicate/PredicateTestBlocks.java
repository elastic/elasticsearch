/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.predicate;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.LongBlock;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.IntStream;

/** Builds the blocks a test source loads and reads the three-valued blocks a predicate returns. */
final class PredicateTestBlocks {
    private PredicateTestBlocks() {}

    /** The value of {@code field} at each selected position, compacted into selection order. A null value is a null position. */
    static Block column(BlockFactory factory, FieldRef field, Object[] values, int[] selection) {
        return switch (field.type()) {
            case KEYWORD -> {
                try (BytesRefBlock.Builder builder = factory.newBytesRefBlockBuilder(selection.length)) {
                    for (int position : selection) {
                        if (values[position] == null) {
                            builder.appendNull();
                        } else {
                            builder.appendBytesRef(new BytesRef((String) values[position]));
                        }
                    }
                    yield builder.build();
                }
            }
            case LONG -> {
                try (LongBlock.Builder builder = factory.newLongBlockBuilder(selection.length)) {
                    for (int position : selection) {
                        if (values[position] == null) {
                            builder.appendNull();
                        } else {
                            builder.appendLong((Long) values[position]);
                        }
                    }
                    yield builder.build();
                }
            }
            default -> throw new IllegalArgumentException("no test column for [" + field.type() + "]");
        };
    }

    /** {@code true}, {@code false}, or {@code null} for each position of the block. */
    static List<Boolean> values(BooleanBlock block) {
        List<Boolean> values = new ArrayList<>(block.getPositionCount());
        for (int position = 0; position < block.getPositionCount(); position++) {
            values.add(block.isNull(position) ? null : block.getBoolean(block.getFirstValueIndex(position)));
        }
        return values;
    }

    static List<Boolean> tri(Boolean... values) {
        return Arrays.asList(values);
    }

    static int[] all(int positionCount) {
        return IntStream.range(0, positionCount).toArray();
    }

    /** A source that can neither produce candidates nor refine a predicate, so the engine evaluates every leaf itself. */
    static PredicateSource scanOnly() {
        return new PredicateSource() {
            @Override
            public PredicateCapability analyze(PredicateExpr predicate) {
                return PredicateCapability.unsupported();
            }

            @Override
            public CandidateCursor open(CandidateConstraint constraint, Projection projection) {
                throw new UnsupportedOperationException();
            }

            @Override
            public Optional<PredicateRefiner> refiner(PredicateExpr predicate) {
                return Optional.empty();
            }
        };
    }
}
