/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.nested;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockUtils;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.core.Releasables;

import java.io.IOException;
import java.io.UncheckedIOException;

/**
 * Evaluates a {@code NESTED_ANY} body over the child documents that a container column carries: true for a parent when the body
 * is true for at least one child of any of its values. As for a {@code WHERE}, a null or multivalued result is like false.
 */
final class NestedContainerPredicateEvaluator implements ExpressionEvaluator {
    private final DriverContext context;
    private final ExpressionEvaluator container;
    private final ExpressionEvaluator predicate;
    private final int inputs;

    NestedContainerPredicateEvaluator(DriverContext context, ExpressionEvaluator container, ExpressionEvaluator predicate, int inputs) {
        this.context = context;
        this.container = container;
        this.predicate = predicate;
        this.inputs = inputs;
    }

    @Override
    public Block eval(Page page) {
        try (
            BytesRefBlock carried = (BytesRefBlock) container.eval(page);
            BooleanBlock.Builder matches = context.blockFactory().newBooleanBlockBuilder(page.getPositionCount())
        ) {
            BytesRef scratch = new BytesRef();
            for (int p = 0; p < page.getPositionCount(); p++) {
                boolean matched = false;
                int values = carried.getValueCount(p);
                for (int v = 0; v < values && matched == false; v++) {
                    try (
                        Page children = NestedContainerCodec.decode(
                            context.blockFactory(),
                            carried.getBytesRef(carried.getFirstValueIndex(p) + v, scratch)
                        )
                    ) {
                        matched = anyChildMatches(children);
                    }
                }
                matches.appendBoolean(matched);
            }
            return matches.build();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private boolean anyChildMatches(Page children) {
        if (inputs == 0) {
            if (children.getPositionCount() == 0) {
                return false;
            }
            try (Block constant = predicate.eval(new Page(1))) {
                return Boolean.TRUE.equals(BlockUtils.toJavaObject(constant, 0));
            }
        }
        try (Block values = predicate.eval(children)) {
            for (int c = 0; c < values.getPositionCount(); c++) {
                if (values.getValueCount(c) == 1 && Boolean.TRUE.equals(BlockUtils.toJavaObject(values, c))) {
                    return true;
                }
            }
            return false;
        }
    }

    @Override
    public long baseRamBytesUsed() {
        return predicate.baseRamBytesUsed() + container.baseRamBytesUsed();
    }

    @Override
    public void close() {
        Releasables.close(predicate, container);
    }
}
