/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.nested;

import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockUtils;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.DocBlock;
import org.elasticsearch.compute.data.DocVector;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.core.Releasables;

/**
 * Evaluates a {@link NestedPredicate} for each parent of a page by reading the parent's child documents through
 * {@link NestedChildren}, running the body over each child, and ORing the results per parent. Each outer expression is
 * evaluated once on the parent page and its value is repeated for every child of that parent. A parent stops being read once
 * a page of its children has matched it.
 */
final class NestedPredicateEvaluator implements ExpressionEvaluator {
    private final DriverContext context;
    private final NestedChildren children;
    private final ExpressionEvaluator predicate;
    private final ExpressionEvaluator[] outers;
    private final int[] fieldChannels;

    NestedPredicateEvaluator(
        DriverContext context,
        NestedChildren children,
        ExpressionEvaluator predicate,
        ExpressionEvaluator[] outers,
        int[] fieldChannels
    ) {
        this.context = context;
        this.children = children;
        this.predicate = predicate;
        this.outers = outers;
        this.fieldChannels = fieldChannels;
    }

    @Override
    public Block eval(Page page) {
        DocVector docs = docs(page);
        boolean[] matched = new boolean[docs.getPositionCount()];
        Block[] outerBlocks = new Block[outers.length];
        try {
            for (int i = 0; i < outers.length; i++) {
                outerBlocks[i] = outers[i].eval(page);
            }
            children.read(docs, position -> true, position -> matched[position] == false, loaded -> accept(loaded, matched, outerBlocks));
        } finally {
            Releasables.closeWhileHandlingException(outerBlocks);
        }
        BooleanBlock.Builder builder = context.blockFactory().newBooleanBlockBuilder(matched.length);
        Block result = null;
        try {
            for (boolean bit : matched) {
                builder.appendBoolean(bit);
            }
            result = builder.build();
            return result;
        } finally {
            if (result == null) {
                builder.close();
            }
        }
    }

    private void accept(Page loaded, boolean[] matched, Block[] outerBlocks) {
        IntVector parents = loaded.<IntBlock>getBlock(0).asVector();
        if (parents == null) {
            throw new IllegalStateException("NESTED_ANY expected one parent position per child");
        }
        if (fieldChannels.length == 0 && outerBlocks.length == 0) {
            Block constant = predicate.eval(new Page(1));
            try {
                boolean childMatched = Boolean.TRUE.equals(BlockUtils.toJavaObject(constant, 0));
                for (int c = 0; c < parents.getPositionCount(); c++) {
                    matched[parents.getInt(c)] |= childMatched;
                }
            } finally {
                constant.close();
            }
            return;
        }
        Page inputs = bodyInput(loaded, parents, outerBlocks);
        Block values = null;
        try {
            values = predicate.eval(inputs);
            for (int c = 0; c < values.getPositionCount(); c++) {
                // As for a WHERE, a null or multivalued result is like false
                if (values.getValueCount(c) != 1) {
                    continue;
                }
                boolean childMatched = Boolean.TRUE.equals(BlockUtils.toJavaObject(values, c));
                matched[parents.getInt(c)] |= childMatched;
            }
        } finally {
            if (values != null) {
                values.close();
            }
            inputs.releaseBlocks();
        }
    }

    /** The child fields of {@code loaded}, then each outer value repeated for every child of its parent. */
    private Page bodyInput(Page loaded, IntVector parents, Block[] outerBlocks) {
        int[] parentPositions = new int[parents.getPositionCount()];
        for (int c = 0; c < parentPositions.length; c++) {
            parentPositions[c] = parents.getInt(c);
        }
        Block[] blocks = new Block[fieldChannels.length + outerBlocks.length];
        boolean built = false;
        try {
            for (int i = 0; i < fieldChannels.length; i++) {
                blocks[i] = loaded.getBlock(fieldChannels[i]);
                blocks[i].incRef();
            }
            for (int i = 0; i < outerBlocks.length; i++) {
                blocks[fieldChannels.length + i] = outerBlocks[i].filter(true, parentPositions);
            }
            Page page = new Page(loaded.getPositionCount(), blocks);
            built = true;
            return page;
        } finally {
            if (built == false) {
                Releasables.closeWhileHandlingException(blocks);
            }
        }
    }

    private static DocVector docs(Page page) {
        for (int i = 0; i < page.getBlockCount(); i++) {
            if (page.getBlock(i) instanceof DocBlock doc) {
                return doc.asVector();
            }
        }
        throw new IllegalStateException("NESTED_ANY needs the parent documents, which this page does not carry");
    }

    @Override
    public long baseRamBytesUsed() {
        return predicate.baseRamBytesUsed();
    }

    @Override
    public void close() {
        Releasables.close(predicate, children, Releasables.wrap(outers));
    }
}
