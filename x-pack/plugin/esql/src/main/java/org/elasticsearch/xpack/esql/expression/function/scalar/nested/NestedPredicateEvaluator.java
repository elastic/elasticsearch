/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.nested;

import org.apache.lucene.search.Query;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockUtils;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.DocBlock;
import org.elasticsearch.compute.data.DocVector;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.compute.lucene.IndexedByShardId;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasables;

/**
 * Evaluates a {@link NestedPredicate} for each parent of a page by reading the parent's child documents through
 * {@link NestedChildren}, running the body over each child, and ORing the results per parent. A parent stops being read once
 * a page of its children has matched it.
 * <p>
 * This is the production child cursor. The predicate prototype describes the same shape and stays unused here: the batch
 * is this page, and the body is the expression evaluator a {@code FilterExec} would run.
 */
final class NestedPredicateEvaluator implements ExpressionEvaluator {
    private final DriverContext context;
    private final NestedChildren children;
    private final ExpressionEvaluator predicate;
    private final int[] fieldChannels;
    @Nullable
    private final IndexedByShardId<Query> matchingChildren;

    NestedPredicateEvaluator(
        DriverContext context,
        NestedChildren children,
        ExpressionEvaluator predicate,
        int[] fieldChannels,
        @Nullable IndexedByShardId<Query> matchingChildren
    ) {
        this.context = context;
        this.children = children;
        this.predicate = predicate;
        this.fieldChannels = fieldChannels;
        this.matchingChildren = matchingChildren;
    }

    @Override
    public Block eval(Page page) {
        DocVector docs = docs(page);
        boolean[] matched = new boolean[docs.getPositionCount()];
        children.read(docs, position -> true, position -> matched[position] == false, matchingChildren, loaded -> accept(loaded, matched));
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

    private void accept(Page loaded, boolean[] matched) {
        IntVector parents = loaded.<IntBlock>getBlock(0).asVector();
        if (parents == null) {
            throw new IllegalStateException("NESTED_ANY expected one parent position per child");
        }
        if (fieldChannels.length == 0) {
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
        Page fields = loaded.projectBlocks(fieldChannels);
        Block values = null;
        try {
            values = predicate.eval(fields);
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
            fields.releaseBlocks();
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
        Releasables.close(predicate, children);
    }
}
