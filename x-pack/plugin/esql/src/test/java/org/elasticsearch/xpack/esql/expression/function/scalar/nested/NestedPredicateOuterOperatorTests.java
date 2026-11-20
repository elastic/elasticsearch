/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.nested;

import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.SourceOperator;
import org.elasticsearch.compute.test.CannedSourceOperator;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;

import java.util.ArrayList;
import java.util.List;

/**
 * The predicate compares each child with a parent column, {@code limit}, which is {@code 10 * i + 1}, so a parent matches when it
 * has a child priced above that: the third child of a parent with at least three, and for a parent with two none.
 */
public class NestedPredicateOuterOperatorTests extends NestedPredicateOperatorTestCase {
    private static final ReferenceAttribute LIMIT = new ReferenceAttribute(Source.EMPTY, "limit", DataType.LONG);
    private static final ReferenceAttribute OUTER = new ReferenceAttribute(Source.EMPTY, "$$outer", DataType.LONG);

    @Override
    NestedPredicate predicate() {
        return nested(priceAbove(OUTER), List.of(OUTER), List.of(LIMIT), null);
    }

    @Override
    boolean matches(int parent) {
        return highestPrice(parent) > 10L * parent + 1;
    }

    @Override
    List<Attribute> extraParentColumns() {
        return List.of(LIMIT);
    }

    /** The parents in pages of random size, each with its own limit in the block after the documents. */
    @Override
    protected SourceOperator simpleInput(BlockFactory blockFactory, int size) {
        List<Page> pages = new ArrayList<>();
        int parents = Math.min(size, PARENTS);
        for (int start = 0; start < parents;) {
            int count = Math.min(between(1, 200), parents - start);
            Page docs = parents(blockFactory, start, count);
            LongBlock.Builder limits = blockFactory.newLongBlockBuilder(count);
            for (int i = start; i < start + count; i++) {
                limits.appendLong(10L * i + 1);
            }
            pages.add(docs.appendBlock(limits.build()));
            start += count;
        }
        return new CannedSourceOperator(pages.iterator());
    }
}
