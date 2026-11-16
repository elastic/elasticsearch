/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.expression.function.scalar.nested;

import org.apache.lucene.util.ArrayUtil;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockUtils;
import org.elasticsearch.compute.data.DocBlock;
import org.elasticsearch.compute.data.DocVector;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.AbstractPageMappingOperator;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.planner.PlannerUtils;

import java.util.ArrayList;
import java.util.List;

/**
 * Emits one row per object of a mapped nested field. Parent blocks are repeated once per object; one block per child
 * field is appended. A parent without objects keeps one row with null child values. Each input page becomes one output
 * page, so its size grows with the number of objects. With {@code flatten} each parent keeps its one row instead, and each
 * child block holds the values of all of its objects.
 */
public final class NestedExpandOperator extends AbstractPageMappingOperator {
    static final String BREAKER_LABEL = "nested_expand";

    public record Factory(int docChannel, NestedChildren.Factory children, String path, List<DataType> types, boolean flatten)
        implements
            Operator.OperatorFactory {
        @Override
        public Operator get(DriverContext context) {
            return new NestedExpandOperator(context, this);
        }

        @Override
        public String describe() {
            return "NestedExpandOperator[path=" + path + (flatten ? ", flatten" : "") + "]";
        }
    }

    private final DriverContext context;
    private final Factory factory;
    private final NestedChildren children;

    private NestedExpandOperator(DriverContext context, Factory factory) {
        this.context = context;
        this.factory = factory;
        this.children = factory.children().get(context);
    }

    @Override
    protected Page process(Page page) {
        try {
            return expand(page);
        } finally {
            page.releaseBlocks();
        }
    }

    /** Copies each parent's child rows from the pages the index gives, with one null row for a parent without children. */
    private Page expand(Page page) {
        int columns = factory.types().size();
        Block.Builder[] built = new Block.Builder[columns];
        Block[] blocks = new Block[columns];
        // The positions grow with the children of the page, and no block may account for them
        var rows = new Object() {
            int[] positions = new int[0];
            long reserved;
            int count;
            int next;

            void add(int position) {
                if (count == positions.length) {
                    int size = ArrayUtil.oversize(count + 1, Integer.BYTES);
                    long bytes = (long) (size - positions.length) * Integer.BYTES;
                    context.breaker().addEstimateBytesAndMaybeBreak(bytes, BREAKER_LABEL);
                    reserved += bytes;
                    positions = ArrayUtil.growExact(positions, size);
                }
                positions[count++] = position;
            }

            /** One null row for each parent before {@code parent}, which have no children. */
            void fillTo(int parent) {
                for (; next < parent; next++) {
                    add(next);
                    for (Block.Builder builder : built) {
                        builder.appendNull();
                    }
                }
            }
        };
        try {
            for (int c = 0; c < columns; c++) {
                built[c] = PlannerUtils.toElementType(factory.types().get(c))
                    .newBlockBuilder(page.getPositionCount(), context.blockFactory());
            }
            DocVector docs = ((DocBlock) page.getBlock(factory.docChannel())).asVector();
            children.read(docs, loaded -> {
                IntVector parentPositions = loaded.<IntBlock>getBlock(0).asVector();
                int start = 0;
                for (int c = 0; c < loaded.getPositionCount(); c++) {
                    int parent = parentPositions.getInt(c);
                    if (parent >= rows.next) {
                        copy(loaded, built, start, c);
                        start = c;
                        rows.fillTo(parent);
                        rows.next = parent + 1;
                    }
                    rows.add(parent);
                }
                copy(loaded, built, start, loaded.getPositionCount());
            });
            rows.fillTo(page.getPositionCount());
            for (int c = 0; c < columns; c++) {
                blocks[c] = built[c].build();
            }
            Block[] owned = blocks;
            blocks = null;
            return output(page, rows.positions, rows.count, owned);
        } finally {
            context.breaker().addWithoutBreaking(-rows.reserved);
            Releasables.closeExpectNoException(built);
            if (blocks != null) {
                Releasables.closeExpectNoException(blocks);
            }
        }
    }

    private static void copy(Page loaded, Block.Builder[] built, int from, int to) {
        if (from < to) {
            for (int c = 0; c < built.length; c++) {
                built[c].copyFrom(loaded.getBlock(c + 1), from, to);
            }
        }
    }

    /**
     * Appends {@code blocks}, which hold one row per object of the parent at the same index of {@code positions}, to the
     * parents. Takes ownership of {@code blocks}.
     */
    private Page output(Page page, int[] positions, int count, Block[] blocks) {
        Page parents = null;
        try {
            if (factory.flatten()) {
                Block[] collapsed = collapse(blocks, positions, count, page.getPositionCount());
                Releasables.closeExpectNoException(blocks);
                blocks = collapsed;
                parents = page.shallowCopy();
            } else {
                parents = page.filter(true, positions, 0, count);
            }
            Page result = parents.appendBlocks(blocks);
            parents = null;
            blocks = null;
            return result;
        } finally {
            if (blocks != null) {
                Releasables.closeExpectNoException(blocks);
            }
            if (parents != null) {
                parents.releaseBlocks();
            }
        }
    }

    /**
     * Merges the rows of each parent into one multivalued position. {@code parents} is ascending and names every parent
     * at least once.
     */
    private Block[] collapse(Block[] perObject, int[] parents, int count, int positionCount) {
        Block[] result = new Block[perObject.length];
        boolean success = false;
        try {
            List<Object> values = new ArrayList<>();
            for (int c = 0; c < perObject.length; c++) {
                ElementType type = PlannerUtils.toElementType(factory.types().get(c));
                try (BlockUtils.BuilderWrapper builder = BlockUtils.wrapperFor(context.blockFactory(), type, positionCount)) {
                    int row = 0;
                    for (int parent = 0; parent < positionCount; parent++) {
                        values.clear();
                        for (; row < count && parents[row] == parent; row++) {
                            Object value = BlockUtils.toJavaObject(perObject[c], row);
                            if (value instanceof List<?> list) {
                                values.addAll(list);
                            } else if (value != null) {
                                values.add(value);
                            }
                        }
                        builder.accept(values.isEmpty() ? null : values.size() == 1 ? values.getFirst() : values);
                    }
                    result[c] = builder.builder().build();
                }
            }
            success = true;
            return result;
        } finally {
            if (success == false) {
                Releasables.closeExpectNoException(result);
            }
        }
    }

    @Override
    public String toString() {
        return factory.describe();
    }

    @Override
    public void close() {
        Releasables.closeExpectNoException(children, super::close);
    }
}
