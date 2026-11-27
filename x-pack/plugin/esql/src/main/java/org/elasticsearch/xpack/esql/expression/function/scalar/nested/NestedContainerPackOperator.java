/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.nested;

import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.DocBlock;
import org.elasticsearch.compute.data.DocVector;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.AbstractPageMappingOperator;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.planner.PlannerUtils;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;

/**
 * Appends a container column: for each parent, its child documents as one value written by {@link NestedContainerCodec}, or
 * null for a parent without children. One input position stays one output position. The children come from
 * {@link NestedChildren}, the reader {@code NESTED_EXPAND} uses, so a node that has no access to the parent's index can still
 * read them through the column.
 */
public final class NestedContainerPackOperator extends AbstractPageMappingOperator {
    public record Factory(int docChannel, @Nullable NestedChildren.Factory children, List<DataType> types)
        implements
            Operator.OperatorFactory {
        @Override
        public Operator get(DriverContext context) {
            return new NestedContainerPackOperator(context, this);
        }

        @Override
        public String describe() {
            return "NestedContainerPackOperator[columns=" + types.size() + "]";
        }
    }

    private final DriverContext context;
    private final Factory factory;
    @Nullable
    private final NestedChildren children;

    private NestedContainerPackOperator(DriverContext context, Factory factory) {
        this.context = context;
        this.factory = factory;
        this.children = factory.children() == null ? null : factory.children().get(context);
    }

    @Override
    protected Page process(Page page) {
        Block container = null;
        Page result = null;
        try {
            container = pack(page);
            result = page.appendBlocks(new Block[] { container });
            container = null;
            return result;
        } finally {
            if (result == null) {
                Releasables.closeExpectNoException(container);
                page.releaseBlocks();
            }
        }
    }

    private Block pack(Page page) {
        int columns = factory.types().size();
        try (BytesRefBlock.Builder packed = context.blockFactory().newBytesRefBlockBuilder(page.getPositionCount())) {
            if (children == null) {
                for (int p = 0; p < page.getPositionCount(); p++) {
                    packed.appendNull();
                }
                return packed.build();
            }
            DocVector docs = ((DocBlock) page.getBlock(factory.docChannel())).asVector();
            Block.Builder[] current = new Block.Builder[columns];
            var state = new Object() {
                int next;
                int parent = -1;
                int rows;

                void open(int newParent) {
                    for (int c = 0; c < columns; c++) {
                        current[c] = PlannerUtils.toElementType(factory.types().get(c)).newBlockBuilder(1, context.blockFactory());
                    }
                    parent = newParent;
                    rows = 0;
                }

                void flush() {
                    if (parent < 0) {
                        return;
                    }
                    for (; next < parent; next++) {
                        packed.appendNull();
                    }
                    Block[] blocks = new Block[columns];
                    try {
                        for (int c = 0; c < columns; c++) {
                            blocks[c] = current[c].build();
                        }
                        try (Page children = new Page(rows, blocks)) {
                            blocks = null;
                            packed.appendBytesRef(NestedContainerCodec.encode(children));
                        }
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    } finally {
                        Releasables.closeExpectNoException(current);
                        if (blocks != null) {
                            Releasables.closeExpectNoException(blocks);
                        }
                    }
                    next = parent + 1;
                    parent = -1;
                }
            };
            try {
                children.read(docs, p -> true, loaded -> {
                    IntVector parents = loaded.<IntBlock>getBlock(0).asVector();
                    for (int c = 0; c < loaded.getPositionCount(); c++) {
                        int parent = parents.getInt(c);
                        if (parent != state.parent) {
                            state.flush();
                            state.open(parent);
                        }
                        for (int column = 0; column < columns; column++) {
                            current[column].copyFrom(loaded.getBlock(column + 1), c, c + 1);
                        }
                        state.rows++;
                    }
                });
                state.flush();
            } finally {
                Releasables.closeExpectNoException(current);
            }
            for (; state.next < page.getPositionCount(); state.next++) {
                packed.appendNull();
            }
            return packed.build();
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
