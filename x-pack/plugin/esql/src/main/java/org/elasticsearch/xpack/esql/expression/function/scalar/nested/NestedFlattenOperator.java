/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.expression.function.scalar.nested;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.DocBlock;
import org.elasticsearch.compute.data.DocVector;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.FloatBlock;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.AbstractPageMappingOperator;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.planner.PlannerUtils;

import java.util.List;

/**
 * Packs the objects of a mapped nested field onto the parent row. One input position stays one output position, and each
 * child column holds every object's value, in child-document order. A parent without objects keeps nulls. The child pages
 * come from {@link NestedChildren}, the same reader {@code NESTED_EXPAND} uses.
 */
public final class NestedFlattenOperator extends AbstractPageMappingOperator {
    public record Factory(int docChannel, @Nullable NestedChildren.Factory children, String path, List<DataType> types)
        implements
            Operator.OperatorFactory {
        @Override
        public Operator get(DriverContext context) {
            return new NestedFlattenOperator(context, this);
        }

        @Override
        public String describe() {
            return "NestedFlattenOperator[path=" + path + "]";
        }
    }

    private final DriverContext context;
    private final Factory factory;
    @Nullable
    private final NestedChildren children;

    private NestedFlattenOperator(DriverContext context, Factory factory) {
        this.context = context;
        this.factory = factory;
        this.children = factory.children() == null || factory.types().isEmpty() ? null : factory.children().get(context);
    }

    @Override
    protected Page process(Page page) {
        if (children == null) {
            return page;
        }
        int columns = factory.types().size();
        Block.Builder[] built = new Block.Builder[columns];
        Block[] blocks = null;
        try {
            for (int c = 0; c < columns; c++) {
                built[c] = PlannerUtils.toElementType(factory.types().get(c))
                    .newBlockBuilder(page.getPositionCount(), context.blockFactory());
            }
            var rows = new Rows(built);
            DocVector docs = page.<DocBlock>getBlock(factory.docChannel()).asVector();
            children.read(docs, position -> true, rows::add);
            rows.finish(page.getPositionCount());
            blocks = new Block[columns];
            for (int c = 0; c < columns; c++) {
                blocks[c] = built[c].build();
                built[c] = null;
            }
            Block[] owned = blocks;
            blocks = null;
            return page.appendBlocks(owned);
        } finally {
            Releasables.closeExpectNoException(built);
            if (blocks != null) {
                Releasables.closeExpectNoException(blocks);
            }
        }
    }

    /**
     * Writes one position per parent. A parent's values stay open across the child pages {@link NestedChildren} returns,
     * since one parent's objects can start on one page and end on the next.
     */
    private static final class Rows {
        private final Block.Builder[] built;
        private final boolean[] open;
        private final BytesRef scratch = new BytesRef();
        /** The parent whose position is open, or -1. */
        private int current = -1;
        /** Positions before this one are written. */
        private int next;

        private Rows(Block.Builder[] built) {
            this.built = built;
            this.open = new boolean[built.length];
        }

        private void add(Page loaded) {
            IntVector parents = loaded.<IntBlock>getBlock(0).asVector();
            for (int c = 0; c < loaded.getPositionCount(); c++) {
                int parent = parents.getInt(c);
                if (parent != current) {
                    fillTo(parent);
                    current = parent;
                }
                for (int column = 0; column < built.length; column++) {
                    Block values = loaded.getBlock(1 + column);
                    if (values.isNull(c)) {
                        continue;
                    }
                    if (open[column] == false) {
                        built[column].beginPositionEntry();
                        open[column] = true;
                    }
                    append(built[column], values, c, scratch);
                }
            }
        }

        /** Closes the open parent and writes nulls for every parent before {@code end}. */
        private void fillTo(int end) {
            if (current >= 0) {
                closeOpen();
                next = current + 1;
                current = -1;
            }
            for (; next < end; next++) {
                for (Block.Builder builder : built) {
                    builder.appendNull();
                }
            }
        }

        private void finish(int positions) {
            fillTo(positions);
        }

        private void closeOpen() {
            for (int column = 0; column < built.length; column++) {
                if (open[column]) {
                    built[column].endPositionEntry();
                    open[column] = false;
                } else {
                    built[column].appendNull();
                }
            }
        }
    }

    private static void append(Block.Builder builder, Block values, int position, BytesRef scratch) {
        int first = values.getFirstValueIndex(position);
        int count = values.getValueCount(position);
        switch (values.elementType()) {
            case BOOLEAN -> {
                BooleanBlock.Builder booleans = (BooleanBlock.Builder) builder;
                BooleanBlock block = (BooleanBlock) values;
                for (int v = first; v < first + count; v++) {
                    booleans.appendBoolean(block.getBoolean(v));
                }
            }
            case INT -> {
                IntBlock.Builder ints = (IntBlock.Builder) builder;
                IntBlock block = (IntBlock) values;
                for (int v = first; v < first + count; v++) {
                    ints.appendInt(block.getInt(v));
                }
            }
            case LONG -> {
                LongBlock.Builder longs = (LongBlock.Builder) builder;
                LongBlock block = (LongBlock) values;
                for (int v = first; v < first + count; v++) {
                    longs.appendLong(block.getLong(v));
                }
            }
            case DOUBLE -> {
                DoubleBlock.Builder doubles = (DoubleBlock.Builder) builder;
                DoubleBlock block = (DoubleBlock) values;
                for (int v = first; v < first + count; v++) {
                    doubles.appendDouble(block.getDouble(v));
                }
            }
            case FLOAT -> {
                FloatBlock.Builder floats = (FloatBlock.Builder) builder;
                FloatBlock block = (FloatBlock) values;
                for (int v = first; v < first + count; v++) {
                    floats.appendFloat(block.getFloat(v));
                }
            }
            case BYTES_REF -> {
                BytesRefBlock.Builder bytes = (BytesRefBlock.Builder) builder;
                BytesRefBlock block = (BytesRefBlock) values;
                for (int v = first; v < first + count; v++) {
                    bytes.appendBytesRef(block.getBytesRef(v, scratch));
                }
            }
            default -> throw new IllegalArgumentException("cannot flatten [" + values.elementType() + "]");
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
