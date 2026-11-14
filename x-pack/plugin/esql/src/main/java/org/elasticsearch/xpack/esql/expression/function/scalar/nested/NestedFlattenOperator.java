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
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.AbstractPageMappingOperator;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.core.Releasables;

/**
 * Reads a field under a nested object as an ordinary column: one multivalued position per parent holding the values of
 * all of its objects, in document order. Field extraction loads nulls for such a field, as the parent document holds
 * none of its values; this replaces them on the shards that map the field under a nested object and keeps what
 * extraction loaded on the others.
 */
public final class NestedFlattenOperator extends AbstractPageMappingOperator {
    public record Factory(int docChannel, int channel, ElementType elementType, NestedChildren.Factory children)
        implements
            Operator.OperatorFactory {
        @Override
        public Operator get(DriverContext context) {
            return new NestedFlattenOperator(context, this, children.get(context));
        }

        @Override
        public String describe() {
            return "NestedFlattenOperator[channel=" + channel + "]";
        }
    }

    /** Whether values of {@code elementType} can be flattened. */
    static boolean canFlatten(ElementType elementType) {
        return switch (elementType) {
            case BOOLEAN, INT, LONG, DOUBLE, BYTES_REF -> true;
            default -> false;
        };
    }

    private final DriverContext context;
    private final Factory factory;
    private final NestedChildren children;

    private NestedFlattenOperator(DriverContext context, Factory factory, NestedChildren children) {
        this.context = context;
        this.factory = factory;
        this.children = children;
    }

    @Override
    protected Page process(Page page) {
        DocVector docs = ((DocBlock) page.getBlock(factory.docChannel())).asVector();
        if (hasChildren(docs) == false) {
            return page;
        }
        Block flattened = null;
        try {
            flattened = flatten(docs, page.getBlock(factory.channel()));
            Block[] blocks = new Block[page.getBlockCount()];
            for (int b = 0; b < blocks.length; b++) {
                blocks[b] = b == factory.channel() ? flattened : page.getBlock(b);
            }
            Page result = new Page(page.getPositionCount(), blocks);
            page.getBlock(factory.channel()).close();
            flattened = null;
            page = null;
            return result;
        } finally {
            Releasables.closeExpectNoException(flattened);
            if (page != null) {
                page.releaseBlocks();
            }
        }
    }

    private boolean hasChildren(DocVector docs) {
        int last = -1;
        for (int p = 0; p < docs.getPositionCount(); p++) {
            int shard = docs.shards().getInt(p);
            if (shard != last && children.hasChildren(shard)) {
                return true;
            }
            last = shard;
        }
        return false;
    }

    private Block flatten(DocVector docs, Block loaded) {
        try (Block.Builder builder = factory.elementType().newBlockBuilder(docs.getPositionCount(), context.blockFactory())) {
            var rows = new Object() {
                final BytesRef scratch = new BytesRef();
                /** Positions before this one are written. */
                int next;
                /** The parent whose values are being written, or -1. */
                int open = -1;
                boolean started;

                void add(Page page) {
                    IntVector parents = page.<IntBlock>getBlock(0).asVector();
                    Block values = page.getBlock(1);
                    for (int c = 0; c < page.getPositionCount(); c++) {
                        int parent = parents.getInt(c);
                        if (parent != open) {
                            fillTo(parent);
                            open = parent;
                        }
                        if (values.isNull(c)) {
                            continue;
                        }
                        if (started == false) {
                            builder.beginPositionEntry();
                            started = true;
                        }
                        int first = values.getFirstValueIndex(c);
                        for (int v = first; v < first + values.getValueCount(c); v++) {
                            append(builder, values, v, scratch);
                        }
                    }
                }

                /** Writes every position before {@code end}: the parents of shards without children keep what was loaded. */
                void fillTo(int end) {
                    if (open >= 0) {
                        if (started) {
                            builder.endPositionEntry();
                        } else {
                            builder.appendNull();
                        }
                        next = open + 1;
                        open = -1;
                        started = false;
                    }
                    for (; next < end; next++) {
                        if (children.hasChildren(docs.shards().getInt(next))) {
                            builder.appendNull();
                        } else {
                            builder.copyFrom(loaded, next, next + 1);
                        }
                    }
                }
            };
            children.read(docs, p -> true, rows::add);
            rows.fillTo(docs.getPositionCount());
            return builder.build();
        }
    }

    private static void append(Block.Builder builder, Block values, int v, BytesRef scratch) {
        switch (values.elementType()) {
            case BOOLEAN -> ((BooleanBlock.Builder) builder).appendBoolean(((BooleanBlock) values).getBoolean(v));
            case INT -> ((IntBlock.Builder) builder).appendInt(((IntBlock) values).getInt(v));
            case LONG -> ((LongBlock.Builder) builder).appendLong(((LongBlock) values).getLong(v));
            case DOUBLE -> ((DoubleBlock.Builder) builder).appendDouble(((DoubleBlock) values).getDouble(v));
            case BYTES_REF -> ((BytesRefBlock.Builder) builder).appendBytesRef(((BytesRefBlock) values).getBytesRef(v, scratch));
            default -> throw new IllegalArgumentException("cannot flatten [" + values.elementType() + "]");
        }
    }

    @Override
    public String toString() {
        return "NestedFlattenOperator[channel=" + factory.channel() + "]";
    }

    @Override
    public void close() {
        Releasables.closeExpectNoException(children, super::close);
    }
}
