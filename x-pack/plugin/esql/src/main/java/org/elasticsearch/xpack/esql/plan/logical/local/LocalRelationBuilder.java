/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.plan.logical.local;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * Builds a {@link LocalRelation} a column at a time: a column is declared and the declaration hands back something
 * that appends values of that column's type and nothing else.
 * <p>
 * Declaring and writing are one act on purpose. A schema stated separately from the values has to be checked against
 * them — the arity per row, the Java type per cell, the element type of each block against the declared
 * {@link DataType} — and all of that is checking at runtime what a type can state once.
 * <p>
 * {@code owner} names whoever is building, and appears in the exception a misuse raises, since every one of them is
 * a bug in that caller rather than anything a query can provoke.
 */
public final class LocalRelationBuilder implements Releasable {

    private final String owner;
    private final BlockFactory blockFactory;
    private final List<Attribute> columns = new ArrayList<>();
    private final List<Block.Builder> builders = new ArrayList<>();
    private final Set<String> names = new LinkedHashSet<>();
    private boolean built;

    public LocalRelationBuilder(String owner, BlockFactory blockFactory) {
        this.owner = owner;
        this.blockFactory = blockFactory;
    }

    /** A column of keyword values. */
    public Keyword keyword(String name) {
        return new Keyword(declare(name, DataType.KEYWORD, blockFactory.newBytesRefBlockBuilder(1)));
    }

    /** A column of text values, which differ from {@link #keyword} only in the type a client is told. */
    public Keyword text(String name) {
        return new Keyword(declare(name, DataType.TEXT, blockFactory.newBytesRefBlockBuilder(1)));
    }

    public Longs longs(String name) {
        return new Longs(declare(name, DataType.LONG, blockFactory.newLongBlockBuilder(1)));
    }

    /** A column of milliseconds since the epoch, which is what a {@code datetime} block holds. */
    public Longs datetime(String name) {
        return new Longs(declare(name, DataType.DATETIME, blockFactory.newLongBlockBuilder(1)));
    }

    public Ints ints(String name) {
        return new Ints(declare(name, DataType.INTEGER, blockFactory.newIntBlockBuilder(1)));
    }

    public Doubles doubles(String name) {
        return new Doubles(declare(name, DataType.DOUBLE, blockFactory.newDoubleBlockBuilder(1)));
    }

    public Booleans booleans(String name) {
        return new Booleans(declare(name, DataType.BOOLEAN, blockFactory.newBooleanBlockBuilder(1)));
    }

    private <B extends Block.Builder> B declare(String name, DataType type, B builder) {
        if (built) {
            builder.close();
            throw new IllegalStateException("[" + owner + "] declared the column [" + name + "] after finishing");
        }
        if (names.add(name) == false) {
            builder.close();
            throw new IllegalStateException("[" + owner + "] declared the column [" + name + "] more than once");
        }
        columns.add(new ReferenceAttribute(Source.EMPTY, null, name, type));
        builders.add(builder);
        return builder;
    }

    /** A column of keyword or text values. */
    public static final class Keyword {
        private final BytesRefBlock.Builder builder;

        private Keyword(BytesRefBlock.Builder builder) {
            this.builder = builder;
        }

        public void append(String value) {
            builder.appendBytesRef(new BytesRef(value));
        }

        public void append(BytesRef value) {
            builder.appendBytesRef(value);
        }

        /** The value, or an empty cell when it is {@code null}, which is what most callers have in hand. */
        public void appendOrNull(String value) {
            if (value == null) {
                appendNull();
            } else {
                append(value);
            }
        }

        public void appendNull() {
            builder.appendNull();
        }
    }

    public static final class Longs {
        private final LongBlock.Builder builder;

        private Longs(LongBlock.Builder builder) {
            this.builder = builder;
        }

        public void append(long value) {
            builder.appendLong(value);
        }

        public void appendNull() {
            builder.appendNull();
        }
    }

    public static final class Ints {
        private final IntBlock.Builder builder;

        private Ints(IntBlock.Builder builder) {
            this.builder = builder;
        }

        public void append(int value) {
            builder.appendInt(value);
        }

        public void appendNull() {
            builder.appendNull();
        }
    }

    public static final class Doubles {
        private final DoubleBlock.Builder builder;

        private Doubles(DoubleBlock.Builder builder) {
            this.builder = builder;
        }

        public void append(double value) {
            builder.appendDouble(value);
        }

        public void appendNull() {
            builder.appendNull();
        }
    }

    public static final class Booleans {
        private final BooleanBlock.Builder builder;

        private Booleans(BooleanBlock.Builder builder) {
            this.builder = builder;
        }

        public void append(boolean value) {
            builder.appendBoolean(value);
        }

        public void appendNull() {
            builder.appendNull();
        }
    }

    List<Attribute> columns() {
        return List.copyOf(columns);
    }

    /**
     * The relation the declared columns and appended values denote, which owns the blocks from here: releasing them
     * is whoever holds the relation's job, as it is for any other {@link LocalRelation}.
     */
    public LocalRelation relation(Source source) {
        return new LocalRelation(source, columns(), LocalSupplier.of(page()));
    }

    /**
     * The page the declared columns hold, which this stops owning: every block is the caller's to release from here.
     * A column short of the longest is a caller that stopped appending part way through, and is refused rather than
     * padded, because which column it meant to leave empty is not something this can know.
     */
    Page page() {
        if (built) {
            throw new IllegalStateException("[" + owner + "] built its page twice");
        }
        if (columns.isEmpty()) {
            throw new IllegalStateException("[" + owner + "] declared no columns");
        }
        built = true;
        Block[] blocks = new Block[builders.size()];
        boolean success = false;
        try {
            for (int i = 0; i < builders.size(); i++) {
                blocks[i] = builders.get(i).build();
            }
            int rows = blocks[0].getPositionCount();
            for (int i = 1; i < blocks.length; i++) {
                if (blocks[i].getPositionCount() != rows) {
                    throw new IllegalStateException(
                        "["
                            + owner
                            + "] wrote ["
                            + blocks[i].getPositionCount()
                            + "] values to the column ["
                            + columns.get(i).name()
                            + "] and ["
                            + rows
                            + "] to ["
                            + columns.get(0).name()
                            + "]"
                    );
                }
            }
            Page page = new Page(rows, blocks);
            success = true;
            return page;
        } finally {
            Releasables.close(builders);
            if (success == false) {
                Releasables.closeExpectNoException(blocks);
            }
        }
    }

    /** Releases whatever was written. A no-op once {@link #page} has handed its blocks to the caller. */
    @Override
    public void close() {
        if (built == false) {
            Releasables.close(builders);
        }
    }
}
