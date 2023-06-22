/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import org.apache.lucene.util.BytesRef;

import java.util.List;

/**
 * A generic block builder that builds blocks from boxed data. Allows to build similarly shaped and
 * valued blocks of different types.
 */
public abstract class TestBlockBuilder implements Block.Builder {

    public abstract TestBlockBuilder appendObject(Object object);

    @Override
    public abstract TestBlockBuilder appendNull();

    @Override
    public abstract TestBlockBuilder beginPositionEntry();

    @Override
    public abstract TestBlockBuilder endPositionEntry();

    static Block blockFromValues(List<List<Object>> blockValues, ElementType elementType) {
        TestBlockBuilder builder = builderOf(elementType);
        for (List<Object> rowValues : blockValues) {
            if (rowValues.isEmpty()) {
                builder.appendNull();
            } else {
                builder.beginPositionEntry();
                for (Object rowValue : rowValues) {
                    builder.appendObject(rowValue);
                }
                builder.endPositionEntry();
            }
        }
        return builder.build();
    }

    static TestBlockBuilder builderOf(ElementType type) {
        return switch (type) {
            case INT -> new TestIntBlockBuilder(0);
            case LONG -> new TestLongBlockBuilder(0);
            case DOUBLE -> new TestDoubleBlockBuilder(0);
            case BYTES_REF -> new TestBytesRefBlockBuilder(0);
            default -> throw new AssertionError(type);
        };
    }

    static TestBlockBuilder ofInt(int estimatedSize) {
        return new TestIntBlockBuilder(estimatedSize);
    }

    static TestBlockBuilder ofLong(int estimatedSize) {
        return new TestLongBlockBuilder(estimatedSize);
    }

    static TestBlockBuilder ofDouble(int estimatedSize) {
        return new TestDoubleBlockBuilder(estimatedSize);
    }

    static TestBlockBuilder ofBytesRef(int estimatedSize) {
        return new TestBytesRefBlockBuilder(estimatedSize);
    }

    private static class TestIntBlockBuilder extends TestBlockBuilder {

        private final IntBlock.Builder builder;

        TestIntBlockBuilder(int estimatedSize) {
            builder = IntBlock.newBlockBuilder(estimatedSize);
        }

        @Override
        public TestBlockBuilder appendObject(Object object) {
            builder.appendInt(((Number) object).intValue());
            return this;
        }

        @Override
        public TestBlockBuilder appendNull() {
            builder.appendNull();
            return this;
        }

        @Override
        public TestBlockBuilder beginPositionEntry() {
            builder.beginPositionEntry();
            return this;
        }

        @Override
        public TestBlockBuilder endPositionEntry() {
            builder.endPositionEntry();
            return this;
        }

        @Override
        public TestBlockBuilder copyFrom(Block block, int beginInclusive, int endExclusive) {
            builder.copyFrom(block, beginInclusive, endExclusive);
            return this;
        }

        @Override
        public TestBlockBuilder mvOrdering(Block.MvOrdering mvOrdering) {
            builder.mvOrdering(mvOrdering);
            return this;
        }

        @Override
        public Block.Builder appendAllValuesToCurrentPosition(Block block) {
            builder.appendAllValuesToCurrentPosition(block);
            return this;
        }

        @Override
        public IntBlock build() {
            return builder.build();
        }
    }

    private static class TestLongBlockBuilder extends TestBlockBuilder {

        private final LongBlock.Builder builder;

        TestLongBlockBuilder(int estimatedSize) {
            builder = LongBlock.newBlockBuilder(estimatedSize);
        }

        @Override
        public TestBlockBuilder appendObject(Object object) {
            builder.appendLong(((Number) object).longValue());
            return this;
        }

        @Override
        public TestBlockBuilder appendNull() {
            builder.appendNull();
            return this;
        }

        @Override
        public TestBlockBuilder beginPositionEntry() {
            builder.beginPositionEntry();
            return this;
        }

        @Override
        public TestBlockBuilder endPositionEntry() {
            builder.endPositionEntry();
            return this;
        }

        @Override
        public TestBlockBuilder copyFrom(Block block, int beginInclusive, int endExclusive) {
            builder.copyFrom(block, beginInclusive, endExclusive);
            return this;
        }

        @Override
        public TestBlockBuilder mvOrdering(Block.MvOrdering mvOrdering) {
            builder.mvOrdering(mvOrdering);
            return this;
        }

        @Override
        public Block.Builder appendAllValuesToCurrentPosition(Block block) {
            builder.appendAllValuesToCurrentPosition(block);
            return this;
        }

        @Override
        public LongBlock build() {
            return builder.build();
        }
    }

    private static class TestDoubleBlockBuilder extends TestBlockBuilder {

        private final DoubleBlock.Builder builder;

        TestDoubleBlockBuilder(int estimatedSize) {
            builder = DoubleBlock.newBlockBuilder(estimatedSize);
        }

        @Override
        public TestBlockBuilder appendObject(Object object) {
            builder.appendDouble(((Number) object).doubleValue());
            return this;
        }

        @Override
        public TestBlockBuilder appendNull() {
            builder.appendNull();
            return this;
        }

        @Override
        public TestBlockBuilder beginPositionEntry() {
            builder.beginPositionEntry();
            return this;
        }

        @Override
        public TestBlockBuilder endPositionEntry() {
            builder.endPositionEntry();
            return this;
        }

        @Override
        public TestBlockBuilder copyFrom(Block block, int beginInclusive, int endExclusive) {
            builder.copyFrom(block, beginInclusive, endExclusive);
            return this;
        }

        @Override
        public TestBlockBuilder mvOrdering(Block.MvOrdering mvOrdering) {
            builder.mvOrdering(mvOrdering);
            return this;
        }

        @Override
        public Block.Builder appendAllValuesToCurrentPosition(Block block) {
            builder.appendAllValuesToCurrentPosition(block);
            return this;
        }

        @Override
        public DoubleBlock build() {
            return builder.build();
        }
    }

    private static class TestBytesRefBlockBuilder extends TestBlockBuilder {

        private final BytesRefBlock.Builder builder;

        TestBytesRefBlockBuilder(int estimatedSize) {
            builder = BytesRefBlock.newBlockBuilder(estimatedSize);
        }

        @Override
        public TestBlockBuilder appendObject(Object object) {
            builder.appendBytesRef(new BytesRef(((Integer) object).toString()));
            return this;
        }

        @Override
        public TestBlockBuilder appendNull() {
            builder.appendNull();
            return this;
        }

        @Override
        public TestBlockBuilder beginPositionEntry() {
            builder.beginPositionEntry();
            return this;
        }

        @Override
        public TestBlockBuilder endPositionEntry() {
            builder.endPositionEntry();
            return this;
        }

        @Override
        public TestBlockBuilder copyFrom(Block block, int beginInclusive, int endExclusive) {
            builder.copyFrom(block, beginInclusive, endExclusive);
            return this;
        }

        @Override
        public TestBlockBuilder mvOrdering(Block.MvOrdering mvOrdering) {
            builder.mvOrdering(mvOrdering);
            return this;
        }

        @Override
        public Block.Builder appendAllValuesToCurrentPosition(Block block) {
            builder.appendAllValuesToCurrentPosition(block);
            return this;
        }

        @Override
        public BytesRefBlock build() {
            return builder.build();
        }
    }
}
