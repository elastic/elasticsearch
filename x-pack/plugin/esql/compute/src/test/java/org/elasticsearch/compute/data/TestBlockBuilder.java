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

    public static Block blockFromValues(List<List<Object>> blockValues, ElementType elementType) {
        TestBlockBuilder builder = builderOf(TestBlockFactory.getNonBreakingInstance(), elementType);
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

    // Builds a block of single values. Each value can be null or non-null.
    // Differs from blockFromValues, as it does not use begin/endPositionEntry
    public static Block blockFromSingleValues(List<Object> blockValues, ElementType elementType) {
        TestBlockBuilder builder = builderOf(TestBlockFactory.getNonBreakingInstance(), elementType);
        for (Object rowValue : blockValues) {
            if (rowValue == null) {
                builder.appendNull();
            } else {
                builder.appendObject(rowValue);
            }
        }
        return builder.build();
    }

    static TestBlockBuilder builderOf(BlockFactory blockFactory, ElementType type) {
        return switch (type) {
            case INT -> new TestIntBlockBuilder(blockFactory, 0);
            case LONG -> new TestLongBlockBuilder(blockFactory, 0);
            case DOUBLE -> new TestDoubleBlockBuilder(blockFactory, 0);
            case FLOAT -> new TestFloatBlockBuilder(blockFactory, 0);
            case BYTES_REF -> new TestBytesRefBlockBuilder(blockFactory, 0);
            case BOOLEAN -> new TestBooleanBlockBuilder(blockFactory, 0);
            default -> throw new AssertionError(type);
        };
    }

    private static class TestIntBlockBuilder extends TestBlockBuilder {

        private final IntBlock.Builder builder;

        TestIntBlockBuilder(BlockFactory blockFactory, int estimatedSize) {
            builder = blockFactory.newIntBlockBuilder(estimatedSize);
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
        public long estimatedBytes() {
            return builder.estimatedBytes();
        }

        @Override
        public IntBlock build() {
            return builder.build();
        }

        @Override
        public void close() {
            builder.close();
        }
    }

    private static class TestLongBlockBuilder extends TestBlockBuilder {

        private final LongBlock.Builder builder;

        TestLongBlockBuilder(BlockFactory blockFactory, int estimatedSize) {
            builder = blockFactory.newLongBlockBuilder(estimatedSize);
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
        public long estimatedBytes() {
            return builder.estimatedBytes();
        }

        @Override
        public LongBlock build() {
            return builder.build();
        }

        @Override
        public void close() {
            builder.close();
        }
    }

    private static class TestDoubleBlockBuilder extends TestBlockBuilder {

        private final DoubleBlock.Builder builder;

        TestDoubleBlockBuilder(BlockFactory blockFactory, int estimatedSize) {
            builder = blockFactory.newDoubleBlockBuilder(estimatedSize);
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
        public long estimatedBytes() {
            return builder.estimatedBytes();
        }

        @Override
        public DoubleBlock build() {
            return builder.build();
        }

        @Override
        public void close() {
            builder.close();
        }
    }

    private static class TestFloatBlockBuilder extends TestBlockBuilder {

        private final FloatBlock.Builder builder;

        TestFloatBlockBuilder(BlockFactory blockFactory, int estimatedSize) {
            builder = blockFactory.newFloatBlockBuilder(estimatedSize);
        }

        @Override
        public TestBlockBuilder appendObject(Object object) {
            builder.appendFloat(((Number) object).floatValue());
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
        public long estimatedBytes() {
            return builder.estimatedBytes();
        }

        @Override
        public FloatBlock build() {
            return builder.build();
        }

        @Override
        public void close() {
            builder.close();
        }
    }

    private static class TestBytesRefBlockBuilder extends TestBlockBuilder {

        private final BytesRefBlock.Builder builder;

        TestBytesRefBlockBuilder(BlockFactory blockFactory, int estimatedSize) {
            builder = blockFactory.newBytesRefBlockBuilder(estimatedSize);
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
        public long estimatedBytes() {
            return builder.estimatedBytes();
        }

        @Override
        public BytesRefBlock build() {
            return builder.build();
        }

        @Override
        public void close() {
            builder.close();
        }
    }

    private static class TestBooleanBlockBuilder extends TestBlockBuilder {

        private final BooleanBlock.Builder builder;

        TestBooleanBlockBuilder(BlockFactory blockFactory, int estimatedSize) {
            builder = blockFactory.newBooleanBlockBuilder(estimatedSize);
        }

        @Override
        public TestBlockBuilder appendObject(Object object) {
            if (object instanceof Number number) {
                object = number.intValue() % 2 == 0;
            }
            builder.appendBoolean((boolean) object);
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
        public long estimatedBytes() {
            return builder.estimatedBytes();
        }

        @Override
        public BooleanBlock build() {
            return builder.build();
        }

        @Override
        public void close() {
            builder.close();
        }
    }
}
