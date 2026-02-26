/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.test.OperatorTestCase;
import org.hamcrest.Matcher;

import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import static org.hamcrest.Matchers.equalTo;

public class DistinctByOperatorTests extends OperatorTestCase {

    @Override
    protected DistinctByOperator.Factory simple(SimpleOptions options) {
        return new DistinctByOperator.Factory(0);
    }

    @Override
    protected SourceOperator simpleInput(BlockFactory blockFactory, int size) {
        return new SourceOperator() {
            private int position = 0;
            private static final int PAGE_SIZE = 100;

            @Override
            public void finish() {
                position = size * 2;
            }

            @Override
            public boolean isFinished() {
                return position >= size * 2;
            }

            @Override
            public Page getOutput() {
                if (isFinished()) {
                    return null;
                }
                int remaining = size * 2 - position;
                int pageSize = Math.min(PAGE_SIZE, remaining);
                try (BytesRefBlock.Builder keyBuilder = blockFactory.newBytesRefBlockBuilder(pageSize)) {
                    for (int i = 0; i < pageSize; i++) {
                        // Keys repeat: 0, 1, 2, ..., size-1, 0, 1, 2, ...
                        long keyValue = (position + i) % size;
                        keyBuilder.appendBytesRef(new BytesRef(String.valueOf(keyValue)));
                    }
                    position += pageSize;
                    return new Page(keyBuilder.build());
                }
            }

            @Override
            public void close() {}
        };
    }

    @Override
    protected Matcher<String> expectedDescriptionOfSimple() {
        return equalTo("DistinctByOperator[keyChannel=0]");
    }

    @Override
    protected Matcher<String> expectedToStringOfSimple() {
        return org.hamcrest.Matchers.startsWith("DistinctByOperator[keyChannel=0, seenKeys=");
    }

    @Override
    protected void assertSimpleOutput(List<Page> input, List<Page> results) {
        Set<String> uniqueKeys = new HashSet<>();
        for (Page page : input) {
            BytesRefBlock keyBlock = page.getBlock(0);
            BytesRef scratch = new BytesRef();
            for (int p = 0; p < page.getPositionCount(); p++) {
                if (keyBlock.isNull(p) == false) {
                    uniqueKeys.add(keyBlock.getBytesRef(p, scratch).utf8ToString());
                }
            }
        }

        int outputCount = results.stream().mapToInt(Page::getPositionCount).sum();
        assertThat(outputCount, equalTo(uniqueKeys.size()));

        Set<String> outputKeys = new HashSet<>();
        for (Page page : results) {
            BytesRefBlock keyBlock = page.getBlock(0);
            BytesRef scratch = new BytesRef();
            for (int p = 0; p < page.getPositionCount(); p++) {
                String key = keyBlock.getBytesRef(p, scratch).utf8ToString();
                assertTrue("Duplicate key in output: " + key, outputKeys.add(key));
            }
        }
    }

    public void testAllUniqueValues() {
        BlockFactory blockFactory = driverContext().blockFactory();
        try (DistinctByOperator op = new DistinctByOperator(0, blockFactory)) {
            try (BytesRefBlock.Builder builder = blockFactory.newBytesRefBlockBuilder(3)) {
                builder.appendBytesRef(new BytesRef("a"));
                builder.appendBytesRef(new BytesRef("b"));
                builder.appendBytesRef(new BytesRef("c"));
                Page input = new Page(builder.build());
                op.addInput(input);
                Page output = op.getOutput();
                assertThat(Objects.requireNonNull(output).getPositionCount(), equalTo(3));
                output.releaseBlocks();
            }
        }
    }

    public void testAllSameValues() {
        BlockFactory blockFactory = driverContext().blockFactory();
        try (DistinctByOperator op = new DistinctByOperator(0, blockFactory)) {
            try (BytesRefBlock.Builder builder = blockFactory.newBytesRefBlockBuilder(3)) {
                builder.appendBytesRef(new BytesRef("same"));
                builder.appendBytesRef(new BytesRef("same"));
                builder.appendBytesRef(new BytesRef("same"));
                Page input = new Page(builder.build());
                op.addInput(input);
                Page output = op.getOutput();
                assertThat(Objects.requireNonNull(output).getPositionCount(), equalTo(1));
                output.releaseBlocks();
            }
        }
    }

    public void testNullsAreSkipped() {
        BlockFactory blockFactory = driverContext().blockFactory();
        try (DistinctByOperator op = new DistinctByOperator(0, blockFactory)) {
            try (BytesRefBlock.Builder builder = blockFactory.newBytesRefBlockBuilder(4)) {
                builder.appendBytesRef(new BytesRef("a"));
                builder.appendNull();
                builder.appendBytesRef(new BytesRef("b"));
                builder.appendNull();
                Page input = new Page(builder.build());
                op.addInput(input);
                Page output = op.getOutput();
                assertThat(Objects.requireNonNull(output).getPositionCount(), equalTo(2));
                output.releaseBlocks();
            }
        }
    }

    public void testDeduplicationAcrossPages() {
        BlockFactory blockFactory = driverContext().blockFactory();
        try (DistinctByOperator op = new DistinctByOperator(0, blockFactory)) {
            // First page: a, b, c
            try (BytesRefBlock.Builder builder1 = blockFactory.newBytesRefBlockBuilder(3)) {
                builder1.appendBytesRef(new BytesRef("a"));
                builder1.appendBytesRef(new BytesRef("b"));
                builder1.appendBytesRef(new BytesRef("c"));
                Page input1 = new Page(builder1.build());
                op.addInput(input1);
                Page output1 = op.getOutput();
                assertThat(Objects.requireNonNull(output1).getPositionCount(), equalTo(3));
                output1.releaseBlocks();
            }

            // Second page: b, c, d - b and c should be filtered, only d passes
            try (BytesRefBlock.Builder builder2 = blockFactory.newBytesRefBlockBuilder(3)) {
                builder2.appendBytesRef(new BytesRef("b"));
                builder2.appendBytesRef(new BytesRef("c"));
                builder2.appendBytesRef(new BytesRef("d"));
                Page input2 = new Page(builder2.build());
                op.addInput(input2);
                Page output2 = op.getOutput();
                assertThat(output2.getPositionCount(), equalTo(1));
                // Verify it's "d"
                BytesRefBlock keyBlock = output2.getBlock(0);
                assertThat(keyBlock.getBytesRef(0, new BytesRef()).utf8ToString(), equalTo("d"));
                output2.releaseBlocks();
            }
        }
    }

    public void testAllDuplicatesReturnsNull() {
        BlockFactory blockFactory = driverContext().blockFactory();
        try (DistinctByOperator op = new DistinctByOperator(0, blockFactory)) {
            // First page introduces "a"
            try (BytesRefBlock.Builder builder1 = blockFactory.newBytesRefBlockBuilder(1)) {
                builder1.appendBytesRef(new BytesRef("a"));
                Page input1 = new Page(builder1.build());
                op.addInput(input1);
                Page output1 = op.getOutput();
                assertThat(Objects.requireNonNull(output1).getPositionCount(), equalTo(1));
                output1.releaseBlocks();
            }

            // Second page has only "a" - all duplicates, should return null
            try (BytesRefBlock.Builder builder2 = blockFactory.newBytesRefBlockBuilder(3)) {
                builder2.appendBytesRef(new BytesRef("a"));
                builder2.appendBytesRef(new BytesRef("a"));
                builder2.appendBytesRef(new BytesRef("a"));
                Page input2 = new Page(builder2.build());
                op.addInput(input2);
                Page output2 = op.getOutput();
                assertNull(output2);
            }
        }
    }

    public void testPreservesOtherColumns() {
        BlockFactory blockFactory = driverContext().blockFactory();
        try (DistinctByOperator op = new DistinctByOperator(0, blockFactory)) {
            // Page with key column (0) and value column (1)
            try (
                BytesRefBlock.Builder keyBuilder = blockFactory.newBytesRefBlockBuilder(4);
                LongBlock.Builder valueBuilder = blockFactory.newLongBlockBuilder(4)
            ) {
                keyBuilder.appendBytesRef(new BytesRef("a"));
                valueBuilder.appendLong(100);

                keyBuilder.appendBytesRef(new BytesRef("b"));
                valueBuilder.appendLong(200);

                keyBuilder.appendBytesRef(new BytesRef("a")); // duplicate
                valueBuilder.appendLong(300);

                keyBuilder.appendBytesRef(new BytesRef("c"));
                valueBuilder.appendLong(400);

                Page input = new Page(keyBuilder.build(), valueBuilder.build());
                op.addInput(input);
                Page output = op.getOutput();

                // Should have 3 rows: a, b, c (first occurrence of each)
                assertThat(Objects.requireNonNull(output).getPositionCount(), equalTo(3));

                // Verify values correspond to first occurrences
                LongBlock outputValues = output.getBlock(1);
                assertThat(outputValues.getLong(0), equalTo(100L)); // first "a"
                assertThat(outputValues.getLong(1), equalTo(200L)); // first "b"
                assertThat(outputValues.getLong(2), equalTo(400L)); // first "c"

                output.releaseBlocks();
            }
        }
    }

    public void testEmptyPage() {
        BlockFactory blockFactory = driverContext().blockFactory();
        try (DistinctByOperator op = new DistinctByOperator(0, blockFactory)) {
            try (BytesRefBlock.Builder builder = blockFactory.newBytesRefBlockBuilder(0)) {
                Page input = new Page(builder.build());
                op.addInput(input);
                Page output = op.getOutput();
                assertNotNull(output);
                assertThat(output.getPositionCount(), equalTo(0));
                output.releaseBlocks();
            }
        }
    }

    public void testFactoryDescribe() {
        DistinctByOperator.Factory factory = new DistinctByOperator.Factory(5);
        assertThat(factory.describe(), equalTo("DistinctByOperator[keyChannel=5]"));
    }

    public void testToString() {
        BlockFactory blockFactory = driverContext().blockFactory();
        try (DistinctByOperator op = new DistinctByOperator(3, blockFactory)) {
            assertThat(op.toString(), equalTo("DistinctByOperator[keyChannel=3, seenKeys=0]"));
        }
    }

    public void testToStringAfterProcessing() {
        BlockFactory blockFactory = driverContext().blockFactory();
        try (DistinctByOperator op = new DistinctByOperator(0, blockFactory)) {
            try (BytesRefBlock.Builder builder = blockFactory.newBytesRefBlockBuilder(3)) {
                builder.appendBytesRef(new BytesRef("a"));
                builder.appendBytesRef(new BytesRef("b"));
                builder.appendBytesRef(new BytesRef("a")); // duplicate
                Page input = new Page(builder.build());
                op.addInput(input);
                Page output = op.getOutput();
                Objects.requireNonNull(output).releaseBlocks();
            }
            assertThat(op.toString(), equalTo("DistinctByOperator[keyChannel=0, seenKeys=2]"));
        }
    }

    public void testConstantVectorNewKey() {
        BlockFactory blockFactory = driverContext().blockFactory();
        try (DistinctByOperator op = new DistinctByOperator(0, blockFactory)) {
            // A constant block with 100 positions all having the same value
            BytesRefBlock constantBlock = blockFactory.newConstantBytesRefBlockWith(new BytesRef("constant_key"), 100);
            assertTrue("Block should be constant", constantBlock.asVector() != null && constantBlock.asVector().isConstant());

            Page input = new Page(constantBlock);
            op.addInput(input);
            Page output = op.getOutput();

            // Should output 1 row (the first occurrence)
            assertThat(Objects.requireNonNull(output).getPositionCount(), equalTo(1));

            BytesRefBlock keyBlock = output.getBlock(0);
            assertThat(keyBlock.getBytesRef(0, new BytesRef()).utf8ToString(), equalTo("constant_key"));

            output.releaseBlocks();
        }
    }

    public void testConstantVectorSeenKey() {
        BlockFactory blockFactory = driverContext().blockFactory();
        try (DistinctByOperator op = new DistinctByOperator(0, blockFactory)) {
            try (BytesRefBlock.Builder builder = blockFactory.newBytesRefBlockBuilder(1)) {
                builder.appendBytesRef(new BytesRef("seen_key"));
                Page input1 = new Page(builder.build());
                op.addInput(input1);
                Page output1 = op.getOutput();
                assertThat(Objects.requireNonNull(output1).getPositionCount(), equalTo(1));
                output1.releaseBlocks();
            }

            BytesRefBlock constantBlock = blockFactory.newConstantBytesRefBlockWith(new BytesRef("seen_key"), 50);
            assertTrue("Block should be constant", constantBlock.asVector() != null && constantBlock.asVector().isConstant());

            Page input2 = new Page(constantBlock);
            op.addInput(input2);
            Page output2 = op.getOutput();

            assertNull(output2);
        }
    }

    public void testConstantVectorWithOtherColumns() {
        BlockFactory blockFactory = driverContext().blockFactory();
        try (DistinctByOperator op = new DistinctByOperator(0, blockFactory)) {
            BytesRefBlock constantKey = blockFactory.newConstantBytesRefBlockWith(new BytesRef("key"), 5);
            try (LongBlock.Builder valueBuilder = blockFactory.newLongBlockBuilder(5)) {
                for (int i = 0; i < 5; i++) {
                    valueBuilder.appendLong(i * 100);
                }
                Page input = new Page(constantKey, valueBuilder.build());
                op.addInput(input);
                Page output = op.getOutput();

                assertThat(Objects.requireNonNull(output).getPositionCount(), equalTo(1));

                LongBlock outputValues = output.getBlock(1);
                assertThat(outputValues.getLong(0), equalTo(0L));

                output.releaseBlocks();
            }
        }
    }

    public void testConstantVectorAcrossPages() {
        BlockFactory blockFactory = driverContext().blockFactory();
        try (DistinctByOperator op = new DistinctByOperator(0, blockFactory)) {
            // First constant page with "a"
            BytesRefBlock constantA = blockFactory.newConstantBytesRefBlockWith(new BytesRef("a"), 10);
            op.addInput(new Page(constantA));
            Page output1 = op.getOutput();
            assertThat(Objects.requireNonNull(output1).getPositionCount(), equalTo(1));
            output1.releaseBlocks();

            // Second constant page with "b" - should pass (new key)
            BytesRefBlock constantB = blockFactory.newConstantBytesRefBlockWith(new BytesRef("b"), 20);
            op.addInput(new Page(constantB));
            Page output2 = op.getOutput();
            assertThat(output2.getPositionCount(), equalTo(1));
            output2.releaseBlocks();

            // Third constant page with "a" again - should be filtered (seen key)
            BytesRefBlock constantA2 = blockFactory.newConstantBytesRefBlockWith(new BytesRef("a"), 30);
            op.addInput(new Page(constantA2));
            Page output3 = op.getOutput();
            assertNull(output3);

            // Verify we have 2 seen keys
            assertThat(op.toString(), equalTo("DistinctByOperator[keyChannel=0, seenKeys=2]"));
        }
    }
}
