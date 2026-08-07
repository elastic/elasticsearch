/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.test.OperatorTestCase;
import org.hamcrest.Matcher;

import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import static org.hamcrest.Matchers.equalTo;

/**
 * Tests the {@code INT} member of the {@link DistinctByOperator} family. The
 * {@link OperatorTestCase} harness exercises the drop-duplicates mode. The guard mode
 * ({@code failOnDuplicate=true}, throw-on-repeat) is exercised by the explicit tests below.
 */
public class OrdinalDistinctByOperatorTests extends OperatorTestCase {

    @Override
    protected DistinctByOperator.OrdinalIntKeyFactory simple(SimpleOptions options) {
        return new DistinctByOperator.OrdinalIntKeyFactory(0);
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
                try (IntBlock.Builder keyBuilder = blockFactory.newIntBlockBuilder(pageSize)) {
                    for (int i = 0; i < pageSize; i++) {
                        // Ordinals repeat: 0, 1, ..., size-1, 0, 1, ...
                        keyBuilder.appendInt((position + i) % size);
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
        return equalTo("DistinctByOperator[keyChannel=0, failOnDuplicate=false, factory=OrdinalIntKeyFactory]");
    }

    @Override
    protected Matcher<String> expectedToStringOfSimple() {
        return org.hamcrest.Matchers.startsWith(
            "DistinctByOperator[keyChannel=0, processor=org.elasticsearch.compute.operator.DistinctByOperator$IntOrdinalProcessor@"
        );
    }

    @Override
    protected void assertSimpleOutput(List<Page> input, List<Page> results) {
        Set<Integer> uniqueOrdinals = new HashSet<>();
        for (Page page : input) {
            IntBlock keyBlock = page.getBlock(0);
            for (int p = 0; p < page.getPositionCount(); p++) {
                if (keyBlock.isNull(p) == false) {
                    uniqueOrdinals.add(keyBlock.getInt(keyBlock.getFirstValueIndex(p)));
                }
            }
        }

        int outputCount = results.stream().mapToInt(Page::getPositionCount).sum();
        assertThat(outputCount, equalTo(uniqueOrdinals.size()));

        Set<Integer> outputOrdinals = new HashSet<>();
        for (Page page : results) {
            IntBlock keyBlock = page.getBlock(0);
            for (int p = 0; p < page.getPositionCount(); p++) {
                int ord = keyBlock.getInt(keyBlock.getFirstValueIndex(p));
                assertTrue("Duplicate ordinal in output: " + ord, outputOrdinals.add(ord));
            }
        }
    }

    private DistinctByOperator guard() {
        return ordinal(0, true);
    }

    private DistinctByOperator ordinal(int keyChannel) {
        return (DistinctByOperator) new DistinctByOperator.OrdinalIntKeyFactory(keyChannel).get(driverContext());
    }

    private DistinctByOperator ordinal(int keyChannel, boolean failOnDuplicate) {
        return (DistinctByOperator) new DistinctByOperator.OrdinalIntKeyFactory(keyChannel, failOnDuplicate).get(driverContext());
    }

    private static Page intPage(BlockFactory blockFactory, Integer... ordinals) {
        try (IntBlock.Builder builder = blockFactory.newIntBlockBuilder(ordinals.length)) {
            for (Integer ordinal : ordinals) {
                if (ordinal == null) {
                    builder.appendNull();
                } else {
                    builder.appendInt(ordinal);
                }
            }
            return new Page(builder.build());
        }
    }

    public void testGuardPassesUniqueOrdinals() {
        BlockFactory blockFactory = driverContext().blockFactory();
        try (DistinctByOperator op = guard()) {
            op.addInput(intPage(blockFactory, 0, 1, 2));
            Page output = op.getOutput();
            assertThat(Objects.requireNonNull(output).getPositionCount(), equalTo(3));
            output.releaseBlocks();
        }
    }

    public void testGuardIgnoresNulls() {
        BlockFactory blockFactory = driverContext().blockFactory();
        try (DistinctByOperator op = guard()) {
            // Two nulls (misses) plus distinct ordinals - nulls must not count as duplicates.
            op.addInput(intPage(blockFactory, 0, null, 1, null));
            Page output = op.getOutput();
            assertThat(Objects.requireNonNull(output).getPositionCount(), equalTo(4));
            output.releaseBlocks();
        }
    }

    public void testGuardThrowsOnDuplicateWithinPage() {
        BlockFactory blockFactory = driverContext().blockFactory();
        try (DistinctByOperator op = guard()) {
            op.addInput(intPage(blockFactory, 0, 1, 0));
            expectThrows(IllegalArgumentException.class, op::getOutput);
        }
    }

    public void testGuardThrowsOnDuplicateMultivaluedBlock() {
        BlockFactory blockFactory = driverContext().blockFactory();
        try (DistinctByOperator op = guard(); IntBlock.Builder builder = blockFactory.newIntBlockBuilder(2)) {
            builder.beginPositionEntry();
            builder.appendInt(0);
            builder.appendInt(1);
            builder.endPositionEntry();
            builder.appendInt(1);
            op.addInput(new Page(builder.build()));
            expectThrows(IllegalArgumentException.class, op::getOutput);
        }
    }

    public void testGuardThrowsOnDuplicateAcrossPages() {
        BlockFactory blockFactory = driverContext().blockFactory();
        try (DistinctByOperator op = guard()) {
            op.addInput(intPage(blockFactory, 0, 1));
            op.getOutput().releaseBlocks();
            op.addInput(intPage(blockFactory, 1)); // ordinal 1 already matched
            expectThrows(IllegalArgumentException.class, op::getOutput);
        }
    }

    public void testDedupDropsDuplicatesAndNulls() {
        BlockFactory blockFactory = driverContext().blockFactory();
        try (DistinctByOperator op = ordinal(0)) {
            op.addInput(intPage(blockFactory, 0, 1, 0, null, 2, 1));
            Page output = op.getOutput();
            // Keep first of {0, 1, 2}; drop the repeats and the null.
            assertThat(Objects.requireNonNull(output).getPositionCount(), equalTo(3));
            IntBlock out = output.getBlock(0);
            assertThat(out.getInt(0), equalTo(0));
            assertThat(out.getInt(1), equalTo(1));
            assertThat(out.getInt(2), equalTo(2));
            output.releaseBlocks();
        }
    }

    public void testDedupTransfersUnchangedPage() {
        BlockFactory blockFactory = driverContext().blockFactory();
        try (DistinctByOperator op = ordinal(0)) {
            Page input = intPage(blockFactory, 0, 1, 2);
            op.addInput(input);
            Page output = op.getOutput();
            assertSame(input, output);
            Objects.requireNonNull(output).releaseBlocks();
        }
    }

    public void testDedupKeepsValuesAfterRejectedFirstPosition() {
        BlockFactory blockFactory = driverContext().blockFactory();
        try (DistinctByOperator op = ordinal(0)) {
            op.addInput(intPage(blockFactory, null, 0, 1, 2));
            Page output = op.getOutput();
            assertThat(Objects.requireNonNull(output).getPositionCount(), equalTo(3));
            output.releaseBlocks();
        }
    }

    public void testDedupConstantVector() {
        BlockFactory blockFactory = driverContext().blockFactory();
        try (DistinctByOperator op = ordinal(0)) {
            op.addInput(new Page(blockFactory.newConstantIntBlockWith(7, 10)));
            Page output = op.getOutput();
            assertThat(Objects.requireNonNull(output).getPositionCount(), equalTo(1));
            output.releaseBlocks();
        }
    }

    public void testGuardThrowsOnConstantVector() {
        BlockFactory blockFactory = driverContext().blockFactory();
        try (DistinctByOperator op = guard()) {
            op.addInput(new Page(blockFactory.newConstantIntBlockWith(7, 2)));
            expectThrows(IllegalArgumentException.class, op::getOutput);
        }
    }

    public void testDedupRejectsNegativeOrdinals() {
        BlockFactory blockFactory = driverContext().blockFactory();
        Page input = intPage(blockFactory, -1);
        IntBlock inputBlock = input.getBlock(0);
        try (DistinctByOperator op = ordinal(0)) {
            op.addInput(input);
            IllegalArgumentException e = expectThrows(IllegalArgumentException.class, op::getOutput);
            assertThat(e.getMessage(), equalTo("ordinal key must be non-negative but was [-1]"));
            assertFalse(inputBlock.isReleased());
        }
        assertTrue(inputBlock.isReleased());
    }

    public void testGuardRejectsNegativeOrdinals() {
        BlockFactory blockFactory = driverContext().blockFactory();
        try (DistinctByOperator op = guard()) {
            op.addInput(intPage(blockFactory, -1));
            IllegalArgumentException e = expectThrows(IllegalArgumentException.class, op::getOutput);
            assertThat(e.getMessage(), equalTo("ordinal key must be non-negative but was [-1]"));
        }
    }
}
