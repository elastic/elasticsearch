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
import org.elasticsearch.compute.operator.DistinctByOperator.OrdinalDistinctByOperator;
import org.elasticsearch.compute.test.OperatorTestCase;
import org.hamcrest.Matcher;

import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import static org.hamcrest.Matchers.equalTo;

/**
 * Tests the {@code INT} member of the {@link DistinctByOperator} family
 * ({@link OrdinalDistinctByOperator}). The {@link OperatorTestCase} harness exercises the
 * drop-duplicates mode ({@code ignoreDuplicate=true}); the guard mode ({@code ignoreDuplicate=false},
 * throw-on-repeat) is exercised by the explicit tests below.
 */
public class OrdinalDistinctByOperatorTests extends OperatorTestCase {

    @Override
    protected DistinctByOperator.IntFactory simple(SimpleOptions options) {
        return new DistinctByOperator.IntFactory(0, true);
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
        return equalTo("DistinctByOperator[keyChannel=0]");
    }

    @Override
    protected Matcher<String> expectedToStringOfSimple() {
        return equalTo("OrdinalDistinctByOperator[channel=0, ignoreDuplicate=true]");
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

    private OrdinalDistinctByOperator guard() {
        return new OrdinalDistinctByOperator(0, false, driverContext().bigArrays());
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
        try (OrdinalDistinctByOperator op = guard()) {
            op.addInput(intPage(blockFactory, 0, 1, 2));
            Page output = op.getOutput();
            assertThat(Objects.requireNonNull(output).getPositionCount(), equalTo(3));
            output.releaseBlocks();
        }
    }

    public void testGuardIgnoresNulls() {
        BlockFactory blockFactory = driverContext().blockFactory();
        try (OrdinalDistinctByOperator op = guard()) {
            // Two nulls (misses) plus distinct ordinals - nulls must not count as duplicates.
            op.addInput(intPage(blockFactory, 0, null, 1, null));
            Page output = op.getOutput();
            assertThat(Objects.requireNonNull(output).getPositionCount(), equalTo(4));
            output.releaseBlocks();
        }
    }

    public void testGuardThrowsOnDuplicateWithinPage() {
        BlockFactory blockFactory = driverContext().blockFactory();
        try (OrdinalDistinctByOperator op = guard()) {
            op.addInput(intPage(blockFactory, 0, 1, 0));
            expectThrows(IllegalArgumentException.class, op::getOutput);
        }
    }

    public void testGuardThrowsOnDuplicateAcrossPages() {
        BlockFactory blockFactory = driverContext().blockFactory();
        try (OrdinalDistinctByOperator op = guard()) {
            op.addInput(intPage(blockFactory, 0, 1));
            op.getOutput().releaseBlocks();
            op.addInput(intPage(blockFactory, 1)); // ordinal 1 already matched
            expectThrows(IllegalArgumentException.class, op::getOutput);
        }
    }

    public void testDedupDropsDuplicatesAndNulls() {
        BlockFactory blockFactory = driverContext().blockFactory();
        try (OrdinalDistinctByOperator op = new OrdinalDistinctByOperator(0, true, driverContext().bigArrays())) {
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
}
