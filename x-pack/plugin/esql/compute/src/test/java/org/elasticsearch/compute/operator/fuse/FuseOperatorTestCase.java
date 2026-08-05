/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.fuse;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.TriConsumer;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.DoubleVectorBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.compute.operator.SourceOperator;
import org.elasticsearch.compute.test.CannedSourceOperator;
import org.elasticsearch.compute.test.OperatorTestCase;
import org.elasticsearch.compute.test.operator.blocksource.AbstractBlockSourceOperator;
import org.elasticsearch.core.Releasables;
import org.junit.Before;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

public abstract class FuseOperatorTestCase extends OperatorTestCase {
    protected int blocksCount;
    protected int discriminatorPosition;
    protected int scorePosition;
    protected int discriminatorCount;

    @Before
    public void initialize() {
        discriminatorPosition = randomIntBetween(1, 20);
        scorePosition = randomIntBetween(discriminatorPosition + 1, 50);
        blocksCount = randomIntBetween(scorePosition + 1, 100);
        discriminatorCount = randomIntBetween(1, 20);
    }

    protected void assertOutput(List<Page> input, List<Page> results, TriConsumer<String, Double, Double> assertScore) {
        assertEquals(input.size(), results.size());

        for (int i = 0; i < results.size(); i++) {
            Page resultPage = results.get(i);
            Page initialPage = input.get(i);

            assertEquals(initialPage.getPositionCount(), resultPage.getPositionCount());
            assertEquals(resultPage.getBlockCount(), blocksCount);

            BytesRefBlock discriminatorBlock = resultPage.getBlock(discriminatorPosition);
            DoubleVectorBlock actualScoreBlock = resultPage.getBlock(scorePosition);
            DoubleVectorBlock initialScoreBlock = initialPage.getBlock(scorePosition);

            for (int j = 0; j < resultPage.getPositionCount(); j++) {
                String discriminator = discriminatorBlock.getBytesRef(j, new BytesRef()).utf8ToString();
                double actualScore = actualScoreBlock.getDouble(j);
                double initialScore = initialScoreBlock.getDouble(j);
                assertScore.apply(discriminator, actualScore, initialScore);
            }
        }
    }

    @Override
    protected SourceOperator simpleInput(BlockFactory blockFactory, int size) {
        return new AbstractBlockSourceOperator(blockFactory, 8 * 1024) {
            @Override
            protected int remaining() {
                return size - currentPosition;
            }

            @Override
            protected Page createPage(int positionOffset, int length) {
                length = Integer.min(length, remaining());
                Block[] blocks = new Block[blocksCount];

                try {
                    for (int b = 0; b < blocksCount; b++) {
                        if (b == scorePosition) {
                            try (var builder = blockFactory.newDoubleBlockBuilder(length)) {
                                for (int i = 0; i < length; i++) {
                                    builder.appendDouble(randomDoubleBetween(-1000, 1000, true));
                                }
                                blocks[b] = builder.build();
                            }
                        } else {
                            try (var builder = blockFactory.newBytesRefBlockBuilder(length)) {
                                for (int i = 0; i < length; i++) {
                                    String stringInput = b == discriminatorPosition
                                        ? "fork" + randomIntBetween(0, discriminatorCount)
                                        : randomAlphaOfLength(10);

                                    builder.appendBytesRef(new BytesRef(stringInput));
                                }
                                blocks[b] = builder.build();
                            }
                        }
                    }
                } catch (Exception e) {
                    Releasables.closeExpectNoException(blocks);
                    throw e;
                }

                currentPosition += length;
                return new Page(blocks);
            }
        };
    }

    protected Map<String, Double> randomWeights() {
        Map<String, Double> weights = new HashMap<>();
        for (int i = 0; i < discriminatorCount; i++) {
            if (randomBoolean()) {
                weights.put("fork" + i, randomDouble());
            }
        }
        return weights;
    }

    /**
     * Drives an operator to completion on the current (test) thread and returns its output pages.
     * Running on the test thread is what makes warnings deterministic.
     */
    protected static List<Page> fuseOutput(Operator op, List<Page> input) {
        List<Page> output = new ArrayList<>();
        try (op) {
            for (Page page : input) {
                while (op.needsInput() == false) {
                    Page out = op.getOutput();
                    if (out != null) {
                        output.add(out);
                    }
                }
                op.addInput(page);
            }
            op.finish();
            Page out;
            while ((out = op.getOutput()) != null) {
                output.add(out);
            }
        }
        return output;
    }

    /**
     * A deterministic counterpart to {@link #simpleInput}: same page shape (group at
     * {@link #discriminatorPosition}, score at {@link #scorePosition}), except the very first row's
     * group column is <b>multivalued</b> ({@code [foo, bar]}). FUSE cannot group a multivalued
     * discriminator, so it assigns that row a null score and emits a warning. Being deterministic lets
     * a test assert the exact warning, which the random {@link #simpleInput} can't (it only ever
     * produces single-valued groups).
     */
    protected SourceOperator simpleInputWithMultivaluedGroup(BlockFactory blockFactory, int size) {
        return new AbstractBlockSourceOperator(blockFactory, 8 * 1024) {
            @Override
            protected int remaining() {
                return size - currentPosition;
            }

            @Override
            protected Page createPage(int positionOffset, int length) {
                length = Integer.min(length, remaining());
                Block[] blocks = new Block[blocksCount];
                try {
                    for (int b = 0; b < blocksCount; b++) {
                        if (b == scorePosition) {
                            try (var builder = blockFactory.newDoubleBlockBuilder(length)) {
                                for (int i = 0; i < length; i++) {
                                    builder.appendDouble(positionOffset + i);
                                }
                                blocks[b] = builder.build();
                            }
                        } else if (b == discriminatorPosition) {
                            try (var builder = blockFactory.newBytesRefBlockBuilder(length)) {
                                for (int i = 0; i < length; i++) {
                                    if (positionOffset + i == 0) {
                                        // the single multivalued group row that triggers the warning
                                        builder.beginPositionEntry();
                                        builder.appendBytesRef(new BytesRef("foo"));
                                        builder.appendBytesRef(new BytesRef("bar"));
                                        builder.endPositionEntry();
                                    } else {
                                        builder.appendBytesRef(new BytesRef("fork"));
                                    }
                                }
                                blocks[b] = builder.build();
                            }
                        } else {
                            try (var builder = blockFactory.newBytesRefBlockBuilder(length)) {
                                for (int i = 0; i < length; i++) {
                                    builder.appendBytesRef(new BytesRef("filler"));
                                }
                                blocks[b] = builder.build();
                            }
                        }
                    }
                } catch (Exception e) {
                    Releasables.closeExpectNoException(blocks);
                    throw e;
                }
                currentPosition += length;
                return new Page(blocks);
            }
        };
    }

    /**
     * A multivalued group column cannot be grouped, so FUSE assigns that row a null score and emits two
     * warnings. This mirrors the {@code fuse.fuseWithRowRRFAndMultiValueGroupColumn} and
     * {@code fuse.fuseWithRowLinearAndMultiValueGroupColumn} csv-spec cases asserted deterministically
     * at the operator level.
     */
    public void testMultivaluedGroupColumnProducesWarning() {
        DriverContext ctx = driverContext();

        // The first row's group is multivalued; the rest are ordinary. size >= 3 so rows 1 and 2 exist.
        // AbstractBlockSourceOperator splits its output at random page boundaries, so merge the chunks
        // into one page, the assertions below assume a single output page
        List<Page> input = List.of(
            CannedSourceOperator.mergePages(
                CannedSourceOperator.collectPages(simpleInputWithMultivaluedGroup(ctx.blockFactory(), between(3, 100)))
            )
        );
        List<Page> output = fuseOutput(simple().get(ctx), input);

        try {
            assertThat(output, hasSize(1));
            DoubleBlock scores = output.get(0).getBlock(scorePosition);
            assertThat(scores.isNull(0), equalTo(true));   // multivalued group -> null score
            assertThat(scores.isNull(1), equalTo(false));  // ordinary rows keep a score
            assertThat(scores.isNull(2), equalTo(false));
            assertWarnings(
                "Line 1:1: evaluation of [null] failed, treating result as null. Only first 20 failures recorded.",
                "Line 1:1: java.lang.IllegalArgumentException: group column contains multivalued entries; assigning null scores"
            );
        } finally {
            output.forEach(Page::releaseBlocks);
        }
    }
}
