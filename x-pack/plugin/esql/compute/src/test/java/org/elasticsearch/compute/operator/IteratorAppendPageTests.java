/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.test.OperatorTestCase;
import org.elasticsearch.compute.test.SequenceLongBlockSourceOperator;
import org.elasticsearch.compute.test.TestBlockFactory;
import org.elasticsearch.core.ReleasableIterator;
import org.hamcrest.Matcher;

import java.util.List;
import java.util.stream.LongStream;

import static org.hamcrest.Matchers.equalTo;

/**
 * Tests {@link AbstractPageMappingToIteratorOperator} against a test
 * subclass that appends {@code 1} and chunks the incoming {@link Page}
 * at {@code 100} positions.
 */
public class IteratorAppendPageTests extends OperatorTestCase {
    private static final int ADDED_VALUE = 1;
    private static final int CHUNK = 100;

    private static class IteratorAppendPage extends AbstractPageMappingToIteratorOperator {
        private static class Factory implements Operator.OperatorFactory {
            @Override
            public Operator get(DriverContext driverContext) {
                return new IteratorAppendPage(driverContext.blockFactory());
            }

            @Override
            public String describe() {
                return "IteratorAppendPage[]";
            }
        }

        private final BlockFactory blockFactory;

        private IteratorAppendPage(BlockFactory blockFactory) {
            this.blockFactory = blockFactory;
        }

        @Override
        protected ReleasableIterator<Page> receive(Page page) {
            return appendBlocks(page, new ReleasableIterator<>() {
                private int positionOffset;

                @Override
                public boolean hasNext() {
                    return positionOffset < page.getPositionCount();
                }

                @Override
                public Block next() {
                    if (hasNext() == false) {
                        throw new IllegalStateException();
                    }
                    int positions = Math.min(page.getPositionCount() - positionOffset, CHUNK);
                    positionOffset += positions;
                    return blockFactory.newConstantIntBlockWith(ADDED_VALUE, positions);
                }

                @Override
                public void close() {
                    // Nothing to do, appendBlocks iterator closes the page for us.
                }
            });
        }

        @Override
        public String toString() {
            return "IteratorAppendPage[]";
        }
    }

    @Override
    protected SourceOperator simpleInput(BlockFactory blockFactory, int size) {
        return new SequenceLongBlockSourceOperator(blockFactory, LongStream.range(0, size).map(l -> randomLong()));
    }

    @Override
    protected void assertSimpleOutput(List<Page> input, List<Page> results) {
        int r = 0;
        for (Page in : input) {
            for (int offset = 0; offset < in.getPositionCount(); offset += CHUNK) {
                Page resultPage = results.get(r++);
                assertThat(resultPage.getPositionCount(), equalTo(Math.min(CHUNK, in.getPositionCount() - offset)));
                assertThat(
                    resultPage.getBlock(1),
                    equalTo(TestBlockFactory.getNonBreakingInstance().newConstantIntBlockWith(ADDED_VALUE, resultPage.getPositionCount()))
                );
            }
        }
    }

    @Override
    protected Operator.OperatorFactory simple(SimpleOptions options) {
        return new IteratorAppendPage.Factory();
    }

    @Override
    protected Matcher<String> expectedDescriptionOfSimple() {
        return equalTo("IteratorAppendPage[]");
    }

    @Override
    protected Matcher<String> expectedToStringOfSimple() {
        return expectedDescriptionOfSimple();
    }
}
