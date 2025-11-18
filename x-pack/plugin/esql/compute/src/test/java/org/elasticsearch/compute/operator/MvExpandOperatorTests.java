/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.test.AbstractBlockSourceOperator;
import org.elasticsearch.compute.test.CannedSourceOperator;
import org.elasticsearch.compute.test.OperatorTestCase;
import org.elasticsearch.compute.test.TestBlockFactory;
import org.hamcrest.Matcher;

import java.util.Iterator;
import java.util.List;

import static org.elasticsearch.compute.test.BlockTestUtils.deepCopyOf;
import static org.elasticsearch.compute.test.BlockTestUtils.valuesAtPositions;
import static org.elasticsearch.compute.test.RandomBlock.randomBlock;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

public class MvExpandOperatorTests extends OperatorTestCase {
    @Override
    protected SourceOperator simpleInput(BlockFactory blockFactory, int end) {
        return new AbstractBlockSourceOperator(blockFactory, 8 * 1024) {
            private int idx;

            @Override
            protected int remaining() {
                return end - idx;
            }

            @Override
            protected Page createPage(int positionOffset, int length) {
                idx += length;
                return new Page(
                    randomBlock(blockFactory, ElementType.INT, length, true, 1, 10, 0, 0).block(),
                    randomBlock(blockFactory, ElementType.INT, length, false, 1, 10, 0, 0).block()
                );
            }
        };
    }

    @Override
    protected Operator.OperatorFactory simple(SimpleOptions options) {
        return new MvExpandOperator.Factory(0, randomIntBetween(1, 1000));
    }

    @Override
    protected Matcher<String> expectedDescriptionOfSimple() {
        return equalTo("MvExpandOperator[channel=0]");
    }

    @Override
    protected Matcher<String> expectedToStringOfSimple() {
        return expectedDescriptionOfSimple();
    }

    class BlockListIterator implements Iterator<Object> {
        private final Iterator<Page> pagesIterator;
        private final int channel;
        private Block currentBlock;
        private int nextPosition;

        BlockListIterator(List<Page> pages, int channel) {
            this.pagesIterator = pages.iterator();
            this.channel = channel;
            this.currentBlock = pagesIterator.next().getBlock(channel);
            this.nextPosition = 0;
        }

        @Override
        public boolean hasNext() {
            if (currentBlock == null) {
                return false;
            }

            return currentBlock.getValueCount(nextPosition) == 0
                || nextPosition < currentBlock.getPositionCount()
                || pagesIterator.hasNext();
        }

        @Override
        public Object next() {
            if (currentBlock != null && currentBlock.getValueCount(nextPosition) == 0) {
                nextPosition++;
                if (currentBlock.getPositionCount() == nextPosition) {
                    loadNextBlock();
                }
                return null;
            }
            List<Object> items = valuesAtPositions(currentBlock, nextPosition, nextPosition + 1).get(0);
            nextPosition++;
            if (currentBlock.getPositionCount() == nextPosition) {
                loadNextBlock();
            }
            return items.size() == 1 ? items.get(0) : items;
        }

        private void loadNextBlock() {
            if (pagesIterator.hasNext() == false) {
                currentBlock = null;
                return;
            }
            this.currentBlock = pagesIterator.next().getBlock(channel);
            nextPosition = 0;
        }
    }

    class BlockListIteratorExpander implements Iterator<Object> {
        private final Iterator<Page> pagesIterator;
        private final int channel;
        private Block currentBlock;
        private int nextPosition;
        private int nextInPosition;

        BlockListIteratorExpander(List<Page> pages, int channel) {
            this.pagesIterator = pages.iterator();
            this.channel = channel;
            this.currentBlock = pagesIterator.next().getBlock(channel);
            this.nextPosition = 0;
            this.nextInPosition = 0;
        }

        @Override
        public boolean hasNext() {
            if (currentBlock == null) {
                return false;
            }

            return currentBlock.getValueCount(nextPosition) == 0
                || nextInPosition < currentBlock.getValueCount(nextPosition)
                || nextPosition < currentBlock.getPositionCount()
                || pagesIterator.hasNext();
        }

        @Override
        public Object next() {
            if (currentBlock != null && currentBlock.getValueCount(nextPosition) == 0) {
                nextPosition++;
                if (currentBlock.getPositionCount() == nextPosition) {
                    loadNextBlock();
                }
                return null;
            }
            List<Object> items = valuesAtPositions(currentBlock, nextPosition, nextPosition + 1).get(0);
            Object result = items == null ? null : items.get(nextInPosition++);
            if (nextInPosition == currentBlock.getValueCount(nextPosition)) {
                nextPosition++;
                nextInPosition = 0;
            }
            if (currentBlock.getPositionCount() == nextPosition) {
                loadNextBlock();
            }
            return result;
        }

        private void loadNextBlock() {
            if (pagesIterator.hasNext() == false) {
                currentBlock = null;
                return;
            }
            this.currentBlock = pagesIterator.next().getBlock(channel);
            nextPosition = 0;
            nextInPosition = 0;
        }
    }

    @Override
    protected void assertSimpleOutput(List<Page> input, List<Page> results) {
        assertThat(results, hasSize(results.size()));

        var inputIter = new BlockListIteratorExpander(input, 0);
        var resultIter = new BlockListIteratorExpander(results, 0);

        while (inputIter.hasNext()) {
            assertThat(resultIter.hasNext(), equalTo(true));
            assertThat(resultIter.next(), equalTo(inputIter.next()));
        }
        assertThat(resultIter.hasNext(), equalTo(false));

        var originalMvIter = new BlockListIterator(input, 0);
        var inputIter2 = new BlockListIterator(input, 1);
        var resultIter2 = new BlockListIterator(results, 1);

        while (originalMvIter.hasNext()) {
            Object originalMv = originalMvIter.next();
            int originalMvSize = originalMv instanceof List<?> l ? l.size() : 1;
            assertThat(resultIter2.hasNext(), equalTo(true));
            Object inputValue = inputIter2.next();
            for (int j = 0; j < originalMvSize; j++) {
                assertThat(resultIter2.next(), equalTo(inputValue));
            }
        }
        assertThat(resultIter2.hasNext(), equalTo(false));
    }

    public void testNoopStatus() {
        BlockFactory blockFactory = blockFactory();
        MvExpandOperator op = new MvExpandOperator(0, randomIntBetween(1, 1000));
        List<Page> result = drive(
            op,
            List.of(new Page(blockFactory.newIntVectorBuilder(2).appendInt(1).appendInt(2).build().asBlock())).iterator(),
            driverContext()
        );
        assertThat(result, hasSize(1));
        assertThat(valuesAtPositions(result.get(0).getBlock(0), 0, 2), equalTo(List.of(List.of(1), List.of(2))));
        MvExpandOperator.Status status = op.status();
        assertThat(status.pagesReceived(), equalTo(1));
        assertThat(status.pagesEmitted(), equalTo(1));
        assertThat(status.noops(), equalTo(1));
    }

    public void testExpandStatus() {
        MvExpandOperator op = new MvExpandOperator(0, randomIntBetween(1, 1));
        BlockFactory blockFactory = blockFactory();
        var builder = blockFactory.newIntBlockBuilder(2).beginPositionEntry().appendInt(1).appendInt(2).endPositionEntry();
        List<Page> result = drive(op, List.of(new Page(builder.build())).iterator(), driverContext());
        assertThat(result, hasSize(1));
        assertThat(valuesAtPositions(result.get(0).getBlock(0), 0, 2), equalTo(List.of(List.of(1), List.of(2))));
        MvExpandOperator.Status status = op.status();
        assertThat(status.pagesReceived(), equalTo(1));
        assertThat(status.pagesEmitted(), equalTo(1));
        assertThat(status.noops(), equalTo(0));
        result.forEach(Page::releaseBlocks);
    }

    public void testExpandWithBytesRefs() {
        DriverContext context = driverContext();
        List<Page> input = CannedSourceOperator.collectPages(new AbstractBlockSourceOperator(context.blockFactory(), 8 * 1024) {
            private int idx;

            @Override
            protected int remaining() {
                return 10000 - idx;
            }

            @Override
            protected Page createPage(int positionOffset, int length) {
                idx += length;
                return new Page(
                    randomBlock(context.blockFactory(), ElementType.BYTES_REF, length, true, 1, 10, 0, 0).block(),
                    randomBlock(context.blockFactory(), ElementType.INT, length, false, 1, 10, 0, 0).block()
                );
            }
        });
        List<Page> origInput = deepCopyOf(input, TestBlockFactory.getNonBreakingInstance());
        List<Page> results = drive(new MvExpandOperator(0, randomIntBetween(1, 1000)), input.iterator(), context);
        assertSimpleOutput(origInput, results);
    }
}
