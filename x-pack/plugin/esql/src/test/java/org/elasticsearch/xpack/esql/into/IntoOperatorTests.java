/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.into;

import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.IsBlockedResult;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.compute.operator.PageConsumer;
import org.elasticsearch.compute.test.ComputeTestCase;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

import static org.hamcrest.Matchers.equalTo;

public class IntoOperatorTests extends ComputeTestCase {

    private static class CollectingConsumer implements PageConsumer {
        final List<Page> pages = new ArrayList<>();
        boolean finished;

        @Override
        public void consume(Page page) {
            pages.add(page);
        }

        @Override
        public void finish() {
            finished = true;
        }

        @Override
        public boolean isFinished() {
            return finished;
        }

        @Override
        public IsBlockedResult isBlocked() {
            return Operator.NOT_BLOCKED;
        }
    }

    public void testZeroPagesEmitsZeroZero() {
        BlockFactory bf = blockFactory();
        CollectingConsumer consumer = new CollectingConsumer();
        SyncIntoOperator op = new SyncIntoOperator(consumer, Function.identity(), bf);
        op.finish();
        assertTrue(op.canProduceMoreDataWithoutExtraInput());
        Page out = op.getOutput();
        assertNotNull(out);
        assertThat(out.getPositionCount(), equalTo(1));
        assertThat(((LongBlock) out.getBlock(0)).getLong(0), equalTo(0L));
        assertThat(((LongBlock) out.getBlock(1)).getLong(0), equalTo(0L));
        assertTrue(op.isFinished());
        out.releaseBlocks();
    }

    public void testMultiplePagesAreSummed() {
        BlockFactory bf = blockFactory();
        CollectingConsumer consumer = new CollectingConsumer();
        SyncIntoOperator op = new SyncIntoOperator(consumer, Function.identity(), bf);
        for (int i = 0; i < 5; i++) {
            op.addInput(singleRowPage(bf));
        }
        op.finish();
        assertThat(consumer.pages.size(), equalTo(5));
        Page out = op.getOutput();
        assertNotNull(out);
        assertThat(((LongBlock) out.getBlock(0)).getLong(0), equalTo(5L));
        assertThat(((LongBlock) out.getBlock(1)).getLong(0), equalTo(5L));
        out.releaseBlocks();
        consumer.pages.forEach(Page::releaseBlocks);
    }

    private Page singleRowPage(BlockFactory bf) {
        return new Page(bf.newConstantLongBlockWith(42L, 1));
    }

    /**
     * A test-only synchronous IntoOperator variant that uses a counting consumer rather than IntoPageWriter.
     */
    private static final class SyncIntoOperator implements Operator {
        private final CollectingConsumer consumer;
        private final Function<Page, Page> alignment;
        private final BlockFactory blockFactory;
        private long count;
        private boolean inputFinished;
        private boolean outputEmitted;

        SyncIntoOperator(CollectingConsumer consumer, Function<Page, Page> alignment, BlockFactory blockFactory) {
            this.consumer = consumer;
            this.alignment = alignment;
            this.blockFactory = blockFactory;
        }

        @Override
        public boolean needsInput() {
            return inputFinished == false;
        }

        @Override
        public void addInput(Page page) {
            count += page.getPositionCount();
            consumer.consume(alignment.apply(page));
        }

        @Override
        public void finish() {
            inputFinished = true;
            consumer.finish();
        }

        @Override
        public boolean isFinished() {
            return outputEmitted;
        }

        @Override
        public boolean canProduceMoreDataWithoutExtraInput() {
            return inputFinished && consumer.isFinished() && outputEmitted == false;
        }

        @Override
        public IsBlockedResult isBlocked() {
            return consumer.isBlocked();
        }

        @Override
        public Page getOutput() {
            if (inputFinished == false || consumer.isFinished() == false) {
                return null;
            }
            outputEmitted = true;
            return new IntoStats(count, count).toPage(blockFactory);
        }

        @Override
        public void close() {}
    }
}
