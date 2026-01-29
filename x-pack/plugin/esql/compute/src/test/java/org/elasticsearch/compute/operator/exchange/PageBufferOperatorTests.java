/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.exchange;

import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.IsBlockedResult;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class PageBufferOperatorTests extends ESTestCase {

    private BlockFactory blockFactory;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        MockBigArrays bigArrays = new MockBigArrays(PageCacheRecycler.NON_RECYCLING_INSTANCE, ByteSizeValue.ofGb(1));
        CircuitBreaker breaker = bigArrays.breakerService().getBreaker(CircuitBreaker.REQUEST);
        blockFactory = new BlockFactory(breaker, bigArrays);
    }

    private BatchPage createTestBatchPage(long batchId, int pageIndex) {
        IntBlock block = blockFactory.newConstantIntBlockWith(42, 1);
        Page page = new Page(block);
        return new BatchPage(page, batchId, pageIndex, pageIndex == 0);  // First page is also last for simplicity
    }

    public void testInitialState() {
        PageBufferOperator sink = new PageBufferOperator();
        assertThat(sink.size(), equalTo(0));
        assertThat(sink.needsInput(), is(true));
        assertThat(sink.isFinished(), is(false));
        assertThat(sink.isDone(), is(false));
    }

    public void testAddAndPollSinglePage() {
        PageBufferOperator sink = new PageBufferOperator();

        BatchPage page = createTestBatchPage(1, 0);
        sink.addInput(page);

        assertThat(sink.size(), equalTo(1));
        assertThat(sink.needsInput(), is(true));  // Cache has room for more (size 100)

        BatchPage polled = sink.poll();
        assertThat(polled, notNullValue());
        assertThat(polled.batchId(), equalTo(1L));

        assertThat(sink.size(), equalTo(0));
        assertThat(sink.needsInput(), is(true));  // Cache is empty now
    }

    public void testBlocksWhenFull() {
        PageBufferOperator sink = new PageBufferOperator();

        // Fill the cache to capacity
        for (int i = 0; i < PageBufferOperator.CACHE_SIZE; i++) {
            BatchPage page = createTestBatchPage(i, 0);
            sink.addInput(page);
        }

        // Should be blocked now (cache full)
        IsBlockedResult blocked = sink.isBlocked();
        assertThat(blocked.listener().isDone(), is(false));

        // Poll a page
        BatchPage polled = sink.poll();
        assertThat(polled, notNullValue());

        // Should be unblocked now
        assertThat(blocked.listener().isDone(), is(true));

        // Clean up remaining pages
        sink.close();
    }

    public void testWaitForPage() {
        PageBufferOperator sink = new PageBufferOperator();

        // Initially empty - should be blocked waiting for page
        IsBlockedResult waitResult = sink.waitForPage();
        assertThat(waitResult.listener().isDone(), is(false));

        // Add a page
        BatchPage page = createTestBatchPage(1, 0);
        sink.addInput(page);

        // Should be unblocked now
        assertThat(waitResult.listener().isDone(), is(true));

        // Wait again - should be not blocked since page is available
        IsBlockedResult waitResult2 = sink.waitForPage();
        assertThat(waitResult2, equalTo(Operator.NOT_BLOCKED));
    }

    public void testFinishWithEmptyCache() {
        PageBufferOperator sink = new PageBufferOperator();
        assertThat(sink.isFinished(), is(false));

        sink.finish();

        assertThat(sink.isFinished(), is(true));
        assertThat(sink.isDone(), is(true));
        assertThat(sink.isUpstreamFinished(), is(true));
    }

    public void testFinishWithPagesInCache() {
        PageBufferOperator sink = new PageBufferOperator();

        BatchPage page = createTestBatchPage(1, 0);
        sink.addInput(page);

        sink.finish();

        // Not finished yet - still has pages
        assertThat(sink.isFinished(), is(false));
        assertThat(sink.isDone(), is(false));
        assertThat(sink.isUpstreamFinished(), is(true));

        // Poll the page
        BatchPage polled = sink.poll();
        assertThat(polled, notNullValue());

        // Now should be finished
        assertThat(sink.isFinished(), is(true));
        assertThat(sink.isDone(), is(true));
    }

    public void testWaitForPageUnblocksOnFinish() {
        PageBufferOperator sink = new PageBufferOperator();

        // Wait for page
        IsBlockedResult waitResult = sink.waitForPage();
        assertThat(waitResult.listener().isDone(), is(false));

        // Finish upstream
        sink.finish();

        // Should be unblocked (no more pages coming)
        assertThat(waitResult.listener().isDone(), is(true));
    }

    public void testPollReturnsNullWhenEmpty() {
        PageBufferOperator sink = new PageBufferOperator();
        assertThat(sink.poll(), nullValue());
    }

    public void testClose() {
        PageBufferOperator sink = new PageBufferOperator();

        // Add a page
        BatchPage page = createTestBatchPage(1, 0);
        sink.addInput(page);

        // Close
        sink.close();

        assertThat(sink.size(), equalTo(0));
        assertThat(sink.isFinished(), is(true));
    }

    public void testCloseUnblocksFutures() {
        PageBufferOperator sink = new PageBufferOperator();

        // Fill cache to capacity to get blocked future
        for (int i = 0; i < PageBufferOperator.CACHE_SIZE; i++) {
            sink.addInput(createTestBatchPage(i, 0));
        }
        IsBlockedResult blocked = sink.isBlocked();
        assertThat(blocked.listener().isDone(), is(false));

        // Get wait for page future on another sink
        PageBufferOperator sink2 = new PageBufferOperator();
        IsBlockedResult waitResult = sink2.waitForPage();
        assertThat(waitResult.listener().isDone(), is(false));

        // Close both sinks
        sink.close();
        sink2.close();

        // Both futures should be resolved
        assertThat(blocked.listener().isDone(), is(true));
        assertThat(waitResult.listener().isDone(), is(true));
    }

    public void testOnlyAcceptsBatchPage() {
        PageBufferOperator sink = new PageBufferOperator();

        IntBlock block = blockFactory.newConstantIntBlockWith(42, 1);
        Page regularPage = new Page(block);

        expectThrows(IllegalArgumentException.class, () -> sink.addInput(regularPage));

        regularPage.releaseBlocks();
    }
}
