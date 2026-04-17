/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.IsBlockedResult;
import org.elasticsearch.test.ESTestCase;

/**
 * Tests for {@link AsyncExternalSourceBuffer}.
 *
 * Tests the thread-safe buffer for async external source data,
 * including byte-based backpressure via waitForSpace() and waitForWriting().
 */
public class AsyncExternalSourceBufferTests extends ESTestCase {

    private static final BlockFactory BLOCK_FACTORY = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE)
        .breaker(new NoopCircuitBreaker("none"))
        .build();

    private Page createTestPage() {
        IntBlock block = BLOCK_FACTORY.newIntBlockBuilder(1).appendInt(42).build();
        return new Page(block);
    }

    private Page createTestPage(int numColumns, int numRows) {
        IntBlock.Builder[] builders = new IntBlock.Builder[numColumns];
        for (int c = 0; c < numColumns; c++) {
            builders[c] = BLOCK_FACTORY.newIntBlockBuilder(numRows);
            for (int r = 0; r < numRows; r++) {
                builders[c].appendInt(r);
            }
        }
        IntBlock[] blocks = new IntBlock[numColumns];
        for (int c = 0; c < numColumns; c++) {
            blocks[c] = builders[c].build();
        }
        return new Page(blocks);
    }

    public void testConstructorValidation() {
        expectThrows(IllegalArgumentException.class, () -> new AsyncExternalSourceBuffer(0));
        expectThrows(IllegalArgumentException.class, () -> new AsyncExternalSourceBuffer(-1));

        AsyncExternalSourceBuffer buffer = new AsyncExternalSourceBuffer(1);
        assertNotNull(buffer);
        assertEquals(0, buffer.size());
        assertEquals(0L, buffer.bytesInBuffer());
        assertFalse(buffer.isFinished());
        assertFalse(buffer.noMoreInputs());
    }

    public void testAddAndPollPage() {
        AsyncExternalSourceBuffer buffer = new AsyncExternalSourceBuffer(AsyncExternalSourceBuffer.DEFAULT_MAX_BUFFER_BYTES);

        Page page = createTestPage();

        buffer.addPage(page);
        assertEquals(1, buffer.size());
        assertTrue(buffer.bytesInBuffer() > 0);

        Page polled = buffer.pollPage();
        assertSame(page, polled);
        assertEquals(0, buffer.size());
        assertEquals(0L, buffer.bytesInBuffer());

        assertNull(buffer.pollPage());

        page.releaseBlocks();
    }

    public void testWaitForSpaceWhenNotFull() {
        AsyncExternalSourceBuffer buffer = new AsyncExternalSourceBuffer(AsyncExternalSourceBuffer.DEFAULT_MAX_BUFFER_BYTES);

        SubscribableListener<Void> listener = buffer.waitForSpace();
        assertTrue("Listener should be done when buffer has space", listener.isDone());
    }

    public void testWaitForSpaceWhenFull() {
        Page page1 = createTestPage();
        long pageBytes = page1.ramBytesUsedByBlocks();
        AsyncExternalSourceBuffer buffer = new AsyncExternalSourceBuffer(pageBytes * 2);

        Page page2 = createTestPage();
        buffer.addPage(page1);
        buffer.addPage(page2);
        assertEquals(2, buffer.size());

        SubscribableListener<Void> listener = buffer.waitForSpace();
        assertFalse("Listener should not be done when buffer is full", listener.isDone());

        Page polled = buffer.pollPage();
        polled.releaseBlocks();
        assertEquals(1, buffer.size());

        assertTrue("Listener should be done after space is made", listener.isDone());

        buffer.finish(true);
    }

    public void testWaitForWritingWhenNotFull() {
        AsyncExternalSourceBuffer buffer = new AsyncExternalSourceBuffer(AsyncExternalSourceBuffer.DEFAULT_MAX_BUFFER_BYTES);

        IsBlockedResult result = buffer.waitForWriting();
        assertTrue("Should not be blocked when buffer has space", result.listener().isDone());
    }

    public void testWaitForWritingWhenFull() {
        Page page1 = createTestPage();
        long pageBytes = page1.ramBytesUsedByBlocks();
        AsyncExternalSourceBuffer buffer = new AsyncExternalSourceBuffer(pageBytes * 2);

        Page page2 = createTestPage();
        buffer.addPage(page1);
        buffer.addPage(page2);

        IsBlockedResult result = buffer.waitForWriting();
        assertFalse("Should be blocked when buffer is full", result.listener().isDone());
        assertEquals("async external source buffer full", result.reason());

        Page polled = buffer.pollPage();
        polled.releaseBlocks();

        assertTrue("Should not be blocked after space is made", result.listener().isDone());

        buffer.finish(true);
    }

    public void testWaitForReadingWhenEmpty() {
        AsyncExternalSourceBuffer buffer = new AsyncExternalSourceBuffer(AsyncExternalSourceBuffer.DEFAULT_MAX_BUFFER_BYTES);

        IsBlockedResult result = buffer.waitForReading();
        assertFalse("Should be blocked when buffer is empty", result.listener().isDone());
        assertEquals("async external source buffer empty", result.reason());

        Page page = createTestPage();
        buffer.addPage(page);

        assertTrue("Should not be blocked after page is added", result.listener().isDone());

        buffer.finish(true);
    }

    public void testWaitForReadingWhenNotEmpty() {
        AsyncExternalSourceBuffer buffer = new AsyncExternalSourceBuffer(AsyncExternalSourceBuffer.DEFAULT_MAX_BUFFER_BYTES);

        Page page = createTestPage();
        buffer.addPage(page);

        IsBlockedResult result = buffer.waitForReading();
        assertTrue("Should not be blocked when buffer has data", result.listener().isDone());

        buffer.finish(true);
    }

    public void testFinish() {
        AsyncExternalSourceBuffer buffer = new AsyncExternalSourceBuffer(AsyncExternalSourceBuffer.DEFAULT_MAX_BUFFER_BYTES);

        assertFalse(buffer.noMoreInputs());
        assertFalse(buffer.isFinished());

        buffer.finish(false);

        assertTrue(buffer.noMoreInputs());
        assertTrue(buffer.isFinished());
    }

    public void testFinishWithDraining() {
        AsyncExternalSourceBuffer buffer = new AsyncExternalSourceBuffer(AsyncExternalSourceBuffer.DEFAULT_MAX_BUFFER_BYTES);

        Page page1 = createTestPage();
        Page page2 = createTestPage();
        buffer.addPage(page1);
        buffer.addPage(page2);
        assertEquals(2, buffer.size());

        buffer.finish(true);

        assertTrue(buffer.noMoreInputs());
        assertTrue(buffer.isFinished());
        assertEquals(0, buffer.size());
        assertEquals(0L, buffer.bytesInBuffer());
    }

    public void testOnFailure() {
        AsyncExternalSourceBuffer buffer = new AsyncExternalSourceBuffer(AsyncExternalSourceBuffer.DEFAULT_MAX_BUFFER_BYTES);

        Page page = createTestPage();
        buffer.addPage(page);

        Exception failure = new RuntimeException("test failure");
        buffer.onFailure(failure);

        assertTrue(buffer.noMoreInputs());
        assertFalse("Completion is deferred until queued pages are drained", buffer.isFinished());
        assertSame(failure, buffer.failure());
        assertEquals(1, buffer.size());

        Page drained = buffer.pollPage();
        assertNotNull(drained);
        drained.releaseBlocks();
        assertTrue(buffer.isFinished());
    }

    public void testAddPageAfterFinish() {
        AsyncExternalSourceBuffer buffer = new AsyncExternalSourceBuffer(AsyncExternalSourceBuffer.DEFAULT_MAX_BUFFER_BYTES);

        buffer.finish(false);

        Page page = createTestPage();
        buffer.addPage(page);
    }

    public void testAddPageAfterFailure() {
        AsyncExternalSourceBuffer buffer = new AsyncExternalSourceBuffer(AsyncExternalSourceBuffer.DEFAULT_MAX_BUFFER_BYTES);

        buffer.onFailure(new RuntimeException("test"));

        Page page = createTestPage();
        buffer.addPage(page);
    }

    public void testWaitForSpaceAfterFinish() {
        Page page1 = createTestPage();
        long pageBytes = page1.ramBytesUsedByBlocks();
        AsyncExternalSourceBuffer buffer = new AsyncExternalSourceBuffer(pageBytes * 2);

        Page page2 = createTestPage();
        buffer.addPage(page1);
        buffer.addPage(page2);

        buffer.finish(true);

        SubscribableListener<Void> listener = buffer.waitForSpace();
        assertTrue("Listener should be done after finish", listener.isDone());
    }

    public void testCompletionListener() {
        AsyncExternalSourceBuffer buffer = new AsyncExternalSourceBuffer(AsyncExternalSourceBuffer.DEFAULT_MAX_BUFFER_BYTES);

        java.util.concurrent.atomic.AtomicBoolean completed = new java.util.concurrent.atomic.AtomicBoolean(false);
        buffer.addCompletionListener(org.elasticsearch.action.ActionListener.wrap(v -> completed.set(true), e -> fail("Should not fail")));

        assertFalse(completed.get());

        buffer.finish(false);

        assertTrue("Completion listener should be called", completed.get());
    }

    public void testCompletionListenerOnFailure() {
        AsyncExternalSourceBuffer buffer = new AsyncExternalSourceBuffer(AsyncExternalSourceBuffer.DEFAULT_MAX_BUFFER_BYTES);

        java.util.concurrent.atomic.AtomicReference<Exception> failureRef = new java.util.concurrent.atomic.AtomicReference<>();
        buffer.addCompletionListener(org.elasticsearch.action.ActionListener.wrap(v -> fail("Should not succeed"), failureRef::set));

        assertNull(failureRef.get());

        Exception failure = new RuntimeException("test failure");
        buffer.onFailure(failure);

        assertNotNull("Completion listener should receive failure", failureRef.get());
        assertEquals("test failure", failureRef.get().getCause().getMessage());
    }

    public void testByteBasedBackpressure() {
        Page smallPage = createTestPage(1, 10);
        long pageBytes = smallPage.ramBytesUsedByBlocks();
        assertTrue("Test page should have non-zero bytes", pageBytes > 0);

        long maxBytes = pageBytes + pageBytes / 2;
        AsyncExternalSourceBuffer buffer = new AsyncExternalSourceBuffer(maxBytes);

        buffer.addPage(smallPage);
        assertEquals(pageBytes, buffer.bytesInBuffer());

        SubscribableListener<Void> spaceListener = buffer.waitForSpace();
        assertTrue("Should have space for one page", spaceListener.isDone());

        Page secondPage = createTestPage(1, 10);
        buffer.addPage(secondPage);
        assertTrue("Should exceed byte limit with two pages", buffer.bytesInBuffer() >= maxBytes);

        spaceListener = buffer.waitForSpace();
        assertFalse("Should be blocked when byte limit exceeded", spaceListener.isDone());

        Page polled = buffer.pollPage();
        polled.releaseBlocks();

        assertTrue("Should unblock after polling", spaceListener.isDone());

        buffer.finish(true);
    }

    public void testWidePagesBackpressureSooner() {
        long maxBytes = 2000;
        AsyncExternalSourceBuffer narrowBuffer = new AsyncExternalSourceBuffer(maxBytes);
        AsyncExternalSourceBuffer wideBuffer = new AsyncExternalSourceBuffer(maxBytes);

        int narrowCount = 0;
        while (narrowBuffer.waitForSpace().isDone()) {
            narrowBuffer.addPage(createTestPage(1, 10));
            narrowCount++;
            if (narrowCount > 100) break;
        }

        int wideCount = 0;
        while (wideBuffer.waitForSpace().isDone()) {
            wideBuffer.addPage(createTestPage(10, 10));
            wideCount++;
            if (wideCount > 100) break;
        }

        assertTrue("Wide pages should trigger backpressure sooner than narrow pages", wideCount < narrowCount);

        narrowBuffer.finish(true);
        wideBuffer.finish(true);
    }

    public void testBytesTrackedCorrectly() {
        AsyncExternalSourceBuffer buffer = new AsyncExternalSourceBuffer(AsyncExternalSourceBuffer.DEFAULT_MAX_BUFFER_BYTES);

        Page page1 = createTestPage(2, 10);
        Page page2 = createTestPage(3, 10);
        long bytes1 = page1.ramBytesUsedByBlocks();
        long bytes2 = page2.ramBytesUsedByBlocks();

        buffer.addPage(page1);
        assertEquals(bytes1, buffer.bytesInBuffer());

        buffer.addPage(page2);
        assertEquals(bytes1 + bytes2, buffer.bytesInBuffer());

        Page polled = buffer.pollPage();
        polled.releaseBlocks();
        assertEquals(bytes2, buffer.bytesInBuffer());

        polled = buffer.pollPage();
        polled.releaseBlocks();
        assertEquals(0L, buffer.bytesInBuffer());
    }

    public void testBytesResetOnDrain() {
        AsyncExternalSourceBuffer buffer = new AsyncExternalSourceBuffer(AsyncExternalSourceBuffer.DEFAULT_MAX_BUFFER_BYTES);

        buffer.addPage(createTestPage(2, 10));
        buffer.addPage(createTestPage(3, 10));
        assertTrue(buffer.bytesInBuffer() > 0);

        buffer.finish(true);

        assertEquals(0L, buffer.bytesInBuffer());
    }

    public void testDefaultMaxBufferBytes() {
        assertEquals(10L * 262144, AsyncExternalSourceBuffer.DEFAULT_MAX_BUFFER_BYTES);
    }
}
