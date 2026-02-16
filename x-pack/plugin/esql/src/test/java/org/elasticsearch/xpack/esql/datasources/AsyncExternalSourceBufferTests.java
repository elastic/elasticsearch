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
 * including backpressure via waitForSpace() and waitForWriting().
 */
public class AsyncExternalSourceBufferTests extends ESTestCase {

    private static final BlockFactory BLOCK_FACTORY = BlockFactory.getInstance(
        new NoopCircuitBreaker("test"),
        BigArrays.NON_RECYCLING_INSTANCE
    );

    /**
     * Creates a test page with a single integer block.
     */
    private Page createTestPage() {
        IntBlock block = BLOCK_FACTORY.newIntBlockBuilder(1).appendInt(42).build();
        return new Page(block);
    }

    public void testConstructorValidation() {
        // Test invalid max size
        expectThrows(IllegalArgumentException.class, () -> new AsyncExternalSourceBuffer(0));
        expectThrows(IllegalArgumentException.class, () -> new AsyncExternalSourceBuffer(-1));

        // Valid construction
        AsyncExternalSourceBuffer buffer = new AsyncExternalSourceBuffer(1);
        assertNotNull(buffer);
        assertEquals(0, buffer.size());
        assertFalse(buffer.isFinished());
        assertFalse(buffer.noMoreInputs());
    }

    public void testAddAndPollPage() {
        AsyncExternalSourceBuffer buffer = new AsyncExternalSourceBuffer(10);

        // Create test page
        Page page = createTestPage();

        // Add page
        buffer.addPage(page);
        assertEquals(1, buffer.size());

        // Poll page
        Page polled = buffer.pollPage();
        assertSame(page, polled);
        assertEquals(0, buffer.size());

        // Poll from empty buffer
        assertNull(buffer.pollPage());

        // Clean up
        page.releaseBlocks();
    }

    public void testWaitForSpaceWhenNotFull() {
        AsyncExternalSourceBuffer buffer = new AsyncExternalSourceBuffer(10);

        // Buffer is not full - should return completed listener
        SubscribableListener<Void> listener = buffer.waitForSpace();
        assertTrue("Listener should be done when buffer has space", listener.isDone());
    }

    public void testWaitForSpaceWhenFull() {
        AsyncExternalSourceBuffer buffer = new AsyncExternalSourceBuffer(2);

        // Fill the buffer
        Page page1 = createTestPage();
        Page page2 = createTestPage();
        buffer.addPage(page1);
        buffer.addPage(page2);
        assertEquals(2, buffer.size());

        // Buffer is full - should return pending listener
        SubscribableListener<Void> listener = buffer.waitForSpace();
        assertFalse("Listener should not be done when buffer is full", listener.isDone());

        // Poll a page to make space
        Page polled = buffer.pollPage();
        polled.releaseBlocks();
        assertEquals(1, buffer.size());

        // Now the listener should be completed
        assertTrue("Listener should be done after space is made", listener.isDone());

        // Clean up
        buffer.finish(true);
    }

    public void testWaitForWritingWhenNotFull() {
        AsyncExternalSourceBuffer buffer = new AsyncExternalSourceBuffer(10);

        // Buffer is not full - should return NOT_BLOCKED
        IsBlockedResult result = buffer.waitForWriting();
        assertTrue("Should not be blocked when buffer has space", result.listener().isDone());
    }

    public void testWaitForWritingWhenFull() {
        AsyncExternalSourceBuffer buffer = new AsyncExternalSourceBuffer(2);

        // Fill the buffer
        Page page1 = createTestPage();
        Page page2 = createTestPage();
        buffer.addPage(page1);
        buffer.addPage(page2);

        // Buffer is full - should return blocked result
        IsBlockedResult result = buffer.waitForWriting();
        assertFalse("Should be blocked when buffer is full", result.listener().isDone());
        assertEquals("async external source buffer full", result.reason());

        // Poll a page to make space
        Page polled = buffer.pollPage();
        polled.releaseBlocks();

        // Now should not be blocked
        assertTrue("Should not be blocked after space is made", result.listener().isDone());

        // Clean up
        buffer.finish(true);
    }

    public void testWaitForReadingWhenEmpty() {
        AsyncExternalSourceBuffer buffer = new AsyncExternalSourceBuffer(10);

        // Buffer is empty - should return blocked result
        IsBlockedResult result = buffer.waitForReading();
        assertFalse("Should be blocked when buffer is empty", result.listener().isDone());
        assertEquals("async external source buffer empty", result.reason());

        // Add a page
        Page page = createTestPage();
        buffer.addPage(page);

        // Now should not be blocked
        assertTrue("Should not be blocked after page is added", result.listener().isDone());

        // Clean up
        buffer.finish(true);
    }

    public void testWaitForReadingWhenNotEmpty() {
        AsyncExternalSourceBuffer buffer = new AsyncExternalSourceBuffer(10);

        // Add a page
        Page page = createTestPage();
        buffer.addPage(page);

        // Buffer has data - should return NOT_BLOCKED
        IsBlockedResult result = buffer.waitForReading();
        assertTrue("Should not be blocked when buffer has data", result.listener().isDone());

        // Clean up
        buffer.finish(true);
    }

    public void testFinish() {
        AsyncExternalSourceBuffer buffer = new AsyncExternalSourceBuffer(10);

        assertFalse(buffer.noMoreInputs());
        assertFalse(buffer.isFinished());

        // Finish without draining
        buffer.finish(false);

        assertTrue(buffer.noMoreInputs());
        assertTrue(buffer.isFinished());
    }

    public void testFinishWithDraining() {
        AsyncExternalSourceBuffer buffer = new AsyncExternalSourceBuffer(10);

        // Add some pages
        Page page1 = createTestPage();
        Page page2 = createTestPage();
        buffer.addPage(page1);
        buffer.addPage(page2);
        assertEquals(2, buffer.size());

        // Finish with draining - pages should be released
        buffer.finish(true);

        assertTrue(buffer.noMoreInputs());
        assertTrue(buffer.isFinished());
        assertEquals(0, buffer.size());
    }

    public void testOnFailure() {
        AsyncExternalSourceBuffer buffer = new AsyncExternalSourceBuffer(10);

        // Add a page
        Page page = createTestPage();
        buffer.addPage(page);

        // Fail the buffer
        Exception failure = new RuntimeException("test failure");
        buffer.onFailure(failure);

        assertTrue(buffer.noMoreInputs());
        assertTrue(buffer.isFinished());
        assertSame(failure, buffer.failure());
        // Pages should be released by onFailure
    }

    public void testAddPageAfterFinish() {
        AsyncExternalSourceBuffer buffer = new AsyncExternalSourceBuffer(10);

        // Finish the buffer
        buffer.finish(false);

        // Try to add a page after finish - it should be released
        Page page = createTestPage();
        buffer.addPage(page);
        // Page should be released since buffer is finished
    }

    public void testAddPageAfterFailure() {
        AsyncExternalSourceBuffer buffer = new AsyncExternalSourceBuffer(10);

        // Fail the buffer
        buffer.onFailure(new RuntimeException("test"));

        // Try to add a page after failure - it should be released
        Page page = createTestPage();
        buffer.addPage(page);
        // Page should be released since buffer has failed
    }

    public void testWaitForSpaceAfterFinish() {
        AsyncExternalSourceBuffer buffer = new AsyncExternalSourceBuffer(2);

        // Fill the buffer
        Page page1 = createTestPage();
        Page page2 = createTestPage();
        buffer.addPage(page1);
        buffer.addPage(page2);

        // Finish the buffer (with draining to release pages)
        buffer.finish(true);

        // waitForSpace should return completed listener even if buffer was full
        SubscribableListener<Void> listener = buffer.waitForSpace();
        assertTrue("Listener should be done after finish", listener.isDone());
    }

    public void testCompletionListener() {
        AsyncExternalSourceBuffer buffer = new AsyncExternalSourceBuffer(10);

        // Add completion listener
        java.util.concurrent.atomic.AtomicBoolean completed = new java.util.concurrent.atomic.AtomicBoolean(false);
        buffer.addCompletionListener(org.elasticsearch.action.ActionListener.wrap(v -> completed.set(true), e -> fail("Should not fail")));

        assertFalse(completed.get());

        // Finish the buffer
        buffer.finish(false);

        assertTrue("Completion listener should be called", completed.get());
    }

    public void testCompletionListenerOnFailure() {
        AsyncExternalSourceBuffer buffer = new AsyncExternalSourceBuffer(10);

        // Add completion listener
        java.util.concurrent.atomic.AtomicReference<Exception> failureRef = new java.util.concurrent.atomic.AtomicReference<>();
        buffer.addCompletionListener(org.elasticsearch.action.ActionListener.wrap(v -> fail("Should not succeed"), failureRef::set));

        assertNull(failureRef.get());

        // Fail the buffer
        Exception failure = new RuntimeException("test failure");
        buffer.onFailure(failure);

        assertNotNull("Completion listener should receive failure", failureRef.get());
        assertEquals("test failure", failureRef.get().getCause().getMessage());
    }
}
