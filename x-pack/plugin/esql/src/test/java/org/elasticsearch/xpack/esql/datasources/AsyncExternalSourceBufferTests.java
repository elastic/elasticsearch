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
import org.elasticsearch.test.ESTestCase;

/**
 * Unit tests for {@link AsyncExternalSourceBuffer} backpressure via {@link AsyncExternalSourceBuffer#waitForSpace()}.
 */
public class AsyncExternalSourceBufferTests extends ESTestCase {

    private static final BlockFactory BLOCK_FACTORY = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE)
        .breaker(new NoopCircuitBreaker("none"))
        .build();

    private static Page createTestPage(int numColumns, int numRows) {
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

    public void testWaitForSpaceReturnsCompletedWhenBufferHasRoom() {
        AsyncExternalSourceBuffer buffer = new AsyncExternalSourceBuffer(1024 * 1024);
        SubscribableListener<Void> space = buffer.waitForSpace();
        assertTrue("waitForSpace should be immediately done when buffer is empty", space.isDone());
        buffer.finish(true);
    }

    public void testWaitForSpaceReturnsPendingWhenBufferFull() {
        long maxBufferBytes = 1500;
        AsyncExternalSourceBuffer buffer = new AsyncExternalSourceBuffer(maxBufferBytes);

        while (buffer.bytesInBuffer() < maxBufferBytes) {
            buffer.addPage(createTestPage(2, 50));
        }
        assertTrue(buffer.bytesInBuffer() >= maxBufferBytes);

        SubscribableListener<Void> space = buffer.waitForSpace();
        assertFalse("waitForSpace should NOT be done when buffer is full", space.isDone());

        buffer.pollPage().releaseBlocks();
        assertTrue("waitForSpace should complete after pollPage frees space", space.isDone());

        buffer.finish(true);
    }

    public void testWaitForSpaceCompletesOnFinish() {
        long maxBufferBytes = 1500;
        AsyncExternalSourceBuffer buffer = new AsyncExternalSourceBuffer(maxBufferBytes);

        while (buffer.bytesInBuffer() < maxBufferBytes) {
            buffer.addPage(createTestPage(2, 50));
        }

        SubscribableListener<Void> space = buffer.waitForSpace();
        assertFalse(space.isDone());

        buffer.finish(true);
        assertTrue("waitForSpace should complete when buffer is finished (cancelled)", space.isDone());
    }

    public void testBufferConsistentAfterFullAndDrain() {
        long maxBufferBytes = 1500;
        AsyncExternalSourceBuffer buffer = new AsyncExternalSourceBuffer(maxBufferBytes);

        long expectedBytes = 0;
        while (buffer.bytesInBuffer() < maxBufferBytes) {
            Page p = createTestPage(2, 50);
            expectedBytes += p.ramBytesUsedByBlocks();
            buffer.addPage(p);
        }
        assertTrue(buffer.bytesInBuffer() >= maxBufferBytes);
        int sizeBeforeWait = buffer.size();

        assertEquals(sizeBeforeWait, buffer.size());
        assertEquals(expectedBytes, buffer.bytesInBuffer());
        assertFalse(buffer.noMoreInputs());

        for (int i = 0; i < sizeBeforeWait; i++) {
            Page p = buffer.pollPage();
            assertNotNull(p);
            p.releaseBlocks();
        }
        assertEquals(0, buffer.size());
        assertEquals(0, buffer.bytesInBuffer());
        buffer.finish(true);
    }
}
