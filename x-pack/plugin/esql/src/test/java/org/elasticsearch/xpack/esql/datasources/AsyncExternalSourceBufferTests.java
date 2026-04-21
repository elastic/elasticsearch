/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.ElasticsearchTimeoutException;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.CloseableIterator;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * Unit tests for {@link AsyncExternalSourceBuffer} producer backpressure, including
 * {@link AsyncExternalSourceBuffer#awaitSpaceForProducer} timeout behavior.
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

    /**
     * When the buffer stays full because no consumer calls {@link AsyncExternalSourceBuffer#pollPage()},
     * {@link AsyncExternalSourceBuffer#awaitSpaceForProducer} must time out with
     * {@link ElasticsearchTimeoutException}. The buffer must remain consistent and recover after the
     * producer gives up (drain pages, {@link AsyncExternalSourceBuffer#finish}).
     */
    public void testAwaitSpaceForProducerTimeoutLeavesBufferConsistent() {
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

        var ex = expectThrows(ElasticsearchTimeoutException.class, () -> buffer.awaitSpaceForProducer(TimeValue.timeValueMillis(50)));
        assertTrue(ex.getMessage().contains("timeout waiting for async external source buffer space"));

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

    /**
     * Same scenario through {@link ExternalSourceDrainUtils}: a tight drain timeout and a stuck consumer
     * surface {@link ElasticsearchTimeoutException}. Byte accounting for queued pages matches
     * {@link AsyncExternalSourceBuffer#bytesInBuffer()}, and the buffer is fully recoverable.
     */
    public void testExternalSourceDrainUtilsTimeoutWithStuckConsumer() {
        long maxBufferBytes = 1500;
        AsyncExternalSourceBuffer buffer = new AsyncExternalSourceBuffer(maxBufferBytes);

        int totalPages = 8;
        List<Page> sourcePages = new ArrayList<>();
        for (int i = 0; i < totalPages; i++) {
            sourcePages.add(createTestPage(2, 50));
        }

        CloseableIterator<Page> it = new CloseableIterator<>() {
            private int index = 0;

            @Override
            public boolean hasNext() {
                return index < sourcePages.size();
            }

            @Override
            public Page next() {
                if (index >= sourcePages.size()) {
                    throw new NoSuchElementException();
                }
                return sourcePages.get(index++);
            }

            @Override
            public void close() {}
        };

        var ex = expectThrows(
            ElasticsearchTimeoutException.class,
            () -> ExternalSourceDrainUtils.drainPages(it, buffer, TimeValue.timeValueMillis(200))
        );
        assertTrue(ex.getMessage().contains("timeout waiting for async external source buffer space"));

        assertTrue(buffer.size() >= 1);
        long bytesInBuffer = buffer.bytesInBuffer();
        long sumBytes = 0;
        for (Page p = buffer.pollPage(); p != null; p = buffer.pollPage()) {
            sumBytes += p.ramBytesUsedByBlocks();
            p.releaseBlocks();
        }
        assertEquals(0, buffer.size());
        assertEquals(0, buffer.bytesInBuffer());
        assertEquals(bytesInBuffer, sumBytes);
        assertTrue("drain should not have ingested all pages before timing out", it.hasNext());
        buffer.finish(true);
    }
}
