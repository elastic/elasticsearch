/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.compute.test.AnyOperatorTestCase;
import org.elasticsearch.compute.test.TestBlockFactory;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.xpack.esql.action.ColumnInfoImpl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Mockito.mock;

public class StreamingOutputOperatorTests extends AnyOperatorTestCase {

    @Override
    protected Operator.OperatorFactory simple(SimpleOptions options) {
        return new StreamingOutputOperator.StreamingOutputOperatorFactory(createTestProvider(3), p -> p);
    }

    @Override
    protected org.hamcrest.Matcher<String> expectedDescriptionOfSimple() {
        return equalTo("StreamingOutputOperator[pageSize=3]");
    }

    @Override
    protected org.hamcrest.Matcher<String> expectedToStringOfSimple() {
        return expectedDescriptionOfSimple();
    }

    @Override
    protected void assertStatus(Map<String, Object> map, List<Page> input, List<Page> output) {
        assertThat(map, nullValue());
    }

    public void testSinglePageFitsInPageSize() {
        var recorder = new ChunkRecorder();
        var provider = createTestProvider(10, recorder);
        try (var op = new StreamingOutputOperator(provider, p -> p)) {
            op.addInput(buildPage(0, 5));
            assertFalse(op.isFinished());
            op.finish();
            assertTrue(op.isFinished());

            assertThat(recorder.chunks.size(), equalTo(1));
            int totalRows = recorder.totalRowCount();
            assertThat(totalRows, equalTo(5));
            assertTrue(provider.isCompleted());
        }
    }

    public void testMultiplePagesExactFit() {
        var recorder = new ChunkRecorder();
        var provider = createTestProvider(3, recorder);
        try (var op = new StreamingOutputOperator(provider, p -> p)) {
            op.addInput(buildPage(0, 3));
            op.addInput(buildPage(3, 3));
            op.addInput(buildPage(6, 3));
            op.finish();

            assertThat(recorder.chunks.size(), equalTo(3));
            assertThat(recorder.totalRowCount(), equalTo(9));
            assertTrue(provider.isCompleted());
        }
    }

    public void testMultiplePagesWithRemainder() {
        var recorder = new ChunkRecorder();
        var provider = createTestProvider(3, recorder);
        try (var op = new StreamingOutputOperator(provider, p -> p)) {
            op.addInput(buildPage(0, 5));
            op.addInput(buildPage(5, 3));
            op.finish();

            assertThat(recorder.chunks.size(), equalTo(3));
            assertThat(recorder.totalRowCount(), equalTo(8));
            assertTrue(provider.isCompleted());
        }
    }

    public void testLargeInputSplitIntoManyPages() {
        int pageSize = 4;
        int totalRows = 25;
        var recorder = new ChunkRecorder();
        var provider = createTestProvider(pageSize, recorder);
        try (var op = new StreamingOutputOperator(provider, p -> p)) {
            op.addInput(buildPage(0, totalRows));
            op.finish();

            int expectedChunks = (totalRows + pageSize - 1) / pageSize;
            assertThat(recorder.chunks.size(), equalTo(expectedChunks));
            assertThat(recorder.totalRowCount(), equalTo(totalRows));
            assertTrue(provider.isCompleted());
        }
    }

    public void testEmptyResult() {
        var recorder = new ChunkRecorder();
        var provider = createTestProvider(10, recorder);
        try (var op = new StreamingOutputOperator(provider, p -> p)) {
            op.finish();
            assertTrue(op.isFinished());

            assertThat(recorder.chunks.size(), equalTo(0));
            assertTrue(provider.isCompleted());
        }
    }

    public void testFinishIsIdempotent() {
        var recorder = new ChunkRecorder();
        var provider = createTestProvider(3, recorder);
        try (var op = new StreamingOutputOperator(provider, p -> p)) {
            op.finish();
            assertTrue(op.isFinished());
            op.finish();
            assertTrue(op.isFinished());
        }
    }

    public void testReleaseAllChunksFreesQueuedPages() {
        RestChannel mockChannel = mock(RestChannel.class);
        var provider = new StreamingProvider(mockChannel, 3);
        provider.setColumns(List.of(new ColumnInfoImpl("val", "integer", null)));

        Page page1 = buildPage(0, 3);
        Page page2 = buildPage(3, 3);
        Page page3 = buildPage(6, 3);
        assertFalse(page2.getBlock(0).isReleased());
        assertFalse(page3.getBlock(0).isReleased());

        // First emitChunk sends to channel (mock no-op), subsequent ones go to the queue
        provider.emitChunk(List.of(page1));
        provider.emitChunk(List.of(page2));
        provider.emitChunk(List.of(page3));

        // page2 and page3 are in the chunk queue
        provider.releaseAllChunks();

        assertTrue("page2 block should be released", page2.getBlock(0).isReleased());
        assertTrue("page3 block should be released", page3.getBlock(0).isReleased());
    }

    public void testBackpressureWhenBufferFull() {
        var recorder = new ChunkRecorder();
        RestChannel mockChannel = mock(RestChannel.class);
        var provider = new BackpressureTestStreamingProvider(mockChannel, 1, recorder);
        provider.setColumns(List.of(new ColumnInfoImpl("val", "integer", null)));
        try (var op = new StreamingOutputOperator(provider, p -> p)) {
            // Emit a few pages then simulate backpressure
            op.addInput(buildPage(0, 1));
            provider.simulateBufferFull = true;

            op.addInput(buildPage(1, 1));

            assertFalse("should not need input when buffer full", op.needsInput());

            provider.simulateBufferFull = false;
            op.finish();
        }
    }

    public void testMapperIsApplied() {
        var recorder = new ChunkRecorder();
        var provider = createTestProvider(10, recorder);
        Function<Page, Page> mapper = page -> {
            try (IntBlock.Builder builder = TestBlockFactory.getNonBreakingInstance().newIntBlockBuilder(1)) {
                builder.appendInt(42);
                return new Page(builder.build());
            }
        };
        try (var op = new StreamingOutputOperator(provider, mapper)) {
            op.addInput(buildPage(0, 3));
            op.finish();
            assertTrue(op.isFinished());
        }
    }

    // --- helpers ---

    private static StreamingProvider createTestProvider(int pageSize) {
        return createTestProvider(pageSize, new ChunkRecorder());
    }

    private static StreamingProvider createTestProvider(int pageSize, ChunkRecorder recorder) {
        RestChannel mockChannel = mock(RestChannel.class);
        StreamingProvider provider = new TestableStreamingProvider(mockChannel, pageSize, recorder);
        provider.setColumns(List.of(new ColumnInfoImpl("val", "integer", null)));
        return provider;
    }

    private static Page buildPage(int startValue, int count) {
        try (IntBlock.Builder builder = TestBlockFactory.getNonBreakingInstance().newIntBlockBuilder(count)) {
            for (int i = 0; i < count; i++) {
                builder.appendInt(startValue + i);
            }
            return new Page(builder.build());
        }
    }

    /**
     * Records chunks emitted by the streaming provider without actually sending HTTP responses.
     */
    static class ChunkRecorder {
        final List<List<Page>> chunks = Collections.synchronizedList(new ArrayList<>());

        int totalRowCount() {
            return chunks.stream().flatMap(List::stream).mapToInt(Page::getPositionCount).sum();
        }
    }

    /**
     * Testable streaming provider that captures emitted chunks without requiring a real REST channel.
     */
    static class TestableStreamingProvider extends StreamingProvider {
        private final ChunkRecorder recorder;

        TestableStreamingProvider(RestChannel channel, int pageSize, ChunkRecorder recorder) {
            super(channel, pageSize);
            this.recorder = recorder;
        }

        @Override
        public void emitChunk(List<Page> pages) {
            recorder.chunks.add(new ArrayList<>(pages));
        }
    }

    /**
     * Testable streaming provider that can simulate a full buffer for backpressure tests.
     */
    static class BackpressureTestStreamingProvider extends TestableStreamingProvider {
        volatile boolean simulateBufferFull = false;

        BackpressureTestStreamingProvider(RestChannel channel, int pageSize, ChunkRecorder recorder) {
            super(channel, pageSize, recorder);
        }

        @Override
        public boolean isBufferFull() {
            return simulateBufferFull;
        }

        @Override
        public org.elasticsearch.action.support.SubscribableListener<Void> createBackpressureListener() {
            if (simulateBufferFull) {
                return new org.elasticsearch.action.support.SubscribableListener<>();
            }
            return null;
        }
    }
}
