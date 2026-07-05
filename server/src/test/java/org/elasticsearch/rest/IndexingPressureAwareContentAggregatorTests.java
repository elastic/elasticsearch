/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.rest;

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.CompositeBytesReference;
import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.http.HttpBody;
import org.elasticsearch.index.IndexingPressure;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.rest.FakeHttpBodyStream;
import org.elasticsearch.test.rest.FakeRestChannel;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.junit.After;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.stream.IntStream.range;
import static org.elasticsearch.common.bytes.BytesReferenceTestUtils.equalBytes;

public class IndexingPressureAwareContentAggregatorTests extends ESTestCase {

    private final FakeHttpBodyStream stream = new FakeHttpBodyStream();
    private final IndexingPressure indexingPressure = new IndexingPressure(Settings.EMPTY);
    private final AtomicReference<ReleasableBytesReference> contentRef = new AtomicReference<>();
    private final AtomicReference<Releasable> pressureRef = new AtomicReference<>();
    private FakeRestChannel channel;
    private IndexingPressureAwareContentAggregator aggregator;

    @After
    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        Optional.ofNullable(contentRef.get()).ifPresent(ReleasableBytesReference::close);
        assertEquals(0, indexingPressure.stats().getCurrentCoordinatingBytes());
    }

    public void testSingleChunkAggregation() {
        long maxSize = 1024;
        initAggregator(maxSize);

        var chunk = randomReleasableBytesReference(64);
        stream.sendNext(chunk, true);

        assertNotNull(contentRef.get());
        assertThat(contentRef.get(), equalBytes(chunk));
        assertEquals(chunk.length(), indexingPressure.stats().getCurrentCoordinatingBytes());
        pressureRef.get().close();
        assertEquals(0, indexingPressure.stats().getCurrentCoordinatingBytes());
    }

    public void testMultiChunkAggregation() {
        int chunkSize = between(1, 256);
        int nChunks = between(2, 20);
        long maxSize = (long) chunkSize * nChunks + 1024;
        initAggregator(maxSize);

        var chunks = range(0, nChunks).mapToObj(i -> randomReleasableBytesReference(chunkSize)).toList();
        for (int i = 0; i < nChunks - 1; i++) {
            assertTrue(stream.isRequested());
            stream.sendNext(chunks.get(i), false);
        }
        stream.sendNext(chunks.getLast(), true);

        assertNotNull(contentRef.get());
        var expected = CompositeBytesReference.of(chunks.toArray(new ReleasableBytesReference[0]));
        assertThat(contentRef.get(), equalBytes(expected));

        long actualSize = (long) chunkSize * nChunks;
        assertEquals(actualSize, indexingPressure.stats().getCurrentCoordinatingBytes());
        pressureRef.get().close();
        assertEquals(0, indexingPressure.stats().getCurrentCoordinatingBytes());
    }

    public void testRejectsOversizedBody() {
        long maxSize = 100;
        initAggregator(maxSize);

        var oversizedChunk = randomReleasableBytesReference((int) maxSize + 1);
        stream.sendNext(oversizedChunk, false);

        assertTooLargeRejected();
        assertFalse(oversizedChunk.hasReferences());
    }

    public void testRejectsBodyExceedingMaxSizeAcrossChunks() {
        long maxSize = 100;
        initAggregator(maxSize);

        var chunk1 = randomReleasableBytesReference(60);
        stream.sendNext(chunk1, false);

        var chunk2 = randomReleasableBytesReference(60);
        stream.sendNext(chunk2, false);

        assertTooLargeRejected();
        assertFalse(chunk1.hasReferences());
        assertFalse(chunk2.hasReferences());
    }

    public void testStreamCloseReleasesResources() {
        long maxSize = 1024;
        initAggregator(maxSize);

        var chunk = randomReleasableBytesReference(64);
        stream.sendNext(chunk, false);

        assertEquals(maxSize, indexingPressure.stats().getCurrentCoordinatingBytes());

        stream.close();

        assertNull(contentRef.get());
        assertFalse(chunk.hasReferences());
        assertEquals(0, indexingPressure.stats().getCurrentCoordinatingBytes());
    }

    public void testChunksAfterCloseAreReleased() {
        long maxSize = 1024;
        initAggregator(maxSize);

        stream.close();

        var lateChunk = randomReleasableBytesReference(64);
        aggregator.handleChunk(channel, lateChunk, false);
        assertFalse(lateChunk.hasReferences());
    }

    public void testRejectsOversizedLastChunk() {
        long maxSize = 100;
        initAggregator(maxSize);

        var oversizedChunk = randomReleasableBytesReference((int) maxSize + 1);
        stream.sendNext(oversizedChunk, true);

        assertTooLargeRejected();
        assertFalse(oversizedChunk.hasReferences());
    }

    public void testBodyExactlyAtMaxSizeSucceeds() {
        int maxSize = 200;
        initAggregator(maxSize);

        var chunk = randomReleasableBytesReference(maxSize);
        stream.sendNext(chunk, true);

        assertNotNull(contentRef.get());
        assertThat(contentRef.get(), equalBytes(chunk));
        assertEquals(maxSize, indexingPressure.stats().getCurrentCoordinatingBytes());
        pressureRef.get().close();
        assertEquals(0, indexingPressure.stats().getCurrentCoordinatingBytes());
    }

    public void testRejectsWhenIndexingPressureLimitExceeded() {
        long limitBytes = 1024;
        var tightPressure = new IndexingPressure(
            Settings.builder().put(IndexingPressure.MAX_COORDINATING_BYTES.getKey(), ByteSizeValue.ofBytes(limitBytes)).build()
        );
        var request = newStreamedRequest(stream);
        channel = new FakeRestChannel(request, true);
        var failureRef = new AtomicReference<Exception>();
        aggregator = new IndexingPressureAwareContentAggregator(
            request,
            tightPressure,
            limitBytes + 1,
            new IndexingPressureAwareContentAggregator.CompletionHandler() {
                @Override
                public void onComplete(RestChannel ch, ReleasableBytesReference content, Releasable pressure) {
                    fail("should not complete");
                }

                @Override
                public void onFailure(RestChannel ch, Exception e) {
                    failureRef.set(e);
                }
            },
            IndexingPressureAwareContentAggregator.BodyPostProcessor.NOOP
        );
        stream.setHandler(new HttpBody.ChunkHandler() {
            @Override
            public void onNext(ReleasableBytesReference chunk, boolean isLast) {
                aggregator.handleChunk(channel, chunk, isLast);
            }

            @Override
            public void close() {
                aggregator.streamClose();
            }
        });
        try {
            aggregator.accept(channel);
        } catch (Exception e) {
            throw new AssertionError(e);
        }

        assertNotNull("onFailure should have been called", failureRef.get());
        assertNull(contentRef.get());
        assertEquals(0, tightPressure.stats().getCurrentCoordinatingBytes());
    }

    public void testReducesPressureToActualSize() {
        long maxSize = 1024;
        int actualSize = 100;
        initAggregator(maxSize);

        assertEquals(maxSize, indexingPressure.stats().getCurrentCoordinatingBytes());

        var chunk = randomReleasableBytesReference(actualSize);
        stream.sendNext(chunk, true);

        assertEquals(actualSize, indexingPressure.stats().getCurrentCoordinatingBytes());

        pressureRef.get().close();
        assertEquals(0, indexingPressure.stats().getCurrentCoordinatingBytes());
    }

    public void testPostProcessorExpandsContent() {
        long maxSize = 1024;
        int compressedSize = 50;
        int expandedSize = 200;
        byte[] expanded = randomByteArrayOfLength(expandedSize);

        initAggregator(maxSize, (body, max) -> {
            body.close();
            return new ReleasableBytesReference(new BytesArray(expanded), () -> {});
        });

        assertEquals(maxSize, indexingPressure.stats().getCurrentCoordinatingBytes());

        var chunk = randomReleasableBytesReference(compressedSize);
        stream.sendNext(chunk, true);

        assertNotNull(contentRef.get());
        assertEquals(expandedSize, contentRef.get().length());
        assertEquals(expandedSize, indexingPressure.stats().getCurrentCoordinatingBytes());

        pressureRef.get().close();
        assertEquals(0, indexingPressure.stats().getCurrentCoordinatingBytes());
    }

    public void testPostProcessorResultExceedsMaxSize() {
        long maxSize = 100;
        int compressedSize = 50;
        int expandedSize = 200;
        byte[] expanded = randomByteArrayOfLength(expandedSize);

        initAggregator(maxSize, (body, max) -> {
            body.close();
            return new ReleasableBytesReference(new BytesArray(expanded), () -> {});
        });

        var chunk = randomReleasableBytesReference(compressedSize);
        stream.sendNext(chunk, true);

        assertTooLargeRejected();
    }

    public void testPostProcessorThrowsReleasesResources() {
        long maxSize = 1024;
        initAggregator(maxSize, (body, max) -> { throw new IOException("decompression failed"); });

        var chunk = randomReleasableBytesReference(64);
        stream.sendNext(chunk, true);

        assertNull(contentRef.get());
        assertNotNull(channel.capturedResponse());
        assertFalse(chunk.hasReferences());
        assertEquals(0, indexingPressure.stats().getCurrentCoordinatingBytes());
    }

    private RestRequest newStreamedRequest(FakeHttpBodyStream stream) {
        var httpRequest = new FakeRestRequest.FakeHttpRequest(
            RestRequest.Method.POST,
            "/",
            Map.of("Content-Type", List.of("application/x-protobuf")),
            stream
        );
        return RestRequest.request(parserConfig(), httpRequest, new FakeRestRequest.FakeHttpChannel(null));
    }

    private void assertTooLargeRejected() {
        assertNull(contentRef.get());
        assertNotNull(channel.capturedResponse());
        assertEquals(RestStatus.REQUEST_ENTITY_TOO_LARGE, channel.capturedResponse().status());
        assertEquals(0, indexingPressure.stats().getCurrentCoordinatingBytes());
    }

    private void initAggregator(long maxSize) {
        initAggregator(maxSize, IndexingPressureAwareContentAggregator.BodyPostProcessor.NOOP);
    }

    private void initAggregator(long maxSize, IndexingPressureAwareContentAggregator.BodyPostProcessor postProcessor) {
        var request = newStreamedRequest(stream);
        channel = new FakeRestChannel(request, true);
        aggregator = new IndexingPressureAwareContentAggregator(
            request,
            indexingPressure,
            maxSize,
            new IndexingPressureAwareContentAggregator.CompletionHandler() {
                @Override
                public void onComplete(RestChannel ch, ReleasableBytesReference content, Releasable pressure) {
                    contentRef.set(content);
                    pressureRef.set(pressure);
                }

                @Override
                public void onFailure(RestChannel ch, Exception e) {
                    ch.sendResponse(new RestResponse(RestStatus.REQUEST_ENTITY_TOO_LARGE, e.getMessage()));
                }
            },
            postProcessor
        );
        stream.setHandler(new HttpBody.ChunkHandler() {
            @Override
            public void onNext(ReleasableBytesReference chunk, boolean isLast) {
                aggregator.handleChunk(channel, chunk, isLast);
            }

            @Override
            public void close() {
                aggregator.streamClose();
            }
        });
        try {
            aggregator.accept(channel);
        } catch (Exception e) {
            throw new AssertionError(e);
        }
    }

}
