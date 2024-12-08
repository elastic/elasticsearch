/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.rest;

import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.core.AbstractRefCounted;
import org.elasticsearch.http.HttpBody;
import org.elasticsearch.http.HttpRequest;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.rest.FakeRestChannel;
import org.elasticsearch.xcontent.XContentParserConfiguration;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.IntStream;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.test.rest.FakeRestRequest.FakeHttpRequest;

public class RestContentAggregatorTests extends ESTestCase {

    static RestRequest restRequest(HttpRequest request) {
        return RestRequest.request(XContentParserConfiguration.EMPTY, request, null);
    }

    static RestRequest restRequest(HttpStream stream) {
        return restRequest(new FakeHttpRequest(GET, "/", stream, Map.of("content-length", List.of("" + stream.contentLength))));
    }

    static FakeRestChannel restChan(RestRequest request) {
        return new FakeRestChannel(request, false, 1);
    }

    static List<byte[]> randomChunksList() {
        return fixedChunksList(between(1, 16), between(1024, 8192));
    }

    static List<byte[]> fixedChunksList(int n, int size) {
        return IntStream.range(0, n).mapToObj(i -> randomByteArrayOfLength(size)).toList();
    }

    static byte[] concatChunks(List<byte[]> chunks) {
        var size = chunks.stream().mapToInt(c -> c.length).sum();
        var out = new byte[size];
        var off = 0;
        for (var chunk : chunks) {
            System.arraycopy(chunk, 0, out, off, chunk.length);
            off += chunk.length;
        }
        return out;
    }

    public void testNoContent() {
        try (var stream = HttpStream.noContent()) {
            var request = restRequest(stream);
            var result = new SubscribableListener<RestRequest>();
            RestContentAggregator.aggregate(request, restChan(request), (req, chan) -> result.onResponse(req));
            assertEquals(0, safeAwait(result).content().length());
        }
    }

    public void testAggregateChunks() {
        var chunks = randomChunksList();
        try (var stream = HttpStream.of(chunks)) {
            var request = restRequest(stream);
            var result = new SubscribableListener<RestRequest>();
            RestContentAggregator.aggregate(request, restChan(request), (r, c) -> result.onResponse(r));
            var aggBytes = safeAwait(result).content().toBytesRef().bytes;
            assertArrayEquals(concatChunks(chunks), aggBytes);
        }
    }

    /**
     * An HttpBody Stream implementation with single thread executor. Must be closed after use.
     */
    static class HttpStream implements HttpBody.Stream {
        static final byte[] EMPTY = new byte[] {};
        final ExecutorService executor = Executors.newSingleThreadExecutor();
        final List<ReleasableBytesReference> chunks;
        final int contentLength;
        final SubscribableListener<Exception> err = new SubscribableListener<>();
        int ind;
        ChunkHandler handler;

        HttpStream(List<ReleasableBytesReference> chunks) {
            this.chunks = chunks;
            this.contentLength = chunks.stream().mapToInt(ReleasableBytesReference::length).sum();
        }

        static HttpStream noContent() {
            return of(EMPTY);
        }

        static HttpStream of(byte[]... chunks) {
            return new HttpStream(fromArrays(chunks));
        }

        static HttpStream of(List<byte[]> chunks) {
            return new HttpStream(fromList(chunks));
        }

        static ReleasableBytesReference wrappedChunk(byte[] arr) {
            var bytesArray = new BytesArray(arr);
            return new ReleasableBytesReference(bytesArray, new AbstractRefCounted() {
                @Override
                protected void closeInternal() {

                }
            });
        }

        static List<ReleasableBytesReference> fromArrays(byte[]... chunks) {
            return java.util.stream.Stream.of(chunks).map(HttpStream::wrappedChunk).toList();
        }

        static List<ReleasableBytesReference> fromList(List<byte[]> chunks) {
            return chunks.stream().map(HttpStream::wrappedChunk).toList();
        }

        @Override
        public HttpBody.ChunkHandler handler() {
            return handler;
        }

        @Override
        public void addTracingHandler(ChunkHandler chunkHandler) {}

        @Override
        public void setHandler(ChunkHandler chunkHandler) {
            this.handler = chunkHandler;
        }

        @Override
        public void next() {
            executor.submit(() -> {
                var chunk = chunks.get(ind);
                if (chunk != null) {
                    ind++;
                    try {
                        handler.onNext(chunk, ind == chunks.size());
                    } catch (Exception e) {
                        err.onResponse(e);
                    }
                }
            });
        }

        @Override
        public void close() {
            handler.close();
            executor.shutdownNow();
        }
    }

}
