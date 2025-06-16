/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.rest;

import org.elasticsearch.common.bytes.CompositeBytesReference;
import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.http.HttpBody;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.rest.FakeHttpBodyStream;
import org.elasticsearch.test.rest.FakeRestRequest.FakeHttpChannel;
import org.elasticsearch.test.rest.FakeRestRequest.FakeHttpRequest;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.stream.IntStream.range;
import static org.elasticsearch.rest.RestContentAggregator.aggregate;

public class RestContentAggregatorTests extends ESTestCase {

    RestRequest newRestRequest(int size) {
        return RestRequest.request(
            parserConfig(),
            new FakeHttpRequest(
                RestRequest.Method.POST,
                "/",
                Map.of("Content-Length", List.of(Integer.toString(size))),
                HttpBody.fromBytesReference(randomBytesReference(size))
            ),
            new FakeHttpChannel(null)
        );
    }

    public void testFullBodyPassThrough() {
        var fullRequest = newRestRequest(between(1, 1024));
        aggregate(fullRequest, (aggregated) -> assertEquals(fullRequest.content(), aggregated.content()));
    }

    public void testZeroLengthStream() {
        var stream = new FakeHttpBodyStream();
        var request = newRestRequest(0);
        request.getHttpRequest().setBody(stream);
        var aggregatedRef = new AtomicReference<RestRequest>();
        aggregate(request, aggregatedRef::set);
        stream.sendNext(ReleasableBytesReference.empty(), true);
        assertEquals(0, aggregatedRef.get().contentLength());
    }

    public void testAggregateRandomSize() {
        var chunkSize = between(1, 1024);
        var nChunks = between(1, 1000);
        var stream = new FakeHttpBodyStream();
        var streamChunks = range(0, nChunks).mapToObj(i -> randomReleasableBytesReference(chunkSize)).toList();
        var request = newRestRequest(chunkSize * nChunks);
        request.getHttpRequest().setBody(stream);
        AtomicReference<RestRequest> aggregatedRef = new AtomicReference<>();

        aggregate(request, aggregatedRef::set);

        for (var i = 0; i < nChunks - 1; i++) {
            assertTrue(stream.isRequested());
            stream.sendNext(streamChunks.get(i), false);
        }
        assertTrue(stream.isRequested());
        stream.sendNext(streamChunks.getLast(), true);

        var aggregated = aggregatedRef.get();
        var expectedBytes = CompositeBytesReference.of(streamChunks.toArray(new ReleasableBytesReference[0]));
        assertEquals(expectedBytes, aggregated.content());
        aggregated.content().close();
    }

    public void testReleaseChunksOnClose() {
        var chunkSize = between(1, 1024);
        var nChunks = between(1, 100);

        var stream = new FakeHttpBodyStream();
        var request = newRestRequest(chunkSize * nChunks * 2);
        request.getHttpRequest().setBody(stream);
        AtomicReference<RestRequest> aggregatedRef = new AtomicReference<>();

        aggregate(request, aggregatedRef::set);

        // buffered chunks, must be released after close()
        var chunksBeforeClose = range(0, nChunks).mapToObj(i -> randomReleasableBytesReference(chunkSize)).toList();
        for (var chunk : chunksBeforeClose) {
            assertTrue(stream.isRequested());
            stream.sendNext(chunk, false);
        }
        stream.close();
        assertFalse(chunksBeforeClose.stream().anyMatch(ReleasableBytesReference::hasReferences));

        // non-buffered, must be released on arrival
        var chunksAfterClose = range(0, nChunks).mapToObj(i -> randomReleasableBytesReference(chunkSize)).toList();
        for (var chunk : chunksAfterClose) {
            assertTrue(stream.isRequested());
            stream.sendNext(chunk, false);
        }
        assertFalse(chunksAfterClose.stream().anyMatch(ReleasableBytesReference::hasReferences));

        assertNull(aggregatedRef.get());
    }

}
