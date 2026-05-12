/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

public class RemoteFetchHandleTests extends ESTestCase {
    public void testBytesRefRoundTrip() {
        RemoteFetchHandle handle = randomHandle();

        BytesRef encoded = handle.toBytesRef();

        assertEquals(handle, RemoteFetchHandle.fromBytesRef(encoded));
    }

    public void testWriteableRoundTrip() throws IOException {
        RemoteFetchHandle handle = randomHandle();

        try (BytesStreamOutput out = new BytesStreamOutput()) {
            handle.writeTo(out);
            try (StreamInput in = StreamInput.wrap(out.bytes().toBytesRef().bytes)) {
                assertEquals(handle, RemoteFetchHandle.READER.read(in));
            }
        }
    }

    public void testRejectsNegativeShard() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> new RemoteFetchHandle(randomAlphaOfLength(8), randomAlphaOfLength(8), -1, 0, 0)
        );
        assertEquals("shard must be non-negative", e.getMessage());
    }

    public void testRejectsNegativeSegment() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> new RemoteFetchHandle(randomAlphaOfLength(8), randomAlphaOfLength(8), 0, -1, 0)
        );
        assertEquals("segment must be non-negative", e.getMessage());
    }

    public void testRejectsNegativeDoc() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> new RemoteFetchHandle(randomAlphaOfLength(8), randomAlphaOfLength(8), 0, 0, -1)
        );
        assertEquals("doc must be non-negative", e.getMessage());
    }

    private RemoteFetchHandle randomHandle() {
        return new RemoteFetchHandle(
            randomAlphaOfLengthBetween(5, 12),
            randomAlphaOfLengthBetween(5, 16),
            randomIntBetween(0, 1024),
            randomIntBetween(0, 4096),
            randomIntBetween(0, 1 << 20)
        );
    }
}
