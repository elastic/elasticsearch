/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.action;

import org.elasticsearch.common.bytes.CompositeBytesReference;
import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

public class GetVirtualBatchedCompoundCommitChunkResponseTests extends ESTestCase {

    public void testWriteToMatchesWriteThinPlusBytes() throws IOException {
        final int length = randomIntBetween(1, 4096);
        final ReleasableBytesReference data = randomReleasableBytesReference(length);
        final var response = new GetVirtualBatchedCompoundCommitChunkResponse(data);
        try {
            final var writeToOutput = new BytesStreamOutput();
            response.writeTo(writeToOutput);
            final var writeToBytes = writeToOutput.bytes();

            final var thinOutput = new BytesStreamOutput();
            response.writeThin(thinOutput);
            final ReleasableBytesReference payload = response.bytes();
            payload.mustIncRef();
            try {
                final var zeroCopyBytes = CompositeBytesReference.of(thinOutput.bytes(), payload);
                assertEquals(writeToBytes, zeroCopyBytes);

                try (StreamInput in = writeToBytes.streamInput()) {
                    final var deserializedFromWriteTo = new GetVirtualBatchedCompoundCommitChunkResponse(in);
                    try {
                        assertEquals(response.getData(), deserializedFromWriteTo.getData());
                    } finally {
                        deserializedFromWriteTo.decRef();
                    }
                }
                try (StreamInput in = zeroCopyBytes.streamInput()) {
                    final var deserializedFromZeroCopy = new GetVirtualBatchedCompoundCommitChunkResponse(in);
                    try {
                        assertEquals(response.getData(), deserializedFromZeroCopy.getData());
                    } finally {
                        deserializedFromZeroCopy.decRef();
                    }
                }
            } finally {
                payload.decRef();
            }
        } finally {
            response.decRef();
        }
    }
}
