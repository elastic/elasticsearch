/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.action;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.bytes.CompositeBytesReference;
import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.BytesTransportMessageTestUtils;

import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

public class GetVirtualBatchedCompoundCommitChunkResponseTests extends ESTestCase {

    public void testWriteThinPlusBytesRoundTripNewFormat() throws IOException {
        final int length = randomIntBetween(0, 4096);
        roundTrip(TransportVersion.current(), length, 0);
    }

    public void testWriteThinPlusBytesRoundTripLegacyFormat() throws IOException {
        final TransportVersion version = TransportVersion.minimumCompatible();
        assumeFalse(
            "minimum compatible version already supports the new wire format",
            version.supports(GetVirtualBatchedCompoundCommitChunkResponse.VBCC_CHUNK_RESPONSE_WITHOUT_LENGTH_PREFIX)
        );
        final int length = randomIntBetween(0, 4096);
        roundTrip(version, length, greaterThan(0));
    }

    private void roundTrip(TransportVersion version, int length, int expectedThinLength) throws IOException {
        roundTrip(version, length, equalTo(expectedThinLength));
    }

    private void roundTrip(TransportVersion version, int length, org.hamcrest.Matcher<Integer> thinLengthMatcher) throws IOException {
        final ReleasableBytesReference data = randomReleasableBytesReference(length);
        final var response = new GetVirtualBatchedCompoundCommitChunkResponse(data);
        try {
            final var wireOutput = new BytesStreamOutput();
            wireOutput.setTransportVersion(version);
            BytesTransportMessageTestUtils.writeThinWithBytes(wireOutput, response);

            final var thinOnlyOutput = new BytesStreamOutput();
            thinOnlyOutput.setTransportVersion(version);
            response.writeThin(thinOnlyOutput);
            assertThat(thinOnlyOutput.bytes().length(), thinLengthMatcher);

            try (StreamInput in = ReleasableBytesReference.wrap(wireOutput.bytes()).streamInput()) {
                in.setTransportVersion(version);
                final var deserialized = new GetVirtualBatchedCompoundCommitChunkResponse(in);
                try {
                    assertEquals(response.getData(), deserialized.getData());
                } finally {
                    deserialized.decRef();
                }
            }
        } finally {
            response.decRef();
        }
    }

    public void testWriteThinPlusBytesRoundTripViaCompositeBytesReference() throws IOException {
        final TransportVersion version = randomFrom(TransportVersion.current(), TransportVersion.minimumCompatible());
        final int length = randomIntBetween(0, 4096);
        final ReleasableBytesReference data = randomReleasableBytesReference(length);
        final var response = new GetVirtualBatchedCompoundCommitChunkResponse(data);
        try {
            final var thinOutput = new BytesStreamOutput();
            thinOutput.setTransportVersion(version);
            response.writeThin(thinOutput);
            final ReleasableBytesReference payload = response.bytes();
            payload.mustIncRef();
            try {
                final var zeroCopyBytes = CompositeBytesReference.of(thinOutput.bytes(), payload);

                try (
                    ReleasableBytesReference wireBytes = ReleasableBytesReference.adopt(zeroCopyBytes);
                    StreamInput in = wireBytes.streamInput()
                ) {
                    in.setTransportVersion(version);
                    final var deserialized = new GetVirtualBatchedCompoundCommitChunkResponse(in);
                    try {
                        assertEquals(response.getData(), deserialized.getData());
                    } finally {
                        deserialized.decRef();
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
