/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.transport;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.Build;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.InputStreamStreamInput;
import org.elasticsearch.common.io.stream.OutputStreamStreamOutput;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.core.UpdateForV10;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.test.TransportVersionUtils;

import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;

import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThan;

public class TransportHandshakerRawMessageTests extends ESSingleNodeTestCase {

    @UpdateForV10(owner = UpdateForV10.Owner.CORE_INFRA) // remove support for v8 handshakes in v10
    public void testV8Handshake() throws Exception {
        final BytesRef handshakeRequestBytes;
        final var requestId = randomNonNegativeLong();
        final var requestNodeTransportVersionId = TransportVersionUtils.randomCompatibleVersion(random()).id();
        try (var outputStream = new BytesStreamOutput()) {
            outputStream.setTransportVersion(TransportHandshaker.V8_HANDSHAKE_VERSION);
            outputStream.writeLong(requestId);
            outputStream.writeByte(TransportStatus.setRequest(TransportStatus.setHandshake((byte) 0)));
            outputStream.writeInt(TransportHandshaker.V8_HANDSHAKE_VERSION.id());
            outputStream.writeInt(0x1a); // length of variable-length header, always 0x1a
            outputStream.writeByte((byte) 0); // no request headers;
            outputStream.writeByte((byte) 0); // no response headers;
            outputStream.writeByte((byte) 0); // no features;
            outputStream.writeString("internal:tcp/handshake");
            outputStream.writeByte((byte) 0); // no parent task ID;

            assertThat(requestNodeTransportVersionId, allOf(greaterThanOrEqualTo(1 << 22), lessThan(1 << 28))); // 4-byte vInt
            outputStream.writeByte((byte) 4); // payload length
            outputStream.writeVInt(requestNodeTransportVersionId);

            handshakeRequestBytes = outputStream.bytes().toBytesRef();
        }

        final BytesRef handshakeResponseBytes;
        try (var socket = openTransportConnection()) {
            var streamOutput = new OutputStreamStreamOutput(socket.getOutputStream());
            streamOutput.write("ES".getBytes(StandardCharsets.US_ASCII));
            streamOutput.writeInt(handshakeRequestBytes.length);
            streamOutput.writeBytes(handshakeRequestBytes.bytes, handshakeRequestBytes.offset, handshakeRequestBytes.length);
            streamOutput.flush();

            var streamInput = new InputStreamStreamInput(socket.getInputStream());
            assertEquals((byte) 'E', streamInput.readByte());
            assertEquals((byte) 'S', streamInput.readByte());
            var responseLength = streamInput.readInt();
            handshakeResponseBytes = streamInput.readBytesRef(responseLength);
        }

        try (var inputStream = new BytesArray(handshakeResponseBytes).streamInput()) {
            assertEquals(requestId, inputStream.readLong());
            assertEquals(TransportStatus.setResponse(TransportStatus.setHandshake((byte) 0)), inputStream.readByte());
            assertEquals(TransportHandshaker.V8_HANDSHAKE_VERSION.id(), inputStream.readInt());
            assertEquals(2, inputStream.readInt()); // length of variable-length header, always 0x02
            assertEquals((byte) 0, inputStream.readByte()); // no request headers
            assertEquals((byte) 0, inputStream.readByte()); // no response headers
            inputStream.setTransportVersion(TransportHandshaker.V8_HANDSHAKE_VERSION);
            assertEquals(requestNodeTransportVersionId, inputStream.readVInt());
            assertEquals(-1, inputStream.read());
        }
    }

    @UpdateForV10(owner = UpdateForV10.Owner.CORE_INFRA) // remove support for v9 handshakes in v11
    public void testV9Handshake() throws Exception {
        final BytesRef handshakeRequestBytes;
        final var requestId = randomNonNegativeLong();
        final var requestNodeTransportVersionId = TransportVersionUtils.randomCompatibleVersion(random()).id();
        try (var outputStream = new BytesStreamOutput()) {
            outputStream.setTransportVersion(TransportHandshaker.V9_HANDSHAKE_VERSION);
            outputStream.writeLong(requestId);
            outputStream.writeByte(TransportStatus.setRequest(TransportStatus.setHandshake((byte) 0)));
            outputStream.writeInt(TransportHandshaker.V9_HANDSHAKE_VERSION.id());
            outputStream.writeInt(0x19); // length of variable-length header, always 0x19
            outputStream.writeByte((byte) 0); // no request headers;
            outputStream.writeByte((byte) 0); // no response headers;
            outputStream.writeString("internal:tcp/handshake");
            outputStream.writeByte((byte) 0); // no parent task ID;

            assertThat(requestNodeTransportVersionId, allOf(greaterThanOrEqualTo(1 << 22), lessThan(1 << 28))); // 4-byte vInt
            final var releaseVersionLength = between(0, 127 - 5); // so that its length, and the length of the payload, is a one-byte vInt
            final var requestNodeReleaseVersion = randomAlphaOfLength(releaseVersionLength);
            outputStream.writeByte((byte) (4 + 1 + releaseVersionLength)); // payload length
            outputStream.writeVInt(requestNodeTransportVersionId);
            outputStream.writeString(requestNodeReleaseVersion);

            handshakeRequestBytes = outputStream.bytes().toBytesRef();
        }

        final BytesRef handshakeResponseBytes;
        try (var socket = openTransportConnection()) {
            var streamOutput = new OutputStreamStreamOutput(socket.getOutputStream());
            streamOutput.write("ES".getBytes(StandardCharsets.US_ASCII));
            streamOutput.writeInt(handshakeRequestBytes.length);
            streamOutput.writeBytes(handshakeRequestBytes.bytes, handshakeRequestBytes.offset, handshakeRequestBytes.length);
            streamOutput.flush();

            var streamInput = new InputStreamStreamInput(socket.getInputStream());
            assertEquals((byte) 'E', streamInput.readByte());
            assertEquals((byte) 'S', streamInput.readByte());
            var responseLength = streamInput.readInt();
            handshakeResponseBytes = streamInput.readBytesRef(responseLength);
        }

        try (var inputStream = new BytesArray(handshakeResponseBytes).streamInput()) {
            assertEquals(requestId, inputStream.readLong());
            assertEquals(TransportStatus.setResponse(TransportStatus.setHandshake((byte) 0)), inputStream.readByte());
            assertEquals(TransportHandshaker.V9_HANDSHAKE_VERSION.id(), inputStream.readInt());
            assertEquals(2, inputStream.readInt()); // length of variable-length header, always 0x02
            assertEquals((byte) 0, inputStream.readByte()); // no request headers
            assertEquals((byte) 0, inputStream.readByte()); // no response headers
            inputStream.setTransportVersion(TransportHandshaker.V9_HANDSHAKE_VERSION);
            assertEquals(requestNodeTransportVersionId, inputStream.readVInt());
            assertEquals(Build.current().version(), inputStream.readString());
            assertEquals(-1, inputStream.read());
        }
    }

    public void testOutboundHandshake() throws Exception {
        final BytesRef handshakeRequestBytes;

        try (var serverSocket = new ServerSocket(0, 1, InetAddress.getLoopbackAddress())) {
            getInstanceFromNode(TransportService.class).openConnection(
                DiscoveryNodeUtils.builder(randomIdentifier())
                    .address(new TransportAddress(serverSocket.getInetAddress(), serverSocket.getLocalPort()))
                    .build(),
                ConnectionProfile.buildSingleChannelProfile(TransportRequestOptions.Type.REG, null, null, null, null, null),
                ActionListener.noop()
            );

            try (
                var acceptedSocket = serverSocket.accept();
                var streamInput = new InputStreamStreamInput(acceptedSocket.getInputStream())
            ) {
                assertEquals((byte) 'E', streamInput.readByte());
                assertEquals((byte) 'S', streamInput.readByte());
                var responseLength = streamInput.readInt();
                handshakeRequestBytes = streamInput.readBytesRef(responseLength);
            }
        }

        final BytesRef payloadBytes;

        try (var inputStream = new BytesArray(handshakeRequestBytes).streamInput()) {
            assertThat(inputStream.readLong(), greaterThan(0L));
            assertEquals(TransportStatus.setRequest(TransportStatus.setHandshake((byte) 0)), inputStream.readByte());
            assertEquals(TransportHandshaker.V9_HANDSHAKE_VERSION.id(), inputStream.readInt());
            assertEquals(0x19, inputStream.readInt()); // length of variable-length header, always 0x19
            assertEquals((byte) 0, inputStream.readByte()); // no request headers
            assertEquals((byte) 0, inputStream.readByte()); // no response headers
            assertEquals("internal:tcp/handshake", inputStream.readString());
            assertEquals((byte) 0, inputStream.readByte()); // no parent task
            inputStream.setTransportVersion(TransportHandshaker.V8_HANDSHAKE_VERSION);
            payloadBytes = inputStream.readBytesRef();
            assertEquals(-1, inputStream.read());
        }

        try (var inputStream = new BytesArray(payloadBytes).streamInput()) {
            inputStream.setTransportVersion(TransportHandshaker.V9_HANDSHAKE_VERSION);
            assertEquals(TransportVersion.current().id(), inputStream.readVInt());
            assertEquals(Build.current().version(), inputStream.readString());
            assertEquals(-1, inputStream.read());
        }
    }

    private Socket openTransportConnection() throws Exception {
        final var transportAddress = randomFrom(getInstanceFromNode(TransportService.class).boundAddress().boundAddresses()).address();
        return new Socket(transportAddress.getAddress(), transportAddress.getPort());
    }
}
