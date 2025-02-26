/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.transport;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.RecyclerBytesStreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.MockPageCacheRecycler;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.TransportVersionUtils;
import org.elasticsearch.transport.InboundDecoder.ChannelType;

import java.io.IOException;
import java.util.ArrayList;

import static org.elasticsearch.common.bytes.ReleasableBytesReferenceStreamInputTests.wrapAsReleasable;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.instanceOf;

public class InboundDecoderTests extends ESTestCase {

    private ThreadContext threadContext;
    private BytesRefRecycler recycler;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        threadContext = new ThreadContext(Settings.EMPTY);
        recycler = new BytesRefRecycler(new MockPageCacheRecycler(Settings.EMPTY));
    }

    public void testDecode() throws IOException {
        boolean isRequest = randomBoolean();
        String action = "test-request";
        long requestId = randomNonNegativeLong();
        final String headerKey = randomAlphaOfLength(10);
        final String headerValue = randomAlphaOfLength(20);
        if (isRequest) {
            threadContext.putHeader(headerKey, headerValue);
        } else {
            threadContext.addResponseHeader(headerKey, headerValue);
        }
        OutboundMessage message;
        if (isRequest) {
            message = new OutboundMessage.Request(
                threadContext,
                new TestRequest(randomAlphaOfLength(100)),
                TransportVersion.current(),
                action,
                requestId,
                false,
                null
            );
        } else {
            message = new OutboundMessage.Response(
                threadContext,
                new TestResponse(randomAlphaOfLength(100)),
                TransportVersion.current(),
                requestId,
                false,
                null
            );
        }

        try (RecyclerBytesStreamOutput os = new RecyclerBytesStreamOutput(recycler)) {
            final BytesReference totalBytes = message.serialize(os);
            int totalHeaderSize = TcpHeader.headerSize(TransportVersion.current()) + totalBytes.getInt(
                TcpHeader.VARIABLE_HEADER_SIZE_POSITION
            );
            final BytesReference messageBytes = totalBytes.slice(totalHeaderSize, totalBytes.length() - totalHeaderSize);

            InboundDecoder decoder = new InboundDecoder(recycler);
            final ArrayList<Object> fragments = new ArrayList<>();
            final ReleasableBytesReference releasable1 = wrapAsReleasable(totalBytes);
            int bytesConsumed = decoder.decode(releasable1, fragments::add);
            assertEquals(totalHeaderSize, bytesConsumed);
            assertTrue(releasable1.hasReferences());

            final Header header = (Header) fragments.get(0);
            assertEquals(requestId, header.getRequestId());
            assertEquals(TransportVersion.current(), header.getVersion());
            assertFalse(header.isCompressed());
            assertFalse(header.isHandshake());
            if (isRequest) {
                assertEquals(action, header.getActionName());
                assertTrue(header.isRequest());
                assertEquals(header.getHeaders().v1().get(headerKey), headerValue);
            } else {
                assertTrue(header.isResponse());
                assertThat(header.getHeaders().v2().get(headerKey), hasItems(headerValue));
            }
            assertFalse(header.needsToReadVariableHeader());
            fragments.clear();

            final BytesReference bytes2 = totalBytes.slice(bytesConsumed, totalBytes.length() - bytesConsumed);
            final ReleasableBytesReference releasable2 = wrapAsReleasable(bytes2);
            int bytesConsumed2 = decoder.decode(releasable2, fragments::add);
            assertEquals(totalBytes.length() - totalHeaderSize, bytesConsumed2);

            final Object content = fragments.get(0);
            final Object endMarker = fragments.get(1);

            assertEquals(messageBytes, content);
            // Ref count is incremented since the bytes are forwarded as a fragment
            assertTrue(releasable2.hasReferences());
            releasable2.decRef();
            assertTrue(releasable2.hasReferences());
            assertTrue(releasable2.decRef());
            assertEquals(InboundDecoder.END_CONTENT, endMarker);
        }

    }

    public void testDecodeHandshakeV8Compatibility() throws IOException {
        doHandshakeCompatibilityTest(TransportHandshaker.V8_HANDSHAKE_VERSION, null);
        doHandshakeCompatibilityTest(TransportHandshaker.V8_HANDSHAKE_VERSION, Compression.Scheme.DEFLATE);
    }

    public void testDecodeHandshakeV9Compatibility() throws IOException {
        doHandshakeCompatibilityTest(TransportHandshaker.V9_HANDSHAKE_VERSION, null);
        doHandshakeCompatibilityTest(TransportHandshaker.V9_HANDSHAKE_VERSION, Compression.Scheme.DEFLATE);
    }

    private void doHandshakeCompatibilityTest(TransportVersion transportVersion, Compression.Scheme compressionScheme) throws IOException {
        String action = "test-request";
        long requestId = randomNonNegativeLong();
        final String headerKey = randomAlphaOfLength(10);
        final String headerValue = randomAlphaOfLength(20);
        threadContext.putHeader(headerKey, headerValue);
        OutboundMessage message = new OutboundMessage.Request(
            threadContext,
            new TestRequest(randomAlphaOfLength(100)),
            transportVersion,
            action,
            requestId,
            true,
            compressionScheme
        );

        try (RecyclerBytesStreamOutput os = new RecyclerBytesStreamOutput(recycler)) {
            final BytesReference bytes = message.serialize(os);
            int totalHeaderSize = TcpHeader.headerSize(transportVersion);

            InboundDecoder decoder = new InboundDecoder(recycler);
            final ArrayList<Object> fragments = new ArrayList<>();
            final ReleasableBytesReference releasable1 = wrapAsReleasable(bytes);
            int bytesConsumed = decoder.decode(releasable1, fragments::add);
            assertThat(bytesConsumed, greaterThan(totalHeaderSize));
            assertTrue(releasable1.hasReferences());

            final Header header = (Header) fragments.get(0);
            assertEquals(requestId, header.getRequestId());
            assertEquals(transportVersion, header.getVersion());
            assertEquals(compressionScheme == Compression.Scheme.DEFLATE, header.isCompressed());
            assertTrue(header.isHandshake());
            assertTrue(header.isRequest());
            assertFalse(header.needsToReadVariableHeader());
            assertEquals(headerValue, header.getRequestHeaders().get(headerKey));
            fragments.clear();
        }
    }

    public void testClientChannelTypeFailsDecodingRequests() throws Exception {
        String action = "test-request";
        long requestId = randomNonNegativeLong();
        if (randomBoolean()) {
            final String headerKey = randomAlphaOfLength(10);
            final String headerValue = randomAlphaOfLength(20);
            if (randomBoolean()) {
                threadContext.putHeader(headerKey, headerValue);
            } else {
                threadContext.addResponseHeader(headerKey, headerValue);
            }
        }
        // a request
        final var isHandshake = randomBoolean();
        final var version = isHandshake
            ? randomFrom(TransportHandshaker.ALLOWED_HANDSHAKE_VERSIONS)
            : TransportVersionUtils.randomCompatibleVersion(random());
        logger.info("--> version = {}", version);
        OutboundMessage message = new OutboundMessage.Request(
            threadContext,
            new TestRequest(randomAlphaOfLength(100)),
            version,
            action,
            requestId,
            isHandshake,
            randomFrom(Compression.Scheme.DEFLATE, Compression.Scheme.LZ4, null)
        );

        try (RecyclerBytesStreamOutput os = new RecyclerBytesStreamOutput(recycler)) {
            final BytesReference bytes = message.serialize(os);
            try (InboundDecoder clientDecoder = new InboundDecoder(recycler, ChannelType.CLIENT)) {
                IllegalArgumentException e = expectThrows(
                    IllegalArgumentException.class,
                    () -> clientDecoder.decode(wrapAsReleasable(bytes), ignored -> {})
                );
                assertThat(e.getMessage(), containsString("client channels do not accept inbound requests, only responses"));
            }
            // the same message will be decoded by a server or mixed decoder
            try (InboundDecoder decoder = new InboundDecoder(recycler, randomFrom(ChannelType.SERVER, ChannelType.MIX))) {
                final ArrayList<Object> fragments = new ArrayList<>();
                int bytesConsumed = decoder.decode(wrapAsReleasable(bytes), fragments::add);
                int totalHeaderSize = TcpHeader.headerSize(version) + (version.onOrAfter(TransportVersions.V_7_6_0)
                    ? bytes.getInt(TcpHeader.VARIABLE_HEADER_SIZE_POSITION)
                    : 0);
                assertEquals(totalHeaderSize, bytesConsumed);
                final Header header = (Header) fragments.get(0);
                assertEquals(requestId, header.getRequestId());
            }
        }
    }

    public void testServerChannelTypeFailsDecodingResponses() throws Exception {
        long requestId = randomNonNegativeLong();
        if (randomBoolean()) {
            final String headerKey = randomAlphaOfLength(10);
            final String headerValue = randomAlphaOfLength(20);
            if (randomBoolean()) {
                threadContext.putHeader(headerKey, headerValue);
            } else {
                threadContext.addResponseHeader(headerKey, headerValue);
            }
        }
        // a response
        final var isHandshake = randomBoolean();
        final var version = isHandshake
            ? randomFrom(TransportHandshaker.ALLOWED_HANDSHAKE_VERSIONS)
            : TransportVersionUtils.randomCompatibleVersion(random());
        OutboundMessage message = new OutboundMessage.Response(
            threadContext,
            new TestResponse(randomAlphaOfLength(100)),
            version,
            requestId,
            isHandshake,
            randomFrom(Compression.Scheme.DEFLATE, Compression.Scheme.LZ4, null)
        );

        try (RecyclerBytesStreamOutput os = new RecyclerBytesStreamOutput(recycler)) {
            final BytesReference bytes = message.serialize(os);
            try (InboundDecoder decoder = new InboundDecoder(recycler, ChannelType.SERVER)) {
                final ReleasableBytesReference releasable1 = wrapAsReleasable(bytes);
                IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> decoder.decode(releasable1, ignored -> {}));
                assertThat(e.getMessage(), containsString("server channels do not accept inbound responses, only requests"));
            }
            // the same message will be decoded by a client or mixed decoder
            try (InboundDecoder decoder = new InboundDecoder(recycler, randomFrom(ChannelType.CLIENT, ChannelType.MIX))) {
                final ArrayList<Object> fragments = new ArrayList<>();
                int bytesConsumed = decoder.decode(wrapAsReleasable(bytes), fragments::add);
                int totalHeaderSize = TcpHeader.headerSize(version) + (version.onOrAfter(TransportVersions.V_7_6_0)
                    ? bytes.getInt(TcpHeader.VARIABLE_HEADER_SIZE_POSITION)
                    : 0);
                assertEquals(totalHeaderSize, bytesConsumed);
                final Header header = (Header) fragments.get(0);
                assertEquals(requestId, header.getRequestId());
            }
        }
    }

    public void testCompressedDecode() throws IOException {
        boolean isRequest = randomBoolean();
        String action = "test-request";
        long requestId = randomNonNegativeLong();
        final String headerKey = randomAlphaOfLength(10);
        final String headerValue = randomAlphaOfLength(20);
        if (isRequest) {
            threadContext.putHeader(headerKey, headerValue);
        } else {
            threadContext.addResponseHeader(headerKey, headerValue);
        }
        OutboundMessage message;
        TransportMessage transportMessage;
        Compression.Scheme scheme = randomFrom(Compression.Scheme.DEFLATE, Compression.Scheme.LZ4);
        if (isRequest) {
            transportMessage = new TestRequest(randomAlphaOfLength(100));
            message = new OutboundMessage.Request(
                threadContext,
                transportMessage,
                TransportVersion.current(),
                action,
                requestId,
                false,
                scheme
            );
        } else {
            transportMessage = new TestResponse(randomAlphaOfLength(100));
            message = new OutboundMessage.Response(threadContext, transportMessage, TransportVersion.current(), requestId, false, scheme);
        }

        try (RecyclerBytesStreamOutput os = new RecyclerBytesStreamOutput(recycler)) {
            final BytesReference totalBytes = message.serialize(os);
            final BytesStreamOutput out = new BytesStreamOutput();
            transportMessage.writeTo(out);
            final BytesReference uncompressedBytes = out.bytes();
            int totalHeaderSize = TcpHeader.headerSize(TransportVersion.current()) + totalBytes.getInt(
                TcpHeader.VARIABLE_HEADER_SIZE_POSITION
            );

            InboundDecoder decoder = new InboundDecoder(recycler);
            final ArrayList<Object> fragments = new ArrayList<>();
            final ReleasableBytesReference releasable1 = wrapAsReleasable(totalBytes);
            int bytesConsumed = decoder.decode(releasable1, fragments::add);
            assertEquals(totalHeaderSize, bytesConsumed);
            assertTrue(releasable1.hasReferences());

            final Header header = (Header) fragments.get(0);
            assertEquals(requestId, header.getRequestId());
            assertEquals(TransportVersion.current(), header.getVersion());
            assertTrue(header.isCompressed());
            assertFalse(header.isHandshake());
            if (isRequest) {
                assertEquals(action, header.getActionName());
                assertTrue(header.isRequest());
                assertEquals(header.getHeaders().v1().get(headerKey), headerValue);
            } else {
                assertTrue(header.isResponse());
                assertThat(header.getHeaders().v2().get(headerKey), hasItems(headerValue));
            }
            assertFalse(header.needsToReadVariableHeader());
            fragments.clear();

            final BytesReference bytes2 = totalBytes.slice(bytesConsumed, totalBytes.length() - bytesConsumed);
            final ReleasableBytesReference releasable2 = wrapAsReleasable(bytes2);
            int bytesConsumed2 = decoder.decode(releasable2, fragments::add);
            assertEquals(totalBytes.length() - totalHeaderSize, bytesConsumed2);

            final Object compressionScheme = fragments.get(0);
            final Object content = fragments.get(1);
            final Object endMarker = fragments.get(2);

            assertEquals(scheme, compressionScheme);
            assertEquals(uncompressedBytes, content);
            assertThat(content, instanceOf(ReleasableBytesReference.class));
            ((ReleasableBytesReference) content).close();
            // Ref count is not incremented since the bytes are immediately consumed on decompression
            assertTrue(releasable2.hasReferences());
            assertEquals(InboundDecoder.END_CONTENT, endMarker);
        }

    }

    public void testVersionIncompatibilityDecodeException() throws IOException {
        String action = "test-request";
        long requestId = randomNonNegativeLong();
        TransportVersion incompatibleVersion = TransportVersionUtils.getPreviousVersion(TransportVersions.MINIMUM_COMPATIBLE);
        OutboundMessage message = new OutboundMessage.Request(
            threadContext,
            new TestRequest(randomAlphaOfLength(100)),
            incompatibleVersion,
            action,
            requestId,
            false,
            Compression.Scheme.DEFLATE
        );

        final ReleasableBytesReference releasable1;
        try (RecyclerBytesStreamOutput os = new RecyclerBytesStreamOutput(recycler)) {
            final BytesReference bytes = message.serialize(os);

            InboundDecoder decoder = new InboundDecoder(recycler);
            final ArrayList<Object> fragments = new ArrayList<>();
            try (ReleasableBytesReference r = wrapAsReleasable(bytes)) {
                releasable1 = r;
                expectThrows(IllegalStateException.class, () -> decoder.decode(releasable1, fragments::add));
            }
        }
        // No bytes are retained
        assertFalse(releasable1.hasReferences());
    }

    public void testCheckVersionCompatibility() {
        try {
            InboundDecoder.checkVersionCompatibility(
                TransportVersionUtils.randomVersionBetween(random(), TransportVersions.MINIMUM_COMPATIBLE, TransportVersion.current())
            );
        } catch (IllegalStateException e) {
            throw new AssertionError(e);
        }

        TransportVersion invalid = TransportVersionUtils.getPreviousVersion(TransportVersions.MINIMUM_COMPATIBLE);
        try {
            InboundDecoder.checkVersionCompatibility(invalid);
            fail();
        } catch (IllegalStateException expected) {
            assertEquals(
                "Received message from unsupported version: ["
                    + invalid.toReleaseVersion()
                    + "] minimal compatible version is: ["
                    + TransportVersions.MINIMUM_COMPATIBLE.toReleaseVersion()
                    + "]",
                expected.getMessage()
            );
        }
    }

    public void testCheckHandshakeCompatibility() {
        for (final var allowedHandshakeVersion : TransportHandshaker.ALLOWED_HANDSHAKE_VERSIONS) {
            InboundDecoder.checkHandshakeVersionCompatibility(allowedHandshakeVersion); // should not throw

            var invalid = TransportVersion.fromId(allowedHandshakeVersion.id() + randomFrom(-1, +1));
            assertEquals(
                "Received message from unsupported version: ["
                    + invalid
                    + "] allowed versions are: "
                    + TransportHandshaker.ALLOWED_HANDSHAKE_VERSIONS,
                expectThrows(IllegalStateException.class, () -> InboundDecoder.checkHandshakeVersionCompatibility(invalid)).getMessage()
            );
        }
    }
}
