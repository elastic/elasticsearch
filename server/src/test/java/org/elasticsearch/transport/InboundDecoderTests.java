/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.transport;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.RecyclerBytesStreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.MockPageCacheRecycler;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.TransportVersionUtils;

import java.io.IOException;
import java.util.ArrayList;

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

            InboundDecoder decoder = new InboundDecoder(TransportVersion.current(), recycler);
            final ArrayList<Object> fragments = new ArrayList<>();
            final ReleasableBytesReference releasable1 = ReleasableBytesReference.wrap(totalBytes);
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
            final ReleasableBytesReference releasable2 = ReleasableBytesReference.wrap(bytes2);
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

    public void testDecodePreHeaderSizeVariableInt() throws IOException {
        // TODO: Can delete test on 9.0
        Compression.Scheme compressionScheme = randomFrom(Compression.Scheme.DEFLATE, Compression.Scheme.DEFLATE, null);
        String action = "test-request";
        long requestId = randomNonNegativeLong();
        final TransportVersion preHeaderVariableInt = TransportHandshaker.EARLIEST_HANDSHAKE_VERSION;
        final String contentValue = randomAlphaOfLength(100);
        // 8.0 is only compatible with handshakes on a pre-variable int version
        final OutboundMessage message = new OutboundMessage.Request(
            threadContext,
            new TestRequest(contentValue),
            preHeaderVariableInt,
            action,
            requestId,
            true,
            compressionScheme
        );

        try (RecyclerBytesStreamOutput os = new RecyclerBytesStreamOutput(recycler)) {
            final BytesReference totalBytes = message.serialize(os);
            int partialHeaderSize = TcpHeader.headerSize(preHeaderVariableInt);

            InboundDecoder decoder = new InboundDecoder(TransportVersion.current(), recycler);
            final ArrayList<Object> fragments = new ArrayList<>();
            final ReleasableBytesReference releasable1 = ReleasableBytesReference.wrap(totalBytes);
            int bytesConsumed = decoder.decode(releasable1, fragments::add);
            assertEquals(partialHeaderSize, bytesConsumed);
            assertTrue(releasable1.hasReferences());

            final Header header = (Header) fragments.get(0);
            assertEquals(requestId, header.getRequestId());
            assertEquals(preHeaderVariableInt, header.getVersion());
            if (compressionScheme == null) {
                assertFalse(header.isCompressed());
            } else {
                assertTrue(header.isCompressed());
            }
            assertTrue(header.isHandshake());
            assertTrue(header.isRequest());
            assertTrue(header.needsToReadVariableHeader());
            fragments.clear();

            final BytesReference bytes2 = totalBytes.slice(bytesConsumed, totalBytes.length() - bytesConsumed);
            final ReleasableBytesReference releasable2 = ReleasableBytesReference.wrap(bytes2);
            int bytesConsumed2 = decoder.decode(releasable2, fragments::add);
            if (compressionScheme == null) {
                assertEquals(2, fragments.size());
            } else {
                assertEquals(3, fragments.size());
                final Object body = fragments.get(1);
                assertThat(body, instanceOf(ReleasableBytesReference.class));
                ((ReleasableBytesReference) body).close();
            }
            assertEquals(InboundDecoder.END_CONTENT, fragments.get(fragments.size() - 1));
            assertEquals(totalBytes.length() - bytesConsumed, bytesConsumed2);
        }
    }

    public void testDecodeHandshakeCompatibility() throws IOException {
        String action = "test-request";
        long requestId = randomNonNegativeLong();
        final String headerKey = randomAlphaOfLength(10);
        final String headerValue = randomAlphaOfLength(20);
        threadContext.putHeader(headerKey, headerValue);
        TransportVersion handshakeCompat = TransportHandshaker.EARLIEST_HANDSHAKE_VERSION;
        OutboundMessage message = new OutboundMessage.Request(
            threadContext,
            new TestRequest(randomAlphaOfLength(100)),
            handshakeCompat,
            action,
            requestId,
            true,
            null
        );

        try (RecyclerBytesStreamOutput os = new RecyclerBytesStreamOutput(recycler)) {
            final BytesReference bytes = message.serialize(os);
            int totalHeaderSize = TcpHeader.headerSize(handshakeCompat);

            InboundDecoder decoder = new InboundDecoder(TransportVersion.current(), recycler);
            final ArrayList<Object> fragments = new ArrayList<>();
            final ReleasableBytesReference releasable1 = ReleasableBytesReference.wrap(bytes);
            int bytesConsumed = decoder.decode(releasable1, fragments::add);
            assertEquals(totalHeaderSize, bytesConsumed);
            assertTrue(releasable1.hasReferences());

            final Header header = (Header) fragments.get(0);
            assertEquals(requestId, header.getRequestId());
            assertEquals(handshakeCompat, header.getVersion());
            assertFalse(header.isCompressed());
            assertTrue(header.isHandshake());
            assertTrue(header.isRequest());
            // TODO: On 9.0 this will be true because all compatible versions with contain the variable header int
            assertTrue(header.needsToReadVariableHeader());
            fragments.clear();
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

            InboundDecoder decoder = new InboundDecoder(TransportVersion.current(), recycler);
            final ArrayList<Object> fragments = new ArrayList<>();
            final ReleasableBytesReference releasable1 = ReleasableBytesReference.wrap(totalBytes);
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
            final ReleasableBytesReference releasable2 = ReleasableBytesReference.wrap(bytes2);
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

    public void testCompressedDecodeHandshakeCompatibility() throws IOException {
        String action = "test-request";
        long requestId = randomNonNegativeLong();
        final String headerKey = randomAlphaOfLength(10);
        final String headerValue = randomAlphaOfLength(20);
        threadContext.putHeader(headerKey, headerValue);
        TransportVersion handshakeCompat = TransportHandshaker.EARLIEST_HANDSHAKE_VERSION;
        OutboundMessage message = new OutboundMessage.Request(
            threadContext,
            new TestRequest(randomAlphaOfLength(100)),
            handshakeCompat,
            action,
            requestId,
            true,
            Compression.Scheme.DEFLATE
        );

        try (RecyclerBytesStreamOutput os = new RecyclerBytesStreamOutput(recycler)) {
            final BytesReference bytes = message.serialize(os);
            int totalHeaderSize = TcpHeader.headerSize(handshakeCompat);

            InboundDecoder decoder = new InboundDecoder(TransportVersion.current(), recycler);
            final ArrayList<Object> fragments = new ArrayList<>();
            final ReleasableBytesReference releasable1 = ReleasableBytesReference.wrap(bytes);
            int bytesConsumed = decoder.decode(releasable1, fragments::add);
            assertEquals(totalHeaderSize, bytesConsumed);
            assertTrue(releasable1.hasReferences());

            final Header header = (Header) fragments.get(0);
            assertEquals(requestId, header.getRequestId());
            assertEquals(handshakeCompat, header.getVersion());
            assertTrue(header.isCompressed());
            assertTrue(header.isHandshake());
            assertTrue(header.isRequest());
            // TODO: On 9.0 this will be true because all compatible versions with contain the variable header int
            assertTrue(header.needsToReadVariableHeader());
            fragments.clear();
        }
    }

    public void testVersionIncompatibilityDecodeException() throws IOException {
        String action = "test-request";
        long requestId = randomNonNegativeLong();
        TransportVersion incompatibleVersion = TransportVersionUtils.getPreviousVersion(TransportVersion.MINIMUM_COMPATIBLE);
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

            InboundDecoder decoder = new InboundDecoder(TransportVersion.current(), recycler);
            final ArrayList<Object> fragments = new ArrayList<>();
            try (ReleasableBytesReference r = ReleasableBytesReference.wrap(bytes)) {
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
                TransportVersionUtils.randomVersionBetween(random(), TransportVersion.MINIMUM_COMPATIBLE, TransportVersion.current())
            );
        } catch (IllegalStateException e) {
            throw new AssertionError(e);
        }

        TransportVersion invalid = TransportVersionUtils.getPreviousVersion(TransportVersion.MINIMUM_COMPATIBLE);
        try {
            InboundDecoder.checkVersionCompatibility(invalid);
            fail();
        } catch (IllegalStateException expected) {
            assertEquals(
                "Received message from unsupported version: ["
                    + invalid
                    + "] minimal compatible version is: ["
                    + TransportVersion.MINIMUM_COMPATIBLE
                    + "]",
                expected.getMessage()
            );
        }
    }

    public void testCheckHandshakeCompatibility() {
        try {
            InboundDecoder.checkHandshakeVersionCompatibility(randomFrom(TransportHandshaker.ALLOWED_HANDSHAKE_VERSIONS));
        } catch (IllegalStateException e) {
            throw new AssertionError(e);
        }

        var invalid = TransportVersion.fromId(TransportHandshaker.EARLIEST_HANDSHAKE_VERSION.id() - 1);
        try {
            InboundDecoder.checkHandshakeVersionCompatibility(invalid);
            fail();
        } catch (IllegalStateException expected) {
            assertEquals(
                "Received message from unsupported version: ["
                    + invalid
                    + "] allowed versions are: "
                    + TransportHandshaker.ALLOWED_HANDSHAKE_VERSIONS,
                expected.getMessage()
            );
        }
    }
}
