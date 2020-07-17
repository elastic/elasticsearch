/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.transport;

import org.elasticsearch.Version;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.VersionUtils;

import java.io.IOException;
import java.util.ArrayList;

import static org.hamcrest.Matchers.hasItems;

public class InboundDecoderTests extends ESTestCase {

    private ThreadContext threadContext;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        threadContext = new ThreadContext(Settings.EMPTY);
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
            message = new OutboundMessage.Request(threadContext, new TestRequest(randomAlphaOfLength(100)),
                Version.CURRENT, action, requestId, false, false);
        } else {
            message = new OutboundMessage.Response(threadContext, new TestResponse(randomAlphaOfLength(100)),
                Version.CURRENT, requestId, false, false);
        }

        final BytesReference totalBytes = message.serialize(new BytesStreamOutput());
        int totalHeaderSize = TcpHeader.headerSize(Version.CURRENT) + totalBytes.getInt(TcpHeader.VARIABLE_HEADER_SIZE_POSITION);
        final BytesReference messageBytes = totalBytes.slice(totalHeaderSize, totalBytes.length() - totalHeaderSize);

        InboundDecoder decoder = new InboundDecoder(Version.CURRENT, PageCacheRecycler.NON_RECYCLING_INSTANCE);
        final ArrayList<Object> fragments = new ArrayList<>();
        final ReleasableBytesReference releasable1 = ReleasableBytesReference.wrap(totalBytes);
        int bytesConsumed = decoder.decode(releasable1, fragments::add);
        assertEquals(totalHeaderSize, bytesConsumed);
        assertEquals(1, releasable1.refCount());

        final Header header = (Header) fragments.get(0);
        assertEquals(requestId, header.getRequestId());
        assertEquals(Version.CURRENT, header.getVersion());
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
        assertEquals(2, releasable2.refCount());
        assertEquals(InboundDecoder.END_CONTENT, endMarker);
    }

    public void testDecodePreHeaderSizeVariableInt() throws IOException {
        // TODO: Can delete test on 9.0
        boolean isCompressed = randomBoolean();
        String action = "test-request";
        long requestId = randomNonNegativeLong();
        final Version preHeaderVariableInt = Version.V_7_5_0;
        final String contentValue = randomAlphaOfLength(100);
        // 8.0 is only compatible with handshakes on a pre-variable int version
        final OutboundMessage message = new OutboundMessage.Request(threadContext, new TestRequest(contentValue),
            preHeaderVariableInt, action, requestId, true, isCompressed);

        final BytesReference totalBytes = message.serialize(new BytesStreamOutput());
        int partialHeaderSize = TcpHeader.headerSize(preHeaderVariableInt);

        InboundDecoder decoder = new InboundDecoder(Version.CURRENT, PageCacheRecycler.NON_RECYCLING_INSTANCE);
        final ArrayList<Object> fragments = new ArrayList<>();
        final ReleasableBytesReference releasable1 = ReleasableBytesReference.wrap(totalBytes);
        int bytesConsumed = decoder.decode(releasable1, fragments::add);
        assertEquals(partialHeaderSize, bytesConsumed);
        assertEquals(1, releasable1.refCount());

        final Header header = (Header) fragments.get(0);
        assertEquals(requestId, header.getRequestId());
        assertEquals(preHeaderVariableInt, header.getVersion());
        assertEquals(isCompressed, header.isCompressed());
        assertTrue(header.isHandshake());
        assertTrue(header.isRequest());
        assertTrue(header.needsToReadVariableHeader());
        fragments.clear();

        final BytesReference bytes2 = totalBytes.slice(bytesConsumed, totalBytes.length() - bytesConsumed);
        final ReleasableBytesReference releasable2 = ReleasableBytesReference.wrap(bytes2);
        int bytesConsumed2 = decoder.decode(releasable2, fragments::add);
        assertEquals(2, fragments.size());
        assertEquals(InboundDecoder.END_CONTENT, fragments.get(fragments.size() - 1));
        assertEquals(totalBytes.length() - bytesConsumed, bytesConsumed2);
    }

    public void testDecodeHandshakeCompatibility() throws IOException {
        String action = "test-request";
        long requestId = randomNonNegativeLong();
        final String headerKey = randomAlphaOfLength(10);
        final String headerValue = randomAlphaOfLength(20);
        threadContext.putHeader(headerKey, headerValue);
        Version handshakeCompat = Version.CURRENT.minimumCompatibilityVersion().minimumCompatibilityVersion();
        OutboundMessage message = new OutboundMessage.Request(threadContext, new TestRequest(randomAlphaOfLength(100)),
            handshakeCompat, action, requestId, true, false);

        final BytesReference bytes = message.serialize(new BytesStreamOutput());
        int totalHeaderSize = TcpHeader.headerSize(handshakeCompat);

        InboundDecoder decoder = new InboundDecoder(Version.CURRENT, PageCacheRecycler.NON_RECYCLING_INSTANCE);
        final ArrayList<Object> fragments = new ArrayList<>();
        final ReleasableBytesReference releasable1 = ReleasableBytesReference.wrap(bytes);
        int bytesConsumed = decoder.decode(releasable1, fragments::add);
        assertEquals(totalHeaderSize, bytesConsumed);
        assertEquals(1, releasable1.refCount());

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
        if (isRequest) {
            transportMessage = new TestRequest(randomAlphaOfLength(100));
            message = new OutboundMessage.Request(threadContext, transportMessage, Version.CURRENT, action, requestId, false, true);
        } else {
            transportMessage = new TestResponse(randomAlphaOfLength(100));
            message = new OutboundMessage.Response(threadContext, transportMessage, Version.CURRENT, requestId, false, true);
        }

        final BytesReference totalBytes = message.serialize(new BytesStreamOutput());
        final BytesStreamOutput out = new BytesStreamOutput();
        transportMessage.writeTo(out);
        final BytesReference uncompressedBytes =out.bytes();
        int totalHeaderSize = TcpHeader.headerSize(Version.CURRENT) + totalBytes.getInt(TcpHeader.VARIABLE_HEADER_SIZE_POSITION);

        InboundDecoder decoder = new InboundDecoder(Version.CURRENT, PageCacheRecycler.NON_RECYCLING_INSTANCE);
        final ArrayList<Object> fragments = new ArrayList<>();
        final ReleasableBytesReference releasable1 = ReleasableBytesReference.wrap(totalBytes);
        int bytesConsumed = decoder.decode(releasable1, fragments::add);
        assertEquals(totalHeaderSize, bytesConsumed);
        assertEquals(1, releasable1.refCount());

        final Header header = (Header) fragments.get(0);
        assertEquals(requestId, header.getRequestId());
        assertEquals(Version.CURRENT, header.getVersion());
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

        final Object content = fragments.get(0);
        final Object endMarker = fragments.get(1);

        assertEquals(uncompressedBytes, content);
        // Ref count is not incremented since the bytes are immediately consumed on decompression
        assertEquals(1, releasable2.refCount());
        assertEquals(InboundDecoder.END_CONTENT, endMarker);
    }

    public void testCompressedDecodeHandshakeCompatibility() throws IOException {
        String action = "test-request";
        long requestId = randomNonNegativeLong();
        final String headerKey = randomAlphaOfLength(10);
        final String headerValue = randomAlphaOfLength(20);
        threadContext.putHeader(headerKey, headerValue);
        Version handshakeCompat = Version.CURRENT.minimumCompatibilityVersion().minimumCompatibilityVersion();
        OutboundMessage message = new OutboundMessage.Request(threadContext, new TestRequest(randomAlphaOfLength(100)),
            handshakeCompat, action, requestId, true, true);

        final BytesReference bytes = message.serialize(new BytesStreamOutput());
        int totalHeaderSize = TcpHeader.headerSize(handshakeCompat);

        InboundDecoder decoder = new InboundDecoder(Version.CURRENT, PageCacheRecycler.NON_RECYCLING_INSTANCE);
        final ArrayList<Object> fragments = new ArrayList<>();
        final ReleasableBytesReference releasable1 = ReleasableBytesReference.wrap(bytes);
        int bytesConsumed = decoder.decode(releasable1, fragments::add);
        assertEquals(totalHeaderSize, bytesConsumed);
        assertEquals(1, releasable1.refCount());

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

    public void testVersionIncompatibilityDecodeException() throws IOException {
        String action = "test-request";
        long requestId = randomNonNegativeLong();
        Version incompatibleVersion = Version.CURRENT.minimumCompatibilityVersion().minimumCompatibilityVersion();
        OutboundMessage message = new OutboundMessage.Request(threadContext, new TestRequest(randomAlphaOfLength(100)),
            incompatibleVersion, action, requestId, false, true);

        final BytesReference bytes = message.serialize(new BytesStreamOutput());

        InboundDecoder decoder = new InboundDecoder(Version.CURRENT, PageCacheRecycler.NON_RECYCLING_INSTANCE);
        final ArrayList<Object> fragments = new ArrayList<>();
        final ReleasableBytesReference releasable1 = ReleasableBytesReference.wrap(bytes);
        expectThrows(IllegalStateException.class, () -> decoder.decode(releasable1, fragments::add));
        // No bytes are retained
        assertEquals(1, releasable1.refCount());
    }

    public void testEnsureVersionCompatibility() throws IOException {
        IllegalStateException ise = InboundDecoder.ensureVersionCompatibility(VersionUtils.randomVersionBetween(random(),
            Version.CURRENT.minimumCompatibilityVersion(), Version.CURRENT), Version.CURRENT, randomBoolean());
        assertNull(ise);

        final Version version = Version.fromString("7.0.0");
        ise = InboundDecoder.ensureVersionCompatibility(Version.fromString("6.0.0"), version, true);
        assertNull(ise);

        ise = InboundDecoder.ensureVersionCompatibility(Version.fromString("6.0.0"), version, false);
        assertEquals("Received message from unsupported version: [6.0.0] minimal compatible version is: ["
            + version.minimumCompatibilityVersion() + "]", ise.getMessage());

        // For handshake we are compatible with N-2
        ise = InboundDecoder.ensureVersionCompatibility(Version.fromString("5.6.0"), version, true);
        assertNull(ise);

        ise = InboundDecoder.ensureVersionCompatibility(Version.fromString("5.6.0"), version, false);
        assertEquals("Received message from unsupported version: [5.6.0] minimal compatible version is: ["
            + version.minimumCompatibilityVersion() + "]", ise.getMessage());
    }
}
