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
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.VersionUtils;
import org.hamcrest.Matchers;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;

public class InboundMessageTests extends ESTestCase {

    private final ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
    private final NamedWriteableRegistry registry = new NamedWriteableRegistry(Collections.emptyList());

    public void testReadRequest() throws IOException {
        String[] features = {"feature1", "feature2"};
        String value = randomAlphaOfLength(10);
        Message message = new Message(value);
        String action = randomAlphaOfLength(10);
        long requestId = randomLong();
        boolean isHandshake = randomBoolean();
        boolean compress = randomBoolean();
        threadContext.putHeader("header", "header_value");
        Version version = randomFrom(Version.CURRENT, Version.CURRENT.minimumCompatibilityVersion());
        OutboundMessage.Request request = new OutboundMessage.Request(threadContext, message, version, action, requestId,
            isHandshake, compress);
        BytesReference reference;
        try (BytesStreamOutput streamOutput = new BytesStreamOutput()) {
            reference = request.serialize(streamOutput);
        }
        // Check that the thread context is not deleted.
        assertEquals("header_value", threadContext.getHeader("header"));

        threadContext.stashContext();
        threadContext.putHeader("header", "header_value2");

        InboundMessage.Reader reader = new InboundMessage.Reader(version, registry, threadContext);
        BytesReference sliced = reference.slice(6, reference.length() - 6);
        InboundMessage.Request inboundMessage = (InboundMessage.Request) reader.deserialize(sliced);
        // Check that deserialize does not overwrite current thread context.
        assertEquals("header_value2", threadContext.getHeader("header"));
        inboundMessage.getStoredContext().restore();
        assertEquals("header_value", threadContext.getHeader("header"));
        assertEquals(isHandshake, inboundMessage.isHandshake());
        assertEquals(compress, inboundMessage.isCompress());
        assertEquals(version, inboundMessage.getVersion());
        assertEquals(action, inboundMessage.getActionName());
        assertTrue(inboundMessage.isRequest());
        assertFalse(inboundMessage.isResponse());
        assertFalse(inboundMessage.isError());
        assertEquals(value, new Message(inboundMessage.getStreamInput()).value);
    }

    public void testReadResponse() throws IOException {
        HashSet<String> features = new HashSet<>(Arrays.asList("feature1", "feature2"));
        String value = randomAlphaOfLength(10);
        Message message = new Message(value);
        long requestId = randomLong();
        boolean isHandshake = randomBoolean();
        boolean compress = randomBoolean();
        threadContext.putHeader("header", "header_value");
        Version version = randomFrom(Version.CURRENT, Version.CURRENT.minimumCompatibilityVersion());
        OutboundMessage.Response request = new OutboundMessage.Response(threadContext, message, version, requestId, isHandshake,
            compress);
        BytesReference reference;
        try (BytesStreamOutput streamOutput = new BytesStreamOutput()) {
            reference = request.serialize(streamOutput);
        }
        // Check that the thread context is not deleted.
        assertEquals("header_value", threadContext.getHeader("header"));

        threadContext.stashContext();
        threadContext.putHeader("header", "header_value2");

        InboundMessage.Reader reader = new InboundMessage.Reader(version, registry, threadContext);
        BytesReference sliced = reference.slice(6, reference.length() - 6);
        InboundMessage.Response inboundMessage = (InboundMessage.Response) reader.deserialize(sliced);
        // Check that deserialize does not overwrite current thread context.
        assertEquals("header_value2", threadContext.getHeader("header"));
        inboundMessage.getStoredContext().restore();
        assertEquals("header_value", threadContext.getHeader("header"));
        assertEquals(isHandshake, inboundMessage.isHandshake());
        assertEquals(compress, inboundMessage.isCompress());
        assertEquals(version, inboundMessage.getVersion());
        assertTrue(inboundMessage.isResponse());
        assertFalse(inboundMessage.isRequest());
        assertFalse(inboundMessage.isError());
        assertEquals(value, new Message(inboundMessage.getStreamInput()).value);
    }

    public void testReadErrorResponse() throws IOException {
        HashSet<String> features = new HashSet<>(Arrays.asList("feature1", "feature2"));
        RemoteTransportException exception = new RemoteTransportException("error", new IOException());
        long requestId = randomLong();
        boolean isHandshake = randomBoolean();
        boolean compress = randomBoolean();
        threadContext.putHeader("header", "header_value");
        Version version = randomFrom(Version.CURRENT, Version.CURRENT.minimumCompatibilityVersion());
        OutboundMessage.Response request = new OutboundMessage.Response(threadContext, exception, version, requestId,
            isHandshake, compress);
        BytesReference reference;
        try (BytesStreamOutput streamOutput = new BytesStreamOutput()) {
            reference = request.serialize(streamOutput);
        }
        // Check that the thread context is not deleted.
        assertEquals("header_value", threadContext.getHeader("header"));

        threadContext.stashContext();
        threadContext.putHeader("header", "header_value2");

        InboundMessage.Reader reader = new InboundMessage.Reader(version, registry, threadContext);
        BytesReference sliced = reference.slice(6, reference.length() - 6);
        InboundMessage.Response inboundMessage = (InboundMessage.Response) reader.deserialize(sliced);
        // Check that deserialize does not overwrite current thread context.
        assertEquals("header_value2", threadContext.getHeader("header"));
        inboundMessage.getStoredContext().restore();
        assertEquals("header_value", threadContext.getHeader("header"));
        assertEquals(isHandshake, inboundMessage.isHandshake());
        assertEquals(compress, inboundMessage.isCompress());
        assertEquals(version, inboundMessage.getVersion());
        assertTrue(inboundMessage.isResponse());
        assertFalse(inboundMessage.isRequest());
        assertTrue(inboundMessage.isError());
        assertEquals("[error]", inboundMessage.getStreamInput().readException().getMessage());
    }

    public void testEnsureVersionCompatibility() throws IOException {
        testVersionIncompatibility(VersionUtils.randomVersionBetween(random(), Version.CURRENT.minimumCompatibilityVersion(),
            Version.CURRENT), Version.CURRENT, randomBoolean());

        final Version version = Version.fromString("7.0.0");
        testVersionIncompatibility(Version.fromString("6.0.0"), version, true);
        IllegalStateException ise = expectThrows(IllegalStateException.class, () ->
            testVersionIncompatibility(Version.fromString("6.0.0"), version, false));
        assertEquals("Received message from unsupported version: [6.0.0] minimal compatible version is: ["
            + version.minimumCompatibilityVersion() + "]", ise.getMessage());

        // For handshake we are compatible with N-2
        testVersionIncompatibility(Version.fromString("5.6.0"), version, true);
        ise = expectThrows(IllegalStateException.class, () ->
            testVersionIncompatibility(Version.fromString("5.6.0"), version, false));
        assertEquals("Received message from unsupported version: [5.6.0] minimal compatible version is: ["
            + version.minimumCompatibilityVersion() + "]", ise.getMessage());

        ise = expectThrows(IllegalStateException.class, () ->
            testVersionIncompatibility(Version.fromString("2.3.0"), version, true));
        assertEquals("Received handshake message from unsupported version: [2.3.0] minimal compatible version is: ["
            + version.minimumCompatibilityVersion() + "]", ise.getMessage());

        ise = expectThrows(IllegalStateException.class, () ->
            testVersionIncompatibility(Version.fromString("2.3.0"), version, false));
        assertEquals("Received message from unsupported version: [2.3.0] minimal compatible version is: ["
            + version.minimumCompatibilityVersion() + "]", ise.getMessage());
    }

    public void testThrowOnNotCompressed() throws Exception {
        OutboundMessage.Response request = new OutboundMessage.Response(
            threadContext, new Message(randomAlphaOfLength(10)), Version.CURRENT, randomLong(), false, false);
        BytesReference reference;
        try (BytesStreamOutput streamOutput = new BytesStreamOutput()) {
            reference = request.serialize(streamOutput);
        }
        final byte[] serialized = BytesReference.toBytes(reference);
        final int statusPosition = TcpHeader.HEADER_SIZE - TcpHeader.VERSION_ID_SIZE - 1;
        // force status byte to signal compressed on the otherwise uncompressed message
        serialized[statusPosition] = TransportStatus.setCompress(serialized[statusPosition]);
        reference = new BytesArray(serialized);
        InboundMessage.Reader reader = new InboundMessage.Reader(Version.CURRENT, registry, threadContext);
        BytesReference sliced = reference.slice(6, reference.length() - 6);
        final IllegalStateException iste = expectThrows(IllegalStateException.class, () -> reader.deserialize(sliced));
        assertThat(iste.getMessage(), Matchers.startsWith("stream marked as compressed, but no compressor found,"));
    }

    private void testVersionIncompatibility(Version version, Version currentVersion, boolean isHandshake) throws IOException {
        String[] features = {};
        String value = randomAlphaOfLength(10);
        Message message = new Message(value);
        String action = randomAlphaOfLength(10);
        long requestId = randomLong();
        boolean compress = randomBoolean();
        OutboundMessage.Request request = new OutboundMessage.Request(threadContext, message, version, action, requestId,
            isHandshake, compress);
        BytesReference reference;
        try (BytesStreamOutput streamOutput = new BytesStreamOutput()) {
            reference = request.serialize(streamOutput);
        }

        BytesReference sliced = reference.slice(6, reference.length() - 6);
        InboundMessage.Reader reader = new InboundMessage.Reader(currentVersion, registry, threadContext);
        reader.deserialize(sliced);
    }

    private static final class Message extends TransportMessage {

        public String value;

        private Message(StreamInput in) throws IOException {
            value = in.readString();
        }

        private Message(String value) {
            this.value = value;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(value);
        }
    }
}
