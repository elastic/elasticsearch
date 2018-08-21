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
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.compress.CompressorFactory;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.VersionUtils;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.io.StreamCorruptedException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.core.IsInstanceOf.instanceOf;

/** Unit tests for {@link TcpTransport} */
public class TcpTransportTests extends ESTestCase {

    /** Test ipv4 host with a default port works */
    public void testParseV4DefaultPort() throws Exception {
        TransportAddress[] addresses = TcpTransport.parse("127.0.0.1", "1234", Integer.MAX_VALUE);
        assertEquals(1, addresses.length);

        assertEquals("127.0.0.1", addresses[0].getAddress());
        assertEquals(1234, addresses[0].getPort());
    }

    /** Test ipv4 host with a default port range works */
    public void testParseV4DefaultRange() throws Exception {
        TransportAddress[] addresses = TcpTransport.parse("127.0.0.1", "1234-1235", Integer.MAX_VALUE);
        assertEquals(2, addresses.length);

        assertEquals("127.0.0.1", addresses[0].getAddress());
        assertEquals(1234, addresses[0].getPort());

        assertEquals("127.0.0.1", addresses[1].getAddress());
        assertEquals(1235, addresses[1].getPort());
    }

    /** Test ipv4 host with port works */
    public void testParseV4WithPort() throws Exception {
        TransportAddress[] addresses = TcpTransport.parse("127.0.0.1:2345", "1234", Integer.MAX_VALUE);
        assertEquals(1, addresses.length);

        assertEquals("127.0.0.1", addresses[0].getAddress());
        assertEquals(2345, addresses[0].getPort());
    }

    /** Test ipv4 host with port range works */
    public void testParseV4WithPortRange() throws Exception {
        TransportAddress[] addresses = TcpTransport.parse("127.0.0.1:2345-2346", "1234", Integer.MAX_VALUE);
        assertEquals(2, addresses.length);

        assertEquals("127.0.0.1", addresses[0].getAddress());
        assertEquals(2345, addresses[0].getPort());

        assertEquals("127.0.0.1", addresses[1].getAddress());
        assertEquals(2346, addresses[1].getPort());
    }

    /** Test unbracketed ipv6 hosts in configuration fail. Leave no ambiguity */
    public void testParseV6UnBracketed() throws Exception {
        try {
            TcpTransport.parse("::1", "1234", Integer.MAX_VALUE);
            fail("should have gotten exception");
        } catch (IllegalArgumentException expected) {
            assertTrue(expected.getMessage().contains("must be bracketed"));
        }
    }

    /** Test ipv6 host with a default port works */
    public void testParseV6DefaultPort() throws Exception {
        TransportAddress[] addresses = TcpTransport.parse("[::1]", "1234", Integer.MAX_VALUE);
        assertEquals(1, addresses.length);

        assertEquals("::1", addresses[0].getAddress());
        assertEquals(1234, addresses[0].getPort());
    }

    /** Test ipv6 host with a default port range works */
    public void testParseV6DefaultRange() throws Exception {
        TransportAddress[] addresses = TcpTransport.parse("[::1]", "1234-1235", Integer.MAX_VALUE);
        assertEquals(2, addresses.length);

        assertEquals("::1", addresses[0].getAddress());
        assertEquals(1234, addresses[0].getPort());

        assertEquals("::1", addresses[1].getAddress());
        assertEquals(1235, addresses[1].getPort());
    }

    /** Test ipv6 host with port works */
    public void testParseV6WithPort() throws Exception {
        TransportAddress[] addresses = TcpTransport.parse("[::1]:2345", "1234", Integer.MAX_VALUE);
        assertEquals(1, addresses.length);

        assertEquals("::1", addresses[0].getAddress());
        assertEquals(2345, addresses[0].getPort());
    }

    /** Test ipv6 host with port range works */
    public void testParseV6WithPortRange() throws Exception {
        TransportAddress[] addresses = TcpTransport.parse("[::1]:2345-2346", "1234", Integer.MAX_VALUE);
        assertEquals(2, addresses.length);

        assertEquals("::1", addresses[0].getAddress());
        assertEquals(2345, addresses[0].getPort());

        assertEquals("::1", addresses[1].getAddress());
        assertEquals(2346, addresses[1].getPort());
    }

    /** Test per-address limit */
    public void testAddressLimit() throws Exception {
        TransportAddress[] addresses = TcpTransport.parse("[::1]:100-200", "1000", 3);
        assertEquals(3, addresses.length);
        assertEquals(100, addresses[0].getPort());
        assertEquals(101, addresses[1].getPort());
        assertEquals(102, addresses[2].getPort());
    }

    public void testEnsureVersionCompatibility() {
        TcpTransport.ensureVersionCompatibility(VersionUtils.randomVersionBetween(random(), Version.CURRENT.minimumCompatibilityVersion(),
            Version.CURRENT), Version.CURRENT, randomBoolean());

        TcpTransport.ensureVersionCompatibility(Version.fromString("5.0.0"), Version.fromString("6.0.0"), true);
        IllegalStateException ise = expectThrows(IllegalStateException.class, () ->
            TcpTransport.ensureVersionCompatibility(Version.fromString("5.0.0"), Version.fromString("6.0.0"), false));
        assertEquals("Received message from unsupported version: [5.0.0] minimal compatible version is: [5.6.0]", ise.getMessage());

        ise = expectThrows(IllegalStateException.class, () ->
            TcpTransport.ensureVersionCompatibility(Version.fromString("2.3.0"), Version.fromString("6.0.0"), true));
        assertEquals("Received handshake message from unsupported version: [2.3.0] minimal compatible version is: [5.6.0]",
            ise.getMessage());

        ise = expectThrows(IllegalStateException.class, () ->
            TcpTransport.ensureVersionCompatibility(Version.fromString("2.3.0"), Version.fromString("6.0.0"), false));
        assertEquals("Received message from unsupported version: [2.3.0] minimal compatible version is: [5.6.0]",
            ise.getMessage());
    }

    public void testCompressRequest() throws IOException {
        final boolean compressed = randomBoolean();
        Req request = new Req(randomRealisticUnicodeOfLengthBetween(10, 100));
        ThreadPool threadPool = new TestThreadPool(TcpTransportTests.class.getName());
        AtomicReference<BytesReference> messageCaptor = new AtomicReference<>();
        try {
            TcpTransport transport = new TcpTransport(
                "test", Settings.builder().put("transport.tcp.compress", compressed).build(), threadPool,
                new BigArrays(new PageCacheRecycler(Settings.EMPTY), null), null, null, null) {

                @Override
                protected FakeChannel bind(String name, InetSocketAddress address) throws IOException {
                    return null;
                }

                @Override
                protected FakeChannel initiateChannel(DiscoveryNode node, ActionListener<Void> connectListener) throws IOException {
                    return new FakeChannel(messageCaptor);
                }

                @Override
                protected void stopInternal() {
                }

                @Override
                public NodeChannels openConnection(DiscoveryNode node, ConnectionProfile connectionProfile) {
                    int numConnections = MockTcpTransport.LIGHT_PROFILE.getNumConnections();
                    ArrayList<TcpChannel> fakeChannels = new ArrayList<>(numConnections);
                    for (int i = 0; i < numConnections; ++i) {
                        fakeChannels.add(new FakeChannel(messageCaptor));
                    }
                    return new NodeChannels(node, fakeChannels, MockTcpTransport.LIGHT_PROFILE, Version.CURRENT);
                }
            };

            DiscoveryNode node = new DiscoveryNode("foo", buildNewFakeTransportAddress(), Version.CURRENT);
            Transport.Connection connection = transport.openConnection(node, null);
            connection.sendRequest(42, "foobar", request, TransportRequestOptions.EMPTY);

            BytesReference reference = messageCaptor.get();
            assertNotNull(reference);

            StreamInput streamIn = reference.streamInput();
            streamIn.skip(TcpHeader.MARKER_BYTES_SIZE);
            int len = streamIn.readInt();
            long requestId = streamIn.readLong();
            assertEquals(42, requestId);
            byte status = streamIn.readByte();
            Version version = Version.fromId(streamIn.readInt());
            assertEquals(Version.CURRENT, version);
            assertEquals(compressed, TransportStatus.isCompress(status));
            if (compressed) {
                final int bytesConsumed = TcpHeader.HEADER_SIZE;
                streamIn = CompressorFactory.compressor(reference.slice(bytesConsumed, reference.length() - bytesConsumed))
                    .streamInput(streamIn);
                }
            threadPool.getThreadContext().readHeaders(streamIn);
            assertThat(streamIn.readStringArray(), equalTo(new String[0])); // features
            assertEquals("foobar", streamIn.readString());
            Req readReq = new Req("");
            readReq.readFrom(streamIn);
            assertEquals(request.value, readReq.value);

        } finally {
            ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS);
        }
    }

    private static final class FakeChannel implements TcpChannel, TcpServerChannel {

        private final AtomicReference<BytesReference> messageCaptor;

        FakeChannel(AtomicReference<BytesReference> messageCaptor) {
            this.messageCaptor = messageCaptor;
        }

        @Override
        public void close() {
        }

        @Override
        public String getProfile() {
            return null;
        }

        @Override
        public void addCloseListener(ActionListener<Void> listener) {
        }

        @Override
        public void setSoLinger(int value) throws IOException {
        }

        @Override
        public boolean isOpen() {
            return false;
        }

        @Override
        public InetSocketAddress getLocalAddress() {
            return null;
        }

        @Override
        public InetSocketAddress getRemoteAddress() {
            return null;
        }

        @Override
        public void sendMessage(BytesReference reference, ActionListener<Void> listener) {
            messageCaptor.set(reference);
        }
    }

    private static final class Req extends TransportRequest {
        public String value;

        private Req(String value) {
            this.value = value;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            value = in.readString();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(value);
        }
    }

    public void testDecodeWithIncompleteHeader() throws IOException {
        BytesStreamOutput streamOutput = new BytesStreamOutput(1 << 14);
        streamOutput.write('E');
        streamOutput.write('S');
        streamOutput.write(1);
        streamOutput.write(1);

        assertNull(TcpTransport.decodeFrame(streamOutput.bytes()));
    }

    public void testDecodePing() throws IOException {
        BytesStreamOutput streamOutput = new BytesStreamOutput(1 << 14);
        streamOutput.write('E');
        streamOutput.write('S');
        streamOutput.writeInt(-1);

        BytesReference message = TcpTransport.decodeFrame(streamOutput.bytes());

        assertEquals(0, message.length());
    }

    public void testDecodePingWithStartOfSecondMessage() throws IOException {
        BytesStreamOutput streamOutput = new BytesStreamOutput(1 << 14);
        streamOutput.write('E');
        streamOutput.write('S');
        streamOutput.writeInt(-1);
        streamOutput.write('E');
        streamOutput.write('S');

        BytesReference message = TcpTransport.decodeFrame(streamOutput.bytes());

        assertEquals(0, message.length());
    }

    public void testDecodeMessage() throws IOException {
        BytesStreamOutput streamOutput = new BytesStreamOutput(1 << 14);
        streamOutput.write('E');
        streamOutput.write('S');
        streamOutput.writeInt(2);
        streamOutput.write('M');
        streamOutput.write('A');

        BytesReference message = TcpTransport.decodeFrame(streamOutput.bytes());

        assertEquals(streamOutput.bytes().slice(6, 2), message);
    }

    public void testDecodeIncompleteMessage() throws IOException {
        BytesStreamOutput streamOutput = new BytesStreamOutput(1 << 14);
        streamOutput.write('E');
        streamOutput.write('S');
        streamOutput.writeInt(3);
        streamOutput.write('M');
        streamOutput.write('A');

        BytesReference message = TcpTransport.decodeFrame(streamOutput.bytes());

        assertNull(message);
    }

    public void testInvalidLength() throws IOException {
        BytesStreamOutput streamOutput = new BytesStreamOutput(1 << 14);
        streamOutput.write('E');
        streamOutput.write('S');
        streamOutput.writeInt(-2);
        streamOutput.write('M');
        streamOutput.write('A');

        try {
            TcpTransport.decodeFrame(streamOutput.bytes());
            fail("Expected exception");
        } catch (Exception ex) {
            assertThat(ex, instanceOf(StreamCorruptedException.class));
            assertEquals("invalid data length: -2", ex.getMessage());
        }
    }

    public void testInvalidHeader() throws IOException {
        BytesStreamOutput streamOutput = new BytesStreamOutput(1 << 14);
        streamOutput.write('E');
        streamOutput.write('C');
        byte byte1 = randomByte();
        byte byte2 = randomByte();
        streamOutput.write(byte1);
        streamOutput.write(byte2);
        streamOutput.write(randomByte());
        streamOutput.write(randomByte());
        streamOutput.write(randomByte());

        try {
            TcpTransport.decodeFrame(streamOutput.bytes());
            fail("Expected exception");
        } catch (Exception ex) {
            assertThat(ex, instanceOf(StreamCorruptedException.class));
            String expected = "invalid internal transport message format, got (45,43,"
                + Integer.toHexString(byte1 & 0xFF) + ","
                + Integer.toHexString(byte2 & 0xFF) + ")";
            assertEquals(expected, ex.getMessage());
        }
    }

    public void testHTTPHeader() throws IOException {
        String[] httpHeaders = {"GET", "POST", "PUT", "HEAD", "DELETE", "OPTIONS", "PATCH", "TRACE"};

        for (String httpHeader : httpHeaders) {
            BytesStreamOutput streamOutput = new BytesStreamOutput(1 << 14);

            for (char c : httpHeader.toCharArray()) {
                streamOutput.write((byte) c);
            }
            streamOutput.write(new byte[6]);

            try {
                BytesReference bytes = streamOutput.bytes();
                TcpTransport.decodeFrame(bytes);
                fail("Expected exception");
            } catch (Exception ex) {
                assertThat(ex, instanceOf(TcpTransport.HttpOnTransportException.class));
                assertEquals("This is not a HTTP port", ex.getMessage());
            }
        }
    }
}
