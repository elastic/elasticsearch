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
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.VersionUtils;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.equalTo;

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
                new BigArrays(Settings.EMPTY, null), null, null, null) {

                @Override
                protected FakeChannel bind(String name, InetSocketAddress address) throws IOException {
                    return null;
                }

                @Override
                protected FakeChannel initiateChannel(DiscoveryNode node, TimeValue connectTimeout, ActionListener<Void> connectListener)
                    throws IOException {
                    return new FakeChannel(messageCaptor);
                }

                @Override
                public long getNumOpenServerConnections() {
                    return 0;
                }

                @Override
                public NodeChannels getConnection(DiscoveryNode node) {
                    int numConnections = MockTcpTransport.LIGHT_PROFILE.getNumConnections();
                    ArrayList<TcpChannel> fakeChannels = new ArrayList<>(numConnections);
                    for (int i = 0; i < numConnections; ++i) {
                        fakeChannels.add(new FakeChannel(messageCaptor));
                    }
                    return new NodeChannels(node, fakeChannels, MockTcpTransport.LIGHT_PROFILE, Version.CURRENT);
                }
            };

            DiscoveryNode node = new DiscoveryNode("foo", buildNewFakeTransportAddress(), Version.CURRENT);
            Transport.Connection connection = transport.getConnection(node);
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
            assertEquals("foobar", streamIn.readString());
            Req readReq = new Req("");
            readReq.readFrom(streamIn);
            assertEquals(request.value, readReq.value);

        } finally {
            ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS);
        }
    }

    private static final class FakeChannel implements TcpChannel {

        private final AtomicReference<BytesReference> messageCaptor;

        FakeChannel(AtomicReference<BytesReference> messageCaptor) {
            this.messageCaptor = messageCaptor;
        }

        @Override
        public void close() {
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

    public void testConnectionProfileResolve() {
        final ConnectionProfile defaultProfile = TcpTransport.buildDefaultConnectionProfile(Settings.EMPTY);
        assertEquals(defaultProfile, TcpTransport.resolveConnectionProfile(null, defaultProfile));

        final ConnectionProfile.Builder builder = new ConnectionProfile.Builder();
        builder.addConnections(randomIntBetween(0, 5), TransportRequestOptions.Type.BULK);
        builder.addConnections(randomIntBetween(0, 5), TransportRequestOptions.Type.RECOVERY);
        builder.addConnections(randomIntBetween(0, 5), TransportRequestOptions.Type.REG);
        builder.addConnections(randomIntBetween(0, 5), TransportRequestOptions.Type.STATE);
        builder.addConnections(randomIntBetween(0, 5), TransportRequestOptions.Type.PING);

        final boolean connectionTimeoutSet = randomBoolean();
        if (connectionTimeoutSet) {
            builder.setConnectTimeout(TimeValue.timeValueMillis(randomNonNegativeLong()));
        }
        final boolean connectionHandshakeSet = randomBoolean();
        if (connectionHandshakeSet) {
            builder.setHandshakeTimeout(TimeValue.timeValueMillis(randomNonNegativeLong()));
        }

        final ConnectionProfile profile = builder.build();
        final ConnectionProfile resolved = TcpTransport.resolveConnectionProfile(profile, defaultProfile);
        assertNotEquals(resolved, defaultProfile);
        assertThat(resolved.getNumConnections(), equalTo(profile.getNumConnections()));
        assertThat(resolved.getHandles(), equalTo(profile.getHandles()));

        assertThat(resolved.getConnectTimeout(),
            equalTo(connectionTimeoutSet ? profile.getConnectTimeout() : defaultProfile.getConnectTimeout()));
        assertThat(resolved.getHandshakeTimeout(),
            equalTo(connectionHandshakeSet ? profile.getHandshakeTimeout() : defaultProfile.getHandshakeTimeout()));
    }

    public void testDefaultConnectionProfile() {
        ConnectionProfile profile = TcpTransport.buildDefaultConnectionProfile(Settings.EMPTY);
        assertEquals(13, profile.getNumConnections());
        assertEquals(1, profile.getNumConnectionsPerType(TransportRequestOptions.Type.PING));
        assertEquals(6, profile.getNumConnectionsPerType(TransportRequestOptions.Type.REG));
        assertEquals(1, profile.getNumConnectionsPerType(TransportRequestOptions.Type.STATE));
        assertEquals(2, profile.getNumConnectionsPerType(TransportRequestOptions.Type.RECOVERY));
        assertEquals(3, profile.getNumConnectionsPerType(TransportRequestOptions.Type.BULK));

        profile = TcpTransport.buildDefaultConnectionProfile(Settings.builder().put("node.master", false).build());
        assertEquals(12, profile.getNumConnections());
        assertEquals(1, profile.getNumConnectionsPerType(TransportRequestOptions.Type.PING));
        assertEquals(6, profile.getNumConnectionsPerType(TransportRequestOptions.Type.REG));
        assertEquals(0, profile.getNumConnectionsPerType(TransportRequestOptions.Type.STATE));
        assertEquals(2, profile.getNumConnectionsPerType(TransportRequestOptions.Type.RECOVERY));
        assertEquals(3, profile.getNumConnectionsPerType(TransportRequestOptions.Type.BULK));

        profile = TcpTransport.buildDefaultConnectionProfile(Settings.builder().put("node.data", false).build());
        assertEquals(11, profile.getNumConnections());
        assertEquals(1, profile.getNumConnectionsPerType(TransportRequestOptions.Type.PING));
        assertEquals(6, profile.getNumConnectionsPerType(TransportRequestOptions.Type.REG));
        assertEquals(1, profile.getNumConnectionsPerType(TransportRequestOptions.Type.STATE));
        assertEquals(0, profile.getNumConnectionsPerType(TransportRequestOptions.Type.RECOVERY));
        assertEquals(3, profile.getNumConnectionsPerType(TransportRequestOptions.Type.BULK));

        profile = TcpTransport.buildDefaultConnectionProfile(Settings.builder().put("node.data", false).put("node.master", false).build());
        assertEquals(10, profile.getNumConnections());
        assertEquals(1, profile.getNumConnectionsPerType(TransportRequestOptions.Type.PING));
        assertEquals(6, profile.getNumConnectionsPerType(TransportRequestOptions.Type.REG));
        assertEquals(0, profile.getNumConnectionsPerType(TransportRequestOptions.Type.STATE));
        assertEquals(0, profile.getNumConnectionsPerType(TransportRequestOptions.Type.RECOVERY));
        assertEquals(3, profile.getNumConnectionsPerType(TransportRequestOptions.Type.BULK));
    }

}
