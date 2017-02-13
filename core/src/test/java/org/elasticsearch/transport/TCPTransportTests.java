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
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.hamcrest.Matchers.equalTo;

/** Unit tests for TCPTransport */
public class TCPTransportTests extends ESTestCase {

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

    public void testCompressRequest() throws IOException {
        final boolean compressed = randomBoolean();
        final AtomicBoolean called = new AtomicBoolean(false);
        Req request = new Req(randomRealisticUnicodeOfLengthBetween(10, 100));
        ThreadPool threadPool = new TestThreadPool(TCPTransportTests.class.getName());
        try {
            TcpTransport transport = new TcpTransport("test", Settings.builder().put("transport.tcp.compress", compressed).build(),
                threadPool, new BigArrays(Settings.EMPTY, null), null, null, null) {
                @Override
                protected InetSocketAddress getLocalAddress(Object o) {
                    return null;
                }

                @Override
                protected Object bind(String name, InetSocketAddress address) throws IOException {
                    return null;
                }

                @Override
                protected void closeChannels(List channel) throws IOException {

                }

                @Override
                protected void sendMessage(Object o, BytesReference reference, Runnable sendListener) throws IOException {
                    StreamInput streamIn = reference.streamInput();
                    streamIn.skip(TcpHeader.MARKER_BYTES_SIZE);
                    int len = streamIn.readInt();
                    long requestId = streamIn.readLong();
                    assertEquals(42, requestId);
                    byte status = streamIn.readByte();
                    Version version = Version.fromId(streamIn.readInt());
                    assertEquals(Version.CURRENT, version);
                    assertEquals(compressed, TransportStatus.isCompress(status));
                    called.compareAndSet(false, true);
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
                }

                @Override
                protected NodeChannels connectToChannels(DiscoveryNode node, ConnectionProfile profile) throws IOException {
                    return new NodeChannels(node, new Object[profile.getNumConnections()], profile);
                }

                @Override
                protected boolean isOpen(Object o) {
                    return false;
                }

                @Override
                public long serverOpen() {
                    return 0;
                }

                @Override
                public NodeChannels getConnection(DiscoveryNode node) {
                    return new NodeChannels(node, new Object[MockTcpTransport.LIGHT_PROFILE.getNumConnections()],
                        MockTcpTransport.LIGHT_PROFILE);
                }
            };
            DiscoveryNode node = new DiscoveryNode("foo", buildNewFakeTransportAddress(), Version.CURRENT);
            Transport.Connection connection = transport.getConnection(node);
            connection.sendRequest(42, "foobar", request, TransportRequestOptions.EMPTY);
            assertTrue(called.get());
        } finally {
            ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS);
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
