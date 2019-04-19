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

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.io.StreamCorruptedException;

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
                assertEquals("This is not an HTTP port", ex.getMessage());
            }
        }
    }
}
