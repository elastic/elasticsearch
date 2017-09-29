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

package org.elasticsearch.transport.nio.channel;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.TcpTransport;

import java.io.IOException;
import java.io.StreamCorruptedException;

import static org.hamcrest.Matchers.instanceOf;

public class TcpFrameDecoderTests extends ESTestCase {

    private TcpFrameDecoder frameDecoder = new TcpFrameDecoder();

    public void testDefaultExceptedMessageLengthIsNegative1() {
        assertEquals(-1, frameDecoder.expectedMessageLength());
    }

    public void testDecodeWithIncompleteHeader() throws IOException {
        BytesStreamOutput streamOutput = new BytesStreamOutput(1 << 14);
        streamOutput.write('E');
        streamOutput.write('S');
        streamOutput.write(1);
        streamOutput.write(1);
        streamOutput.write(0);
        streamOutput.write(0);

        assertNull(frameDecoder.decode(streamOutput.bytes(), 4));
        assertEquals(-1, frameDecoder.expectedMessageLength());
    }

    public void testDecodePing() throws IOException {
        BytesStreamOutput streamOutput = new BytesStreamOutput(1 << 14);
        streamOutput.write('E');
        streamOutput.write('S');
        streamOutput.writeInt(-1);

        BytesReference message = frameDecoder.decode(streamOutput.bytes(), 6);

        assertEquals(-1, frameDecoder.expectedMessageLength());
        assertEquals(streamOutput.bytes(), message);
    }

    public void testDecodePingWithStartOfSecondMessage() throws IOException {
        BytesStreamOutput streamOutput = new BytesStreamOutput(1 << 14);
        streamOutput.write('E');
        streamOutput.write('S');
        streamOutput.writeInt(-1);
        streamOutput.write('E');
        streamOutput.write('S');

        BytesReference message = frameDecoder.decode(streamOutput.bytes(), 8);

        assertEquals(6, message.length());
        assertEquals(streamOutput.bytes().slice(0, 6), message);
    }

    public void testDecodeMessage() throws IOException {
        BytesStreamOutput streamOutput = new BytesStreamOutput(1 << 14);
        streamOutput.write('E');
        streamOutput.write('S');
        streamOutput.writeInt(2);
        streamOutput.write('M');
        streamOutput.write('A');

        BytesReference message = frameDecoder.decode(streamOutput.bytes(), 8);

        assertEquals(-1, frameDecoder.expectedMessageLength());
        assertEquals(streamOutput.bytes(), message);
    }

    public void testDecodeIncompleteMessage() throws IOException {
        BytesStreamOutput streamOutput = new BytesStreamOutput(1 << 14);
        streamOutput.write('E');
        streamOutput.write('S');
        streamOutput.writeInt(3);
        streamOutput.write('M');
        streamOutput.write('A');

        BytesReference message = frameDecoder.decode(streamOutput.bytes(), 8);

        assertEquals(9, frameDecoder.expectedMessageLength());
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
            frameDecoder.decode(streamOutput.bytes(), 8);
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
            frameDecoder.decode(streamOutput.bytes(), 7);
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
                frameDecoder.decode(bytes, bytes.length());
                fail("Expected exception");
            } catch (Exception ex) {
                assertThat(ex, instanceOf(TcpTransport.HttpOnTransportException.class));
                assertEquals("This is not a HTTP port", ex.getMessage());
            }
        }
    }
}
