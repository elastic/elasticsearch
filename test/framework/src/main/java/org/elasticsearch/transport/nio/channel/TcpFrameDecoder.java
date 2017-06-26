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
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.monitor.jvm.JvmInfo;
import org.elasticsearch.transport.TcpHeader;
import org.elasticsearch.transport.TcpTransport;

import java.io.IOException;
import java.io.StreamCorruptedException;

public class TcpFrameDecoder {

    private static final long NINETY_PER_HEAP_SIZE = (long) (JvmInfo.jvmInfo().getMem().getHeapMax().getBytes() * 0.9);
    private static final int HEADER_SIZE = TcpHeader.MARKER_BYTES_SIZE + TcpHeader.MESSAGE_LENGTH_SIZE;

    private int expectedMessageLength = -1;

    public BytesReference decode(BytesReference bytesReference, int currentBufferSize) throws IOException {
        if (currentBufferSize >= 6) {
            int messageLength = readHeaderBuffer(bytesReference);
            int totalLength = messageLength + HEADER_SIZE;
            if (totalLength > currentBufferSize) {
                expectedMessageLength = totalLength;
                return null;
            } else if (totalLength == bytesReference.length()) {
                expectedMessageLength = -1;
                return bytesReference;
            } else {
                expectedMessageLength = -1;
                return bytesReference.slice(0, totalLength);
            }
        } else {
            return null;
        }
    }

    public int expectedMessageLength() {
        return expectedMessageLength;
    }

    private int readHeaderBuffer(BytesReference headerBuffer) throws IOException {
        if (headerBuffer.get(0) != 'E' || headerBuffer.get(1) != 'S') {
            if (appearsToBeHTTP(headerBuffer)) {
                throw new TcpTransport.HttpOnTransportException("This is not a HTTP port");
            }

            throw new StreamCorruptedException("invalid internal transport message format, got ("
                + Integer.toHexString(headerBuffer.get(0) & 0xFF) + ","
                + Integer.toHexString(headerBuffer.get(1) & 0xFF) + ","
                + Integer.toHexString(headerBuffer.get(2) & 0xFF) + ","
                + Integer.toHexString(headerBuffer.get(3) & 0xFF) + ")");
        }
        final int messageLength;
        try (StreamInput input = headerBuffer.streamInput()) {
            input.skip(TcpHeader.MARKER_BYTES_SIZE);
            messageLength = input.readInt();
        }

        if (messageLength == -1) {
            // This is a ping
            return 0;
        }

        if (messageLength <= 0) {
            throw new StreamCorruptedException("invalid data length: " + messageLength);
        }

        if (messageLength > NINETY_PER_HEAP_SIZE) {
            throw new IllegalArgumentException("transport content length received [" + new ByteSizeValue(messageLength) + "] exceeded ["
                + new ByteSizeValue(NINETY_PER_HEAP_SIZE) + "]");
        }

        return messageLength;
    }

    private static boolean appearsToBeHTTP(BytesReference headerBuffer) {
        return bufferStartsWith(headerBuffer, "GET") ||
            bufferStartsWith(headerBuffer, "POST") ||
            bufferStartsWith(headerBuffer, "PUT") ||
            bufferStartsWith(headerBuffer, "HEAD") ||
            bufferStartsWith(headerBuffer, "DELETE") ||
            // TODO: Actually 'OPTIONS'. But that does not currently fit in 6 bytes
            bufferStartsWith(headerBuffer, "OPTION") ||
            bufferStartsWith(headerBuffer, "PATCH") ||
            bufferStartsWith(headerBuffer, "TRACE");
    }

    private static boolean bufferStartsWith(BytesReference buffer, String method) {
        char[] chars = method.toCharArray();
        for (int i = 0; i < chars.length; i++) {
            if (buffer.get(i) != chars[i]) {
                return false;
            }
        }
        return true;
    }
}
