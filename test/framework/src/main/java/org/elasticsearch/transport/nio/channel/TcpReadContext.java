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

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.monitor.jvm.JvmInfo;
import org.elasticsearch.transport.TcpHeader;
import org.elasticsearch.transport.TcpTransport;
import org.elasticsearch.transport.nio.TcpReadHandler;

import java.io.IOException;
import java.io.StreamCorruptedException;
import java.nio.ByteBuffer;

public class TcpReadContext implements ReadContext {

    private static final int READ_INCOMPLETE = 0;
    private static final int PING = 1;
    private static final int READ_COMPLETE = 2;
    private static final int PEER_CLOSED = 3;
    private static final long NINETY_PER_HEAP_SIZE = (long) (JvmInfo.jvmInfo().getMem().getHeapMax().getBytes() * 0.9);

    private final TcpReadHandler handler;
    private final NioSocketChannel channel;
    private final ByteBuffer headerBuffer = ByteBuffer.allocate(TcpHeader.MARKER_BYTES_SIZE + TcpHeader.MESSAGE_LENGTH_SIZE);

    private int bytesRead = 0;
    private int headerBytesRemaining = 6;
    private int messageBytesLength = -1;
    private int messageBytesRemaining = -1;
    private ByteBuffer messageBuffer;

    public TcpReadContext(NioSocketChannel channel, TcpReadHandler handler) {
        this.handler = handler;
        this.channel = channel;
    }

    @Override
    public int read() throws IOException {
        bytesRead = 0;

        try {
            if (headerBytesRemaining > 0) {
                int headerStatus = readPartialHeader();
                switch (headerStatus) {
                    case PING:
                        resetMessageStateMachine();
                        handler.handlePing(channel, channel.getProfile());
                        return bytesRead;
                    case READ_INCOMPLETE:
                        return bytesRead;
                    case PEER_CLOSED:
                        resetMessageStateMachine();
                        return -1;
                    case READ_COMPLETE:
                }
            }

            int readStatus = readMessage();
            switch (readStatus) {
                case READ_INCOMPLETE:
                    return bytesRead;
                case PEER_CLOSED:
                    resetMessageStateMachine();
                    return -1;
                case READ_COMPLETE:

            }
            BytesReference reference = new BytesArray(messageBuffer.array());

            handler.handleMessage(reference, channel, channel.getProfile(), messageBytesLength);
            resetMessageStateMachine();
        } catch (Exception e) {
            resetMessageStateMachine();
            throw e;
        }
        return bytesRead;
    }

    public int newRead() throws IOException {
        bytesRead = 0;

        TcpFrameDecoder frameDecoder = new TcpFrameDecoder();
        byte[] bytes = new byte[frameDecoder.nextReadLength()];
        int bytesRead = channel.read(ByteBuffer.wrap(bytes));
        BytesReference message = frameDecoder.decode(new BytesArray(bytes));

        return bytesRead;
    }

    private int readPartialHeader() throws IOException {
        int headerBytesRead = channel.read(headerBuffer);
        if (headerBytesRead < 0) {
            return PEER_CLOSED;
        }

        bytesRead += headerBytesRead;
        headerBytesRemaining -= headerBytesRead;
        if (headerBytesRemaining != 0) {
            return READ_INCOMPLETE;
        }
        int messageLength = readHeaderBuffer(headerBuffer);
        if (messageLength == 0) {
            return PING;
        }
        messageBytesLength = messageLength;
        messageBytesRemaining = messageLength;
        messageBuffer = ByteBuffer.allocate(messageLength);
        return READ_COMPLETE;
    }

    private int readMessage() throws IOException {
        int messageBytesRead = channel.read(messageBuffer);
        if (messageBytesRead < 0) {
            return PEER_CLOSED;
        }

        bytesRead += messageBytesRead;
        messageBytesRemaining -= messageBytesRead;

        if (messageBytesRemaining != 0) {
            return READ_INCOMPLETE;
        }
        return READ_COMPLETE;
    }

    private void resetMessageStateMachine() {
        headerBuffer.clear();
        headerBytesRemaining = 6;
        messageBytesLength = -1;
        messageBytesRemaining = -1;
    }

    private int readHeaderBuffer(ByteBuffer headerBuffer) throws StreamCorruptedException {
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
        int messageLength = headerBuffer.getInt(2);

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

    private static boolean appearsToBeHTTP(ByteBuffer headerBuffer) {
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

    private static boolean bufferStartsWith(ByteBuffer buffer, String method) {
        char[] chars = method.toCharArray();
        for (int i = 0; i < chars.length; i++) {
            if (buffer.get(i) != chars[i]) {
                return false;
            }
        }
        return true;
    }
}
