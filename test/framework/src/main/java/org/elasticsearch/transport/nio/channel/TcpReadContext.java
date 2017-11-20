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
import org.elasticsearch.common.bytes.CompositeBytesReference;
import org.elasticsearch.transport.nio.NetworkBytesReference;
import org.elasticsearch.transport.nio.TcpReadHandler;

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;

public class TcpReadContext implements ReadContext {

    private static final int DEFAULT_READ_LENGTH = 1 << 14;

    private final TcpReadHandler handler;
    private final NioSocketChannel channel;
    private final TcpFrameDecoder frameDecoder;
    private final LinkedList<NetworkBytesReference> references = new LinkedList<>();
    private int rawBytesCount = 0;

    public TcpReadContext(NioSocketChannel channel, TcpReadHandler handler) {
        this(channel, handler, new TcpFrameDecoder());
    }

    public TcpReadContext(NioSocketChannel channel, TcpReadHandler handler, TcpFrameDecoder frameDecoder) {
        this.handler = handler;
        this.channel = channel;
        this.frameDecoder = frameDecoder;
        this.references.add(NetworkBytesReference.wrap(new BytesArray(new byte[DEFAULT_READ_LENGTH])));
    }

    @Override
    public int read() throws IOException {
        NetworkBytesReference last = references.peekLast();
        if (last == null || last.hasWriteRemaining() == false) {
            this.references.add(NetworkBytesReference.wrap(new BytesArray(new byte[DEFAULT_READ_LENGTH])));
        }

        int bytesRead = channel.read(references.getLast());

        if (bytesRead == -1) {
            return bytesRead;
        }

        rawBytesCount += bytesRead;

        BytesReference message;

        // Frame decoder will throw an exception if the message is improperly formatted, the header is incorrect,
        // or the message is corrupted
        while ((message = frameDecoder.decode(createCompositeBuffer(), rawBytesCount)) != null) {
            int messageLengthWithHeader = message.length();
            NetworkBytesReference.vectorizedIncrementReadIndexes(references, messageLengthWithHeader);
            trimDecodedMessages(messageLengthWithHeader);
            rawBytesCount -= messageLengthWithHeader;

            try {
                BytesReference messageWithoutHeader = message.slice(6, message.length() - 6);

                // A message length of 6 bytes it is just a ping. Ignore for now.
                if (messageLengthWithHeader != 6) {
                    handler.handleMessage(messageWithoutHeader, channel, messageWithoutHeader.length());
                }
            } catch (Exception e) {
                handler.handleException(channel, e);
            }
        }

        return bytesRead;
    }

    private CompositeBytesReference createCompositeBuffer() {
        return new CompositeBytesReference(references.toArray(new BytesReference[references.size()]));
    }

    private void trimDecodedMessages(int bytesToTrim) {
        while (bytesToTrim != 0) {
            NetworkBytesReference ref = references.getFirst();
            int readIndex = ref.getReadIndex();
            bytesToTrim -= readIndex;
            if (readIndex == ref.length()) {
                references.removeFirst();
            } else {
                assert bytesToTrim == 0;
                if (readIndex != 0) {
                    references.removeFirst();
                    NetworkBytesReference slicedRef = ref.slice(readIndex, ref.length() - readIndex);
                    references.addFirst(slicedRef);
                }
            }

        }
    }
}
