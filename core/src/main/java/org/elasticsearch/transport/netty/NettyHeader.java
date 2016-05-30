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

package org.elasticsearch.transport.netty;

import org.elasticsearch.Version;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;

/**
 */
public class NettyHeader {
    public static final int MARKER_BYTES_SIZE = 2 * 1;

    public static final int MESSAGE_LENGTH_SIZE = 4;

    public static final int REQUEST_ID_SIZE = 8;

    public static final int STATUS_SIZE = 1;

    public static final int VERSION_ID_SIZE = 4;

    public static final int HEADER_SIZE = MARKER_BYTES_SIZE + MESSAGE_LENGTH_SIZE + REQUEST_ID_SIZE + STATUS_SIZE + VERSION_ID_SIZE;

    /**
     * The magic number (must be lower than 0) for a ping message. This is handled
     * specifically in {@link org.elasticsearch.transport.netty.SizeHeaderFrameDecoder}.
     */
    public static final int PING_DATA_SIZE = -1;
    private final static ChannelBuffer pingHeader;
    static {
        pingHeader = ChannelBuffers.buffer(6);
        pingHeader.writeByte('E');
        pingHeader.writeByte('S');
        pingHeader.writeInt(PING_DATA_SIZE);
    }

    /**
     * A ping header is same as regular header, just with -1 for the size of the message.
     */
    public static ChannelBuffer pingHeader() {
        return pingHeader.duplicate();
    }

    public static void writeHeader(ChannelBuffer buffer, long requestId, byte status, Version version) {
        int index = buffer.readerIndex();
        buffer.setByte(index, 'E');
        index += 1;
        buffer.setByte(index, 'S');
        index += 1;
        // write the size, the size indicates the remaining message size, not including the size int
        buffer.setInt(index, buffer.readableBytes() - MARKER_BYTES_SIZE - MESSAGE_LENGTH_SIZE);
        index += MESSAGE_LENGTH_SIZE;
        buffer.setLong(index, requestId);
        index += REQUEST_ID_SIZE;
        buffer.setByte(index, status);
        index += STATUS_SIZE;
        buffer.setInt(index, version.id);
    }
}
