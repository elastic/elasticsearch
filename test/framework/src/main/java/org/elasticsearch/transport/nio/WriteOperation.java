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

package org.elasticsearch.transport.nio;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.transport.nio.channel.NioChannel;
import org.elasticsearch.transport.nio.channel.NioSocketChannel;

import java.io.IOException;
import java.nio.ByteBuffer;

public class WriteOperation {

    private final NioSocketChannel channel;
    private final ActionListener<NioChannel> listener;
    private final NetworkBytesReference networkBuffer;

    public WriteOperation(NioSocketChannel channel, NetworkBytesReference networkBuffer, ActionListener<NioChannel> listener) {
        this.channel = channel;
        this.listener = listener;
        this.networkBuffer = networkBuffer;
    }

    public NetworkBytesReference getNetworkBuffer() throws IOException {
        return networkBuffer;
    }

    public ActionListener<NioChannel> getListener() {
        return listener;
    }

    public NioSocketChannel getChannel() {
        return channel;
    }

    public boolean isFullyFlushed() {
        return networkBuffer.getReadRemaining() == 0;
    }

    public int flush() throws IOException {
        if (networkBuffer.hasMultipleBuffers()) {
            return vectorizedFlush(networkBuffer.getReadByteBuffers());
        } else {
            return singleFlush(networkBuffer.getReadByteBuffer());
        }
    }

    private int singleFlush(ByteBuffer buffer) throws IOException {
        int totalWritten = 0;
        while (networkBuffer.getReadRemaining() != 0) {
            int written = channel.write(buffer);
            if (written <= 0) {
                break;
            }
            totalWritten += written;
            networkBuffer.incrementRead(written);
        }
        return totalWritten;
    }

    private int vectorizedFlush(ByteBuffer[] buffers) throws IOException {
        int totalWritten = 0;
        while (networkBuffer.getReadRemaining() != 0) {
            int written = (int) channel.vectorizedWrite(buffers);
            if (written <= 0) {
                break;
            }
            totalWritten += written;
            networkBuffer.incrementRead(written);
        }
        return totalWritten;
    }
}
