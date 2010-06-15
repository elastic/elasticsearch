/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

import org.elasticsearch.common.netty.buffer.ChannelBuffer;
import org.elasticsearch.util.io.stream.StreamInput;

import java.io.IOException;

/**
 * A Netty {@link org.elasticsearch.common.netty.buffer.ChannelBuffer} based {@link org.elasticsearch.util.io.stream.StreamInput}.
 *
 * @author kimchy (shay.banon)
 */
public class ChannelBufferStreamInput extends StreamInput {

    private final ChannelBuffer buffer;

    public ChannelBufferStreamInput(ChannelBuffer buffer) {
        this.buffer = buffer;
    }

    // Not really maps to InputStream, but good enough for us

    @Override public int read() throws IOException {
        return buffer.readByte() & 0xFF;
    }

    @Override public int read(byte[] b, int off, int len) throws IOException {
        readBytes(b, off, len);
        return len;
    }

    @Override public byte readByte() throws IOException {
        return buffer.readByte();
    }

    @Override public void readBytes(byte[] b, int offset, int len) throws IOException {
        buffer.readBytes(b, offset, len);
    }

    @Override public void reset() throws IOException {
        buffer.resetReaderIndex();
    }

    @Override public void close() throws IOException {
        // nothing to do here
    }
}
