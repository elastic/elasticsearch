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

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefIterator;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.transport.nio.channel.NioChannel;
import org.elasticsearch.transport.nio.channel.NioSocketChannel;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;

public class WriteOperation {

    private final NioSocketChannel channel;
    private final ActionListener<NioChannel> listener;
    private BytesReference reference;
    private ByteBuffer[] buffers;
    private long bytesRemaining = 0;

    public WriteOperation(NioSocketChannel channel, BytesReference reference, ActionListener<NioChannel> listener) {
        this.channel = channel;
        this.listener = listener;
        this.reference = reference;
        this.bytesRemaining = reference.length();
    }

    public ByteBuffer[] getBuffers() throws IOException {
        if (buffers == null) {
            ArrayList<ByteBuffer> buffers = new ArrayList<>(3);
            BytesRefIterator byteRefIterator = reference.iterator();
            BytesRef r;
            while ((r = byteRefIterator.next()) != null) {
                buffers.add(ByteBuffer.wrap(r.bytes, r.offset, r.length));
            }
            this.buffers = buffers.toArray(new ByteBuffer[buffers.size()]);
        }
        return buffers;
    }

    public ActionListener<NioChannel> getListener() {
        return listener;
    }

    public NioSocketChannel getChannel() {
        return channel;
    }

    public void decrementRemaining(long delta) {
        bytesRemaining -= delta;
    }

    public long bytesRemaining() {
        return bytesRemaining;
    }

}
