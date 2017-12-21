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
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.nio.NioSocketChannel;
import org.elasticsearch.nio.SocketSelector;
import org.elasticsearch.nio.WriteContext;
import org.elasticsearch.nio.WriteOperation;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.function.BiConsumer;

public class TcpWriteContext implements WriteContext {

    private final NioSocketChannel channel;
    private final LinkedList<WriteOperation> queued = new LinkedList<>();

    public TcpWriteContext(NioSocketChannel channel) {
        this.channel = channel;
    }

    @Override
    public void sendMessage(Object message, BiConsumer<Void, Throwable> listener) {
        BytesReference reference = (BytesReference) message;
        if (channel.isWritable() == false) {
            listener.accept(null, new ClosedChannelException());
            return;
        }

        WriteOperation writeOperation = new WriteOperation(channel, toByteBuffers(reference), listener);
        SocketSelector selector = channel.getSelector();
        if (selector.isOnCurrentThread() == false) {
            selector.queueWrite(writeOperation);
            return;
        }

        // TODO: Eval if we will allow writes from sendMessage
        selector.queueWriteInChannelBuffer(writeOperation);
    }

    @Override
    public void queueWriteOperations(WriteOperation writeOperation) {
        assert channel.getSelector().isOnCurrentThread() : "Must be on selector thread to queue writes";
        queued.add(writeOperation);
    }

    @Override
    public void flushChannel() throws IOException {
        assert channel.getSelector().isOnCurrentThread() : "Must be on selector thread to flush writes";
        int ops = queued.size();
        if (ops == 1) {
            singleFlush(queued.pop());
        } else if (ops > 1) {
            multiFlush();
        }
    }

    @Override
    public boolean hasQueuedWriteOps() {
        assert channel.getSelector().isOnCurrentThread() : "Must be on selector thread to access queued writes";
        return queued.isEmpty() == false;
    }

    @Override
    public void clearQueuedWriteOps(Exception e) {
        assert channel.getSelector().isOnCurrentThread() : "Must be on selector thread to clear queued writes";
        for (WriteOperation op : queued) {
            channel.getSelector().executeFailedListener(op.getListener(), e);
        }
        queued.clear();
    }

    private void singleFlush(WriteOperation headOp) throws IOException {
        try {
            headOp.flush();
        } catch (IOException e) {
            channel.getSelector().executeFailedListener(headOp.getListener(), e);
            throw e;
        }

        if (headOp.isFullyFlushed()) {
            channel.getSelector().executeListener(headOp.getListener(), null);
        } else {
            queued.push(headOp);
        }
    }

    private void multiFlush() throws IOException {
        boolean lastOpCompleted = true;
        while (lastOpCompleted && queued.isEmpty() == false) {
            WriteOperation op = queued.pop();
            singleFlush(op);
            lastOpCompleted = op.isFullyFlushed();
        }
    }

        private static ByteBuffer[] toByteBuffers(BytesReference bytesReference) {
        BytesRefIterator byteRefIterator = bytesReference.iterator();
        BytesRef r;
        try {
            // Most network messages are composed of three buffers.
            ArrayList<ByteBuffer> buffers = new ArrayList<>(3);
            while ((r = byteRefIterator.next()) != null) {
                buffers.add(ByteBuffer.wrap(r.bytes, r.offset, r.length));
            }
            return buffers.toArray(new ByteBuffer[buffers.size()]);

        } catch (IOException e) {
            // this is really an error since we don't do IO in our bytesreferences
            throw new AssertionError("won't happen", e);
        }
    }
}
