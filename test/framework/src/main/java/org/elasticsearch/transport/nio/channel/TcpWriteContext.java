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

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.transport.nio.SocketSelector;
import org.elasticsearch.transport.nio.WriteOperation;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.util.LinkedList;

public class TcpWriteContext implements WriteContext {

    private final NioSocketChannel channel;
    private final LinkedList<WriteOperation> queued = new LinkedList<>();

    public TcpWriteContext(NioSocketChannel channel) {
        this.channel = channel;
    }

    @Override
    public void sendMessage(BytesReference reference, ActionListener<NioChannel> listener) {
        if (channel.isWritable() == false) {
            listener.onFailure(new ClosedChannelException());
            return;
        }

        WriteOperation writeOperation = new WriteOperation(channel, reference, listener);
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
        if (queued.size() == 1) {
            singleFlush(queued.pop());
        } else {
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
            op.getListener().onFailure(e);
        }
        queued.clear();
    }

    private void singleFlush(WriteOperation headOp) throws IOException {
        ByteBuffer[] buffers = headOp.getBuffers();
        long remaining = headOp.bytesRemaining();
        long written;
        if (buffers.length == 1) {
            written = channel.write(buffers[0]);
        } else {
            written = channel.vectorizedWrite(buffers);
        }

        if (written < remaining) {
            headOp.decrementRemaining(written);
            queued.push(headOp);
        } else {
            headOp.getListener().onResponse(channel);
        }
    }

    private void multiFlush() throws IOException {
        int writeCount = queued.size();
        int bufferCount = 0;
        long totalBytes = 0;
        WriteOperation[] ops = new WriteOperation[writeCount];
        for (int i = 0; i < writeCount; ++i) {
            WriteOperation op = queued.pop();
            totalBytes += op.bytesRemaining();
            ops[i] = op;
            bufferCount += op.getBuffers().length;
        }

        ByteBuffer[] buffers = new ByteBuffer[bufferCount];
        int j = 0;
        for (int i = 0; i < writeCount; ++i) {
            WriteOperation op = ops[i];
            for (ByteBuffer buffer : op.getBuffers()) {
                buffers[j] = buffer;
                ++j;
            }
        }

        long bytesWritten = channel.vectorizedWrite(buffers);

        long remainingBytes = bytesWritten;
        if (totalBytes != bytesWritten) {
            for (WriteOperation operation : ops) {
                long operationBytesRemaining = operation.bytesRemaining();
                if (remainingBytes < operationBytesRemaining) {
                    operation.decrementRemaining(remainingBytes);
                    queued.add(operation);
                    remainingBytes -= remainingBytes;
                } else {
                    operation.getListener().onResponse(channel);
                    remainingBytes -= operationBytesRemaining;
                }
            }
        } else {
            for (WriteOperation operation : ops) {
                operation.getListener().onResponse(channel);
            }
        }
    }
}
