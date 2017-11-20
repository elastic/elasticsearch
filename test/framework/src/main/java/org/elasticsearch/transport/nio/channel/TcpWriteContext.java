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
import java.nio.channels.ClosedChannelException;
import java.util.LinkedList;

public class TcpWriteContext implements WriteContext {

    private final NioSocketChannel channel;
    private final LinkedList<WriteOperation> queued = new LinkedList<>();

    public TcpWriteContext(NioSocketChannel channel) {
        this.channel = channel;
    }

    @Override
    public void sendMessage(BytesReference reference, ActionListener<Void> listener) {
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
            op.getListener().onFailure(e);
        }
        queued.clear();
    }

    private void singleFlush(WriteOperation headOp) throws IOException {
        try {
            headOp.flush();
        } catch (IOException e) {
            headOp.getListener().onFailure(e);
            throw e;
        }

        if (headOp.isFullyFlushed()) {
            headOp.getListener().onResponse(null);
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
}
