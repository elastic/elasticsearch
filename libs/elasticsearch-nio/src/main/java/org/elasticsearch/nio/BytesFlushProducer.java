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

package org.elasticsearch.nio;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.util.LinkedList;
import java.util.function.BiConsumer;

public class BytesFlushProducer implements FlushProducer {

    private final LinkedList<FlushOperation> flushOperations = new LinkedList<>();
    private final SocketSelector selector;

    public BytesFlushProducer(SocketSelector selector) {
        this.selector = selector;
    }

    @Override
    public void produceWrites(WriteOperation writeOperation) {
        assert writeOperation instanceof FlushReadyWrite : "Write operation must be flush ready";
        flushOperations.addLast((FlushReadyWrite) writeOperation);
    }

    @Override
    public FlushOperation pollFlushOperation() {
        return flushOperations.pollFirst();
    }

    @Override
    public void close() throws IOException {
        for (FlushOperation flushOperation : flushOperations) {
            selector.executeFailedListener(flushOperation.getListener(), new ClosedChannelException());
        }
        flushOperations.clear();
    }

    @Override
    public WriteOperation createWriteOperation(SocketChannelContext channelContext, Object message,
                                               BiConsumer<Void, Throwable> listener) {
        if (message instanceof ByteBuffer[]) {
            return new FlushReadyWrite(channelContext, (ByteBuffer[]) message, listener);
        } else {
            throw new IllegalArgumentException("This channel only supports messages that are of type: ByteBuffer[]");
        }
    }
}
