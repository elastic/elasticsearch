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

import org.elasticsearch.nio.FlushOperation;
import org.elasticsearch.nio.SocketChannelContext;
import org.elasticsearch.nio.SocketSelector;
import org.elasticsearch.nio.WriteOperation;

import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.util.LinkedList;

public class BytesFlushProducer implements SocketChannelContext.FlushProducer {

    private final LinkedList<FlushOperation> flushOperations = new LinkedList<>();
    private final SocketSelector selector;

    public BytesFlushProducer(SocketSelector selector) {
        this.selector = selector;
    }

    @Override
    public void produceWrites(WriteOperation writeOperation) {
        flushOperations.addLast(new FlushOperation((ByteBuffer[]) writeOperation.getObject(), writeOperation.getListener()));
    }

    @Override
    public FlushOperation pollFlushOperation() {
        return flushOperations.pollFirst();
    }

    @Override
    public void close() {
        for (FlushOperation flushOperation : flushOperations) {
            selector.executeFailedListener(flushOperation.getListener(), new ClosedChannelException());
        }
        flushOperations.clear();
    }
}
