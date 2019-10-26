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
import java.util.List;
import java.util.function.BiConsumer;

public abstract class DelegatingHandler implements NioChannelHandler {

    private NioChannelHandler delegate;

    public DelegatingHandler(NioChannelHandler delegate) {
        this.delegate = delegate;
    }

    @Override
    public void channelActive() {
        this.delegate.channelActive();
    }

    @Override
    public WriteOperation createWriteOperation(SocketChannelContext context, Object message, BiConsumer<Void, Exception> listener) {
        return delegate.createWriteOperation(context, message, listener);
    }

    @Override
    public List<FlushOperation> writeToBytes(WriteOperation writeOperation) {
        return delegate.writeToBytes(writeOperation);
    }

    @Override
    public List<FlushOperation> pollFlushOperations() {
        return delegate.pollFlushOperations();
    }

    @Override
    public int consumeReads(InboundChannelBuffer channelBuffer) throws IOException {
        return delegate.consumeReads(channelBuffer);
    }

    @Override
    public boolean closeNow() {
        return delegate.closeNow();
    }

    @Override
    public void close() throws IOException {
        delegate.close();
    }
}
