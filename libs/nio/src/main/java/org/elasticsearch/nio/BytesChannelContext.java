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
import java.util.function.Consumer;

public class BytesChannelContext extends SocketChannelContext {

    public BytesChannelContext(NioSocketChannel channel, NioSelector selector, Config.Socket socketConfig,
                               Consumer<Exception> exceptionHandler, NioChannelHandler handler, InboundChannelBuffer channelBuffer) {
        super(channel, selector, socketConfig, exceptionHandler, handler, channelBuffer);
    }

    @Override
    public int read() throws IOException {
        int bytesRead = readFromChannel(channelBuffer);

        if (bytesRead == 0) {
            return 0;
        }

        handleReadBytes();

        return bytesRead;
    }

    @Override
    public void flushChannel() throws IOException {
        getSelector().assertOnSelectorThread();
        boolean lastOpCompleted = true;
        FlushOperation flushOperation;
        while (lastOpCompleted && (flushOperation = getPendingFlush()) != null) {
            try {
                if (singleFlush(flushOperation)) {
                    currentFlushOperationComplete();
                } else {
                    lastOpCompleted = false;
                }
            } catch (IOException e) {
                currentFlushOperationFailed(e);
                throw e;
            }
        }
    }

    @Override
    public void closeChannel() {
        if (isClosing.compareAndSet(false, true)) {
            getSelector().queueChannelClose(channel);
        }
    }

    @Override
    public boolean selectorShouldClose() {
        return closeNow() || isClosing.get();
    }

    /**
     * Returns a boolean indicating if the operation was fully flushed.
     */
    private boolean singleFlush(FlushOperation flushOperation) throws IOException {
        flushToChannel(flushOperation);
        return flushOperation.isFullyFlushed();
    }
}
