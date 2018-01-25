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
import java.nio.channels.SelectionKey;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;

public abstract class AbstractChannelContext<S extends NioChannel<?>> implements ChannelContext {

    private final S channel;
    private final BiConsumer<S, Exception> exceptionHandler;
    private final CompletableFuture<Void> closeContext = new CompletableFuture<>();
    private volatile SelectionKey selectionKey;

    AbstractChannelContext(S channel, BiConsumer<S, Exception> exceptionHandler) {
        this.channel = channel;
        this.exceptionHandler = exceptionHandler;
    }

    public void register() throws IOException {
        setSelectionKey(channel.getRawChannel().register(getSelector().rawSelector(), 0));
    }

    public SelectionKey getSelectionKey() {
        return selectionKey;
    }

    // Protected for tests
    protected void setSelectionKey(SelectionKey selectionKey) {
        this.selectionKey = selectionKey;
    }

    @Override
    public S getChannel() {
        return channel;
    }

    /**
     * Closes the channel synchronously. This method should only be called from the selector thread.
     * <p>
     * Once this method returns, the channel will be closed.
     */
    @Override
    public void closeFromSelector() throws IOException {
        if (closeContext.isDone() == false) {
            try {
                channel.getRawChannel().close();
                closeContext.complete(null);
            } catch (IOException e) {
                closeContext.completeExceptionally(e);
                throw e;
            }
        }
    }

    @Override
    public void addCloseListener(BiConsumer<Void, Throwable> listener) {
        closeContext.whenComplete(listener);
    }

    @Override
    public boolean isOpen() {
        return closeContext.isDone() == false;
    }

    @Override
    public void handleException(Exception e) {
        exceptionHandler.accept(channel, e);
    }
}
