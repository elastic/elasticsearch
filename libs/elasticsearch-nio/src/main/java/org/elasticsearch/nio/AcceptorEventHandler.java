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
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * Event handler designed to handle events from server sockets
 */
public class AcceptorEventHandler extends EventHandler {

    private final Supplier<SocketSelector> selectorSupplier;

    public AcceptorEventHandler(Supplier<SocketSelector> selectorSupplier, Consumer<Exception> exceptionHandler) {
        super(exceptionHandler);
        this.selectorSupplier = selectorSupplier;
    }

    /**
     * This method is called when a NioServerSocketChannel is being registered with the selector. It should
     * only be called once per channel.
     *
     * @param context that was registered
     */
    protected void handleRegistration(ServerChannelContext context) throws IOException {
        context.register();
        SelectionKey selectionKey = context.getSelectionKey();
        selectionKey.attach(context);
        SelectionKeyUtils.setAcceptInterested(selectionKey);
    }

    /**
     * This method is called when an attempt to register a server channel throws an exception.
     *
     * @param context that was registered
     * @param exception that occurred
     */
    protected void registrationException(ServerChannelContext context, Exception exception) {
        context.handleException(exception);
    }

    /**
     * This method is called when a server channel signals it is ready to accept a connection. All of the
     * accept logic should occur in this call.
     *
     * @param context that can accept a connection
     */
    protected void acceptChannel(ServerChannelContext context) throws IOException {
        context.acceptChannels(selectorSupplier);
    }

    /**
     * This method is called when an attempt to accept a connection throws an exception.
     *
     * @param context that accepting a connection
     * @param exception that occurred
     */
    protected void acceptException(ServerChannelContext context, Exception exception) {
        context.handleException(exception);
    }
}
