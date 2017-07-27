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

import org.apache.logging.log4j.Logger;
import org.elasticsearch.transport.nio.channel.CloseFuture;
import org.elasticsearch.transport.nio.channel.NioChannel;
import org.elasticsearch.transport.nio.channel.NioSocketChannel;

import java.io.IOException;
import java.nio.channels.Selector;

public abstract class EventHandler {

    protected final Logger logger;

    public EventHandler(Logger logger) {
        this.logger = logger;
    }

    /**
     * This method handles an IOException that was thrown during a call to {@link Selector#select(long)}.
     *
     * @param exception the exception
     */
    public void selectException(IOException exception) {
        logger.warn("io exception during select", exception);
    }

    /**
     * This method handles an IOException that was thrown during a call to {@link Selector#close()}.
     *
     * @param exception the exception
     */
    public void closeSelectorException(IOException exception) {
        logger.warn("io exception while closing selector", exception);
    }

    /**
     * This method handles an exception that was uncaught during a select loop.
     *
     * @param exception that was uncaught
     */
    public void uncaughtException(Exception exception) {
        Thread thread = Thread.currentThread();
        thread.getUncaughtExceptionHandler().uncaughtException(thread, exception);
    }

    /**
     * This method handles the closing of an NioChannel
     *
     * @param channel that should be closed
     */
    public void handleClose(NioChannel channel) {
        channel.closeFromSelector();
        CloseFuture closeFuture = channel.getCloseFuture();
        assert closeFuture.isDone() : "Should always be done as we are on the selector thread";
        IOException closeException = closeFuture.getCloseException();
        if (closeException != null) {
            logger.debug("exception while closing channel", closeException);
        }
    }
}
