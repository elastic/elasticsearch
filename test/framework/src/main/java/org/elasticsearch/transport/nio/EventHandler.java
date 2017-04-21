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
import org.elasticsearch.transport.nio.channel.NioChannel;

import java.io.IOException;

public abstract class EventHandler {

    protected final Logger logger;

    public EventHandler(Logger logger) {
        this.logger = logger;
    }

    public void selectException(IOException e) {
        logger.warn("io exception during select", e);
    }

    public void uncaughtException(Exception e) {
        logger.error("unexpected exception during select", e);
        // TODO: maybe add settings to die on unexpected?
    }

    public void handleClose(NioChannel channel) throws IOException {
        CloseFuture closeFuture = channel.close();
        assert closeFuture.isDone() : "Should always be done as we are on the selector thread";
        IOException closeException = closeFuture.getCloseException();
        if (closeException != null) {
            throw closeException;
        }
    }

    public void closeException(NioChannel channel, Exception e) {
        logger.trace("exception while closing channel", e);
    }
}
