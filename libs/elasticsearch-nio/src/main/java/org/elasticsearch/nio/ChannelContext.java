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

public interface ChannelContext {
    /**
     * This method cleans up any context resources that need to be released when a channel is closed. It
     * should only be called by the selector thread.
     *
     * @throws IOException during channel / context close
     */
    void closeFromSelector() throws IOException;

    /**
     * Schedules a channel to be closed by the selector event loop with which it is registered.
     *
     * If the channel is open and the state can be transitioned to closed, the close operation will
     * be scheduled with the event loop.
     *
     * Depending on the underlying protocol of the channel, a close operation might simply close the socket
     * channel or may involve reading and writing messages.
     */
    void closeChannel();

    void handleException(Exception e);
}
