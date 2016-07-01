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

package org.elasticsearch.transport;

import java.io.IOException;

/**
 * Wrapper around transport channel that delegates all requests to the
 * underlying channel
 */
public class DelegatingTransportChannel implements TransportChannel {

    private final TransportChannel channel;

    protected DelegatingTransportChannel(TransportChannel channel) {
        this.channel = channel;
    }

    @Override
    public String action() {
        return channel.action();
    }

    @Override
    public String getProfileName() {
        return channel.getProfileName();
    }

    @Override
    public long getRequestId() {
        return channel.getRequestId();
    }

    @Override
    public String getChannelType() {
        return channel.getChannelType();
    }

    @Override
    public void sendResponse(TransportResponse response) throws IOException {
        channel.sendResponse(response);
    }

    @Override
    public void sendResponse(TransportResponse response, TransportResponseOptions options) throws IOException {
        channel.sendResponse(response, options);
    }

    @Override
    public void sendResponse(Throwable error) throws IOException {
        channel.sendResponse(error);
    }

    public TransportChannel getChannel() {
        return channel;
    }
}
