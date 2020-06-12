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

package org.elasticsearch.http.nio;

import org.elasticsearch.http.HttpPipelinedResponse;
import org.elasticsearch.nio.SocketChannelContext;
import org.elasticsearch.nio.WriteOperation;

import java.util.function.BiConsumer;

public class HttpWriteOperation implements WriteOperation {

    private final SocketChannelContext channelContext;
    private final HttpPipelinedResponse response;
    private final BiConsumer<Void, Exception> listener;

    HttpWriteOperation(SocketChannelContext channelContext, HttpPipelinedResponse response, BiConsumer<Void, Exception> listener) {
        this.channelContext = channelContext;
        this.response = response;
        this.listener = listener;
    }

    @Override
    public BiConsumer<Void, Exception> getListener() {
        return listener;
    }

    @Override
    public SocketChannelContext getChannel() {
        return channelContext;
    }

    @Override
    public HttpPipelinedResponse getObject() {
        return response;
    }
}
