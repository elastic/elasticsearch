/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
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
