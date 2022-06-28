/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.transport;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.threadpool.ThreadPool;

public class EmptyTransportResponseHandler implements TransportResponseHandler<TransportResponse.Empty> {

    public static final EmptyTransportResponseHandler INSTANCE_SAME = new EmptyTransportResponseHandler(ThreadPool.Names.SAME);

    private final String executor;

    public EmptyTransportResponseHandler(String executor) {
        this.executor = executor;
    }

    @Override
    public TransportResponse.Empty read(StreamInput in) {
        return TransportResponse.Empty.INSTANCE;
    }

    @Override
    public void handleResponse(TransportResponse.Empty response) {}

    @Override
    public void handleException(TransportException exp) {}

    @Override
    public String executor() {
        return executor;
    }
}
