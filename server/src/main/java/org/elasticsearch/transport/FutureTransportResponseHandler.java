/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.transport;

import org.elasticsearch.threadpool.ThreadPool;

/**
 * A response handler to be used when all interaction will be done through the {@link TransportFuture}.
 */
public abstract class FutureTransportResponseHandler<T extends TransportResponse> implements TransportResponseHandler<T> {

    @Override
    public void handleResponse(T response) {}

    @Override
    public void handleException(TransportException exp) {}

    @Override
    public String executor() {
        return ThreadPool.Names.SAME;
    }
}
