/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.http.netty4;

import io.netty.util.concurrent.PromiseCombiner;

import org.elasticsearch.rest.ChunkedRestResponseBody;

final class Netty4ChunkedHttpContinuation implements Netty4HttpResponse {
    private final int sequence;
    private final ChunkedRestResponseBody body;
    private final PromiseCombiner combiner;

    Netty4ChunkedHttpContinuation(int sequence, ChunkedRestResponseBody body, PromiseCombiner combiner) {
        this.sequence = sequence;
        this.body = body;
        this.combiner = combiner;
    }

    @Override
    public int getSequence() {
        return sequence;
    }

    public ChunkedRestResponseBody body() {
        return body;
    }

    public PromiseCombiner combiner() {
        return combiner;
    }
}
