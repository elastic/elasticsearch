/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.http.netty4;

import io.netty.util.concurrent.PromiseCombiner;

import org.elasticsearch.rest.ChunkedRestResponseBodyPart;

final class Netty4ChunkedHttpContinuation implements Netty4HttpResponse {
    private final int sequence;
    private final ChunkedRestResponseBodyPart bodyPart;
    private final PromiseCombiner combiner;

    Netty4ChunkedHttpContinuation(int sequence, ChunkedRestResponseBodyPart bodyPart, PromiseCombiner combiner) {
        this.sequence = sequence;
        this.bodyPart = bodyPart;
        this.combiner = combiner;
    }

    @Override
    public int getSequence() {
        return sequence;
    }

    public ChunkedRestResponseBodyPart bodyPart() {
        return bodyPart;
    }

    public PromiseCombiner combiner() {
        return combiner;
    }
}
