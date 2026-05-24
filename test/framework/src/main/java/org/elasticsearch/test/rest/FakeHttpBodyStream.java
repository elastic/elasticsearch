/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.test.rest;

import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.http.HttpBody;

import java.util.ArrayList;
import java.util.List;

public class FakeHttpBodyStream implements HttpBody.Stream {
    private final List<ChunkHandler> tracingHandlers = new ArrayList<>();
    private ChunkHandler handler;
    private boolean requested;
    private boolean closed;

    public boolean isClosed() {
        return closed;
    }

    public boolean isRequested() {
        return requested;
    }

    @Override
    public ChunkHandler handler() {
        return handler;
    }

    @Override
    public void addTracingHandler(ChunkHandler chunkHandler) {
        tracingHandlers.add(chunkHandler);
    }

    @Override
    public void setHandler(ChunkHandler chunkHandler) {
        this.handler = chunkHandler;
    }

    @Override
    public void next() {
        if (closed) {
            return;
        }
        requested = true;
    }

    public void sendNext(ReleasableBytesReference chunk, boolean isLast) {
        if (requested) {
            for (var h : tracingHandlers) {
                h.onNext(chunk, isLast);
            }
            handler.onNext(chunk, isLast);
        } else {
            throw new IllegalStateException("chunk is not requested");
        }
    }

    @Override
    public void close() {
        if (closed == false) {
            closed = true;
            for (var h : tracingHandlers) {
                h.close();
            }
            if (handler != null) {
                handler.close();
            }
        }
    }
}
