/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.rest;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.common.recycler.Recycler;

import java.io.IOException;
import java.io.OutputStream;

public class LoggingChunkedRestResponseBody implements ChunkedRestResponseBody {

    private final ChunkedRestResponseBody inner;
    private final OutputStream loggerStream;

    public LoggingChunkedRestResponseBody(ChunkedRestResponseBody inner, OutputStream loggerStream) {
        this.inner = inner;
        this.loggerStream = loggerStream;
    }

    @Override
    public boolean isDone() {
        return inner.isDone();
    }

    @Override
    public ReleasableBytesReference encodeChunk(int sizeHint, Recycler<BytesRef> recycler) throws IOException {
        var chunk = inner.encodeChunk(sizeHint, recycler);
        try {
            chunk.writeTo(loggerStream);
        } catch (Exception e) {
            assert false : e; // nothing really to go wrong here
        }

        return chunk;
    }

    @Override
    public String getResponseContentTypeString() {
        return inner.getResponseContentTypeString();
    }
}
