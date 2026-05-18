/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.rest;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.common.recycler.Recycler;

import java.io.IOException;
import java.io.OutputStream;

public class LoggingChunkedRestResponseBodyPart implements ChunkedRestResponseBodyPart {

    private final ChunkedRestResponseBodyPart inner;
    private final OutputStream loggerStream;

    public LoggingChunkedRestResponseBodyPart(ChunkedRestResponseBodyPart inner, OutputStream loggerStream) {
        this.inner = inner;
        this.loggerStream = loggerStream;
    }

    @Override
    public boolean isPartComplete() {
        return inner.isPartComplete();
    }

    @Override
    public boolean isLastPart() {
        return inner.isLastPart();
    }

    @Override
    public void getNextPart(ActionListener<ChunkedRestResponseBodyPart> listener) {
        inner.getNextPart(listener.map(continuation -> new LoggingChunkedRestResponseBodyPart(continuation, loggerStream)));
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
