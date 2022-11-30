/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.rest;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.RecyclerBytesStreamOutput;
import org.elasticsearch.common.recycler.Recycler;

import java.io.IOException;

public class RestResponseUtils {
    private RestResponseUtils() {}

    private static final int PAGE_SIZE = 1 << 14;

    public static BytesReference getBodyContent(RestResponse restResponse) throws IOException {
        if (restResponse.isChunked() == false) {
            return restResponse.content();
        }

        final var recycler = new Recycler<BytesRef>() {
            @Override
            public V<BytesRef> obtain() {
                return new V<>() {
                    final BytesRef page = new BytesRef(new byte[PAGE_SIZE], 0, PAGE_SIZE);

                    @Override
                    public BytesRef v() {
                        return page;
                    }

                    @Override
                    public boolean isRecycled() {
                        return false;
                    }

                    @Override
                    public void close() {}
                };
            }
        };

        final var chunkedRestResponseBody = restResponse.chunkedContent();
        assert chunkedRestResponseBody.isDone() == false;

        try (var out = new RecyclerBytesStreamOutput(recycler)) {
            while (chunkedRestResponseBody.isDone() == false) {
                try (var chunk = chunkedRestResponseBody.encodeChunk(PAGE_SIZE, recycler)) {
                    chunk.writeTo(out);
                }
            }

            out.flush();
            return out.bytes();
        }
    }
}
