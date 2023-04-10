/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.rest;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;

import static org.elasticsearch.transport.BytesRefRecycler.NON_RECYCLING_INSTANCE;

public class RestResponseUtils {
    private RestResponseUtils() {}

    public static BytesReference getBodyContent(RestResponse restResponse) {
        if (restResponse.isChunked() == false) {
            return restResponse.content();
        }

        final var chunkedRestResponseBody = restResponse.chunkedContent();
        assert chunkedRestResponseBody.isDone() == false;

        final int pageSize;
        try (var page = NON_RECYCLING_INSTANCE.obtain()) {
            pageSize = page.v().length;
        }

        try (var out = new BytesStreamOutput()) {
            while (chunkedRestResponseBody.isDone() == false) {
                try (var chunk = chunkedRestResponseBody.encodeChunk(pageSize, NON_RECYCLING_INSTANCE)) {
                    chunk.writeTo(out);
                }
            }

            out.flush();
            return out.bytes();
        } catch (Exception e) {
            throw new AssertionError("unexpected", e);
        }
    }
}
