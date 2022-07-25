/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.xcontent;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;

public interface ChunkedToXContent extends ToXContent {

    ChunkedXContentSerialization toXContentChunked(XContentBuilder builder, ToXContent.Params params);

    @Override
    default XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        ChunkedXContentSerialization serialization = toXContentChunked(builder, params);
        XContentBuilder b = null;
        while (b == null) {
            b = serialization.encodeChunk();
        }
        return b;
    }

    interface ChunkedXContentSerialization {

        @Nullable
        XContentBuilder encodeChunk() throws IOException;
    }
}
