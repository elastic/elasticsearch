/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.xcontent;

import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Iterator;

/**
 * Adapts a {@link ChunkedToXContent} to {@link ToXContent}.
 */
record ToXContentWrapper(ChunkedToXContent chunkedToXContent) implements ToXContent {
    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        Iterator<? extends ToXContent> serialization = chunkedToXContent.toXContentChunked(params);
        while (serialization.hasNext()) {
            serialization.next().toXContent(builder, params);
        }
        return builder;
    }

    @Override
    public boolean isFragment() {
        return chunkedToXContent.isFragment();
    }
}
