/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.common.xcontent;

import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.ToXContentObject;

import java.util.Iterator;

/**
 * Chunked equivalent of {@link org.elasticsearch.xcontent.ToXContentObject} that serializes as a full object.
 */
public interface ChunkedToXContentObject extends ChunkedToXContent {

    @Override
    default boolean isFragment() {
        return false;
    }

    /**
     * Wraps the given instance in a {@link ToXContentObject} that will fully serialize the instance when serialized.
     *
     * @param chunkedToXContent instance to wrap
     * @return x-content instance
     */
    static ToXContentObject wrapAsToXContentObject(ChunkedToXContentObject chunkedToXContent) {
        return (builder, params) -> {
            Iterator<? extends ToXContent> serialization = chunkedToXContent.toXContentChunked(params);
            while (serialization.hasNext()) {
                serialization.next().toXContent(builder, params);
            }
            return builder;
        };
    }
}
