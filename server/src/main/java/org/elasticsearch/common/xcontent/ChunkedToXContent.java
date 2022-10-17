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
import org.elasticsearch.xcontent.XContentBuilder;

import java.util.Iterator;

/**
 * An extension of {@link ToXContent} that can be serialized in chunks by creating an {@link Iterator<ToXContent>}.
 * This is used by the REST layer to implement flow control that does not rely on blocking the serializing thread when writing the
 * serialized bytes to a non-blocking channel.
 */
public interface ChunkedToXContent {

    /**
     * Create an iterator of {@link ToXContent} chunks, that must be serialized individually with the same {@link XContentBuilder} and
     * {@link ToXContent.Params} for each call until it is fully drained.
     * @return iterator over chunks of {@link ToXContent}
     */
    Iterator<? extends ToXContent> toXContentChunked();

    /**
     * Wraps the given instance in a {@link ToXContentObject} that will fully serialize the instance when serialized.
     * @param chunkedToXContent instance to wrap
     * @return x-content object
     */
    static ToXContentObject wrapAsXContentObject(ChunkedToXContent chunkedToXContent) {
        return (builder, params) -> {
            Iterator<? extends ToXContent> serialization = chunkedToXContent.toXContentChunked();
            while (serialization.hasNext()) {
                serialization.next().toXContent(builder, params);
            }
            return builder;
        };
    }
}
