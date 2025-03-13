/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.xcontent;

import org.elasticsearch.core.RestApiVersion;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;

/**
 * An alternative to {@link ToXContent} allowing for progressive serialization by creating an {@link Iterator} of {@link ToXContent} chunks.
 * <p>
 * The REST layer only serializes enough chunks at once to keep an outbound buffer full, rather than consuming all the time and memory
 * needed to serialize the entire response as must be done with the regular {@link ToXContent} responses.
 */
public interface ChunkedToXContent {

    /**
     * Create an iterator of {@link ToXContent} chunks for a REST response for the given {@link RestApiVersion}. Each chunk is serialized
     * with the same {@link XContentBuilder} and {@link ToXContent.Params}, which is also the same as the {@link ToXContent.Params} passed
     * as the {@code params} argument. For best results, all chunks should be {@code O(1)} size. The last chunk in the iterator must always
     * yield at least one byte of output. See also {@link ChunkedToXContentHelper} for some handy utilities.
     * <p>
     * Note that chunked response bodies cannot send deprecation warning headers once transmission has started, so implementations must
     * check for deprecated feature use before returning.
     * <p>
     * By default, delegates to {@link #toXContentChunked} or {#toXContentChunkedV8}.
     *
     * @return iterator over chunks of {@link ToXContent}
     */
    default Iterator<? extends ToXContent> toXContentChunked(RestApiVersion restApiVersion, ToXContent.Params params) {
        return switch (restApiVersion) {
            case V_8 -> toXContentChunkedV8(params);
            case V_9 -> toXContentChunked(params);
        };
    }

    /**
     * Create an iterator of {@link ToXContent} chunks for a REST response. Each chunk is serialized with the same {@link XContentBuilder}
     * and {@link ToXContent.Params}, which is also the same as the {@link ToXContent.Params} passed as the {@code params} argument. For
     * best results, all chunks should be {@code O(1)} size. The last chunk in the iterator must always yield at least one byte of output.
     * See also {@link ChunkedToXContentHelper} for some handy utilities.
     * <p>
     * Note that chunked response bodies cannot send deprecation warning headers once transmission has started, so implementations must
     * check for deprecated feature use before returning.
     *
     * @return iterator over chunks of {@link ToXContent}
     */
    Iterator<? extends ToXContent> toXContentChunked(ToXContent.Params params);

    /**
     * Create an iterator of {@link ToXContent} chunks for a response to the {@link RestApiVersion#V_8} API. Each chunk is serialized with
     * the same {@link XContentBuilder} and {@link ToXContent.Params}, which is also the same as the {@link ToXContent.Params} passed as the
     * {@code params} argument. For best results, all chunks should be {@code O(1)} size. The last chunk in the iterator must always yield
     * at least one byte of output. See also {@link ChunkedToXContentHelper} for some handy utilities.
     * <p>
     * Similar to {@link #toXContentChunked} but for the {@link RestApiVersion#V_8} API. By default this method delegates to {@link
     * #toXContentChunked}.
     * <p>
     * Note that chunked response bodies cannot send deprecation warning headers once transmission has started, so implementations must
     * check for deprecated feature use before returning.
     *
     * @return iterator over chunks of {@link ToXContent}
     */
    default Iterator<? extends ToXContent> toXContentChunkedV8(ToXContent.Params params) {
        return toXContentChunked(params);
    }

    /**
     * Wraps the given instance in a {@link ToXContent} that will fully serialize the instance when serialized.
     *
     * @param chunkedToXContent instance to wrap
     * @return x-content instance
     */
    static ToXContent wrapAsToXContent(ChunkedToXContent chunkedToXContent) {
        return new ToXContent() {
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
        };
    }

    /**
     * @return true iff this instance serializes as a fragment. See {@link ToXContentObject} for additional details.
     */
    default boolean isFragment() {
        return true;
    }

    /**
     * A {@link ChunkedToXContent} that yields no chunks
     */
    ChunkedToXContent EMPTY = params -> Collections.emptyIterator();
}
