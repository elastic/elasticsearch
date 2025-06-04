/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.inference.results;

import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.xcontent.ChunkedToXContent;
import org.elasticsearch.common.xcontent.ChunkedToXContentHelper;
import org.elasticsearch.core.RestApiVersion;
import org.elasticsearch.exception.ElasticsearchException;
import org.elasticsearch.exception.ElasticsearchWrapperException;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;

import java.util.Iterator;
import java.util.Objects;

/**
 * Similar to {@link ElasticsearchWrapperException}, this will wrap an Exception to generate an xContent using
 * {@link ElasticsearchException#generateFailureXContent(XContentBuilder, Params, Exception, boolean)}.
 * Extends {@link ElasticsearchException} to provide REST handlers the {@link #status()} method in order to set the response header.
 */
public class XContentFormattedException extends ElasticsearchException implements ChunkedToXContent {

    public static final String X_CONTENT_PARAM = "detailedErrorsEnabled";
    private final RestStatus status;
    private final Throwable cause;

    public XContentFormattedException(String message, RestStatus status) {
        super(message);
        this.status = Objects.requireNonNull(status);
        this.cause = null;
    }

    public XContentFormattedException(Throwable cause, RestStatus status) {
        super(cause);
        this.status = Objects.requireNonNull(status);
        this.cause = cause;
    }

    public XContentFormattedException(String message, Throwable cause, RestStatus status) {
        super(message, cause);
        this.status = Objects.requireNonNull(status);
        this.cause = cause;
    }

    @Override
    public RestStatus status() {
        return status;
    }

    @Override
    public Iterator<? extends ToXContent> toXContentChunked(Params params) {
        return Iterators.concat(
            ChunkedToXContentHelper.startObject(),
            Iterators.single(
                (b, p) -> ElasticsearchException.generateFailureXContent(
                    b,
                    p,
                    cause instanceof Exception e ? e : this,
                    params.paramAsBoolean(X_CONTENT_PARAM, false)
                )
            ),
            Iterators.single((b, p) -> b.field("status", status.getStatus())),
            ChunkedToXContentHelper.endObject()
        );
    }

    @Override
    public Iterator<? extends ToXContent> toXContentChunked(RestApiVersion restApiVersion, Params params) {
        return ChunkedToXContent.super.toXContentChunked(restApiVersion, params);
    }

    @Override
    public Iterator<? extends ToXContent> toXContentChunkedV8(Params params) {
        return ChunkedToXContent.super.toXContentChunkedV8(params);
    }

    @Override
    public boolean isFragment() {
        return super.isFragment();
    }
}
