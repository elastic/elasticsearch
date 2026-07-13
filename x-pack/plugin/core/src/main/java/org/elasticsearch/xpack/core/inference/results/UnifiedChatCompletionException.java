/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.inference.results;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.common.xcontent.ChunkedToXContentHelper;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xcontent.ToXContent;

import java.util.Iterator;
import java.util.Locale;
import java.util.Objects;

import static java.util.Collections.emptyIterator;
import static org.elasticsearch.ExceptionsHelper.maybeError;
import static org.elasticsearch.common.collect.Iterators.concat;
import static org.elasticsearch.common.xcontent.ChunkedToXContentHelper.endObject;
import static org.elasticsearch.common.xcontent.ChunkedToXContentHelper.startObject;

public class UnifiedChatCompletionException extends XContentFormattedException {

    private static final Logger log = LogManager.getLogger(UnifiedChatCompletionException.class);
    private final String message;
    private final String type;
    @Nullable
    private final String code;
    @Nullable
    private final String param;

    public UnifiedChatCompletionException(RestStatus status, String message, String type, @Nullable String code) {
        this(status, message, type, code, null);
    }

    public UnifiedChatCompletionException(RestStatus status, String message, String type, @Nullable String code, @Nullable String param) {
        super(message, status);
        this.message = Objects.requireNonNull(message);
        this.type = Objects.requireNonNull(type);
        this.code = code;
        this.param = param;
    }

    public UnifiedChatCompletionException(
        Throwable cause,
        RestStatus status,
        String message,
        String type,
        @Nullable String code,
        @Nullable String param
    ) {
        super(message, cause, status);
        this.message = Objects.requireNonNull(message);
        this.type = Objects.requireNonNull(type);
        this.code = code;
        this.param = param;
    }

    @Override
    public Iterator<? extends ToXContent> toXContentChunked(Params params) {
        return concat(
            startObject(),
            startObject("error"),
            optionalField("code", code),
            field("message", message),
            optionalField("param", param),
            field("type", type),
            endObject(),
            endObject()
        );
    }

    private static Iterator<ToXContent> field(String key, String value) {
        return ChunkedToXContentHelper.chunk((b, p) -> b.field(key, value));
    }

    private static Iterator<ToXContent> optionalField(String key, String value) {
        return value != null ? ChunkedToXContentHelper.chunk((b, p) -> b.field(key, value)) : emptyIterator();
    }

    public static UnifiedChatCompletionException fromThrowable(Throwable t) {
        if (ExceptionsHelper.unwrapCause(t) instanceof UnifiedChatCompletionException e) {
            return e;
        } else {
            return maybeError(t).map(error -> {
                // we should never be throwing Error, but just in case we are, rethrow it on another thread so the JVM can handle it and
                // return a vague error to the user so that they at least see something went wrong but don't leak JVM details to users
                ExceptionsHelper.maybeDieOnAnotherThread(error);
                var e = new RuntimeException("Fatal error while streaming response. Please retry the request.");
                log.error(e.getMessage(), t);
                return new UnifiedChatCompletionException(
                    RestStatus.INTERNAL_SERVER_ERROR,
                    e.getMessage(),
                    getExceptionName(e),
                    RestStatus.INTERNAL_SERVER_ERROR.name().toLowerCase(Locale.ROOT)
                );
            }).orElseGet(() -> {
                log.atDebug().withThrowable(t).log("UnifiedChatCompletionException stack trace for debugging purposes.");
                var status = ExceptionsHelper.status(t);
                return new UnifiedChatCompletionException(
                    t,
                    status,
                    t.getMessage(),
                    getExceptionName(t),
                    status.name().toLowerCase(Locale.ROOT),
                    null
                );
            });
        }
    }
}
