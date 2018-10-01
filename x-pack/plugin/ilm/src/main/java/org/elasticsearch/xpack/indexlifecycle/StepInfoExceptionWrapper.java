/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.indexlifecycle;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

/**
 * Wraps an Exception so that it can be passed to things which require an
 * {@link org.elasticsearch.common.xcontent.ToXContentObject}.
 */
public class StepInfoExceptionWrapper implements ToXContentObject {
    private final Throwable exception;

    public StepInfoExceptionWrapper(Throwable exception) {
        this.exception = Objects.requireNonNull(exception);
    }

    Class<? extends Throwable> getExceptionType() {
        return exception.getClass();
    }

    String getMessage() {
        return exception.getMessage();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        ElasticsearchException.generateThrowableXContent(builder, params, exception);
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        StepInfoExceptionWrapper that = (StepInfoExceptionWrapper) o;
        return Objects.equals(exception.getMessage(), that.exception.getMessage())
            && Objects.equals(exception.getClass(), that.exception.getClass());
    }

    @Override
    public int hashCode() {
        return Objects.hash(exception.getClass(), exception.getMessage());
    }
}
