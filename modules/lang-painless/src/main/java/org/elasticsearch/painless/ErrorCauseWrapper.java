/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.painless;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;

/**
 * A wrapper for Error which hides the underlying Error from the exception cause chain.
 * <p>
 * Only errors which should be sandboxed and not cause the node to crash are wrapped.
 */
class ErrorCauseWrapper extends ElasticsearchException {

    private static final List<Class<? extends Error>> wrappedErrors = List.of(
        PainlessError.class,
        OutOfMemoryError.class,
        StackOverflowError.class,
        LinkageError.class
    );

    final Throwable realCause;

    private ErrorCauseWrapper(Throwable realCause) {
        super(realCause.getMessage());
        this.realCause = realCause;
    }

    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field("type", getExceptionName(realCause));
        builder.field("reason", realCause.getMessage());
        return builder;
    }

    static Throwable maybeWrap(Throwable t) {
        if (wrappedErrors.contains(t.getClass())) {
            return new ErrorCauseWrapper(t);
        }
        return t;
    }
}
