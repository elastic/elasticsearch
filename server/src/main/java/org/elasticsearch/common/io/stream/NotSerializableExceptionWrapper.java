/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.io.stream;

import org.elasticsearch.exception.ElasticsearchException;
import org.elasticsearch.exception.ExceptionsHelper;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;

/**
 * This exception can be used to wrap a given, not serializable exception
 * to serialize via {@link StreamOutput#writeException(Throwable)}.
 * This class will preserve the stacktrace as well as the suppressed exceptions of
 * the throwable it was created with instead of it's own. The stacktrace has no indication
 * of where this exception was created.
 */
public final class NotSerializableExceptionWrapper extends ElasticsearchException {

    private final String name;
    private final RestStatus status;

    public NotSerializableExceptionWrapper(Throwable other) {
        super(ElasticsearchException.getExceptionName(other) + ": " + other.getMessage(), other.getCause());
        this.name = ElasticsearchException.getExceptionName(other);
        this.status = ExceptionsHelper.status(other);
        setStackTrace(other.getStackTrace());
        for (Throwable otherSuppressed : other.getSuppressed()) {
            addSuppressed(otherSuppressed);
        }
        if (other instanceof ElasticsearchException ex) {
            for (String key : ex.getHeaderKeys()) {
                this.addHeader(key, ex.getHeader(key));
            }
            for (String key : ex.getMetadataKeys()) {
                this.addMetadata(key, ex.getMetadata(key));
            }
        }
    }

    public NotSerializableExceptionWrapper(StreamInput in) throws IOException {
        super(in);
        name = in.readString();
        status = RestStatus.readFrom(in);
    }

    @Override
    protected void writeTo(StreamOutput out, Writer<Throwable> nestedExceptionsWriter) throws IOException {
        super.writeTo(out, nestedExceptionsWriter);
        out.writeString(name);
        RestStatus.writeTo(out, status);
    }

    @Override
    public String getExceptionName() {
        return name;
    }

    @Override
    public RestStatus status() {
        return status;
    }
}
