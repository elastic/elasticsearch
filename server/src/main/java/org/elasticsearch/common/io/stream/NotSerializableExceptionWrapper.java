/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.io.stream;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ExceptionsHelper;
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
        if (other instanceof ElasticsearchException) {
            ElasticsearchException ex = (ElasticsearchException) other;
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
    protected String getExceptionName() {
        return name;
    }

    @Override
    public RestStatus status() {
        return status;
    }
}
