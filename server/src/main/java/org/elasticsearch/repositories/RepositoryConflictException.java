/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.repositories;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;

import static org.elasticsearch.TransportVersions.REPO_CONFLICT_EXCEPTION_UNUSED_STRING;

/**
 * Repository conflict exception
 */
public class RepositoryConflictException extends RepositoryException {
    public RepositoryConflictException(String repository, String message) {
        super(repository, message);
    }

    @Override
    public RestStatus status() {
        return RestStatus.CONFLICT;
    }

    public RepositoryConflictException(StreamInput in) throws IOException {
        super(in);
        if (in.getTransportVersion().before(REPO_CONFLICT_EXCEPTION_UNUSED_STRING)) {
            in.readString();
        }
    }

    @Override
    protected void writeTo(StreamOutput out, Writer<Throwable> nestedExceptionsWriter) throws IOException {
        super.writeTo(out, nestedExceptionsWriter);
        if (out.getTransportVersion().before(REPO_CONFLICT_EXCEPTION_UNUSED_STRING)) {
            out.writeString("");
        }
    }
}
