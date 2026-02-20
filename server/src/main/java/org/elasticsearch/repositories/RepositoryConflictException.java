/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.repositories;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;

/**
 * Repository conflict exception
 */
public class RepositoryConflictException extends RepositoryException {

    private static final TransportVersion REMOVE_REPOSITORY_CONFLICT_MESSAGE = TransportVersion.fromName(
        "remove_repository_conflict_message"
    );

    public RepositoryConflictException(String repository, String message) {
        super(repository, message);
    }

    @Override
    public RestStatus status() {
        return RestStatus.CONFLICT;
    }

    public RepositoryConflictException(StreamInput in) throws IOException {
        super(in);
        if (in.getTransportVersion().supports(REMOVE_REPOSITORY_CONFLICT_MESSAGE) == false) {
            // Deprecated `backwardCompatibleMessage` field
            in.readString();
        }
    }

    @Override
    protected void writeTo(StreamOutput out, Writer<Throwable> nestedExceptionsWriter) throws IOException {
        super.writeTo(out, nestedExceptionsWriter);
        if (out.getTransportVersion().supports(REMOVE_REPOSITORY_CONFLICT_MESSAGE) == false) {
            // Deprecated `backwardCompatibleMessage` field
            out.writeString("");
        }
    }
}
