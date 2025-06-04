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
import org.elasticsearch.exception.ElasticsearchException;

import java.io.IOException;

/**
 * Generic repository exception
 */
public class RepositoryException extends ElasticsearchException {
    private final String repository;

    /**
     * Construct a <code>RepositoryException</code> with the specified detail message.
     *
     * The message can be parameterized using <code>{}</code> as placeholders for the given
     * arguments.
     *
     * @param repository the repository name
     * @param msg        the detail message
     * @param args       the arguments for the message
     */
    public RepositoryException(String repository, String msg, Object... args) {
        this(repository, msg, (Throwable) null, args);
    }

    /**
     * Construct a <code>RepositoryException</code> with the specified detail message
     * and nested exception.
     *
     * The message can be parameterized using <code>{}</code> as placeholders for the given
     * arguments.
     *
     * @param repository the repository name
     * @param msg        the detail message
     * @param cause      the nested exception
     * @param args       the arguments for the message
     */
    public RepositoryException(String repository, String msg, Throwable cause, Object... args) {
        super("[" + (repository == null ? "_na" : repository) + "] " + msg, cause, args);
        this.repository = repository;
    }

    /**
     * Returns repository name
     *
     * @return repository name
     */
    public String repository() {
        return repository;
    }

    public RepositoryException(StreamInput in) throws IOException {
        super(in);
        repository = in.readOptionalString();
    }

    @Override
    protected void writeTo(StreamOutput out, Writer<Throwable> nestedExceptionsWriter) throws IOException {
        super.writeTo(out, nestedExceptionsWriter);
        out.writeOptionalString(repository);
    }
}
