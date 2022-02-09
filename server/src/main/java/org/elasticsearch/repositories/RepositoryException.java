/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.repositories;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

/**
 * Generic repository exception
 */
public class RepositoryException extends ElasticsearchException {
    private final String repository;

    public RepositoryException(String repository, String msg) {
        this(repository, msg, null);
    }

    public RepositoryException(String repository, String msg, Throwable cause) {
        super("[" + (repository == null ? "_na" : repository) + "] " + msg, cause);
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
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeOptionalString(repository);
    }
}
