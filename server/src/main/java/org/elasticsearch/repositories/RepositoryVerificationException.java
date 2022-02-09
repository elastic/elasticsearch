/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.repositories;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;

/**
 * Repository verification exception
 */
public class RepositoryVerificationException extends RepositoryException {

    public RepositoryVerificationException(String repository, String msg) {
        super(repository, msg);
    }

    public RepositoryVerificationException(String repository, String msg, Throwable t) {
        super(repository, msg, t);
    }

    @Override
    public RestStatus status() {
        return RestStatus.INTERNAL_SERVER_ERROR;
    }

    public RepositoryVerificationException(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public synchronized Throwable fillInStackTrace() {
        // stack trace for a verification failure is uninteresting, the message has all the information we need
        return this;
    }
}
