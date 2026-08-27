/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.spi;

import org.elasticsearch.rest.RestStatus;

/**
 * An external-source failure caused by the cluster, not the request: a bug or a broken invariant in
 * our own reading code (an unexpected internal state, a code path that should be unreachable). Maps
 * to {@code 500 Internal Server Error} so the fault stays visible rather than being mislabeled as a
 * client error.
 */
public final class ExternalServerException extends ExternalException {

    public ExternalServerException(String message, Throwable cause) {
        super(message, cause);
    }

    public ExternalServerException(Throwable cause, String message, Object... args) {
        super(cause, message, args);
    }

    public ExternalServerException(String message, Object... args) {
        super(message, args);
    }

    @Override
    public RestStatus status() {
        return RestStatus.INTERNAL_SERVER_ERROR;
    }
}
