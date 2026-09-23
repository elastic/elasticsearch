/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.spi;

import org.elasticsearch.rest.RestStatus;

/**
 * Session or temporary credentials used to talk to an external store have expired or been
 * rejected as invalid. Maps to {@code 400 Bad Request}: the query cannot succeed until the
 * caller refreshes the data source credentials. Distinct from a generic
 * {@link ExternalClientException} so the read path can abort sibling GETs and skip
 * prefetch-to-sync fallback — retrying the same expired token cannot succeed.
 */
public final class ExternalCredentialsExpiredException extends ExternalException {

    public ExternalCredentialsExpiredException(String message, Throwable cause) {
        super(message, cause);
    }

    public ExternalCredentialsExpiredException(Throwable cause, String message, Object... args) {
        super(cause, message, args);
    }

    public ExternalCredentialsExpiredException(String message, Object... args) {
        super(message, args);
    }

    @Override
    public RestStatus status() {
        return RestStatus.BAD_REQUEST;
    }
}
