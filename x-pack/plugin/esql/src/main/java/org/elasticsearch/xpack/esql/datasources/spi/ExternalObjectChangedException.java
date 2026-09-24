/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.spi;

import org.elasticsearch.rest.RestStatus;

/**
 * The object being read was replaced with a different generation while the query was in flight.
 * Maps to {@code 503 Service Unavailable}: the caller's data was valid at query start, and a retry
 * of the query may land on a consistent generation. Distinct from
 * {@link ExternalUnavailableException} so the storage retry/resume layer does not loop the same
 * generation pin (If-Match / generationMatch) until the budget is exhausted.
 */
public final class ExternalObjectChangedException extends ExternalException {

    public ExternalObjectChangedException(String message, Throwable cause) {
        super(message, cause);
    }

    public ExternalObjectChangedException(Throwable cause, String message, Object... args) {
        super(cause, message, args);
    }

    public ExternalObjectChangedException(String message, Object... args) {
        super(message, args);
    }

    @Override
    public RestStatus status() {
        return RestStatus.SERVICE_UNAVAILABLE;
    }
}
