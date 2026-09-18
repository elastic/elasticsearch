/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.spi;

import org.elasticsearch.rest.RestStatus;

/**
 * An external-source failure caused by the request rather than by the cluster: the data we were
 * pointed at cannot be read or decoded (corrupt/truncated/malformed file, an unsupported format
 * feature, a missing object). Maps to {@code 400 Bad Request}.
 * <p>
 * Unlike reading an index, failing to read an external resource is treated as a client-class error:
 * the caller chose the resource. Retryable transport problems are the exception — those are raised
 * as {@link ExternalUnavailableException} (503) instead.
 */
public final class ExternalClientException extends ExternalException {

    public ExternalClientException(String message, Throwable cause) {
        super(message, cause);
    }

    public ExternalClientException(Throwable cause, String message, Object... args) {
        super(cause, message, args);
    }

    public ExternalClientException(String message, Object... args) {
        super(message, args);
    }

    @Override
    public RestStatus status() {
        return RestStatus.BAD_REQUEST;
    }
}
