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

    ExternalClientException(String message, Throwable cause) {
        super(message, cause);
    }

    ExternalClientException(Throwable cause, String message, Object... args) {
        super(cause, message, args);
    }

    ExternalClientException(String message, Object... args) {
        super(message, args);
    }

    public ExternalClientException(Condition condition, StoragePath path, String detailCode, String remedy, Throwable cause) {
        super(condition, path, detailCode, remedy, cause);
    }

    public ExternalClientException(Condition condition, StoragePath path, String detailCode, String remedy) {
        super(condition, path, detailCode, remedy);
    }

    /**
     * The refusal for a schema wider than {@code schema_max_fields}. A 400 rather than the breaker's 429: the limit is
     * configured, so retrying the same request cannot succeed, and {@code detail} says which setting to raise.
     */
    public static ExternalClientException schemaTooWide(String detail) {
        ExternalClientException e = new ExternalClientException(Condition.SCHEMA_TOO_WIDE, StoragePath.NONE, "", "");
        e.setDetail(detail);
        return e;
    }

    private ExternalClientException(ExternalClientException source) {
        super(source);
    }

    @Override
    protected ExternalClientException copyWithoutCause() {
        return new ExternalClientException(this);
    }

    @Override
    public RestStatus status() {
        return RestStatus.BAD_REQUEST;
    }
}
