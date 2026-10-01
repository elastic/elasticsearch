/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.spi;

/**
 * Thrown by the default implementations of {@link ExternalSourceFactory#testConnection} and
 * {@link StorageProviderFactory#testConnection} to signal that the data source type is registered
 * and valid, but has no connectivity probe. The caller ({@code DataSourceModule.testConnection})
 * catches this and returns {@code UNTESTABLE} rather than a hard error.
 *
 * <p>This is intentionally NOT a subclass of {@link IllegalArgumentException}. {@code IAE} is
 * reserved for "type not registered in any factory" (an HTTP 400 condition). An untestable type
 * is valid — it just cannot be probed.
 *
 * <p>An optional {@link #userReason()} string carries a user-visible explanation surfaced in the
 * API response {@code message} field. Leave it {@code null} (single-arg constructor) when the
 * untestable condition is the default "no probe" case — callers omit the field in that case.
 * Provide it when there is actionable guidance, e.g. "create a dataset to validate access."
 */
public class TestConnectionNotSupportedException extends RuntimeException {

    @org.elasticsearch.core.Nullable
    private final String userReason;

    public TestConnectionNotSupportedException(String message) {
        super(message);
        this.userReason = null;
    }

    /**
     * @param message   internal diagnostic message (not sent to the API caller)
     * @param userReason human-readable explanation surfaced in the {@code message} response field
     */
    public TestConnectionNotSupportedException(String message, String userReason) {
        super(message);
        this.userReason = userReason;
    }

    /** User-visible reason, or {@code null} when no additional explanation is available. */
    @org.elasticsearch.core.Nullable
    public String userReason() {
        return userReason;
    }
}
