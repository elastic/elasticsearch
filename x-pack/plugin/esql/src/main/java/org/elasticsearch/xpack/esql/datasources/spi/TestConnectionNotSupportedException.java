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
 */
public class TestConnectionNotSupportedException extends RuntimeException {

    public TestConnectionNotSupportedException(String message) {
        super(message);
    }
}
