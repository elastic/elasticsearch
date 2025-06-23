/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.telemetry.tracing;

/**
 * Required methods from ThreadContext for Tracer
 */
public interface TraceContext {
    /**
     * Returns a transient header object or <code>null</code> if there is no header for the given key
     */
    <T> T getTransient(String key);

    /**
     * Puts a transient header object into this context
     */
    void putTransient(String key, Object value);

    /**
     * Returns the header for the given key or <code>null</code> if not present
     */
    String getHeader(String key);

    /**
     * Puts a header into the context
     */
    void putHeader(String key, String value);
}
