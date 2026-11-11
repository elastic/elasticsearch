/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

/**
 * Thrown when a PUT data-source names a type no validator is registered for.
 * Distinct from a bare {@link IllegalArgumentException} so config-change telemetry
 * can attribute the refusal as {@code unknown_type}.
 */
public final class UnknownDataSourceTypeException extends IllegalArgumentException {
    public UnknownDataSourceTypeException(String type) {
        super("unknown data source type [" + type + "]");
    }
}
