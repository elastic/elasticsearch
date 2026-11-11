/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

/**
 * Thrown when a PUT would create a data source past {@code esql.data_sources.max_count}.
 * Distinct from a bare {@link IllegalArgumentException} so config-change telemetry
 * can attribute the refusal as {@code max_count}.
 */
public final class MaxDataSourcesCountException extends IllegalArgumentException {
    public MaxDataSourcesCountException(int max) {
        super("cannot add data source, the maximum number of data sources is reached: " + max);
    }
}
