/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.telemetry;

import java.util.Locale;

/**
 * The API an ES|QL query came in on. The REST handlers set it on the request; a request built inside the cluster (for example by
 * an ML datafeed) keeps {@link #INTERNAL}.
 */
public enum QueryEntryPoint {
    QUERY,
    QUERY_ASYNC,
    QUERY_STREAM,
    PROMETHEUS_QUERY,
    PROMETHEUS_QUERY_RANGE,
    PROMETHEUS_LABELS,
    PROMETHEUS_LABEL_VALUES,
    PROMETHEUS_SERIES,
    PROMETHEUS_METADATA,
    INTERNAL;

    @Override
    public String toString() {
        return name().toLowerCase(Locale.ROOT);
    }
}
