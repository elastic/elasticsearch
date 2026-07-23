/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.oteldata.otlp.docbuilder;

public enum HistogramMapping {
    TDIGEST,
    /**
     * Indexes histograms using raw bucket boundaries rather than midpoint approximation.
     * Set via the {@code histogram:raw} mapping hint. See {@link MappingHints#HISTOGRAM_RAW}.
     */
    HISTOGRAM_RAW,
    EXPONENTIAL_HISTOGRAM,
    AGGREGATE_METRIC_DOUBLE
}
