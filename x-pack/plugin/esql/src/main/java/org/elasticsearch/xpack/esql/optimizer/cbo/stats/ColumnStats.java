/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.cbo.stats;

import java.util.Objects;
import java.util.Optional;

/** Statistics used to estimate grouping, filtering, and the cost of carrying one column. */
public record ColumnStats(
    Estimate ndv,
    Estimate nullFraction,
    Optional<Object> minValue,
    Optional<Object> maxValue,
    Estimate averageBytes,
    Estimate averageValuesPerRow
) {
    public static final ColumnStats UNKNOWN = new ColumnStats(
        Estimate.UNKNOWN,
        Estimate.UNKNOWN,
        Optional.empty(),
        Optional.empty(),
        Estimate.UNKNOWN,
        Estimate.UNKNOWN
    );

    public ColumnStats {
        Objects.requireNonNull(ndv);
        Objects.requireNonNull(nullFraction);
        Objects.requireNonNull(minValue);
        Objects.requireNonNull(maxValue);
        Objects.requireNonNull(averageBytes);
        Objects.requireNonNull(averageValuesPerRow);
    }
}
