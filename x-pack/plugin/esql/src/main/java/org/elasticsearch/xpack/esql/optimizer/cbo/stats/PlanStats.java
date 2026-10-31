/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.cbo.stats;

import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.NameId;

import java.util.Map;
import java.util.Objects;

/** Row, width, byte, and column estimates for one logical plan node. */
public record PlanStats(Estimate rows, Estimate avgRowBytes, Map<NameId, ColumnStats> columns, Estimate bytes) {
    public static final PlanStats UNKNOWN = new PlanStats(Estimate.UNKNOWN, Estimate.UNKNOWN, Map.of(), Estimate.UNKNOWN);

    public PlanStats(Estimate rows, Estimate avgRowBytes, Map<NameId, ColumnStats> columns) {
        this(rows, avgRowBytes, columns, rows.multiply(avgRowBytes));
    }

    public PlanStats {
        Objects.requireNonNull(rows);
        Objects.requireNonNull(avgRowBytes);
        Objects.requireNonNull(columns);
        Objects.requireNonNull(bytes);
        columns = Map.copyOf(columns);
    }

    /** Returns column statistics by stable attribute id, or unknown statistics when absent. */
    public ColumnStats column(Attribute attribute) {
        return columns.getOrDefault(attribute.id(), ColumnStats.UNKNOWN);
    }
}
