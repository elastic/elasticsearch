/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.spi;

import org.elasticsearch.xpack.esql.core.expression.Expression;

import java.util.List;

/**
 * SPI for declaring that a format can compute aggregates from file-level statistics.
 * Implementations are queried by the optimizer to determine if aggregates can be
 * pushed down. The actual computation happens via file footer reads at execution time.
 */
public interface AggregatePushdownSupport {

    enum Pushability {
        YES,
        NO
    }

    Pushability canPushAggregates(List<Expression> aggregates, List<Expression> groupings);

    /**
     * Whether a column that is absent from a split's per-column statistics can be treated as
     * all-null — the "implicit nulls" contract that makes {@code COUNT(col) = rowCount - columnNullCount}
     * correct without a "column present?" probe.
     * <p>
     * Footer formats (Parquet, ORC) emit one column-family stat for every column they physically
     * contain, so an absent column genuinely means all-null and this returns {@code true} (the
     * default). Line-oriented text formats (CSV / TSV / NDJSON) harvest per-column stats partially —
     * a {@code count}-scope scan harvests no columns, a {@code projected}-scope scan only the query's
     * projected ones — so an absent column key means "not harvested," NOT "all-null." Those formats
     * override this to {@code false} so the optimizer safe-misses {@code COUNT(col)} for an unharvested
     * column (re-scan) instead of serving a wrong {@code 0}.
     */
    default boolean appliesImplicitNullsForAbsentColumn() {
        return true;
    }

    AggregatePushdownSupport UNSUPPORTED = (aggregates, groupings) -> Pushability.NO;
}
