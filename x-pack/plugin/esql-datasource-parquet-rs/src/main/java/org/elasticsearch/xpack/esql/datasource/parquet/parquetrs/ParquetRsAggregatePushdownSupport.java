/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.parquet.parquetrs;

import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.datasources.spi.AggregatePushdownSupport;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Count;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Max;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Min;

import java.util.List;

/**
 * Supports pushing ungrouped aggregates that can be computed from Parquet file-level
 * statistics (row count + per-column null count / min / max). Only forms whose value
 * is fully determined by those stats are advertised as pushable; everything else is
 * left for normal execution.
 *
 * <p>Pushable forms (must also have no {@code BY} groupings and no aggregate filter):
 * <ul>
 *   <li>{@code COUNT(*)} or {@code COUNT(<literal>)} (any foldable argument): answered
 *       from the row count.</li>
 *   <li>{@code COUNT(<attribute>)}: answered as {@code rowCount - nullCount(attribute)}.</li>
 *   <li>{@code MIN(<attribute>)}, {@code MAX(<attribute>)}: answered from per-column
 *       min/max statistics.</li>
 * </ul>
 *
 * <p>Anything else — e.g. {@code COUNT(a + b)}, {@code MIN(LENGTH(s))}, or any aggregate
 * with a {@code WHERE} filter — cannot be answered from file stats and is reported as
 * not pushable. The predicate intentionally mirrors what
 * {@code PushAggregatesToExternalSource.resolveFromStats} actually accepts; keep the two
 * in sync.
 */
public class ParquetRsAggregatePushdownSupport implements AggregatePushdownSupport {

    @Override
    public Pushability canPushAggregates(List<Expression> aggregates, List<Expression> groupings) {
        if (groupings.isEmpty() == false) {
            return Pushability.NO;
        }
        for (Expression agg : aggregates) {
            if (canPushSingle(agg) == false) {
                return Pushability.NO;
            }
        }
        return Pushability.YES;
    }

    private static boolean canPushSingle(Expression agg) {
        if (agg instanceof Count count) {
            if (count.hasFilter()) {
                return false;
            }
            Expression field = count.field();
            // COUNT(*) and COUNT(<literal>) -> total row count.
            // COUNT(<attribute>) -> rowCount - nullCount.
            // Anything else (e.g. COUNT(a + b)) cannot be answered from file stats.
            return field.foldable() || field instanceof Attribute;
        }
        if (agg instanceof Min min) {
            return min.hasFilter() == false && min.field() instanceof Attribute;
        }
        if (agg instanceof Max max) {
            return max.hasFilter() == false && max.field() instanceof Attribute;
        }
        return false;
    }
}
