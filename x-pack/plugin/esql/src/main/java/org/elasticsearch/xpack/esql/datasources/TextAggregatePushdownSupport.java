/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.datasources.pushdown.PushdownPredicates;
import org.elasticsearch.xpack.esql.datasources.spi.AggregatePushdownSupport;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Count;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Max;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Min;

import java.util.List;

/**
 * Aggregate-pushdown opt-in for the line-oriented text-format readers (CSV / TSV / NDJSON).
 * Supports {@code COUNT(*)}, {@code COUNT(<literal>)}, {@code COUNT(<column>)}, {@code MIN(<column>)},
 * {@code MAX(<column>)} when the column's ESQL data type has byte-stable comparison semantics
 * matching ES|QL's runtime ordering.
 * <p>
 * MIN/MAX-eligible types: BOOLEAN, INTEGER, LONG, DOUBLE, DATETIME, DATE_NANOS, KEYWORD, TEXT, IP.
 * Other types (VERSION uses semver order — not byte-lex; UNSIGNED_LONG — signed-byte serialization
 * mismatches the logical unsigned ordering) are rejected here to keep correctness ahead of breadth.
 * Virtual columns ({@code _file.*}, synthetic engine metadata) are rejected because the per-column
 * statistics layer cannot answer about them — same gate Parquet uses.
 */
public class TextAggregatePushdownSupport implements AggregatePushdownSupport {

    @Override
    public Pushability canPushAggregates(List<Expression> aggregates, List<Expression> groupings) {
        if (groupings.isEmpty() == false) {
            return Pushability.NO;
        }
        for (int i = 0; i < aggregates.size(); i++) {
            Expression agg = aggregates.get(i);
            if (agg instanceof Count count) {
                Expression field = count.field();
                // COUNT(*) / COUNT(<literal>) always answer from rowCount; COUNT(<col>) needs the
                // column's null count, which the per-column capture publishes for every tracked
                // attribute, but cannot derive for virtual columns (no underlying source value).
                if (field.foldable() == false && PushdownPredicates.isVirtualColumn(field)) {
                    return Pushability.NO;
                }
            } else if (agg instanceof Min min) {
                if (rejectMinMaxField(min.field())) {
                    return Pushability.NO;
                }
            } else if (agg instanceof Max max) {
                if (rejectMinMaxField(max.field())) {
                    return Pushability.NO;
                }
            } else {
                return Pushability.NO;
            }
        }
        return Pushability.YES;
    }

    /**
     * Text formats harvest per-column stats partially (none / count / projected / all scopes), so a
     * column missing from a split's stats means "not harvested," not "all-null." Returning {@code false}
     * makes the optimizer safe-miss {@code COUNT(<unharvested-col>)} rather than serving a wrong {@code 0}
     * under the footer-format implicit-nulls contract.
     */
    @Override
    public boolean appliesImplicitNullsForAbsentColumn() {
        return false;
    }

    private static boolean rejectMinMaxField(Expression field) {
        if (field instanceof Attribute attr) {
            if (PushdownPredicates.isVirtualColumn(attr)) {
                return true;
            }
            // Text formats can push MIN/MAX only for types the cold-scan ColumnStatsAccumulator harvests
            // (harvestable == text-pushable); the same ColumnStatTypeSupport table drives the accumulator's
            // classification, so the two cannot drift.
            ColumnStatTypeSupport support = ColumnStatTypeSupport.of(attr.dataType());
            return support == null || support.harvestable() == false;
        }
        return true;
    }
}
