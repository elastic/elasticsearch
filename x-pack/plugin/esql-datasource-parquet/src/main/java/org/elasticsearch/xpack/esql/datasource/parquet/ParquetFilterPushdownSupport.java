/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.parquet;

import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.datasources.pushdown.PushdownPredicates;
import org.elasticsearch.xpack.esql.datasources.spi.FilterPushdownSupport;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.StartsWith;
import org.elasticsearch.xpack.esql.expression.predicate.Range;
import org.elasticsearch.xpack.esql.expression.predicate.logical.And;
import org.elasticsearch.xpack.esql.expression.predicate.logical.Not;
import org.elasticsearch.xpack.esql.expression.predicate.logical.Or;
import org.elasticsearch.xpack.esql.expression.predicate.nulls.IsNotNull;
import org.elasticsearch.xpack.esql.expression.predicate.nulls.IsNull;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.Equals;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.EsqlBinaryComparison;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.In;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.NotEquals;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;

import static org.elasticsearch.xpack.esql.expression.Foldables.literalValueOf;

/**
 * Parquet-specific filter pushdown support that validates ESQL filter expressions
 * for pushdown eligibility and collects them into a {@link ParquetPushedExpressions}
 * wrapper for deferred translation at read time.
 * <p>
 * Translation to Parquet {@link org.apache.parquet.filter2.predicate.FilterPredicate}
 * is deferred because DATETIME columns can have different physical representations
 * across files (INT32 DATE, INT64 TIMESTAMP_MILLIS/MICROS/NANOS, INT96). The actual
 * file schema is needed for correct value conversion.
 * <p>
 * When set on {@link org.apache.parquet.ParquetReadOptions}, parquet-java automatically
 * applies four levels of filtering:
 * <ol>
 *   <li>Statistics (min/max) — skips row groups where value is outside range</li>
 *   <li>Dictionary — skips row groups where dictionary-encoded column doesn't contain value</li>
 *   <li>Bloom filter — skips row groups where bloom filter says value definitely absent</li>
 *   <li>Page index (ColumnIndex/OffsetIndex) — skips individual pages within row groups using
 *       per-page min/max statistics (active by default via {@code useColumnIndexFilter=true}
 *       since parquet-mr 1.12.0)</li>
 * </ol>
 * <p>
 * All pushed filters use {@link Pushability#RECHECK} semantics: the original filter remains
 * in FilterExec for per-row correctness since predicate pushdown is a conservative approximation.
 */
public class ParquetFilterPushdownSupport implements FilterPushdownSupport {

    private static final Logger logger = LogManager.getLogger(ParquetFilterPushdownSupport.class);

    static final Predicate<DataType> TYPE_SUPPORTED = dt -> dt == DataType.INTEGER
        || dt == DataType.LONG
        || dt == DataType.DOUBLE
        || dt == DataType.KEYWORD
        || dt == DataType.BOOLEAN
        || dt == DataType.DATETIME;

    @Override
    public PushdownResult pushFilters(List<Expression> filters) {
        List<Expression> pushed = new ArrayList<>();
        List<Expression> remainder = new ArrayList<>(filters);

        for (Expression filter : filters) {
            if (canConvert(filter)) {
                pushed.add(filter);
            }
        }

        if (pushed.isEmpty()) {
            return PushdownResult.none(filters);
        }

        logger.debug("Parquet filter pushdown: validated {} of {} expressions for pushdown", pushed.size(), filters.size());
        return new PushdownResult(new ParquetPushedExpressions(pushed), pushed, remainder);
    }

    @Override
    public Pushability canPush(Expression expr) {
        if (canConvert(expr)) {
            return Pushability.RECHECK;
        }
        return Pushability.NO;
    }

    /**
     * Validates whether an expression can be converted to a Parquet FilterPredicate.
     * For AND, partial pushdown is safe (at least one side convertible).
     * For OR and NOT, all children must be convertible.
     */
    static boolean canConvert(Expression expr) {
        if (expr instanceof EsqlBinaryComparison bc) {
            if (PushdownPredicates.isComparison(bc, TYPE_SUPPORTED) == false) {
                return false;
            }
            // SQL null comparisons are always UNKNOWN — only IsNull/IsNotNull handle nulls
            if (bc.right().foldable() && literalValueOf(bc.right()) == null) {
                return false;
            }
            // BooleanColumn doesn't implement SupportsLtGt — only eq/notEq are valid
            if (bc.left() instanceof NamedExpression ne && ne.dataType() == DataType.BOOLEAN) {
                return bc instanceof Equals || bc instanceof NotEquals;
            }
            return true;
        }
        if (expr instanceof In inExpr) {
            return PushdownPredicates.isIn(inExpr, TYPE_SUPPORTED);
        }
        if (expr instanceof IsNull isNull) {
            return PushdownPredicates.isIsNull(isNull, TYPE_SUPPORTED);
        }
        if (expr instanceof IsNotNull isNotNull) {
            return PushdownPredicates.isIsNotNull(isNotNull, TYPE_SUPPORTED);
        }
        if (expr instanceof Range range) {
            // BooleanColumn doesn't implement SupportsLtGt — Range requires ordered comparisons
            if (range.value() instanceof NamedExpression ne && ne.dataType() == DataType.BOOLEAN) {
                return false;
            }
            return PushdownPredicates.isRange(range, TYPE_SUPPORTED);
        }
        if (expr instanceof And and) {
            return canConvert(and.left()) || canConvert(and.right());
        }
        if (expr instanceof Or or) {
            return canConvert(or.left()) && canConvert(or.right());
        }
        if (expr instanceof Not not) {
            return canConvert(not.field());
        }
        if (expr instanceof StartsWith sw) {
            return PushdownPredicates.isStartsWith(sw, dt -> dt == DataType.KEYWORD) && literalValueOf(sw.prefix()) != null;
        }
        return false;
    }
}
