/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.pushdown;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.predicate.Range;
import org.elasticsearch.xpack.esql.expression.predicate.nulls.IsNotNull;
import org.elasticsearch.xpack.esql.expression.predicate.nulls.IsNull;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.Equals;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.EsqlBinaryComparison;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThan;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThanOrEqual;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.In;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.LessThan;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.LessThanOrEqual;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.NotEquals;

import java.util.function.Predicate;

import static org.elasticsearch.xpack.esql.expression.Foldables.literalValueOf;

/**
 * Shared structural validation for predicate pushdown across file formats (ORC, Parquet, etc.).
 * <p>
 * These methods check whether an ESQL expression has the right structure for pushdown:
 * field reference on the left, foldable literal on the right, supported operator, and
 * a data type the target format can handle. The format-specific type check is provided
 * as a {@code Predicate<DataType>} so each format plugs in its own supported types.
 * <p>
 * Boolean connective rules (AND partial vs full pushdown, OR, NOT) are intentionally
 * excluded — ORC allows partial AND pushdown while Iceberg requires both sides, so
 * the traversal logic stays format-specific.
 */
public final class PushdownPredicates {

    private PushdownPredicates() {}

    /**
     * Checks if a binary comparison can be pushed down: left side is a field reference,
     * right side is a foldable literal, operator is one of the six standard comparisons,
     * and the field's data type is supported by the target format.
     */
    public static boolean isComparison(EsqlBinaryComparison bc, Predicate<DataType> typeSupported) {
        if (bc.left() instanceof NamedExpression ne && bc.right().foldable() && typeSupported.test(ne.dataType())) {
            return bc instanceof Equals
                || bc instanceof NotEquals
                || bc instanceof LessThan
                || bc instanceof LessThanOrEqual
                || bc instanceof GreaterThan
                || bc instanceof GreaterThanOrEqual;
        }
        return false;
    }

    /**
     * Checks if an IN expression can be pushed down: value is a field reference with a
     * supported data type, all list items are foldable, and at least one is non-null.
     */
    public static boolean isIn(In inExpr, Predicate<DataType> typeSupported) {
        if (inExpr.value() instanceof NamedExpression ne && typeSupported.test(ne.dataType())) {
            boolean hasNonNull = false;
            for (Expression item : inExpr.list()) {
                if (item.foldable() == false) {
                    return false;
                }
                if (literalValueOf(item) != null) {
                    hasNonNull = true;
                }
            }
            return hasNonNull;
        }
        return false;
    }

    /**
     * Checks if an IS NULL expression can be pushed down: field is a named expression
     * with a supported data type.
     */
    public static boolean isIsNull(IsNull isNull, Predicate<DataType> typeSupported) {
        return isNull.field() instanceof NamedExpression ne && typeSupported.test(ne.dataType());
    }

    /**
     * Checks if an IS NOT NULL expression can be pushed down: field is a named expression
     * with a supported data type.
     */
    public static boolean isIsNotNull(IsNotNull isNotNull, Predicate<DataType> typeSupported) {
        return isNotNull.field() instanceof NamedExpression ne && typeSupported.test(ne.dataType());
    }

    /**
     * Checks if a range expression can be pushed down: value is a field reference with a
     * supported data type, and both bounds are foldable.
     */
    public static boolean isRange(Range range, Predicate<DataType> typeSupported) {
        return range.value() instanceof NamedExpression ne
            && typeSupported.test(ne.dataType())
            && range.lower().foldable()
            && range.upper().foldable();
    }
}
