/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.datasource.iceberg;

import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.expression.predicate.Range;
import org.elasticsearch.xpack.esql.expression.predicate.logical.And;
import org.elasticsearch.xpack.esql.expression.predicate.logical.BinaryLogic;
import org.elasticsearch.xpack.esql.expression.predicate.logical.Not;
import org.elasticsearch.xpack.esql.expression.predicate.logical.Or;
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

import java.util.ArrayList;
import java.util.List;

import static org.apache.iceberg.expressions.Expressions.and;
import static org.apache.iceberg.expressions.Expressions.equal;
import static org.apache.iceberg.expressions.Expressions.greaterThan;
import static org.apache.iceberg.expressions.Expressions.greaterThanOrEqual;
import static org.apache.iceberg.expressions.Expressions.in;
import static org.apache.iceberg.expressions.Expressions.isNull;
import static org.apache.iceberg.expressions.Expressions.lessThan;
import static org.apache.iceberg.expressions.Expressions.lessThanOrEqual;
import static org.apache.iceberg.expressions.Expressions.not;
import static org.apache.iceberg.expressions.Expressions.notEqual;
import static org.apache.iceberg.expressions.Expressions.notNull;
import static org.apache.iceberg.expressions.Expressions.or;
import static org.elasticsearch.xpack.esql.expression.Foldables.literalValueOf;

/**
 * Converts ESQL expressions to Iceberg filter expressions for predicate pushdown.
 * Supports comparison operators, logical operators, and null checks.
 */
public class IcebergPushdownFilters {

    /**
     * Convert an ESQL expression to an Iceberg filter expression.
     * Returns null if the expression cannot be converted (unsupported predicate).
     */
    public static org.apache.iceberg.expressions.Expression convert(Expression esqlExpr) {
        // Binary comparisons: field op value
        if (esqlExpr instanceof EsqlBinaryComparison bc && bc.left() instanceof NamedExpression ne && bc.right().foldable()) {
            String fieldName = ne.name();
            Object value = convertValue(literalValueOf(bc.right()));

            return switch (bc) {
                case Equals ignored -> equal(fieldName, value);
                case NotEquals ignored -> notEqual(fieldName, value);
                case LessThan ignored -> lessThan(fieldName, value);
                case LessThanOrEqual ignored -> lessThanOrEqual(fieldName, value);
                case GreaterThan ignored -> greaterThan(fieldName, value);
                case GreaterThanOrEqual ignored -> greaterThanOrEqual(fieldName, value);
                default -> null;
            };
        }

        // In: field IN (value1, value2, ...)
        if (esqlExpr instanceof In inExpr && inExpr.value() instanceof NamedExpression ne) {
            List<Expression> list = inExpr.list();
            List<Object> values = new ArrayList<>(list.size());
            for (Expression expr : list) {
                if (expr.foldable() == false) {
                    return null;
                }
                values.add(convertValue(literalValueOf(expr)));
            }
            return in(ne.name(), values);
        }

        // IsNull: field IS NULL
        if (esqlExpr instanceof IsNull isNullExpr && isNullExpr.field() instanceof NamedExpression ne) {
            return isNull(ne.name());
        }

        // IsNotNull: field IS NOT NULL
        if (esqlExpr instanceof IsNotNull isNotNullExpr && isNotNullExpr.field() instanceof NamedExpression ne) {
            return notNull(ne.name());
        }

        // Range: lower <= field <= upper (or variations with < and >)
        if (esqlExpr instanceof Range range
            && range.value() instanceof NamedExpression ne
            && range.lower().foldable()
            && range.upper().foldable()) {
            String fieldName = ne.name();
            Object lowerValue = convertValue(literalValueOf(range.lower()));
            Object upperValue = convertValue(literalValueOf(range.upper()));

            org.apache.iceberg.expressions.Expression lowerBound = range.includeLower()
                ? greaterThanOrEqual(fieldName, lowerValue)
                : greaterThan(fieldName, lowerValue);
            org.apache.iceberg.expressions.Expression upperBound = range.includeUpper()
                ? lessThanOrEqual(fieldName, upperValue)
                : lessThan(fieldName, upperValue);

            return and(lowerBound, upperBound);
        }

        // Binary logical operators: AND, OR
        if (esqlExpr instanceof BinaryLogic bl) {
            org.apache.iceberg.expressions.Expression left = convert(bl.left());
            org.apache.iceberg.expressions.Expression right = convert(bl.right());
            if (left != null && right != null) {
                return switch (bl) {
                    case And ignored -> and(left, right);
                    case Or ignored -> or(left, right);
                    default -> null;
                };
            }
            return null;
        }

        // Not: NOT expr
        if (esqlExpr instanceof Not notExpr) {
            org.apache.iceberg.expressions.Expression inner = convert(notExpr.field());
            if (inner != null) {
                return not(inner);
            }
            return null;
        }

        return null;
    }

    private static Object convertValue(Object value) {
        return BytesRefs.toString(value);
    }
}
