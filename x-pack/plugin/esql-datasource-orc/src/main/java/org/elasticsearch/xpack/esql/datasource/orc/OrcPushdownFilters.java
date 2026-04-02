/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.orc;

import org.apache.hadoop.hive.ql.io.sarg.PredicateLeaf;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgumentFactory;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.datasources.pushdown.PushdownPredicates;
import org.elasticsearch.xpack.esql.datasources.pushdown.StringPrefixUtils;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.StartsWith;
import org.elasticsearch.xpack.esql.expression.predicate.Range;
import org.elasticsearch.xpack.esql.expression.predicate.logical.And;
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

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;

import static org.elasticsearch.xpack.esql.expression.Foldables.literalValueOf;

/**
 * Converts ESQL filter expressions to ORC {@link SearchArgument} for predicate pushdown.
 * <p>
 * The SearchArgument enables the ORC library to skip entire stripes and row groups
 * (10K-row blocks) whose statistics prove they cannot contain matching rows.
 * This is a block-level optimization only — it does NOT filter individual rows.
 * The ESQL FilterExec must always remain in the plan for row-level correctness.
 * <p>
 * Uses a two-pass approach (following Spark's OrcFilters pattern):
 * <ol>
 *   <li>{@link #canConvert(Expression)} validates convertibility without side effects</li>
 *   <li>{@link #buildSearchArgument(List)} builds the SARG from validated expressions</li>
 * </ol>
 * This is necessary because the SARG builder is stateful and cannot be rolled back.
 */
final class OrcPushdownFilters {

    private OrcPushdownFilters() {}

    private static final Predicate<DataType> TYPE_SUPPORTED = dt -> resolveType(dt) != null;

    /**
     * Check if an expression can be converted to an ORC SearchArgument predicate.
     * <p>
     * For compound expressions (AND/OR/NOT), ALL children must be convertible
     * for OR and NOT (partial pushdown is unsafe). For AND, at least one child
     * must be convertible (partial pushdown is safe under AND since the
     * remaining expressions stay in FilterExec via RECHECK semantics).
     */
    static boolean canConvert(Expression expr) {
        if (expr instanceof EsqlBinaryComparison bc) {
            return PushdownPredicates.isComparison(bc, TYPE_SUPPORTED);
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
            return PushdownPredicates.isRange(range, TYPE_SUPPORTED);
        }
        // Boolean connective rules are ORC-specific: AND allows partial pushdown
        // (safe under RECHECK semantics), while OR/NOT require full convertibility.
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
            return PushdownPredicates.isStartsWith(sw, dt -> dt == DataType.KEYWORD || dt == DataType.TEXT);
        }
        return false;
    }

    /**
     * Build an ORC SearchArgument from a list of pre-validated ESQL expressions.
     * All expressions are combined with AND at the top level.
     *
     * @param filters list of filter expressions (must all pass {@link #canConvert})
     * @return the SearchArgument, or null if no expressions could be converted
     */
    static SearchArgument buildSearchArgument(List<Expression> filters) {
        if (filters.isEmpty()) {
            return null;
        }

        // Collect only convertible expressions (for AND partial pushdown safety)
        List<Expression> convertible = new ArrayList<>();
        for (Expression filter : filters) {
            if (canConvert(filter)) {
                convertible.add(filter);
            }
        }
        if (convertible.isEmpty()) {
            return null;
        }

        SearchArgument.Builder builder = SearchArgumentFactory.newBuilder();
        if (convertible.size() == 1) {
            buildPredicate(builder, convertible.get(0));
        } else {
            builder.startAnd();
            for (Expression expr : convertible) {
                buildPredicate(builder, expr);
            }
            builder.end();
        }
        return builder.build();
    }

    private static void buildPredicate(SearchArgument.Builder builder, Expression expr) {
        if (expr instanceof EsqlBinaryComparison bc && bc.left() instanceof NamedExpression ne && bc.right().foldable()) {
            String name = ne.name();
            PredicateLeaf.Type type = resolveType(ne.dataType());
            Object value = convertLiteral(literalValueOf(bc.right()), ne.dataType());

            switch (bc) {
                case Equals ignored -> builder.startAnd().equals(name, type, value).end();
                case NotEquals ignored -> builder.startNot().equals(name, type, value).end();
                case LessThan ignored -> builder.startAnd().lessThan(name, type, value).end();
                case LessThanOrEqual ignored -> builder.startAnd().lessThanEquals(name, type, value).end();
                // ORC has no greaterThan — express as NOT(lessThanEquals)
                case GreaterThan ignored -> builder.startNot().lessThanEquals(name, type, value).end();
                // ORC has no greaterThanOrEqual — express as NOT(lessThan)
                case GreaterThanOrEqual ignored -> builder.startNot().lessThan(name, type, value).end();
                default -> throw new IllegalStateException("Unexpected comparison: " + bc.getClass().getSimpleName());
            }
            return;
        }

        if (expr instanceof In inExpr && inExpr.value() instanceof NamedExpression ne) {
            String name = ne.name();
            PredicateLeaf.Type type = resolveType(ne.dataType());

            // Filter out null values — SARG builder doesn't accept nulls in IN lists
            List<Object> values = new ArrayList<>();
            for (Expression item : inExpr.list()) {
                Object val = literalValueOf(item);
                if (val != null) {
                    values.add(convertLiteral(val, ne.dataType()));
                }
            }
            builder.startAnd().in(name, type, values.toArray(new Object[0])).end();
            return;
        }

        if (expr instanceof IsNull isNull && isNull.field() instanceof NamedExpression ne) {
            PredicateLeaf.Type type = resolveType(ne.dataType());
            builder.startAnd().isNull(ne.name(), type).end();
            return;
        }

        if (expr instanceof IsNotNull isNotNull && isNotNull.field() instanceof NamedExpression ne) {
            PredicateLeaf.Type type = resolveType(ne.dataType());
            builder.startNot().isNull(ne.name(), type).end();
            return;
        }

        if (expr instanceof Range range && range.value() instanceof NamedExpression ne) {
            String name = ne.name();
            PredicateLeaf.Type type = resolveType(ne.dataType());
            Object lower = convertLiteral(literalValueOf(range.lower()), ne.dataType());
            Object upper = convertLiteral(literalValueOf(range.upper()), ne.dataType());

            if (range.includeLower() && range.includeUpper()) {
                // Exact BETWEEN semantics: lower <= x <= upper
                builder.startAnd().between(name, type, lower, upper).end();
            } else {
                // Build compound: lowerBound AND upperBound
                builder.startAnd();
                if (range.includeLower()) {
                    // x >= lower → NOT(x < lower)
                    builder.startNot().lessThan(name, type, lower).end();
                } else {
                    // x > lower → NOT(x <= lower)
                    builder.startNot().lessThanEquals(name, type, lower).end();
                }
                if (range.includeUpper()) {
                    builder.lessThanEquals(name, type, upper);
                } else {
                    builder.lessThan(name, type, upper);
                }
                builder.end();
            }
            return;
        }

        if (expr instanceof And and) {
            boolean leftConvertible = canConvert(and.left());
            boolean rightConvertible = canConvert(and.right());
            if (leftConvertible && rightConvertible) {
                builder.startAnd();
                buildPredicate(builder, and.left());
                buildPredicate(builder, and.right());
                builder.end();
            } else if (leftConvertible) {
                buildPredicate(builder, and.left());
            } else if (rightConvertible) {
                buildPredicate(builder, and.right());
            }
            return;
        }

        if (expr instanceof Or or) {
            builder.startOr();
            buildPredicate(builder, or.left());
            buildPredicate(builder, or.right());
            builder.end();
            return;
        }

        if (expr instanceof Not not) {
            builder.startNot();
            buildPredicate(builder, not.field());
            builder.end();
            return;
        }

        if (expr instanceof StartsWith sw && sw.singleValueField() instanceof NamedExpression ne && sw.prefix().foldable()) {
            Object prefixValue = literalValueOf(sw.prefix());
            if (prefixValue == null) {
                return;
            }
            String name = ne.name();
            PredicateLeaf.Type type = resolveType(ne.dataType());
            String prefix = BytesRefs.toString(prefixValue);
            BytesRef upperBytes = StringPrefixUtils.nextPrefixUpperBound(new BytesRef(prefix));

            if (upperBytes != null) {
                String upper = upperBytes.utf8ToString();
                builder.startAnd();
                builder.startNot().lessThan(name, type, prefix).end();
                builder.lessThan(name, type, upper);
                builder.end();
            } else {
                builder.startNot().lessThan(name, type, prefix).end();
            }
            return;
        }

        throw new IllegalStateException("Unexpected expression type: " + expr.getClass().getSimpleName());
    }

    /**
     * Maps ESQL DataType to ORC PredicateLeaf.Type.
     * Returns null for unsupported types.
     */
    static PredicateLeaf.Type resolveType(DataType dataType) {
        return switch (dataType) {
            case BOOLEAN -> PredicateLeaf.Type.BOOLEAN;
            case INTEGER, LONG -> PredicateLeaf.Type.LONG;
            case DOUBLE -> PredicateLeaf.Type.FLOAT; // ORC names it FLOAT but expects Double.class
            case KEYWORD, TEXT -> PredicateLeaf.Type.STRING;
            // ORC has no PredicateLeaf.Type.TIMESTAMP_INSTANT — only TIMESTAMP.
            // The same type works for both column kinds because the reader is
            // configured with useUTCTimestamp=true, which makes SargApplier
            // compare predicates against UTC statistics (getMinimumUTC) instead
            // of local-timezone-shifted statistics (getMinimum).
            case DATETIME -> PredicateLeaf.Type.TIMESTAMP;
            default -> null;
        };
    }

    /**
     * Converts an ESQL literal value to the Java type expected by ORC's SearchArgument builder.
     * ORC's {@code checkLiteralType()} validates exact class match via {@code getValueClass()}.
     */
    static Object convertLiteral(Object value, DataType dataType) {
        if (value == null) {
            return null;
        }
        return switch (dataType) {
            case BOOLEAN -> {
                if (value instanceof Boolean b) {
                    yield b;
                }
                yield org.elasticsearch.core.Booleans.parseBoolean(value.toString());
            }
            case INTEGER -> {
                // ORC LONG type requires java.lang.Long (not Integer)
                if (value instanceof Number n) {
                    yield n.longValue();
                }
                yield Long.parseLong(value.toString());
            }
            case LONG -> {
                if (value instanceof Long l) {
                    yield l;
                }
                if (value instanceof Number n) {
                    yield n.longValue();
                }
                yield Long.parseLong(value.toString());
            }
            case DOUBLE -> {
                if (value instanceof Double d) {
                    yield d;
                }
                if (value instanceof Number n) {
                    yield n.doubleValue();
                }
                yield Double.parseDouble(value.toString());
            }
            case KEYWORD, TEXT -> {
                // ORC STRING type requires java.lang.String (not BytesRef)
                yield BytesRefs.toString(value);
            }
            case DATETIME -> {
                // ORC TIMESTAMP type requires java.sql.Timestamp.
                // ESQL DATETIME literals are always Long (UTC epoch millis).
                if (value instanceof Long millis) {
                    yield new Timestamp(millis);
                }
                throw new IllegalArgumentException("Expected Long for DATETIME literal but got: " + value.getClass().getSimpleName());
            }
            default -> throw new IllegalArgumentException("Unsupported data type for ORC literal conversion: " + dataType);
        };
    }
}
