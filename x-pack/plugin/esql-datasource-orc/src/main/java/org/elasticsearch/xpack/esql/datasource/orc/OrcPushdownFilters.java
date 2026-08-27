/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.orc;

import org.apache.hadoop.hive.ql.io.sarg.PredicateLeaf;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.xpack.esql.core.QlIllegalArgumentException;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.datasources.pushdown.PushdownPredicates;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.StartsWith;
import org.elasticsearch.xpack.esql.expression.predicate.Range;
import org.elasticsearch.xpack.esql.expression.predicate.logical.And;
import org.elasticsearch.xpack.esql.expression.predicate.logical.Not;
import org.elasticsearch.xpack.esql.expression.predicate.logical.Or;
import org.elasticsearch.xpack.esql.expression.predicate.nulls.IsNotNull;
import org.elasticsearch.xpack.esql.expression.predicate.nulls.IsNull;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.EsqlBinaryComparison;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.In;

import java.sql.Timestamp;
import java.util.function.Predicate;

import static org.elasticsearch.xpack.esql.expression.Foldables.literalValueOf;

/**
 * Shared utilities for ORC predicate pushdown: expression convertibility checks,
 * ESQL-to-ORC type mapping, and literal value conversion.
 * <p>
 * These are used by both {@link OrcFilterPushdownSupport} (at optimize time, to decide
 * which expressions can be pushed) and {@link OrcPushedExpressions} (at read time, to
 * build the schema-aware {@link org.apache.hadoop.hive.ql.io.sarg.SearchArgument}).
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
            return PushdownPredicates.isStartsWith(sw, dt -> dt == DataType.KEYWORD || dt == DataType.TEXT)
                && literalValueOf(sw.prefix()) != null;
        }
        return false;
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
                throw new QlIllegalArgumentException("Expected Long for DATETIME literal but got: " + value.getClass().getSimpleName());
            }
            default -> throw new QlIllegalArgumentException("Unsupported data type for ORC literal conversion: " + dataType);
        };
    }
}
