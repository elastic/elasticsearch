/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.pushdown;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.expression.VirtualAttribute;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.Contains;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.EndsWith;
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

import java.util.function.Predicate;

import static org.elasticsearch.xpack.esql.expression.Foldables.literalValueOf;

/**
 * Shared structural validation for predicate pushdown across file formats (ORC, Parquet, etc.).
 * <p>
 * These methods check whether an ESQL expression has the right structure for pushdown:
 * field reference on the left, foldable literal on the right, supported operator, and
 * a data type the target format can handle. The format-specific type check is provided
 * as a {@code Predicate<DataType>} so each format plugs in its own supported types.
 * Comparison, {@code IN}, and {@code Range} also require a foldable literal whose
 * {@link DataType} agrees with the column for date and numeric pairs
 * ({@link #isAgreeingPushdownLiteral}): dataset readers treat the literal's raw number
 * as already in the column's domain, and ES|QL allows mixed {@code date}/{@code date_nanos}
 * and mixed {@code integer}/{@code long}/{@code double} comparisons that the evaluator
 * reconciles. A mixed leaf stays in {@code FilterExec}.
 * <p>
 * Boolean connective <em>polarity</em> (AND partial vs full pushdown, OR, NOT) stays
 * format-specific: ORC and Parquet allow partial AND, Iceberg requires both sides.
 * {@link #allPushdownLiteralsAgree} is a shared all-leaves check, not a second
 * {@code canConvert}.
 * <p>
 * Virtual columns ({@link MetadataAttribute} for ES-index metadata like {@code _id}/{@code _index},
 * any {@link VirtualAttribute} for engine-synthesized columns like {@code _file.*}) never live in the
 * physical file schema, so format-level readers cannot evaluate predicates / aggregates against
 * them and these helpers always reject them. Identification is type-based: name conventions
 * (e.g. the {@code _file.} prefix) are surface and may be aliased away, but the marker
 * interface stays attached across renames and serialization.
 */
public final class PushdownPredicates {

    private PushdownPredicates() {}

    /**
     * Returns {@code true} when {@code e} represents a column that has no physical presence in
     * the source data and therefore cannot be evaluated by a format-level scan. Two cases:
     * <ul>
     *     <li>{@link MetadataAttribute} — Elasticsearch document metadata ({@code _id},
     *     {@code _index}, {@code _score}, ...). Real per-document values, but external-file
     *     readers (Parquet, ORC) have no concept of them.</li>
     *     <li>Any {@link VirtualAttribute} — engine-synthesized columns ({@code _file.*} today)
     *     materialized by {@code VirtualColumnIterator} on the producer thread.</li>
     * </ul>
     * Format-level filter and aggregate pushdown rules must reject both. Use this helper rather
     * than name- or prefix-based checks: the marker survives rename/aliasing/serialization,
     * names do not.
     */
    public static boolean isVirtualColumn(Expression e) {
        return e instanceof MetadataAttribute || e instanceof VirtualAttribute;
    }

    /**
     * Dataset readers and the split-stats classifier treat a literal's raw number as already in the
     * column's domain. ES|QL allows mixed date / date_nanos and mixed integer / long / double
     * comparisons and reconciles them in the evaluator, so a bound built from the column type alone
     * is wrong when the literal is the other date type (unit) or another numeric type (truncation).
     * Those predicates stay in FilterExec (and the classifier stays AMBIGUOUS).
     */
    public static boolean isAgreeingPushdownLiteral(DataType columnType, Expression literal) {
        DataType literalType = literal.dataType();
        if (columnType.isDate() && literalType.isDate()) {
            return columnType == literalType;
        }
        if (columnType.isNumeric() && literalType.isNumeric()) {
            return columnType == literalType;
        }
        return true;
    }

    /**
     * {@code true} when every comparison / {@code IN} / {@code Range} leaf in {@code expr} that has
     * a named field and a foldable literal agrees under {@link #isAgreeingPushdownLiteral}.
     * Polarized like an all-leaves walk ({@code &&} of children). Column-column compares, non-date
     * non-numeric pairs, and unknown nodes pass. Used at {@code canConvert} entry points so a mixed
     * leaf nested under AND/OR/NOT is not attached.
     */
    public static boolean allPushdownLiteralsAgree(Expression expr) {
        if (expr instanceof EsqlBinaryComparison bc) {
            if (bc.left() instanceof NamedExpression && bc.right().foldable()) {
                return isAgreeingPushdownLiteral(bc.left().dataType(), bc.right());
            }
            if (bc.right() instanceof NamedExpression && bc.left().foldable()) {
                return isAgreeingPushdownLiteral(bc.right().dataType(), bc.left());
            }
        }
        if (expr instanceof In in && in.value() instanceof NamedExpression) {
            for (Expression item : in.list()) {
                if (item.foldable() && isAgreeingPushdownLiteral(in.value().dataType(), item) == false) {
                    return false;
                }
            }
            return true;
        }
        if (expr instanceof Range range && range.value() instanceof NamedExpression) {
            return (range.lower().foldable() == false || isAgreeingPushdownLiteral(range.value().dataType(), range.lower()))
                && (range.upper().foldable() == false || isAgreeingPushdownLiteral(range.value().dataType(), range.upper()));
        }
        if (expr instanceof And and) {
            return allPushdownLiteralsAgree(and.left()) && allPushdownLiteralsAgree(and.right());
        }
        if (expr instanceof Or or) {
            return allPushdownLiteralsAgree(or.left()) && allPushdownLiteralsAgree(or.right());
        }
        if (expr instanceof Not not) {
            return allPushdownLiteralsAgree(not.field());
        }
        return true;
    }

    /**
     * Checks if a binary comparison can be pushed down: left side is a non-virtual field reference,
     * right side is a foldable literal whose date or numeric type agrees with the field, operator is
     * one of the six standard comparisons, and the field's data type is supported by the target format.
     */
    public static boolean isComparison(EsqlBinaryComparison bc, Predicate<DataType> typeSupported) {
        if (bc.left() instanceof NamedExpression ne
            && isVirtualColumn(ne) == false
            && bc.right().foldable()
            && typeSupported.test(ne.dataType())
            && isAgreeingPushdownLiteral(ne.dataType(), bc.right())) {
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
     * Checks if an IN expression can be pushed down: value is a non-virtual field reference with a
     * supported data type, all list items are foldable and agree with the field for date and numeric
     * pairs, and at least one is non-null.
     */
    public static boolean isIn(In inExpr, Predicate<DataType> typeSupported) {
        if (inExpr.value() instanceof NamedExpression ne && isVirtualColumn(ne) == false && typeSupported.test(ne.dataType())) {
            boolean hasNonNull = false;
            for (Expression item : inExpr.list()) {
                if (item.foldable() == false || isAgreeingPushdownLiteral(ne.dataType(), item) == false) {
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
     * Checks if an IS NULL expression can be pushed down: field is a non-virtual named expression
     * with a supported data type.
     */
    public static boolean isIsNull(IsNull isNull, Predicate<DataType> typeSupported) {
        return isNull.field() instanceof NamedExpression ne && isVirtualColumn(ne) == false && typeSupported.test(ne.dataType());
    }

    /**
     * Checks if an IS NOT NULL expression can be pushed down: field is a non-virtual named expression
     * with a supported data type.
     */
    public static boolean isIsNotNull(IsNotNull isNotNull, Predicate<DataType> typeSupported) {
        return isNotNull.field() instanceof NamedExpression ne && isVirtualColumn(ne) == false && typeSupported.test(ne.dataType());
    }

    /**
     * Checks if a range expression can be pushed down: value is a non-virtual field reference with a
     * supported data type, and both bounds are foldable and agree with the field for date and numeric
     * pairs.
     */
    public static boolean isRange(Range range, Predicate<DataType> typeSupported) {
        return range.value() instanceof NamedExpression ne
            && isVirtualColumn(ne) == false
            && typeSupported.test(ne.dataType())
            && range.lower().foldable()
            && range.upper().foldable()
            && isAgreeingPushdownLiteral(ne.dataType(), range.lower())
            && isAgreeingPushdownLiteral(ne.dataType(), range.upper());
    }

    /**
     * Checks if a StartsWith expression can be pushed down: field is a single-value
     * named expression with a supported data type, and the prefix is foldable.
     */
    public static boolean isStartsWith(StartsWith sw, Predicate<DataType> typeSupported) {
        return sw.singleValueField() instanceof NamedExpression ne
            && isVirtualColumn(ne) == false
            && typeSupported.test(ne.dataType())
            && sw.prefix().foldable();
    }

    /**
     * Checks if an EndsWith expression can be pushed down: field is a single-value
     * named expression with a supported data type, and the suffix is foldable.
     */
    public static boolean isEndsWith(EndsWith ew, Predicate<DataType> typeSupported) {
        return ew.singleValueField() instanceof NamedExpression ne
            && isVirtualColumn(ne) == false
            && typeSupported.test(ne.dataType())
            && ew.suffix().foldable();
    }

    /**
     * Checks if a Contains expression can be pushed down: field is a single-value
     * named expression with a supported data type, and the substring is foldable.
     */
    public static boolean isContains(Contains c, Predicate<DataType> typeSupported) {
        return c.singleValueField() instanceof NamedExpression ne
            && isVirtualColumn(ne) == false
            && typeSupported.test(ne.dataType())
            && c.substr().foldable();
    }
}
