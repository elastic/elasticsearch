/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.parquet;

import org.apache.lucene.util.BytesRef;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.filter2.predicate.FilterApi;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.filter2.predicate.Operators;
import org.apache.parquet.io.api.Binary;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.datasources.pushdown.PushdownPredicates;
import org.elasticsearch.xpack.esql.datasources.pushdown.StringPrefixUtils;
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
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThan;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThanOrEqual;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.In;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.LessThan;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.LessThanOrEqual;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.NotEquals;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;

import static org.elasticsearch.xpack.esql.expression.Foldables.literalValueOf;

/**
 * Parquet-specific filter pushdown support that translates ESQL filter expressions
 * to parquet-java {@link FilterPredicate} objects.
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
 * Uses a two-pass approach (like {@code OrcPushdownFilters}):
 * <ol>
 *   <li>{@link #canConvert(Expression)} validates convertibility without side effects</li>
 *   <li>{@link #translateExpression(Expression)} builds the FilterPredicate from validated expressions</li>
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
        || dt == DataType.BOOLEAN;

    @Override
    public PushdownResult pushFilters(List<Expression> filters) {
        List<FilterPredicate> pushed = new ArrayList<>();
        // All filters are returned as remainder (RECHECK semantics)
        List<Expression> remainder = new ArrayList<>(filters);

        for (Expression filter : filters) {
            if (canConvert(filter)) {
                FilterPredicate predicate = translateExpression(filter);
                if (predicate != null) {
                    pushed.add(predicate);
                }
            }
        }

        if (pushed.isEmpty()) {
            return PushdownResult.none(filters);
        }

        FilterPredicate combined = pushed.get(0);
        for (int i = 1; i < pushed.size(); i++) {
            combined = FilterApi.and(combined, pushed.get(i));
        }

        logger.debug("Parquet filter pushdown: translated {} of {} expressions", pushed.size(), filters.size());
        return new PushdownResult(FilterCompat.get(combined), remainder);
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
            return PushdownPredicates.isStartsWith(sw, dt -> dt == DataType.KEYWORD);
        }
        return false;
    }

    /**
     * Translates an ESQL expression to a parquet-java FilterPredicate.
     * Must only be called after {@link #canConvert(Expression)} returns true.
     */
    private FilterPredicate translateExpression(Expression expr) {
        if (expr instanceof EsqlBinaryComparison bc && bc.left() instanceof NamedExpression ne && bc.right().foldable()) {
            String name = ne.name();
            DataType dataType = ne.dataType();
            Object value = literalValueOf(bc.right());

            // SQL null comparisons are always UNKNOWN — only IsNull/IsNotNull use null predicates
            if (value == null) {
                return null;
            }

            return switch (bc) {
                case Equals ignored -> buildPredicate(name, dataType, value, PredicateOp.EQ);
                case NotEquals ignored -> buildPredicate(name, dataType, value, PredicateOp.NOT_EQ);
                case GreaterThan ignored -> buildPredicate(name, dataType, value, PredicateOp.GT);
                case GreaterThanOrEqual ignored -> buildPredicate(name, dataType, value, PredicateOp.GTE);
                case LessThan ignored -> buildPredicate(name, dataType, value, PredicateOp.LT);
                case LessThanOrEqual ignored -> buildPredicate(name, dataType, value, PredicateOp.LTE);
                default -> null;
            };
        }
        if (expr instanceof In inExpr && inExpr.value() instanceof NamedExpression ne) {
            return translateIn(ne.name(), ne.dataType(), inExpr.list());
        }
        if (expr instanceof IsNull isNull && isNull.field() instanceof NamedExpression ne) {
            return buildPredicate(ne.name(), ne.dataType(), null, PredicateOp.EQ);
        }
        if (expr instanceof IsNotNull isNotNull && isNotNull.field() instanceof NamedExpression ne) {
            return buildPredicate(ne.name(), ne.dataType(), null, PredicateOp.NOT_EQ);
        }
        if (expr instanceof Range range && range.value() instanceof NamedExpression ne) {
            return translateRange(ne.name(), ne.dataType(), range);
        }
        if (expr instanceof And and) {
            boolean leftConvertible = canConvert(and.left());
            boolean rightConvertible = canConvert(and.right());
            if (leftConvertible && rightConvertible) {
                return FilterApi.and(translateExpression(and.left()), translateExpression(and.right()));
            } else if (leftConvertible) {
                return translateExpression(and.left());
            } else {
                return translateExpression(and.right());
            }
        }
        if (expr instanceof Or or) {
            return FilterApi.or(translateExpression(or.left()), translateExpression(or.right()));
        }
        if (expr instanceof Not not) {
            return FilterApi.not(translateExpression(not.field()));
        }
        if (expr instanceof StartsWith sw && sw.singleValueField() instanceof NamedExpression ne && sw.prefix().foldable()) {
            Object prefixValue = literalValueOf(sw.prefix());
            if (prefixValue == null) {
                return null;
            }
            BytesRef prefix = (BytesRef) prefixValue;
            var col = FilterApi.binaryColumn(ne.name());
            FilterPredicate lower = FilterApi.gtEq(col, toBinary(prefix));
            BytesRef upper = StringPrefixUtils.nextPrefixUpperBound(prefix);
            if (upper != null) {
                return FilterApi.and(lower, FilterApi.lt(col, toBinary(upper)));
            }
            return lower;
        }
        return null;
    }

    // -----------------------------------------------------------------------------------
    // Predicate building — type dispatch happens once, operations are applied generically
    // -----------------------------------------------------------------------------------

    enum PredicateOp {
        EQ,
        NOT_EQ,
        GT,
        GTE,
        LT,
        LTE;

        boolean isOrdered() {
            return this == GT || this == GTE || this == LT || this == LTE;
        }
    }

    /**
     * Builds a single Parquet FilterPredicate by resolving the column type from the ESQL DataType,
     * converting the value, and applying the requested operation.
     * Value may be null for EQ/NOT_EQ (used by IsNull/IsNotNull).
     */
    private FilterPredicate buildPredicate(String columnName, DataType dataType, Object value, PredicateOp op) {
        if (value == null && op.isOrdered()) {
            return null;
        }
        return switch (dataType) {
            case INTEGER -> orderedPredicate(FilterApi.intColumn(columnName), value != null ? ((Number) value).intValue() : null, op);
            case LONG -> orderedPredicate(FilterApi.longColumn(columnName), value != null ? ((Number) value).longValue() : null, op);
            case DOUBLE -> orderedPredicate(FilterApi.doubleColumn(columnName), value != null ? ((Number) value).doubleValue() : null, op);
            case KEYWORD -> orderedPredicate(FilterApi.binaryColumn(columnName), value != null ? toBinary(value) : null, op);
            case BOOLEAN -> {
                // BooleanColumn only supports eq/notEq (no SupportsLtGt in parquet-java)
                var col = FilterApi.booleanColumn(columnName);
                Boolean v = value != null ? (Boolean) value : null;
                yield switch (op) {
                    case EQ -> FilterApi.eq(col, v);
                    case NOT_EQ -> FilterApi.notEq(col, v);
                    default -> null;
                };
            }
            default -> null;
        };
    }

    /**
     * Applies a predicate operation on a column type that supports all comparison operators.
     * IntColumn, LongColumn, DoubleColumn, and BinaryColumn all implement SupportsLtGt.
     */
    private static <T extends Comparable<T>, C extends Operators.Column<T> & Operators.SupportsLtGt> FilterPredicate orderedPredicate(
        C col,
        T value,
        PredicateOp op
    ) {
        return switch (op) {
            case EQ -> FilterApi.eq(col, value);
            case NOT_EQ -> FilterApi.notEq(col, value);
            case GT -> FilterApi.gt(col, value);
            case GTE -> FilterApi.gtEq(col, value);
            case LT -> FilterApi.lt(col, value);
            case LTE -> FilterApi.ltEq(col, value);
        };
    }

    /**
     * Translates an IN expression by collecting non-null values into a typed set.
     */
    private FilterPredicate translateIn(String columnName, DataType dataType, List<Expression> items) {
        List<Object> rawValues = new ArrayList<>();
        for (Expression item : items) {
            Object val = literalValueOf(item);
            if (val != null) {
                rawValues.add(val);
            }
        }
        if (rawValues.isEmpty()) {
            return null;
        }
        return switch (dataType) {
            case INTEGER -> inPredicate(FilterApi.intColumn(columnName), rawValues, v -> ((Number) v).intValue());
            case LONG -> inPredicate(FilterApi.longColumn(columnName), rawValues, v -> ((Number) v).longValue());
            case DOUBLE -> inPredicate(FilterApi.doubleColumn(columnName), rawValues, v -> ((Number) v).doubleValue());
            case KEYWORD -> inPredicate(FilterApi.binaryColumn(columnName), rawValues, ParquetFilterPushdownSupport::toBinary);
            case BOOLEAN -> inPredicate(FilterApi.booleanColumn(columnName), rawValues, v -> (Boolean) v);
            default -> null;
        };
    }

    private static <T extends Comparable<T>, C extends Operators.Column<T> & Operators.SupportsEqNotEq> FilterPredicate inPredicate(
        C col,
        List<Object> values,
        Function<Object, T> converter
    ) {
        Set<T> converted = new HashSet<>();
        for (Object v : values) {
            converted.add(converter.apply(v));
        }
        return FilterApi.in(col, converted);
    }

    private FilterPredicate translateRange(String columnName, DataType dataType, Range range) {
        Object lower = literalValueOf(range.lower());
        Object upper = literalValueOf(range.upper());

        FilterPredicate lowerBound = buildPredicate(columnName, dataType, lower, range.includeLower() ? PredicateOp.GTE : PredicateOp.GT);
        FilterPredicate upperBound = buildPredicate(columnName, dataType, upper, range.includeUpper() ? PredicateOp.LTE : PredicateOp.LT);

        if (lowerBound != null && upperBound != null) {
            return FilterApi.and(lowerBound, upperBound);
        }
        return null;
    }

    /**
     * Converts an ESQL literal value to a Parquet Binary.
     * KEYWORD literals are stored as BytesRef in ESQL.
     */
    private static Binary toBinary(Object value) {
        if (value instanceof BytesRef bytesRef) {
            return Binary.fromConstantByteArray(bytesRef.bytes, bytesRef.offset, bytesRef.length);
        }
        return Binary.fromString(value.toString());
    }
}
