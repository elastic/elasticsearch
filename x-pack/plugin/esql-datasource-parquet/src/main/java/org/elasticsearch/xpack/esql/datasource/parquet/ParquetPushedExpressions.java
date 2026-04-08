/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.parquet;

import org.apache.lucene.util.BytesRef;
import org.apache.parquet.filter2.predicate.FilterApi;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.filter2.predicate.Operators;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.type.DataType;
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

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Function;

import static org.elasticsearch.xpack.esql.expression.Foldables.literalValueOf;

/**
 * Holds validated ESQL filter expressions whose translation to Parquet {@link FilterPredicate}s
 * is deferred until read time when the per-file physical schema is available.
 * <p>
 * This two-level approach (validate at optimize time, translate at read time) follows Spark's
 * ParquetFilters design (SPARK-24716). It is necessary because DATETIME columns can have
 * different physical representations across Parquet files in the same glob:
 * <ul>
 *   <li>INT32 with DATE annotation (days since epoch)</li>
 *   <li>INT64 with TIMESTAMP_MILLIS/MICROS/NANOS annotation</li>
 *   <li>INT96 (deprecated, not pushable)</li>
 * </ul>
 * Using ESQL's epoch millis directly against non-millis statistics would cause incorrect
 * row group skipping — a correctness issue, not just suboptimal performance.
 */
record ParquetPushedExpressions(List<Expression> expressions) {

    static final long MILLIS_PER_DAY = Duration.ofDays(1).toMillis();

    /**
     * Translates the held expressions to a combined Parquet {@link FilterPredicate} using
     * the actual file schema for type-correct value conversion.
     *
     * @param schema the Parquet file's MessageType schema (from footer metadata)
     * @return a combined FilterPredicate, or null if no expressions could be translated
     */
    FilterPredicate toFilterPredicate(MessageType schema) {
        List<FilterPredicate> translated = new ArrayList<>();
        for (Expression expr : expressions) {
            FilterPredicate fp = translateExpression(expr, schema);
            if (fp != null) {
                translated.add(fp);
            }
        }
        if (translated.isEmpty()) {
            return null;
        }
        FilterPredicate combined = translated.get(0);
        for (int i = 1; i < translated.size(); i++) {
            combined = FilterApi.and(combined, translated.get(i));
        }
        return combined;
    }

    private FilterPredicate translateExpression(Expression expr, MessageType schema) {
        if (expr instanceof EsqlBinaryComparison bc && bc.left() instanceof NamedExpression ne && bc.right().foldable()) {
            String name = ne.name();
            DataType dataType = ne.dataType();
            Object value = literalValueOf(bc.right());

            if (value == null) {
                return null;
            }

            return switch (bc) {
                case Equals ignored -> buildPredicate(name, dataType, value, PredicateOp.EQ, schema);
                case NotEquals ignored -> buildPredicate(name, dataType, value, PredicateOp.NOT_EQ, schema);
                case GreaterThan ignored -> buildPredicate(name, dataType, value, PredicateOp.GT, schema);
                case GreaterThanOrEqual ignored -> buildPredicate(name, dataType, value, PredicateOp.GTE, schema);
                case LessThan ignored -> buildPredicate(name, dataType, value, PredicateOp.LT, schema);
                case LessThanOrEqual ignored -> buildPredicate(name, dataType, value, PredicateOp.LTE, schema);
                default -> null;
            };
        }
        if (expr instanceof In inExpr && inExpr.value() instanceof NamedExpression ne) {
            return translateIn(ne.name(), ne.dataType(), inExpr.list(), schema);
        }
        if (expr instanceof IsNull isNull && isNull.field() instanceof NamedExpression ne) {
            return buildPredicate(ne.name(), ne.dataType(), null, PredicateOp.EQ, schema);
        }
        if (expr instanceof IsNotNull isNotNull && isNotNull.field() instanceof NamedExpression ne) {
            return buildPredicate(ne.name(), ne.dataType(), null, PredicateOp.NOT_EQ, schema);
        }
        if (expr instanceof Range range && range.value() instanceof NamedExpression ne) {
            return translateRange(ne.name(), ne.dataType(), range, schema);
        }
        if (expr instanceof And and) {
            boolean leftConvertible = ParquetFilterPushdownSupport.canConvert(and.left());
            boolean rightConvertible = ParquetFilterPushdownSupport.canConvert(and.right());
            if (leftConvertible && rightConvertible) {
                FilterPredicate leftPred = translateExpression(and.left(), schema);
                FilterPredicate rightPred = translateExpression(and.right(), schema);
                if (leftPred != null && rightPred != null) {
                    return FilterApi.and(leftPred, rightPred);
                }
                return leftPred != null ? leftPred : rightPred;
            } else if (leftConvertible) {
                return translateExpression(and.left(), schema);
            } else {
                return translateExpression(and.right(), schema);
            }
        }
        if (expr instanceof Or or) {
            FilterPredicate leftPred = translateExpression(or.left(), schema);
            FilterPredicate rightPred = translateExpression(or.right(), schema);
            if (leftPred != null && rightPred != null) {
                return FilterApi.or(leftPred, rightPred);
            }
            return null;
        }
        if (expr instanceof Not not) {
            FilterPredicate inner = translateExpression(not.field(), schema);
            return inner != null ? FilterApi.not(inner) : null;
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

    private FilterPredicate buildPredicate(String columnName, DataType dataType, Object value, PredicateOp op, MessageType schema) {
        if (value == null && op.isOrdered()) {
            return null;
        }
        return switch (dataType) {
            case INTEGER -> orderedPredicate(FilterApi.intColumn(columnName), value != null ? ((Number) value).intValue() : null, op);
            case LONG -> orderedPredicate(FilterApi.longColumn(columnName), value != null ? ((Number) value).longValue() : null, op);
            case DOUBLE -> orderedPredicate(FilterApi.doubleColumn(columnName), value != null ? ((Number) value).doubleValue() : null, op);
            case KEYWORD -> orderedPredicate(FilterApi.binaryColumn(columnName), value != null ? toBinary(value) : null, op);
            case BOOLEAN -> {
                var col = FilterApi.booleanColumn(columnName);
                Boolean v = value != null ? (Boolean) value : null;
                yield switch (op) {
                    case EQ -> FilterApi.eq(col, v);
                    case NOT_EQ -> FilterApi.notEq(col, v);
                    default -> null;
                };
            }
            case DATETIME -> buildDatetimePredicate(columnName, value, op, schema);
            default -> null;
        };
    }

    private static FilterPredicate buildDatetimePredicate(String columnName, Object value, PredicateOp op, MessageType schema) {
        if (schema.containsField(columnName) == false) {
            return null;
        }
        PrimitiveType ptype = schema.getType(columnName).asPrimitiveType();
        LogicalTypeAnnotation logical = ptype.getLogicalTypeAnnotation();

        if (value == null) {
            return switch (ptype.getPrimitiveTypeName()) {
                case INT32 -> orderedPredicate(FilterApi.intColumn(columnName), null, op);
                case INT64 -> orderedPredicate(FilterApi.longColumn(columnName), null, op);
                default -> null;
            };
        }

        long millis = ((Number) value).longValue();
        return switch (ptype.getPrimitiveTypeName()) {
            case INT32 -> {
                if (logical instanceof LogicalTypeAnnotation.DateLogicalTypeAnnotation) {
                    int days = (int) Math.floorDiv(millis, MILLIS_PER_DAY);
                    yield orderedPredicate(FilterApi.intColumn(columnName), days, op);
                }
                yield null;
            }
            case INT64 -> {
                try {
                    long physicalValue = convertMillisToPhysical(millis, logical);
                    yield orderedPredicate(FilterApi.longColumn(columnName), physicalValue, op);
                } catch (ArithmeticException e) {
                    yield null;
                }
            }
            default -> null;
        };
    }

    /**
     * Converts ESQL epoch millis to the physical unit used in the Parquet file.
     * Uses {@link Math#multiplyExact} to detect overflow — timestamps beyond ~year 2262
     * would overflow when scaled to nanos.
     */
    static long convertMillisToPhysical(long millis, LogicalTypeAnnotation logical) {
        if (logical instanceof LogicalTypeAnnotation.TimestampLogicalTypeAnnotation ts) {
            return switch (ts.getUnit()) {
                case MILLIS -> millis;
                case MICROS -> Math.multiplyExact(millis, 1000L);
                case NANOS -> Math.multiplyExact(millis, 1_000_000L);
            };
        }
        return millis;
    }

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

    private FilterPredicate translateIn(String columnName, DataType dataType, List<Expression> items, MessageType schema) {
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
            case KEYWORD -> inPredicate(FilterApi.binaryColumn(columnName), rawValues, ParquetPushedExpressions::toBinary);
            case BOOLEAN -> inPredicate(FilterApi.booleanColumn(columnName), rawValues, v -> (Boolean) v);
            case DATETIME -> translateDatetimeIn(columnName, rawValues, schema);
            default -> null;
        };
    }

    private static FilterPredicate translateDatetimeIn(String columnName, List<Object> rawValues, MessageType schema) {
        if (schema.containsField(columnName) == false) {
            return null;
        }
        PrimitiveType ptype = schema.getType(columnName).asPrimitiveType();
        LogicalTypeAnnotation logical = ptype.getLogicalTypeAnnotation();
        try {
            return switch (ptype.getPrimitiveTypeName()) {
                case INT32 -> {
                    if (logical instanceof LogicalTypeAnnotation.DateLogicalTypeAnnotation) {
                        yield inPredicate(
                            FilterApi.intColumn(columnName),
                            rawValues,
                            v -> (int) Math.floorDiv(((Number) v).longValue(), MILLIS_PER_DAY)
                        );
                    }
                    yield null;
                }
                case INT64 -> inPredicate(
                    FilterApi.longColumn(columnName),
                    rawValues,
                    v -> convertMillisToPhysical(((Number) v).longValue(), logical)
                );
                default -> null;
            };
        } catch (ArithmeticException e) {
            return null;
        }
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

    private FilterPredicate translateRange(String columnName, DataType dataType, Range range, MessageType schema) {
        Object lower = literalValueOf(range.lower());
        Object upper = literalValueOf(range.upper());

        FilterPredicate lowerBound = buildPredicate(
            columnName,
            dataType,
            lower,
            range.includeLower() ? PredicateOp.GTE : PredicateOp.GT,
            schema
        );
        FilterPredicate upperBound = buildPredicate(
            columnName,
            dataType,
            upper,
            range.includeUpper() ? PredicateOp.LTE : PredicateOp.LT,
            schema
        );

        if (lowerBound != null && upperBound != null) {
            return FilterApi.and(lowerBound, upperBound);
        }
        return null;
    }

    private static Binary toBinary(Object value) {
        if (value instanceof BytesRef bytesRef) {
            return Binary.fromConstantByteArray(bytesRef.bytes, bytesRef.offset, bytesRef.length);
        }
        return Binary.fromString(value.toString());
    }
}
