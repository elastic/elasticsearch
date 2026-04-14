/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.parquet;

import org.apache.lucene.util.BytesRef;
import org.apache.parquet.column.Dictionary;
import org.apache.parquet.filter2.predicate.FilterApi;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.filter2.predicate.Operators;
import org.apache.parquet.internal.column.columnindex.ColumnIndex;
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

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
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

    // -----------------------------------------------------------------------------------
    // Dictionary-based row group pruning (Stage 1)
    // -----------------------------------------------------------------------------------

    /**
     * Returns true if any pushed expression references the given column name.
     */
    boolean referencesColumn(String columnName) {
        for (Expression expr : expressions) {
            if (expressionReferencesColumn(expr, columnName)) {
                return true;
            }
        }
        return false;
    }

    private static boolean expressionReferencesColumn(Expression expr, String columnName) {
        if (expr instanceof EsqlBinaryComparison bc && bc.left() instanceof NamedExpression ne) {
            return ne.name().equals(columnName);
        }
        if (expr instanceof In inExpr && inExpr.value() instanceof NamedExpression ne) {
            return ne.name().equals(columnName);
        }
        if (expr instanceof IsNull isNull && isNull.field() instanceof NamedExpression ne) {
            return ne.name().equals(columnName);
        }
        if (expr instanceof IsNotNull isNotNull && isNotNull.field() instanceof NamedExpression ne) {
            return ne.name().equals(columnName);
        }
        if (expr instanceof Range range && range.value() instanceof NamedExpression ne) {
            return ne.name().equals(columnName);
        }
        if (expr instanceof And and) {
            return expressionReferencesColumn(and.left(), columnName) || expressionReferencesColumn(and.right(), columnName);
        }
        if (expr instanceof Or or) {
            return expressionReferencesColumn(or.left(), columnName) || expressionReferencesColumn(or.right(), columnName);
        }
        if (expr instanceof Not not) {
            return expressionReferencesColumn(not.field(), columnName);
        }
        if (expr instanceof StartsWith sw && sw.singleValueField() instanceof NamedExpression ne) {
            return ne.name().equals(columnName);
        }
        return false;
    }

    /**
     * Evaluates all pushed expressions that reference the given column against every dictionary
     * value. Returns true if <em>no</em> dictionary value satisfies any of the column's predicates,
     * meaning the row group can be safely skipped.
     *
     * <p>Only simple leaf predicates on the target column are evaluated (equality, comparison,
     * IN, range, starts_with). Compound predicates (AND/OR/NOT) are not evaluated against the
     * dictionary — they fall through to parquet-java's built-in filtering.
     *
     * @param columnName the Parquet column path (dot-separated)
     * @param dictionary the decoded dictionary for this column chunk
     * @param primitiveType the Parquet primitive type of the column
     * @return true if the row group should be skipped
     */
    boolean allDictionaryValuesRejected(String columnName, Dictionary dictionary, PrimitiveType primitiveType) {
        for (Expression expr : expressions) {
            if (isSimpleColumnPredicate(expr, columnName)) {
                if (noDictionaryValueMatches(expr, columnName, dictionary, primitiveType)) {
                    return true;
                }
            }
        }
        return false;
    }

    private static boolean isSimpleColumnPredicate(Expression expr, String columnName) {
        if (expr instanceof EsqlBinaryComparison bc && bc.left() instanceof NamedExpression ne && bc.right().foldable()) {
            return ne.name().equals(columnName);
        }
        if (expr instanceof In inExpr && inExpr.value() instanceof NamedExpression ne) {
            return ne.name().equals(columnName);
        }
        if (expr instanceof Range range && range.value() instanceof NamedExpression ne) {
            return ne.name().equals(columnName);
        }
        if (expr instanceof StartsWith sw && sw.singleValueField() instanceof NamedExpression ne && sw.prefix().foldable()) {
            return ne.name().equals(columnName);
        }
        return false;
    }

    /**
     * Tests whether every dictionary value is rejected by the given expression.
     * Returns true only when we can prove no value matches — false means "might match"
     * (safe fallback: don't skip).
     */
    private static boolean noDictionaryValueMatches(
        Expression expr,
        String columnName,
        Dictionary dictionary,
        PrimitiveType primitiveType
    ) {
        int dictSize = dictionary.getMaxId() + 1;
        if (dictSize <= 0) {
            return false;
        }

        if (expr instanceof Equals eq && eq.left() instanceof NamedExpression && eq.right().foldable()) {
            Object target = literalValueOf(eq.right());
            if (target == null) {
                return false;
            }
            return noDictionaryValueEquals(dictionary, dictSize, primitiveType, target);
        }

        if (expr instanceof In inExpr && inExpr.value() instanceof NamedExpression) {
            Set<Object> targets = new HashSet<>();
            for (Expression item : inExpr.list()) {
                Object val = literalValueOf(item);
                if (val != null) {
                    targets.add(val);
                }
            }
            if (targets.isEmpty()) {
                return false;
            }
            return noDictionaryValueIn(dictionary, dictSize, primitiveType, targets);
        }

        if (expr instanceof NotEquals ne && ne.left() instanceof NamedExpression && ne.right().foldable()) {
            Object target = literalValueOf(ne.right());
            if (target == null) {
                return false;
            }
            return allDictionaryValuesEqual(dictionary, dictSize, primitiveType, target);
        }

        if (expr instanceof GreaterThan gt && gt.left() instanceof NamedExpression && gt.right().foldable()) {
            Object bound = literalValueOf(gt.right());
            if (bound == null) {
                return false;
            }
            return noDictionaryValueSatisfiesComparison(dictionary, dictSize, primitiveType, bound, PredicateOp.GT);
        }
        if (expr instanceof GreaterThanOrEqual gte && gte.left() instanceof NamedExpression && gte.right().foldable()) {
            Object bound = literalValueOf(gte.right());
            if (bound == null) {
                return false;
            }
            return noDictionaryValueSatisfiesComparison(dictionary, dictSize, primitiveType, bound, PredicateOp.GTE);
        }
        if (expr instanceof LessThan lt && lt.left() instanceof NamedExpression && lt.right().foldable()) {
            Object bound = literalValueOf(lt.right());
            if (bound == null) {
                return false;
            }
            return noDictionaryValueSatisfiesComparison(dictionary, dictSize, primitiveType, bound, PredicateOp.LT);
        }
        if (expr instanceof LessThanOrEqual lte && lte.left() instanceof NamedExpression && lte.right().foldable()) {
            Object bound = literalValueOf(lte.right());
            if (bound == null) {
                return false;
            }
            return noDictionaryValueSatisfiesComparison(dictionary, dictSize, primitiveType, bound, PredicateOp.LTE);
        }

        if (expr instanceof Range range && range.value() instanceof NamedExpression) {
            Object lower = literalValueOf(range.lower());
            Object upper = literalValueOf(range.upper());
            if (lower == null || upper == null) {
                return false;
            }
            return noDictionaryValueInRange(dictionary, dictSize, primitiveType, lower, upper, range.includeLower(), range.includeUpper());
        }

        if (expr instanceof StartsWith sw && sw.singleValueField() instanceof NamedExpression && sw.prefix().foldable()) {
            Object prefixVal = literalValueOf(sw.prefix());
            if (prefixVal == null) {
                return false;
            }
            return noDictionaryValueStartsWith(dictionary, dictSize, (BytesRef) prefixVal);
        }

        return false;
    }

    private static boolean noDictionaryValueEquals(Dictionary dict, int size, PrimitiveType type, Object target) {
        for (int i = 0; i < size; i++) {
            if (dictionaryValueEquals(dict, i, type, target)) {
                return false;
            }
        }
        return true;
    }

    private static boolean noDictionaryValueIn(Dictionary dict, int size, PrimitiveType type, Set<Object> targets) {
        for (int i = 0; i < size; i++) {
            for (Object target : targets) {
                if (dictionaryValueEquals(dict, i, type, target)) {
                    return false;
                }
            }
        }
        return true;
    }

    private static boolean allDictionaryValuesEqual(Dictionary dict, int size, PrimitiveType type, Object target) {
        for (int i = 0; i < size; i++) {
            if (dictionaryValueEquals(dict, i, type, target) == false) {
                return false;
            }
        }
        return true;
    }

    private static boolean noDictionaryValueSatisfiesComparison(
        Dictionary dict,
        int size,
        PrimitiveType type,
        Object bound,
        PredicateOp op
    ) {
        for (int i = 0; i < size; i++) {
            int cmp = compareDictionaryValue(dict, i, type, bound);
            if (cmp == Integer.MIN_VALUE) {
                return false;
            }
            boolean matches = switch (op) {
                case GT -> cmp > 0;
                case GTE -> cmp >= 0;
                case LT -> cmp < 0;
                case LTE -> cmp <= 0;
                case EQ, NOT_EQ -> throw new IllegalArgumentException("Use noDictionaryValueEquals for EQ/NOT_EQ");
            };
            if (matches) {
                return false;
            }
        }
        return true;
    }

    private static boolean noDictionaryValueInRange(
        Dictionary dict,
        int size,
        PrimitiveType type,
        Object lower,
        Object upper,
        boolean includeLower,
        boolean includeUpper
    ) {
        for (int i = 0; i < size; i++) {
            int cmpLower = compareDictionaryValue(dict, i, type, lower);
            int cmpUpper = compareDictionaryValue(dict, i, type, upper);
            if (cmpLower == Integer.MIN_VALUE || cmpUpper == Integer.MIN_VALUE) {
                return false;
            }
            boolean aboveLower = includeLower ? cmpLower >= 0 : cmpLower > 0;
            boolean belowUpper = includeUpper ? cmpUpper <= 0 : cmpUpper < 0;
            if (aboveLower && belowUpper) {
                return false;
            }
        }
        return true;
    }

    private static boolean noDictionaryValueStartsWith(Dictionary dict, int size, BytesRef prefix) {
        for (int i = 0; i < size; i++) {
            try {
                Binary bin = dict.decodeToBinary(i);
                byte[] bytes = bin.getBytes();
                if (bytes.length >= prefix.length) {
                    boolean match = true;
                    for (int j = 0; j < prefix.length; j++) {
                        if (bytes[j] != prefix.bytes[prefix.offset + j]) {
                            match = false;
                            break;
                        }
                    }
                    if (match) {
                        return false;
                    }
                }
            } catch (Exception e) {
                return false;
            }
        }
        return true;
    }

    /**
     * Compares a dictionary value at the given index against a target value.
     * Returns negative if dict &lt; target, 0 if equal, positive if dict &gt; target.
     * Returns {@link Integer#MIN_VALUE} if comparison is not possible.
     *
     * <p>For DATETIME columns, the target (ESQL epoch millis) is converted to the column's
     * physical unit before comparison, since dictionary values are already in physical units.
     */
    private static int compareDictionaryValue(Dictionary dict, int index, PrimitiveType type, Object target) {
        try {
            LogicalTypeAnnotation logical = type.getLogicalTypeAnnotation();
            return switch (type.getPrimitiveTypeName()) {
                case INT32 -> {
                    int dictVal = dict.decodeToInt(index);
                    if ((target instanceof Number) == false) {
                        yield Integer.MIN_VALUE;
                    }
                    int targetVal;
                    if (logical instanceof LogicalTypeAnnotation.DateLogicalTypeAnnotation) {
                        targetVal = (int) Math.floorDiv(((Number) target).longValue(), MILLIS_PER_DAY);
                    } else {
                        targetVal = ((Number) target).intValue();
                    }
                    yield Integer.compare(dictVal, targetVal);
                }
                case INT64 -> {
                    long dictVal = dict.decodeToLong(index);
                    if ((target instanceof Number) == false) {
                        yield Integer.MIN_VALUE;
                    }
                    long targetVal;
                    if (logical instanceof LogicalTypeAnnotation.TimestampLogicalTypeAnnotation) {
                        targetVal = convertMillisToPhysical(((Number) target).longValue(), logical);
                    } else {
                        targetVal = ((Number) target).longValue();
                    }
                    yield Long.compare(dictVal, targetVal);
                }
                case FLOAT -> {
                    if ((target instanceof Number) == false) {
                        yield Integer.MIN_VALUE;
                    }
                    yield Float.compare(dict.decodeToFloat(index), ((Number) target).floatValue());
                }
                case DOUBLE -> {
                    if ((target instanceof Number) == false) {
                        yield Integer.MIN_VALUE;
                    }
                    yield Double.compare(dict.decodeToDouble(index), ((Number) target).doubleValue());
                }
                case BINARY, FIXED_LEN_BYTE_ARRAY -> {
                    Binary bin = dict.decodeToBinary(index);
                    if (target instanceof BytesRef br) {
                        yield compareBinaryToBytes(bin, br);
                    }
                    yield Integer.MIN_VALUE;
                }
                default -> Integer.MIN_VALUE;
            };
        } catch (Exception e) {
            return Integer.MIN_VALUE;
        }
    }

    private static boolean dictionaryValueEquals(Dictionary dict, int index, PrimitiveType type, Object target) {
        return compareDictionaryValue(dict, index, type, target) == 0;
    }

    private static int compareBinaryToBytes(Binary bin, BytesRef bytesRef) {
        byte[] binBytes = bin.getBytes();
        return compareBytesUnsigned(binBytes, 0, binBytes.length, bytesRef.bytes, bytesRef.offset, bytesRef.length);
    }

    // -----------------------------------------------------------------------------------
    // Page-level skipping via ColumnIndex (Stage 2)
    // -----------------------------------------------------------------------------------

    /**
     * Evaluates all pushed expressions referencing the given column against a single page's
     * min/max from the ColumnIndex. Returns true if the page <em>might</em> contain matching
     * rows (safe default: true). Returns false only when we can prove no row in the page
     * can match.
     *
     * <p>Only simple leaf predicates are evaluated. Compound predicates (AND/OR/NOT) are
     * conservatively treated as "might match" to avoid incorrect page skipping.
     *
     * <p>DATETIME columns are supported: ESQL epoch millis literals are converted to the
     * column's physical unit (days for DATE/INT32, millis/micros/nanos for TIMESTAMP/INT64)
     * before comparison against ColumnIndex bounds. INT96 timestamps are not evaluated
     * (deprecated format, rarely has ColumnIndex). Overflow from extreme timestamps
     * (e.g., nanos beyond ~year 2262) safely falls back to "might match".
     *
     * @param columnName the column path
     * @param columnIndex the ColumnIndex for this column
     * @param pageIndex the page ordinal within the column chunk
     * @param primitiveType the Parquet primitive type
     * @return true if the page might contain matching rows
     */
    boolean pageCanMatch(String columnName, ColumnIndex columnIndex, int pageIndex, PrimitiveType primitiveType) {
        if (columnIndex.getNullPages().get(pageIndex)) {
            return true;
        }
        for (Expression expr : expressions) {
            if (isSimpleColumnPredicate(expr, columnName)) {
                if (pageRejectedByMinMax(expr, columnName, columnIndex, pageIndex, primitiveType) == false) {
                    continue;
                }
                return false;
            }
        }
        return true;
    }

    /**
     * Returns true if the page's min/max proves no value can match the expression.
     */
    private static boolean pageRejectedByMinMax(
        Expression expr,
        String columnName,
        ColumnIndex columnIndex,
        int pageIndex,
        PrimitiveType primitiveType
    ) {
        ByteBuffer minBuf = columnIndex.getMinValues().get(pageIndex);
        ByteBuffer maxBuf = columnIndex.getMaxValues().get(pageIndex);

        if (expr instanceof Equals eq && eq.left() instanceof NamedExpression && eq.right().foldable()) {
            Object target = literalValueOf(eq.right());
            if (target == null) {
                return false;
            }
            int cmpMin = compareValueToPageBound(target, minBuf, primitiveType);
            int cmpMax = compareValueToPageBound(target, maxBuf, primitiveType);
            if (cmpMin == Integer.MIN_VALUE || cmpMax == Integer.MIN_VALUE) {
                return false;
            }
            return cmpMin < 0 || cmpMax > 0;
        }

        if (expr instanceof NotEquals ne && ne.left() instanceof NamedExpression && ne.right().foldable()) {
            Object target = literalValueOf(ne.right());
            if (target == null) {
                return false;
            }
            int cmpMin = compareValueToPageBound(target, minBuf, primitiveType);
            int cmpMax = compareValueToPageBound(target, maxBuf, primitiveType);
            if (cmpMin == Integer.MIN_VALUE || cmpMax == Integer.MIN_VALUE) {
                return false;
            }
            return cmpMin == 0 && cmpMax == 0;
        }

        if (expr instanceof GreaterThan gt && gt.left() instanceof NamedExpression && gt.right().foldable()) {
            return pageRejectedByOrderedComparison(literalValueOf(gt.right()), maxBuf, primitiveType, PredicateOp.GT);
        }

        if (expr instanceof GreaterThanOrEqual gte && gte.left() instanceof NamedExpression && gte.right().foldable()) {
            return pageRejectedByOrderedComparison(literalValueOf(gte.right()), maxBuf, primitiveType, PredicateOp.GTE);
        }

        if (expr instanceof LessThan lt && lt.left() instanceof NamedExpression && lt.right().foldable()) {
            return pageRejectedByOrderedComparison(literalValueOf(lt.right()), minBuf, primitiveType, PredicateOp.LT);
        }

        if (expr instanceof LessThanOrEqual lte && lte.left() instanceof NamedExpression && lte.right().foldable()) {
            return pageRejectedByOrderedComparison(literalValueOf(lte.right()), minBuf, primitiveType, PredicateOp.LTE);
        }

        if (expr instanceof In inExpr && inExpr.value() instanceof NamedExpression) {
            for (Expression item : inExpr.list()) {
                Object val = literalValueOf(item);
                if (val == null) {
                    return false;
                }
                int cmpMin = compareValueToPageBound(val, minBuf, primitiveType);
                int cmpMax = compareValueToPageBound(val, maxBuf, primitiveType);
                if (cmpMin == Integer.MIN_VALUE || cmpMax == Integer.MIN_VALUE) {
                    return false;
                }
                if (cmpMin >= 0 && cmpMax <= 0) {
                    return false;
                }
            }
            return true;
        }

        if (expr instanceof Range range && range.value() instanceof NamedExpression) {
            Object lower = literalValueOf(range.lower());
            Object upper = literalValueOf(range.upper());
            if (lower == null || upper == null) {
                return false;
            }
            int upperVsMin = compareValueToPageBound(upper, minBuf, primitiveType);
            int lowerVsMax = compareValueToPageBound(lower, maxBuf, primitiveType);
            if (upperVsMin == Integer.MIN_VALUE || lowerVsMax == Integer.MIN_VALUE) {
                return false;
            }
            boolean upperBelowMin = range.includeUpper() ? upperVsMin < 0 : upperVsMin <= 0;
            boolean lowerAboveMax = range.includeLower() ? lowerVsMax > 0 : lowerVsMax >= 0;
            return upperBelowMin || lowerAboveMax;
        }

        if (expr instanceof StartsWith sw && sw.singleValueField() instanceof NamedExpression && sw.prefix().foldable()) {
            Object prefixVal = literalValueOf(sw.prefix());
            if (prefixVal == null) {
                return false;
            }
            BytesRef prefix = (BytesRef) prefixVal;
            BytesRef upperBound = StringPrefixUtils.nextPrefixUpperBound(prefix);
            int prefixVsMax = compareValueToPageBound(prefix, maxBuf, primitiveType);
            if (prefixVsMax == Integer.MIN_VALUE) {
                return false;
            }
            if (prefixVsMax > 0) {
                return true;
            }
            if (upperBound != null) {
                int upperVsMin = compareValueToPageBound(upperBound, minBuf, primitiveType);
                if (upperVsMin == Integer.MIN_VALUE) {
                    return false;
                }
                return upperVsMin <= 0;
            }
            return false;
        }

        return false;
    }

    /**
     * Checks if an ordered comparison (GT, GTE, LT, LTE) rejects a page based on its bound.
     * For GT/GTE, the relevant bound is max (if value &gt;= max, no row can be greater).
     * For LT/LTE, the relevant bound is min (if value &lt;= min, no row can be less).
     */
    private static boolean pageRejectedByOrderedComparison(Object value, ByteBuffer boundBuf, PrimitiveType primitiveType, PredicateOp op) {
        if (value == null) {
            return false;
        }
        int cmp = compareValueToPageBound(value, boundBuf, primitiveType);
        if (cmp == Integer.MIN_VALUE) {
            return false;
        }
        return switch (op) {
            case GT -> cmp >= 0;
            case GTE -> cmp > 0;
            case LT -> cmp <= 0;
            case LTE -> cmp < 0;
            case EQ, NOT_EQ -> false;
        };
    }

    /**
     * Compares a predicate value against a page boundary (min or max) from the ColumnIndex.
     * Returns negative if value &lt; bound, 0 if equal, positive if value &gt; bound.
     * Returns {@link Integer#MIN_VALUE} if comparison is not possible.
     *
     * <p>ColumnIndex stores min/max as raw byte buffers in the column's physical encoding.
     * For DATETIME columns, ESQL epoch millis are converted to the physical unit (days for
     * DATE/INT32, millis/micros/nanos for TIMESTAMP/INT64) before comparison. Overflow from
     * extreme timestamps safely returns {@link Integer#MIN_VALUE} ("can't compare" → "might match").
     */
    private static int compareValueToPageBound(Object value, ByteBuffer boundBuf, PrimitiveType primitiveType) {
        try {
            ByteBuffer buf = boundBuf.duplicate().order(ByteOrder.LITTLE_ENDIAN);
            LogicalTypeAnnotation logical = primitiveType.getLogicalTypeAnnotation();
            return switch (primitiveType.getPrimitiveTypeName()) {
                case INT32 -> {
                    int boundVal = buf.getInt();
                    if ((value instanceof Number) == false) {
                        yield Integer.MIN_VALUE;
                    }
                    int targetVal;
                    if (logical instanceof LogicalTypeAnnotation.DateLogicalTypeAnnotation) {
                        targetVal = (int) Math.floorDiv(((Number) value).longValue(), MILLIS_PER_DAY);
                    } else {
                        targetVal = ((Number) value).intValue();
                    }
                    yield Integer.compare(targetVal, boundVal);
                }
                case INT64 -> {
                    long boundVal = buf.getLong();
                    if ((value instanceof Number) == false) {
                        yield Integer.MIN_VALUE;
                    }
                    long targetVal;
                    if (logical instanceof LogicalTypeAnnotation.TimestampLogicalTypeAnnotation) {
                        targetVal = convertMillisToPhysical(((Number) value).longValue(), logical);
                    } else {
                        targetVal = ((Number) value).longValue();
                    }
                    yield Long.compare(targetVal, boundVal);
                }
                case FLOAT -> {
                    float boundVal = buf.getFloat();
                    if ((value instanceof Number) == false) {
                        yield Integer.MIN_VALUE;
                    }
                    yield Float.compare(((Number) value).floatValue(), boundVal);
                }
                case DOUBLE -> {
                    double boundVal = buf.getDouble();
                    if ((value instanceof Number) == false) {
                        yield Integer.MIN_VALUE;
                    }
                    yield Double.compare(((Number) value).doubleValue(), boundVal);
                }
                case BINARY, FIXED_LEN_BYTE_ARRAY -> {
                    byte[] boundBytes = new byte[buf.remaining()];
                    buf.get(boundBytes);
                    if (value instanceof BytesRef br) {
                        yield compareBytesUnsigned(br.bytes, br.offset, br.length, boundBytes, 0, boundBytes.length);
                    }
                    yield Integer.MIN_VALUE;
                }
                default -> Integer.MIN_VALUE;
            };
        } catch (Exception e) {
            return Integer.MIN_VALUE;
        }
    }

    private static int compareBytesUnsigned(byte[] a, int aOff, int aLen, byte[] b, int bOff, int bLen) {
        int len = Math.min(aLen, bLen);
        for (int i = 0; i < len; i++) {
            int cmp = (a[aOff + i] & 0xFF) - (b[bOff + i] & 0xFF);
            if (cmp != 0) {
                return cmp;
            }
        }
        return Integer.compare(aLen, bLen);
    }
}
