/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.multivalue;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.compute.ann.Evaluator;
import org.elasticsearch.compute.ann.Fixed;
import org.elasticsearch.compute.ann.Position;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.expression.ConstantEvaluators;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.xpack.esql.EsqlIllegalArgumentException;
import org.elasticsearch.xpack.esql.capabilities.TranslationAware;
import org.elasticsearch.xpack.esql.core.InvalidArgumentException;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.MapExpression;
import org.elasticsearch.xpack.esql.core.expression.Nullability;
import org.elasticsearch.xpack.esql.core.querydsl.query.Query;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.OptionalArgument;
import org.elasticsearch.xpack.esql.expression.function.Options;
import org.elasticsearch.xpack.esql.expression.function.scalar.EsqlScalarFunction;
import org.elasticsearch.xpack.esql.expression.predicate.Range;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.elasticsearch.xpack.esql.optimizer.rules.physical.local.LucenePushdownPredicates;
import org.elasticsearch.xpack.esql.planner.PlannerUtils;
import org.elasticsearch.xpack.esql.planner.TranslatorHandler;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.FIRST;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.SECOND;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.THIRD;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isType;

/**
 * Shared base for {@link MvGreater} and {@link MvLess}: any-value one-sided comparison, two-valued
 * (null/empty → {@code false}), Lucene range pushdown.
 */
public abstract class MvCompare extends EsqlScalarFunction implements OptionalArgument, TranslationAware {

    static final String SUPPORTED_TYPES = "date, date_nanos, double, integer, ip, keyword, long, text, unsigned_long or version";

    public static final String INCLUDE_BOUND = "include_bound";
    public static final Map<String, DataType> ALLOWED_OPTIONS = Map.of(INCLUDE_BOUND, DataType.BOOLEAN);

    private final Expression field;
    private final Expression bound;
    private final Expression options;
    /** {@code true} for {@link MvGreater}, {@code false} for {@link MvLess}. */
    private final boolean greater;

    protected MvCompare(Source source, Expression field, Expression bound, Expression options, boolean greater) {
        super(source, options == null ? List.of(field, bound) : List.of(field, bound, options));
        this.field = field;
        this.bound = bound;
        this.options = options;
        this.greater = greater;
    }

    protected MvCompare(StreamInput in, boolean greater) throws IOException {
        this(
            Source.readFrom((PlanStreamInput) in),
            in.readNamedWriteable(Expression.class),
            in.readNamedWriteable(Expression.class),
            in.readOptionalNamedWriteable(Expression.class),
            greater
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        source().writeTo(out);
        out.writeNamedWriteable(field);
        out.writeNamedWriteable(bound);
        out.writeOptionalNamedWriteable(options);
    }

    @Override
    public DataType dataType() {
        return DataType.BOOLEAN;
    }

    @Override
    public Nullability nullable() {
        return Nullability.FALSE;
    }

    @Override
    public boolean foldable() {
        return field.foldable() && bound.foldable();
    }

    @Override
    protected TypeResolution resolveType() {
        if (childrenResolved() == false) {
            return new TypeResolution("Unresolved children");
        }
        // Same types as GreaterThan / LessThan (excludes boolean).
        TypeResolution resolution = isType(field, MvCompare::isSupportedRangeType, sourceText(), FIRST, SUPPORTED_TYPES);
        if (resolution.unresolved()) {
            return resolution;
        }
        // Null field → constant false; bound only needs to be a supported type.
        if (field.dataType() == DataType.NULL) {
            resolution = isType(bound, MvCompare::isSupportedRangeType, sourceText(), SECOND, SUPPORTED_TYPES);
            if (resolution.unresolved()) {
                return resolution;
            }
            return resolution.and(resolveOptions());
        }
        DataType fieldType = field.dataType().noText();
        return isType(bound, t -> t.noText() == fieldType, sourceText(), SECOND, fieldType.typeName()).and(resolveOptions());
    }

    /** Rejects null option values that would NPE when unboxed. */
    private TypeResolution resolveOptions() {
        return Options.resolve(options, source(), THIRD, ALLOWED_OPTIONS, optionsMap -> {
            for (Map.Entry<String, Object> entry : optionsMap.entrySet()) {
                if (entry.getValue() == null) {
                    throw new InvalidArgumentException(
                        "Invalid option [" + entry.getKey() + "] in [" + sourceText() + "], a boolean value is required"
                    );
                }
            }
        });
    }

    /** Types accepted by {@code >}/{@code <}. Keep in sync with {@code @Param} and {@link #toEvaluator}. */
    static boolean isSupportedRangeType(DataType dt) {
        return dt == DataType.INTEGER
            || dt == DataType.LONG
            || dt == DataType.DOUBLE
            || dt == DataType.UNSIGNED_LONG
            || dt == DataType.DATETIME
            || dt == DataType.DATE_NANOS
            || dt == DataType.IP
            || dt == DataType.VERSION
            || dt == DataType.KEYWORD
            || dt == DataType.TEXT;
    }

    /** Null or multivalued bound → no match. */
    private static boolean hasSingleValue(Block bound, int position) {
        return bound.getValueCount(position) == 1;
    }

    @Evaluator(extraName = "Int", allNullsIsNull = false)
    static boolean process(@Position int position, IntBlock field, IntBlock bound, @Fixed boolean greater, @Fixed boolean includeBound) {
        if (hasSingleValue(bound, position) == false) {
            return false;
        }
        int b = bound.getInt(bound.getFirstValueIndex(position));
        int count = field.getValueCount(position);
        if (count == 0) {
            return false;
        }
        int start = field.getFirstValueIndex(position);
        // Ascending: min first, max last — only one value needed.
        if (field.mvSortedAscending()) {
            int v = field.getInt(greater ? start + count - 1 : start);
            return greater ? (includeBound ? v >= b : v > b) : (includeBound ? v <= b : v < b);
        }
        for (int i = start; i < start + count; i++) {
            int v = field.getInt(i);
            if (greater ? (includeBound ? v >= b : v > b) : (includeBound ? v <= b : v < b)) {
                return true;
            }
        }
        return false;
    }

    @Evaluator(extraName = "Long", allNullsIsNull = false)
    static boolean process(@Position int position, LongBlock field, LongBlock bound, @Fixed boolean greater, @Fixed boolean includeBound) {
        if (hasSingleValue(bound, position) == false) {
            return false;
        }
        long b = bound.getLong(bound.getFirstValueIndex(position));
        int count = field.getValueCount(position);
        if (count == 0) {
            return false;
        }
        int start = field.getFirstValueIndex(position);
        if (field.mvSortedAscending()) {
            long v = field.getLong(greater ? start + count - 1 : start);
            return greater ? (includeBound ? v >= b : v > b) : (includeBound ? v <= b : v < b);
        }
        for (int i = start; i < start + count; i++) {
            long v = field.getLong(i);
            if (greater ? (includeBound ? v >= b : v > b) : (includeBound ? v <= b : v < b)) {
                return true;
            }
        }
        return false;
    }

    @Evaluator(extraName = "Double", allNullsIsNull = false)
    static boolean process(
        @Position int position,
        DoubleBlock field,
        DoubleBlock bound,
        @Fixed boolean greater,
        @Fixed boolean includeBound
    ) {
        if (hasSingleValue(bound, position) == false) {
            return false;
        }
        double b = bound.getDouble(bound.getFirstValueIndex(position));
        int count = field.getValueCount(position);
        if (count == 0) {
            return false;
        }
        int start = field.getFirstValueIndex(position);
        if (field.mvSortedAscending()) {
            double v = field.getDouble(greater ? start + count - 1 : start);
            return greater ? (includeBound ? v >= b : v > b) : (includeBound ? v <= b : v < b);
        }
        for (int i = start; i < start + count; i++) {
            double v = field.getDouble(i);
            if (greater ? (includeBound ? v >= b : v > b) : (includeBound ? v <= b : v < b)) {
                return true;
            }
        }
        return false;
    }

    @Evaluator(extraName = "BytesRef", allNullsIsNull = false)
    static boolean process(
        @Position int position,
        BytesRefBlock field,
        BytesRefBlock bound,
        @Fixed boolean greater,
        @Fixed boolean includeBound
    ) {
        if (hasSingleValue(bound, position) == false) {
            return false;
        }
        BytesRef b = bound.getBytesRef(bound.getFirstValueIndex(position), new BytesRef());
        int count = field.getValueCount(position);
        if (count == 0) {
            return false;
        }
        int start = field.getFirstValueIndex(position);
        BytesRef scratch = new BytesRef();
        // Unsigned byte order — correct for keyword/text, ip, and version.
        if (field.mvSortedAscending()) {
            BytesRef v = field.getBytesRef(greater ? start + count - 1 : start, scratch);
            int cmp = v.compareTo(b);
            return greater ? (includeBound ? cmp >= 0 : cmp > 0) : (includeBound ? cmp <= 0 : cmp < 0);
        }
        for (int i = start; i < start + count; i++) {
            BytesRef v = field.getBytesRef(i, scratch);
            int cmp = v.compareTo(b);
            if (greater ? (includeBound ? cmp >= 0 : cmp > 0) : (includeBound ? cmp <= 0 : cmp < 0)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public final ExpressionEvaluator.Factory toEvaluator(ToEvaluator toEvaluator) {
        if (field.dataType() == DataType.NULL || bound.dataType() == DataType.NULL) {
            return ConstantEvaluators.CONSTANT_FALSE_FACTORY;
        }
        var f = toEvaluator.apply(field);
        var b = toEvaluator.apply(bound);
        boolean include = includeBound();
        return switch (PlannerUtils.toElementType(field.dataType())) {
            case INT -> new MvCompareIntEvaluator.Factory(source(), f, b, greater, include);
            case LONG -> new MvCompareLongEvaluator.Factory(source(), f, b, greater, include);
            case DOUBLE -> new MvCompareDoubleEvaluator.Factory(source(), f, b, greater, include);
            case BYTES_REF -> new MvCompareBytesRefEvaluator.Factory(source(), f, b, greater, include);
            default -> throw EsqlIllegalArgumentException.illegalDataType(field.dataType());
        };
    }

    /** Defaults to strict ({@code false}). */
    private boolean includeBound() {
        return (boolean) optionsMap().getOrDefault(INCLUDE_BOUND, Boolean.FALSE);
    }

    private Map<String, Object> optionsMap() {
        if (options == null) {
            return Map.of();
        }
        Map<String, Object> optionsMap = new HashMap<>();
        Options.populateMap((MapExpression) options, optionsMap, source(), THIRD, ALLOWED_OPTIONS);
        return optionsMap;
    }

    @Override
    public Translatable translatable(LucenePushdownPredicates pushdownPredicates) {
        // Text ranges match analyzed tokens, not whole values.
        if (field.dataType() == DataType.TEXT) {
            return Translatable.NO;
        }
        if (pushdownPredicates.isPushableFieldAttribute(field) && isPushableBound(bound)) {
            // Exact types (integral + ip/version/keyword) push a faithful range, so drop the filter (YES) and allow
            // must_not(range) under negation. DOUBLE stays RECHECK — push a superset and re-check: reduced-precision
            // mappers (float/half_float/scaled_float) round the bound outward. RECHECK negation cannot be pushed.
            return isExactRangeType() ? Translatable.YES : Translatable.RECHECK_BUT_NO_NEGATED;
        }
        return Translatable.NO;
    }

    /** True when the pushed Lucene range matches the evaluator exactly (integral types, plus ip/version/keyword). */
    private boolean isExactRangeType() {
        var elementType = PlannerUtils.toElementType(field.dataType());
        if (elementType == ElementType.INT || elementType == ElementType.LONG) {
            return true;
        }
        if (elementType == ElementType.BYTES_REF) {
            DataType dt = field.dataType();
            return dt == DataType.IP || dt == DataType.VERSION || dt == DataType.KEYWORD;
        }
        return false;
    }

    private static boolean isPushableBound(Expression bound) {
        return bound instanceof Literal literal && literal.value() != null && literal.value() instanceof Collection<?> == false;
    }

    @Override
    public Query asQuery(LucenePushdownPredicates pushdownPredicates, TranslatorHandler handler) {
        // Reuse Range's per-type bound formatting (dates, ip, version, unsigned_long). One-sided:
        // open end is Literal.NULL. Exact types (integral + ip/version/keyword) keep real inclusivity;
        // DOUBLE (RECHECK) pushes inclusive so mapper rounding cannot drop a restore-able row.
        // Bare RangeQuery = any-value over MV.
        boolean exact = isExactRangeType();
        boolean include = exact ? includeBound() : true;
        Expression pushBound = widenZeroBound(bound);
        if (greater) {
            return new Range(source(), field, pushBound, include, Literal.NULL, false, null).asQuery(pushdownPredicates, handler);
        }
        return new Range(source(), field, Literal.NULL, false, pushBound, include, null).asQuery(pushdownPredicates, handler);
    }

    /**
     * Lucene orders {@code -0.0} below {@code +0.0}; the evaluator treats them equal.
     * Widen a zero bound so the pushed range stays a superset.
     */
    private Expression widenZeroBound(Expression bound) {
        if (field.dataType() == DataType.DOUBLE && bound instanceof Literal literal && literal.value() instanceof Double d && d == 0.0) {
            return Literal.of(literal, greater ? -0.0 : 0.0);
        }
        return bound;
    }

    public Expression field() {
        return field;
    }

    public Expression bound() {
        return bound;
    }

    public Expression options() {
        return options;
    }

    boolean greater() {
        return greater;
    }
}
