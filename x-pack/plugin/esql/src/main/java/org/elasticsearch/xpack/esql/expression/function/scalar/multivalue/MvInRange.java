/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.multivalue;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
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
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.Example;
import org.elasticsearch.xpack.esql.expression.function.FunctionAppliesTo;
import org.elasticsearch.xpack.esql.expression.function.FunctionAppliesToLifecycle;
import org.elasticsearch.xpack.esql.expression.function.FunctionDefinition;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.MapParam;
import org.elasticsearch.xpack.esql.expression.function.OptionalArgument;
import org.elasticsearch.xpack.esql.expression.function.Options;
import org.elasticsearch.xpack.esql.expression.function.Param;
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
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.FOURTH;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.SECOND;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.THIRD;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isType;

/**
 * Returns {@code true} when a multivalue field has at least one value inside the inclusive range
 * {@code [lower, upper]} (natural order of the type), else {@code false} — including a null/empty field or a null bound.
 *
 * <p>Two properties set it apart from comparison operators: any-value (existential) semantics, so {@code [0,100]} is
 * not in {@code [40,60]} and an {@code mv_min}/{@code mv_max} composition cannot express it; and two-valued (never
 * null), so it composes under {@code AND}/{@code OR}/{@code NOT} — the negation of a range over a missing field is
 * {@code true}.
 */
public class MvInRange extends EsqlScalarFunction implements OptionalArgument, TranslationAware {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Expression.class,
        "MvInRange",
        MvInRange::new
    );

    public static final FunctionDefinition DEFINITION = FunctionDefinition.def(MvInRange.class)
        .quaternary(MvInRange::new)
        .name("mv_in_range");

    private static final String SUPPORTED_TYPES = "a numeric, date, ip, version or string type";

    private static final String INCLUDE_LOWER = "include_lower";
    private static final String INCLUDE_UPPER = "include_upper";
    public static final Map<String, DataType> ALLOWED_OPTIONS = Map.of(INCLUDE_LOWER, DataType.BOOLEAN, INCLUDE_UPPER, DataType.BOOLEAN);

    private final Expression field;
    private final Expression lower;
    private final Expression upper;
    private final Expression options;

    @FunctionInfo(
        returnType = "boolean",
        briefSummary = "Checks whether a multivalue field has any value within a range.",
        description = "Returns `true` if at least one value of `field` is within the range `[lower, upper]`, "
            + "using the natural order of the type. A null or empty field returns `false`, as does a null bound. "
            + "Both bounds are inclusive by default; set `include_lower` or `include_upper` to `false` in the optional "
            + "`options` map to make either bound exclusive, covering all four interval forms. "
            + "Works on any ordered type: numbers, dates, IPs, versions, and strings (compared by their UTF-8 bytes).",
        examples = {
            @Example(file = "ints", tag = "mv_in_range"),
            @Example(
                description = "Neither `0` nor `100` is within `[40, 60]`, so the result is `false`.",
                file = "ints",
                tag = "mv_in_range_element_wise"
            ),
            @Example(description = "Strings are compared by their UTF-8 bytes:", file = "ints", tag = "mv_in_range_keyword"),
            @Example(
                description = "A half-open range `[4, 6)`: `6` sits on the excluded upper bound, so it does not match.",
                file = "ints",
                tag = "mv_in_range_half_open"
            ),
            @Example(
                description = "An open range `(4, 6)`: values equal to either bound are excluded.",
                file = "ints",
                tag = "mv_in_range_open"
            ) },
        preview = true,
        appliesTo = { @FunctionAppliesTo(lifeCycle = FunctionAppliesToLifecycle.PREVIEW, version = "9.6.0") }
    )
    public MvInRange(
        Source source,
        @Param(
            name = "field",
            type = { "date", "date_nanos", "double", "integer", "ip", "keyword", "long", "text", "unsigned_long", "version" },
            description = "Multivalue expression to test. If null or empty, the function returns `false`."
        ) Expression field,
        @Param(
            name = "lower",
            type = { "date", "date_nanos", "double", "integer", "ip", "keyword", "long", "text", "unsigned_long", "version" },
            description = "Lower bound, of the same type as `field`. If null or multivalued, the function returns `false`."
        ) Expression lower,
        @Param(
            name = "upper",
            type = { "date", "date_nanos", "double", "integer", "ip", "keyword", "long", "text", "unsigned_long", "version" },
            description = "Upper bound, of the same type as `field`. If null or multivalued, the function returns `false`."
        ) Expression upper,
        @MapParam(
            name = "options",
            params = {
                @MapParam.MapParamEntry(
                    name = "include_lower",
                    type = "boolean",
                    valueHint = { "true", "false" },
                    description = "Whether the lower bound is inclusive. Defaults to `true`; `false` makes it exclusive (`value > lower`)."
                ),
                @MapParam.MapParamEntry(
                    name = "include_upper",
                    type = "boolean",
                    valueHint = { "true", "false" },
                    description = "Whether the upper bound is inclusive. Defaults to `true`; `false` makes it exclusive (`value < upper`)."
                ) },
            description = "(Optional) Range boundary options.",
            optional = true
        ) Expression options
    ) {
        super(source, options == null ? List.of(field, lower, upper) : List.of(field, lower, upper, options));
        this.field = field;
        this.lower = lower;
        this.upper = upper;
        this.options = options;
    }

    public MvInRange(Source source, Expression field, Expression lower, Expression upper) {
        this(source, field, lower, upper, null);
    }

    private MvInRange(StreamInput in) throws IOException {
        this(
            Source.readFrom((PlanStreamInput) in),
            in.readNamedWriteable(Expression.class),
            in.readNamedWriteable(Expression.class),
            in.readNamedWriteable(Expression.class),
            in.readOptionalNamedWriteable(Expression.class)
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        source().writeTo(out);
        out.writeNamedWriteable(field);
        out.writeNamedWriteable(lower);
        out.writeNamedWriteable(upper);
        // Options change the evaluator's answer (unlike Match, whose options are baked into a pushed query), so they must
        // survive the wire — a dropped options map would silently misbind include_lower/include_upper on a remote node.
        out.writeOptionalNamedWriteable(options);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
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
        return field.foldable() && lower.foldable() && upper.foldable();
    }

    @Override
    protected TypeResolution resolveType() {
        if (childrenResolved() == false) {
            return new TypeResolution("Unresolved children");
        }
        TypeResolution resolution = isType(field, MvInRange::isSupportedRangeType, sourceText(), FIRST, SUPPORTED_TYPES);
        if (resolution.unresolved()) {
            return resolution;
        }
        // A null field folds to constant false, so the bounds only need to be range-able themselves; otherwise they must
        // share the field's type, so one evaluator (chosen on the field type) reads all three without a cast mismatch.
        if (field.dataType() == DataType.NULL) {
            resolution = isType(lower, MvInRange::isSupportedRangeType, sourceText(), SECOND, SUPPORTED_TYPES);
            if (resolution.unresolved()) {
                return resolution;
            }
            resolution = isType(upper, MvInRange::isSupportedRangeType, sourceText(), THIRD, SUPPORTED_TYPES);
            if (resolution.unresolved()) {
                return resolution;
            }
            // With a null field there is no field type to anchor to, so the bounds must agree with each other
            // (keyword and text agree, as everywhere else).
            TypeResolution optionsResolution = resolveOptions();
            if (lower.dataType() != DataType.NULL
                && upper.dataType() != DataType.NULL
                && lower.dataType().noText() != upper.dataType().noText()) {
                return isType(
                    upper,
                    t -> t.noText() == lower.dataType().noText(),
                    sourceText(),
                    THIRD,
                    lower.dataType().noText().typeName()
                ).and(optionsResolution);
            }
            return resolution.and(optionsResolution);
        }
        DataType fieldType = field.dataType().noText();
        resolution = isType(lower, t -> t.noText() == fieldType, sourceText(), SECOND, fieldType.typeName());
        if (resolution.unresolved()) {
            return resolution;
        }
        return isType(upper, t -> t.noText() == fieldType, sourceText(), THIRD, fieldType.typeName()).and(resolveOptions());
    }

    /**
     * Validates the options map: known keys, boolean values, and — beyond what {@link Options#resolve} checks — a non-null
     * value for each key. A {@code null} value parses and would otherwise unbox to an NPE when the flag is read at planning
     * time, so it is rejected here with a friendly error instead.
     */
    private TypeResolution resolveOptions() {
        return Options.resolve(options, source(), FOURTH, ALLOWED_OPTIONS, optionsMap -> {
            for (Map.Entry<String, Object> entry : optionsMap.entrySet()) {
                if (entry.getValue() == null) {
                    throw new InvalidArgumentException(
                        "Invalid option [" + entry.getKey() + "] in [" + sourceText() + "], a boolean value is required"
                    );
                }
            }
        });
    }

    // Must stay in lockstep with the @Param type lists, the toEvaluator switch, and the tests (unit + MvInRangeErrorTests):
    // the function-test framework fails if they disagree, so no supported type is silently missing. boolean is excluded —
    // a range over booleans is not meaningful. The options map has its own parallel chain: the @MapParam entries, the
    // ALLOWED_OPTIONS map, the two @Fixed kernel flags, and the VerifierTests option-error cases must all agree.
    private static boolean isSupportedRangeType(DataType dt) {
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

    /**
     * Reads a single scalar bound out of a (usually constant) block. Returns {@code false} for a null or multivalued
     * bound so the caller can short-circuit: an undefined range matches nothing, which keeps the two-valued contract.
     */
    private static boolean hasSingleValue(Block bound, int position) {
        return bound.getValueCount(position) == 1;
    }

    @Evaluator(extraName = "Int", allNullsIsNull = false)
    static boolean process(
        @Position int position,
        IntBlock field,
        IntBlock lower,
        IntBlock upper,
        @Fixed boolean includeLower,
        @Fixed boolean includeUpper
    ) {
        if (hasSingleValue(lower, position) == false || hasSingleValue(upper, position) == false) {
            return false;
        }
        int lo = lower.getInt(lower.getFirstValueIndex(position));
        int hi = upper.getInt(upper.getFirstValueIndex(position));
        int count = field.getValueCount(position);
        int start = field.getFirstValueIndex(position);
        for (int i = start; i < start + count; i++) {
            int v = field.getInt(i);
            if ((includeLower ? v >= lo : v > lo) && (includeUpper ? v <= hi : v < hi)) {
                return true;
            }
        }
        return false;
    }

    @Evaluator(extraName = "Long", allNullsIsNull = false)
    static boolean process(
        @Position int position,
        LongBlock field,
        LongBlock lower,
        LongBlock upper,
        @Fixed boolean includeLower,
        @Fixed boolean includeUpper
    ) {
        if (hasSingleValue(lower, position) == false || hasSingleValue(upper, position) == false) {
            return false;
        }
        long lo = lower.getLong(lower.getFirstValueIndex(position));
        long hi = upper.getLong(upper.getFirstValueIndex(position));
        int count = field.getValueCount(position);
        int start = field.getFirstValueIndex(position);
        for (int i = start; i < start + count; i++) {
            long v = field.getLong(i);
            if ((includeLower ? v >= lo : v > lo) && (includeUpper ? v <= hi : v < hi)) {
                return true;
            }
        }
        return false;
    }

    @Evaluator(extraName = "Double", allNullsIsNull = false)
    static boolean process(
        @Position int position,
        DoubleBlock field,
        DoubleBlock lower,
        DoubleBlock upper,
        @Fixed boolean includeLower,
        @Fixed boolean includeUpper
    ) {
        if (hasSingleValue(lower, position) == false || hasSingleValue(upper, position) == false) {
            return false;
        }
        double lo = lower.getDouble(lower.getFirstValueIndex(position));
        double hi = upper.getDouble(upper.getFirstValueIndex(position));
        int count = field.getValueCount(position);
        int start = field.getFirstValueIndex(position);
        for (int i = start; i < start + count; i++) {
            double v = field.getDouble(i);
            if ((includeLower ? v >= lo : v > lo) && (includeUpper ? v <= hi : v < hi)) {
                return true;
            }
        }
        return false;
    }

    @Evaluator(extraName = "BytesRef", allNullsIsNull = false)
    static boolean process(
        @Position int position,
        BytesRefBlock field,
        BytesRefBlock lower,
        BytesRefBlock upper,
        @Fixed boolean includeLower,
        @Fixed boolean includeUpper
    ) {
        if (hasSingleValue(lower, position) == false || hasSingleValue(upper, position) == false) {
            return false;
        }
        BytesRef lo = lower.getBytesRef(lower.getFirstValueIndex(position), new BytesRef());
        BytesRef hi = upper.getBytesRef(upper.getFirstValueIndex(position), new BytesRef());
        int count = field.getValueCount(position);
        int start = field.getFirstValueIndex(position);
        BytesRef scratch = new BytesRef();
        for (int i = start; i < start + count; i++) {
            BytesRef v = field.getBytesRef(i, scratch);
            // BytesRef.compareTo is an unsigned byte-by-byte comparison, which is the correct order for keyword/text
            // (UTF-8), ip (fixed-width encoding), and version (order-preserving encoding).
            boolean aboveLower = includeLower ? v.compareTo(lo) >= 0 : v.compareTo(lo) > 0;
            boolean belowUpper = includeUpper ? v.compareTo(hi) <= 0 : v.compareTo(hi) < 0;
            if (aboveLower && belowUpper) {
                return true;
            }
        }
        return false;
    }

    @Override
    public ExpressionEvaluator.Factory toEvaluator(ToEvaluator toEvaluator) {
        // A null-typed argument makes the whole predicate constant false: a null field is out of range, and a null bound
        // leaves the range undefined. Runtime nulls in a non-null-typed argument are handled inside the evaluators.
        if (field.dataType() == DataType.NULL || lower.dataType() == DataType.NULL || upper.dataType() == DataType.NULL) {
            return ConstantEvaluators.CONSTANT_FALSE_FACTORY;
        }
        var f = toEvaluator.apply(field);
        var lo = toEvaluator.apply(lower);
        var hi = toEvaluator.apply(upper);
        var optionsMap = optionsMap();
        boolean il = (boolean) optionsMap.getOrDefault(INCLUDE_LOWER, Boolean.TRUE);
        boolean iu = (boolean) optionsMap.getOrDefault(INCLUDE_UPPER, Boolean.TRUE);
        // date/date_nanos/unsigned_long land on LONG and ip/version/keyword/text on BYTES_REF; resolveType has already
        // pinned the bounds to the field type, so the single per-element-type evaluator reads all three safely.
        return switch (PlannerUtils.toElementType(field.dataType())) {
            case INT -> new MvInRangeIntEvaluator.Factory(source(), f, lo, hi, il, iu);
            case LONG -> new MvInRangeLongEvaluator.Factory(source(), f, lo, hi, il, iu);
            case DOUBLE -> new MvInRangeDoubleEvaluator.Factory(source(), f, lo, hi, il, iu);
            case BYTES_REF -> new MvInRangeBytesRefEvaluator.Factory(source(), f, lo, hi, il, iu);
            default -> throw EsqlIllegalArgumentException.illegalDataType(field.dataType());
        };
    }

    /**
     * Parses the options map once (callers read both flags with {@code getOrDefault(key, true)}). Returns an empty map
     * when there are no options. Resolved lazily (never in the constructor) so {@code resolveType} reports a friendly
     * error before this runs.
     */
    private Map<String, Object> optionsMap() {
        if (options == null) {
            return Map.of();
        }
        Map<String, Object> optionsMap = new HashMap<>();
        Options.populateMap((MapExpression) options, optionsMap, source(), FOURTH, ALLOWED_OPTIONS);
        return optionsMap;
    }

    @Override
    public Translatable translatable(LucenePushdownPredicates pushdownPredicates) {
        // Push only where the range faithfully stands in for this evaluator: text is excluded (its range matches analyzed
        // tokens, not the whole value), and the bounds must be single-valued non-null literals (the same gate the
        // comparison operators apply).
        if (field.dataType() == DataType.TEXT) {
            return Translatable.NO;
        }
        if (pushdownPredicates.isPushableFieldAttribute(field) && isPushableBound(lower) && isPushableBound(upper)) {
            // Integral types push an exact range, so drop the filter (YES). Everything else stays RECHECK — push a superset
            // range and re-check the surfaced rows: the double family widens reduced-precision mappers (float/half_float/
            // scaled_float) that round the bound outward, an over-match the retained evaluator removes; byte-encoded types
            // (ip/version/keyword) push a faithful range but are kept conservatively out of the exact-YES set. Because this
            // predicate is two-valued, RECHECK's negation cannot be pushed: must_not(superset) is a subset of the true
            // complement and the recheck can only drop rows, not restore them — so RECHECK_BUT_NO_NEGATED.
            return isExactRangeType() ? Translatable.YES : Translatable.RECHECK_BUT_NO_NEGATED;
        }
        return Translatable.NO;
    }

    /** Integral element types (integer, long, date, date_nanos, unsigned_long) whose pushed range matches the evaluator exactly. */
    private boolean isExactRangeType() {
        var elementType = PlannerUtils.toElementType(field.dataType());
        return elementType == ElementType.INT || elementType == ElementType.LONG;
    }

    private static boolean isPushableBound(Expression bound) {
        return bound instanceof Literal literal && literal.value() != null && literal.value() instanceof Collection<?> == false;
    }

    @Override
    public Query asQuery(LucenePushdownPredicates pushdownPredicates, TranslatorHandler handler) {
        // Reuse Range's per-type bound formatting (dates, ip, version, unsigned_long). For a plain field attribute Range
        // returns the bare RangeQuery (its single-value wrap is the framework's job, not ours) — exactly the any-value
        // range semantics we want over a multivalue field.
        //
        // The pushed range must stay a superset of the evaluator's matches. Integral types push the real inclusivity (gt/lt
        // is exact on the discrete domain, so YES can drop the filter). For the RECHECK types the range is only a pre-filter
        // and the retained evaluator applies the exclusivity, so we push both bounds INCLUSIVE regardless of the options: an
        // exclusive bound on a reduced-precision mapper (float/half_float/scaled_float, all widened to DOUBLE) rounds inward
        // and would drop a boundary row that RECHECK could never restore. Inclusive keeps the range a true superset.
        boolean exact = isExactRangeType();
        var optionsMap = exact ? optionsMap() : Map.<String, Object>of();
        boolean pushLower = exact ? (boolean) optionsMap.getOrDefault(INCLUDE_LOWER, Boolean.TRUE) : true;
        boolean pushUpper = exact ? (boolean) optionsMap.getOrDefault(INCLUDE_UPPER, Boolean.TRUE) : true;
        return new Range(source(), field, widenZeroBound(lower, true), pushLower, widenZeroBound(upper, false), pushUpper, null).asQuery(
            pushdownPredicates,
            handler
        );
    }

    /**
     * Lucene sorts {@code -0.0} below {@code +0.0}, but this evaluator treats them equal. A double range is always pushed
     * with inclusive bounds (it is a RECHECK pre-filter — see {@link #asQuery}), so widen a {@code 0.0} bound outward to the
     * signed zero that includes both zeros ({@code -0.0} lower, {@code +0.0} upper), keeping the pushed range a superset.
     * Only doubles have a signed zero.
     */
    private Expression widenZeroBound(Expression bound, boolean isLower) {
        if (field.dataType() == DataType.DOUBLE && bound instanceof Literal literal && literal.value() instanceof Double d && d == 0.0) {
            return Literal.of(literal, isLower ? -0.0 : 0.0);
        }
        return bound;
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new MvInRange(
            source(),
            newChildren.get(0),
            newChildren.get(1),
            newChildren.get(2),
            newChildren.size() > 3 ? newChildren.get(3) : null
        );
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, MvInRange::new, field, lower, upper, options);
    }

    public Expression field() {
        return field;
    }

    public Expression options() {
        return options;
    }

    public Expression lower() {
        return lower;
    }

    public Expression upper() {
        return upper;
    }
}
