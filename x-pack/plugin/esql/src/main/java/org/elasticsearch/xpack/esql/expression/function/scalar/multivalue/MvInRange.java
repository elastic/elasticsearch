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
import org.elasticsearch.compute.ann.Position;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.expression.ConstantEvaluators;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.xpack.esql.EsqlIllegalArgumentException;
import org.elasticsearch.xpack.esql.capabilities.TranslationAware;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
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
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.expression.function.scalar.EsqlScalarFunction;
import org.elasticsearch.xpack.esql.expression.predicate.Range;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.elasticsearch.xpack.esql.optimizer.rules.physical.local.LucenePushdownPredicates;
import org.elasticsearch.xpack.esql.planner.PlannerUtils;
import org.elasticsearch.xpack.esql.planner.TranslatorHandler;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.FIRST;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.SECOND;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.THIRD;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isType;

/**
 * Returns {@code true} when a multivalue field has at least one value inside the inclusive range
 * {@code [lower, upper]}, comparing with the natural order of the type, and {@code false} otherwise —
 * including when the field is null or empty, or when a bound is null.
 *
 * <p>Two properties make this function more than sugar over comparison operators:
 * <ul>
 *   <li><b>Any-value (existential) semantics.</b> The result is an existential over the field's values:
 *       {@code true} iff <em>some</em> value is in range. This is not the same as testing the field's
 *       envelope — {@code [0,100]} is not in {@code [40,60]} even though the envelope overlaps — so a
 *       {@code mv_min}/{@code mv_max} composition cannot express it. It matches how the query DSL
 *       {@code range} filter treats a multivalued field (a document matches if any value is in range).</li>
 *   <li><b>Two-valued (never null).</b> Unlike {@code <}/{@code <=} on a multivalued field, which return
 *       null, this returns only {@code true} or {@code false}. That lets it compose under {@code AND}/{@code OR}/
 *       {@code NOT} without a null bridge: a range over a field that a source does not have (a null field) is
 *       {@code false}, so {@code NOT} of it is {@code true}.</li>
 * </ul>
 */
public class MvInRange extends EsqlScalarFunction implements TranslationAware {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Expression.class,
        "MvInRange",
        MvInRange::new
    );

    public static final FunctionDefinition DEFINITION = FunctionDefinition.def(MvInRange.class).ternary(MvInRange::new).name("mv_in_range");

    private static final String SUPPORTED_TYPES = "a numeric, date, ip, version or string type";

    private final Expression field;
    private final Expression lower;
    private final Expression upper;

    @FunctionInfo(
        returnType = "boolean",
        briefSummary = "Checks whether a multivalue field has any value within an inclusive range.",
        description = "Returns `true` if at least one value of `field` is within the inclusive range `[lower, upper]`, "
            + "using the natural order of the type. A null or empty field returns `false`, as does a null bound. "
            + "Works on any ordered type: numbers, dates, IPs, versions, and strings (compared by their UTF-8 bytes).",
        examples = {
            @Example(file = "ints", tag = "mv_in_range"),
            @Example(
                description = "Neither `0` nor `100` is within `[40, 60]`, so the result is `false`.",
                file = "ints",
                tag = "mv_in_range_element_wise"
            ),
            @Example(description = "Strings are compared by their UTF-8 bytes:", file = "ints", tag = "mv_in_range_keyword") },
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
            description = "Inclusive lower bound, of the same type as `field`. If null or multivalued, the function returns `false`."
        ) Expression lower,
        @Param(
            name = "upper",
            type = { "date", "date_nanos", "double", "integer", "ip", "keyword", "long", "text", "unsigned_long", "version" },
            description = "Inclusive upper bound, of the same type as `field`. If null or multivalued, the function returns `false`."
        ) Expression upper
    ) {
        super(source, List.of(field, lower, upper));
        this.field = field;
        this.lower = lower;
        this.upper = upper;
    }

    private MvInRange(StreamInput in) throws IOException {
        this(
            Source.readFrom((PlanStreamInput) in),
            in.readNamedWriteable(Expression.class),
            in.readNamedWriteable(Expression.class),
            in.readNamedWriteable(Expression.class)
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        source().writeTo(out);
        out.writeNamedWriteable(field);
        out.writeNamedWriteable(lower);
        out.writeNamedWriteable(upper);
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
        // A null field folds to a constant false, so we don't constrain the bounds against it — just check they are
        // themselves range-able. Otherwise the bounds must share the field's type, so a single evaluator (chosen on the
        // field type) can read all three without a cast mismatch, exactly as MvContains requires of its subset.
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
            if (lower.dataType() != DataType.NULL
                && upper.dataType() != DataType.NULL
                && lower.dataType().noText() != upper.dataType().noText()) {
                return isType(
                    upper,
                    t -> t.noText() == lower.dataType().noText(),
                    sourceText(),
                    THIRD,
                    lower.dataType().noText().typeName()
                );
            }
            return resolution;
        }
        DataType fieldType = field.dataType().noText();
        resolution = isType(lower, t -> t.noText() == fieldType, sourceText(), SECOND, fieldType.typeName());
        if (resolution.unresolved()) {
            return resolution;
        }
        return isType(upper, t -> t.noText() == fieldType, sourceText(), THIRD, fieldType.typeName());
    }

    // The ordered types this function ranges over. It must stay in lockstep with the @Param type lists, the
    // toEvaluator switch, and the test cases (unit + MvInRangeErrorTests): the function test framework fails if any of
    // them disagree, which is how we guarantee no supported type is silently missing and no unsupported type slips
    // through. boolean is deliberately excluded — a range over booleans is not a meaningful query.
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
    static boolean process(@Position int position, IntBlock field, IntBlock lower, IntBlock upper) {
        if (hasSingleValue(lower, position) == false || hasSingleValue(upper, position) == false) {
            return false;
        }
        int lo = lower.getInt(lower.getFirstValueIndex(position));
        int hi = upper.getInt(upper.getFirstValueIndex(position));
        int count = field.getValueCount(position);
        int start = field.getFirstValueIndex(position);
        for (int i = start; i < start + count; i++) {
            int v = field.getInt(i);
            if (v >= lo && v <= hi) {
                return true;
            }
        }
        return false;
    }

    @Evaluator(extraName = "Long", allNullsIsNull = false)
    static boolean process(@Position int position, LongBlock field, LongBlock lower, LongBlock upper) {
        if (hasSingleValue(lower, position) == false || hasSingleValue(upper, position) == false) {
            return false;
        }
        long lo = lower.getLong(lower.getFirstValueIndex(position));
        long hi = upper.getLong(upper.getFirstValueIndex(position));
        int count = field.getValueCount(position);
        int start = field.getFirstValueIndex(position);
        for (int i = start; i < start + count; i++) {
            long v = field.getLong(i);
            if (v >= lo && v <= hi) {
                return true;
            }
        }
        return false;
    }

    @Evaluator(extraName = "Double", allNullsIsNull = false)
    static boolean process(@Position int position, DoubleBlock field, DoubleBlock lower, DoubleBlock upper) {
        if (hasSingleValue(lower, position) == false || hasSingleValue(upper, position) == false) {
            return false;
        }
        double lo = lower.getDouble(lower.getFirstValueIndex(position));
        double hi = upper.getDouble(upper.getFirstValueIndex(position));
        int count = field.getValueCount(position);
        int start = field.getFirstValueIndex(position);
        for (int i = start; i < start + count; i++) {
            double v = field.getDouble(i);
            if (v >= lo && v <= hi) {
                return true;
            }
        }
        return false;
    }

    @Evaluator(extraName = "BytesRef", allNullsIsNull = false)
    static boolean process(@Position int position, BytesRefBlock field, BytesRefBlock lower, BytesRefBlock upper) {
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
            if (v.compareTo(lo) >= 0 && v.compareTo(hi) <= 0) {
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
        // date/date_nanos/unsigned_long land on LONG and ip/version/keyword/text on BYTES_REF; resolveType has already
        // pinned the bounds to the field type, so the single per-element-type evaluator reads all three safely.
        return switch (PlannerUtils.toElementType(field.dataType())) {
            case INT -> new MvInRangeIntEvaluator.Factory(source(), f, lo, hi);
            case LONG -> new MvInRangeLongEvaluator.Factory(source(), f, lo, hi);
            case DOUBLE -> new MvInRangeDoubleEvaluator.Factory(source(), f, lo, hi);
            case BYTES_REF -> new MvInRangeBytesRefEvaluator.Factory(source(), f, lo, hi);
            default -> throw EsqlIllegalArgumentException.illegalDataType(field.dataType());
        };
    }

    @Override
    public Translatable translatable(LucenePushdownPredicates pushdownPredicates) {
        // We push the range to Lucene as a pre-filter and keep this evaluator as the authority (RECHECK). For that to be
        // sound — including under negation, where NOT pushes must_not(range) and rechecks the survivors — the pushed
        // query must never drop a true match, so we only push when the range is a faithful stand-in for the evaluator:
        // - text is excluded: its range query matches the analyzed tokens, not the whole string, so it is not even a
        // superset of what this evaluator compares;
        // - the bounds must be single-valued, non-null literals: a null bound would push an unbounded range (the
        // evaluator treats it as matching nothing) and a multivalued bound is unsupported — the same gate Equals and
        // the binary comparisons apply.
        if (field.dataType() == DataType.TEXT) {
            return Translatable.NO;
        }
        if (pushdownPredicates.isPushableFieldAttribute(field) && isPushableBound(lower) && isPushableBound(upper)) {
            return Translatable.RECHECK;
        }
        return Translatable.NO;
    }

    private static boolean isPushableBound(Expression bound) {
        return bound instanceof Literal literal && literal.value() != null && literal.value() instanceof Collection<?> == false;
    }

    @Override
    public Query asQuery(LucenePushdownPredicates pushdownPredicates, TranslatorHandler handler) {
        // Reuse Range's per-type bound formatting (dates, ip, version, unsigned_long) instead of duplicating it. Range
        // is SingleValueTranslationAware, but for a plain field attribute (not a keyed FieldExtract) its asQuery returns
        // the bare RangeQuery here — the single-value wrap is the framework's job for Range itself, not ours — which is
        // exactly the any-value range semantics we want over a multivalue field.
        return new Range(source(), field, lower, true, upper, true, null).asQuery(pushdownPredicates, handler);
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new MvInRange(source(), newChildren.get(0), newChildren.get(1), newChildren.get(2));
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, MvInRange::new, field, lower, upper);
    }

    public Expression field() {
        return field;
    }

    public Expression lower() {
        return lower;
    }

    public Expression upper() {
        return upper;
    }
}
