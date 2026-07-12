/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.multivalue;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.compute.ann.Evaluator;
import org.elasticsearch.compute.ann.Position;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.xpack.esql.EsqlIllegalArgumentException;
import org.elasticsearch.xpack.esql.capabilities.TranslationAware;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Nullability;
import org.elasticsearch.xpack.esql.core.expression.TypedAttribute;
import org.elasticsearch.xpack.esql.core.querydsl.query.Query;
import org.elasticsearch.xpack.esql.core.querydsl.query.RangeQuery;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.evaluator.mapper.EvaluatorMapper;
import org.elasticsearch.xpack.esql.expression.function.Example;
import org.elasticsearch.xpack.esql.expression.function.FunctionAppliesTo;
import org.elasticsearch.xpack.esql.expression.function.FunctionAppliesToLifecycle;
import org.elasticsearch.xpack.esql.expression.function.FunctionDefinition;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.expression.function.scalar.EsqlScalarFunction;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.elasticsearch.xpack.esql.optimizer.rules.physical.local.LucenePushdownPredicates;
import org.elasticsearch.xpack.esql.planner.PlannerUtils;
import org.elasticsearch.xpack.esql.planner.TranslatorHandler;

import java.io.IOException;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.List;

import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.FIRST;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.SECOND;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.THIRD;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isType;
import static org.elasticsearch.xpack.esql.expression.Foldables.literalValueOf;

/**
 * Returns {@code true} when a multivalue field has at least one value inside the inclusive range
 * {@code [lower, upper]}, and {@code false} otherwise — including when the field is null or empty.
 *
 * <p>It is two-valued by design (never returns null), which makes it compose correctly under the DSL leniency rules:
 * a range over a field a source does not have (null field) is {@code false}, so {@code NOT} of it is {@code true}
 * (match-all). It also gives the exact DSL any-value range semantics — an existential over the field's values —
 * which a {@code mv_min}/{@code mv_max} composition cannot: {@code [0,100]} does not match {@code [40,60]}.
 */
public class MvInRange extends EsqlScalarFunction implements EvaluatorMapper, TranslationAware {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Expression.class,
        "MvInRange",
        MvInRange::new
    );

    public static final FunctionDefinition DEFINITION = FunctionDefinition.def(MvInRange.class).ternary(MvInRange::new).name("mv_in_range");

    private final Expression field;
    private final Expression lower;
    private final Expression upper;

    @FunctionInfo(
        returnType = "boolean",
        briefSummary = "Checks if a multivalue field has any value within an inclusive numeric range.",
        description = "Returns `true` if at least one value of `field` is within the inclusive range `[lower, upper]`. "
            + "Null or empty fields return `false`.",
        examples = { @Example(file = "ints", tag = "mv_in_range") },
        appliesTo = { @FunctionAppliesTo(lifeCycle = FunctionAppliesToLifecycle.PREVIEW, version = "9.6.0") }
    )
    public MvInRange(
        Source source,
        @Param(name = "field", type = { "double", "integer", "long" }, description = "Multivalue expression to test.") Expression field,
        @Param(name = "lower", type = { "double", "integer", "long" }, description = "Inclusive lower bound.") Expression lower,
        @Param(name = "upper", type = { "double", "integer", "long" }, description = "Inclusive upper bound.") Expression upper
    ) {
        super(source, Arrays.asList(field, lower, upper));
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
        TypeResolution resolution = isType(field, MvInRange::isSupportedRangeType, sourceText(), FIRST, "integer, long or double");
        if (resolution.unresolved()) {
            return resolution;
        }
        resolution = isType(lower, MvInRange::isSupportedRangeType, sourceText(), SECOND, "integer, long or double");
        if (resolution.unresolved()) {
            return resolution;
        }
        return isType(upper, MvInRange::isSupportedRangeType, sourceText(), THIRD, "integer, long or double");
    }

    // The set of types this function currently supports. It must stay in lockstep with the @Param type lists, the
    // toEvaluator switch, and the test cases — the function test framework fails if any of them disagree, which is how
    // we guarantee no supported type is silently missing. Extending to the other ordered types (unsigned_long,
    // date/date_nanos, ip, version, keyword) is adding an evaluator + type here + a test case.
    private static boolean isSupportedRangeType(DataType dt) {
        return dt == DataType.INTEGER || dt == DataType.LONG || dt == DataType.DOUBLE;
    }

    @Evaluator(extraName = "Int", allNullsIsNull = false)
    static boolean process(@Position int position, IntBlock field, int lower, int upper) {
        int count = field.getValueCount(position);
        int start = field.getFirstValueIndex(position);
        for (int i = start; i < start + count; i++) {
            int v = field.getInt(i);
            if (v >= lower && v <= upper) {
                return true;
            }
        }
        return false;
    }

    @Evaluator(extraName = "Long", allNullsIsNull = false)
    static boolean process(@Position int position, LongBlock field, long lower, long upper) {
        int count = field.getValueCount(position);
        int start = field.getFirstValueIndex(position);
        for (int i = start; i < start + count; i++) {
            long v = field.getLong(i);
            if (v >= lower && v <= upper) {
                return true;
            }
        }
        return false;
    }

    @Evaluator(extraName = "Double", allNullsIsNull = false)
    static boolean process(@Position int position, DoubleBlock field, double lower, double upper) {
        int count = field.getValueCount(position);
        int start = field.getFirstValueIndex(position);
        for (int i = start; i < start + count; i++) {
            double v = field.getDouble(i);
            if (v >= lower && v <= upper) {
                return true;
            }
        }
        return false;
    }

    @Override
    public ExpressionEvaluator.Factory toEvaluator(ToEvaluator toEvaluator) {
        var f = toEvaluator.apply(field);
        var lo = toEvaluator.apply(lower);
        var hi = toEvaluator.apply(upper);
        return switch (PlannerUtils.toElementType(field.dataType())) {
            case INT -> new MvInRangeIntEvaluator.Factory(source(), f, lo, hi);
            case LONG -> new MvInRangeLongEvaluator.Factory(source(), f, lo, hi);
            case DOUBLE -> new MvInRangeDoubleEvaluator.Factory(source(), f, lo, hi);
            default -> throw EsqlIllegalArgumentException.illegalDataType(field.dataType());
        };
    }

    @Override
    public Translatable translatable(LucenePushdownPredicates pushdownPredicates) {
        // Lucene's numeric range is already any-value over multivalue, so the raw range query is the right push.
        // RECHECK keeps it correct as a conservative envelope test (esp. min/max stats pruning on datasets): the
        // engine's exact evaluator re-filters the surviving rows.
        if (pushdownPredicates.isPushableFieldAttribute(field) && lower.foldable() && upper.foldable()) {
            return Translatable.RECHECK;
        }
        return Translatable.NO;
    }

    @Override
    public Query asQuery(LucenePushdownPredicates pushdownPredicates, TranslatorHandler handler) {
        TypedAttribute attribute = LucenePushdownPredicates.checkIsPushableAttribute(field);
        String name = handler.nameOf(attribute);
        return new RangeQuery(source(), name, literalValueOf(lower), true, literalValueOf(upper), true, (ZoneId) null);
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
