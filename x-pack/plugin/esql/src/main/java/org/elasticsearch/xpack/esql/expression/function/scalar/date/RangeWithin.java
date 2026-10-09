/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.date;

import org.elasticsearch.common.geo.ShapeRelation;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.compute.ann.Evaluator;
import org.elasticsearch.compute.data.DoubleRangeBlockBuilder;
import org.elasticsearch.compute.data.LongRangeBlockBuilder;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.xpack.esql.capabilities.TranslationAware;
import org.elasticsearch.xpack.esql.core.expression.AnyNullIsNull;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.TypedAttribute;
import org.elasticsearch.xpack.esql.core.querydsl.query.Query;
import org.elasticsearch.xpack.esql.core.querydsl.query.RangeQuery;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.Example;
import org.elasticsearch.xpack.esql.expression.function.FunctionAppliesTo;
import org.elasticsearch.xpack.esql.expression.function.FunctionAppliesToLifecycle;
import org.elasticsearch.xpack.esql.expression.function.FunctionDefinition;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.expression.function.Signature;
import org.elasticsearch.xpack.esql.expression.function.scalar.EsqlScalarFunction;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.elasticsearch.xpack.esql.optimizer.rules.physical.local.LucenePushdownPredicates;
import org.elasticsearch.xpack.esql.planner.TranslatorHandler;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.FIRST;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.SECOND;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isType;
import static org.elasticsearch.xpack.esql.core.type.DataType.DATETIME;
import static org.elasticsearch.xpack.esql.core.type.DataType.DATE_RANGE;
import static org.elasticsearch.xpack.esql.core.type.DataType.DOUBLE;
import static org.elasticsearch.xpack.esql.core.type.DataType.DOUBLE_RANGE;
import static org.elasticsearch.xpack.esql.expression.Foldables.literalValueOf;
import static org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter.DEFAULT_DATE_TIME_FORMATTER;
import static org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter.dateTimeToString;

/**
 * {@code RANGE_WITHIN(value, range) -> boolean}.
 * Returns true if the first argument is within the second (the range).
 * Supported signatures:
 * <ul>
 *   <li>(date, date_range): point within range</li>
 *   <li>(date_range, date_range): first range within second (first fully contained by second)</li>
 *   <li>(double, double_range): point within range</li>
 *   <li>(double_range, double_range): first range within second (first fully contained by second)</li>
 * </ul>
 * The two arguments must belong to the same range family.
 */
public class RangeWithin extends EsqlScalarFunction implements TranslationAware, AnyNullIsNull {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Expression.class,
        "RangeWithin",
        RangeWithin::new
    );
    public static final FunctionDefinition DEFINITION = FunctionDefinition.def(RangeWithin.class)
        .binary(RangeWithin::new)
        .name("range_within");

    private final Expression left;
    private final Expression right;

    @FunctionInfo(
        returnType = "boolean",
        signatures = {
            @Signature(params = { "date|date_range", "date_range" }, returnType = "boolean"),
            @Signature(params = { "double|double_range", "double_range" }, returnType = "boolean") },
        preview = true,
        appliesTo = { @FunctionAppliesTo(lifeCycle = FunctionAppliesToLifecycle.PREVIEW, version = "9.5.0") },
        briefSummary = "Returns true if a date or date range falls within another date range.",
        description = "Returns true if the first argument is "
            + "[within](https://www.elastic.co/docs/reference/query-languages/query-dsl/query-dsl-range-query) "
            + "the second argument. "
            + "Supports (date, date_range) and (date_range, date_range). The second argument must be a date_range.",
        examples = @Example(file = "date_range", tag = "rangeWithin", explanation = "Filter events within a specific date range")
    )
    public RangeWithin(
        Source source,
        @Param(
            name = "left",
            type = { "date", "date_range", "double", "double_range" },
            description = "Value to test (point or range)."
        ) Expression left,
        @Param(name = "right", type = { "date_range", "double_range" }, description = "Container range.") Expression right
    ) {
        super(source, List.of(left, right));
        this.left = left;
        this.right = right;
    }

    private RangeWithin(StreamInput in) throws IOException {
        this(Source.readFrom((PlanStreamInput) in), in.readNamedWriteable(Expression.class), in.readNamedWriteable(Expression.class));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        source().writeTo(out);
        out.writeNamedWriteable(left);
        out.writeNamedWriteable(right);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    public Expression left() {
        return left;
    }

    public Expression right() {
        return right;
    }

    @Override
    public DataType dataType() {
        return DataType.BOOLEAN;
    }

    @Override
    public boolean foldable() {
        return Expressions.foldable(children());
    }

    @Override
    public Object fold(FoldContext foldContext) {
        if (foldable() == false) {
            return this;
        }

        Object leftValue = left.fold(foldContext);
        Object rightValue = right.fold(foldContext);

        if (leftValue == null || rightValue == null) {
            return null;
        }

        if (right.dataType() == DOUBLE_RANGE) {
            if (left.dataType() == DOUBLE_RANGE) {
                return processRange((DoubleRangeBlockBuilder.DoubleRange) leftValue, (DoubleRangeBlockBuilder.DoubleRange) rightValue);
            }
            return processPoint((Double) leftValue, (DoubleRangeBlockBuilder.DoubleRange) rightValue);
        }
        if (left.dataType() == DATE_RANGE) {
            return processRange((LongRangeBlockBuilder.LongRange) leftValue, (LongRangeBlockBuilder.LongRange) rightValue);
        }
        return processPoint((Long) leftValue, (LongRangeBlockBuilder.LongRange) rightValue);
    }

    @Evaluator(extraName = "Point")
    static boolean processPoint(long point, LongRangeBlockBuilder.LongRange range) {
        // Range is [from, to); to is exclusive.
        return point >= range.from() && point < range.to();
    }

    @Evaluator(extraName = "Range")
    static boolean processRange(LongRangeBlockBuilder.LongRange a, LongRangeBlockBuilder.LongRange b) {
        return a.from() >= b.from() && a.to() <= b.to();
    }

    @Evaluator(extraName = "DoublePoint")
    static boolean processPoint(double point, DoubleRangeBlockBuilder.DoubleRange range) {
        return point >= range.from() && point < range.to();
    }

    @Evaluator(extraName = "DoubleRange")
    static boolean processRange(DoubleRangeBlockBuilder.DoubleRange a, DoubleRangeBlockBuilder.DoubleRange b) {
        return a.from() >= b.from() && a.to() <= b.to();
    }

    @Override
    protected TypeResolution resolveType() {
        if (childrenResolved() == false) {
            return new TypeResolution("Unresolved children");
        }

        TypeResolution first = isType(
            left,
            dt -> isRange(dt) || dt == DATETIME || dt == DOUBLE,
            sourceText(),
            FIRST,
            "date",
            "date_range",
            "double",
            "double_range"
        );
        DataType expectedRangeType = switch (left.dataType()) {
            case DATETIME, DATE_RANGE -> DATE_RANGE;
            case DOUBLE, DOUBLE_RANGE -> DOUBLE_RANGE;
            default -> null;
        };
        TypeResolution second = expectedRangeType == null
            ? isType(right, dt -> dt == DATE_RANGE || dt == DOUBLE_RANGE, sourceText(), SECOND, "date_range", "double_range")
            : isType(right, dt -> dt == expectedRangeType, sourceText(), SECOND, expectedRangeType.esType());
        return first.and(second);
    }

    @Override
    public ExpressionEvaluator.Factory toEvaluator(ToEvaluator toEvaluator) {
        var leftEvaluator = toEvaluator.apply(left);
        var rightEvaluator = toEvaluator.apply(right);
        if (right.dataType() == DOUBLE_RANGE) {
            if (left.dataType() == DOUBLE_RANGE) {
                return new RangeWithinDoubleRangeEvaluator.Factory(source(), leftEvaluator, rightEvaluator);
            }
            return new RangeWithinDoublePointEvaluator.Factory(source(), leftEvaluator, rightEvaluator);
        }
        if (left.dataType() == DATE_RANGE) {
            return new RangeWithinRangeEvaluator.Factory(source(), leftEvaluator, rightEvaluator);
        }
        return new RangeWithinPointEvaluator.Factory(source(), leftEvaluator, rightEvaluator);
    }

    @Override
    public Translatable translatable(LucenePushdownPredicates pushdownPredicates) {
        if (isPushable(left, right, pushdownPredicates) || isPushable(right, left, pushdownPredicates)) {
            // date_range MVs are represented as a single BinaryDocValues, so SingleValueQuery does not detect that it's MV.
            // We have to recheck and filter out MVs
            return Translatable.RECHECK;
        }
        return Translatable.NO;
    }

    private static boolean isPushable(Expression maybeField, Expression maybeLiteral, LucenePushdownPredicates pushdownPredicates) {
        return pushdownPredicates.isPushableFieldAttribute(maybeField) && maybeLiteral.foldable();
    }

    private static boolean isRange(DataType dataType) {
        return dataType == DATE_RANGE || dataType == DOUBLE_RANGE;
    }

    @Override
    public Query asQuery(LucenePushdownPredicates pushdownPredicates, TranslatorHandler handler) {
        Expression fieldExp;
        Expression literalExp;
        boolean fieldIsLeft;
        if (pushdownPredicates.isPushableFieldAttribute(left)) {
            fieldExp = left;
            literalExp = right;
            fieldIsLeft = true;
        } else {
            fieldExp = right;
            literalExp = left;
            fieldIsLeft = false;
        }
        TypedAttribute attribute = LucenePushdownPredicates.checkIsPushableAttribute(fieldExp);
        String name = handler.nameOf(attribute);
        Object value = literalValueOf(literalExp);
        String format = attribute.dataType() == DATETIME || attribute.dataType() == DATE_RANGE
            ? DEFAULT_DATE_TIME_FORMATTER.pattern()
            : null;

        if (isRange(attribute.dataType()) == false) {
            // The field is a scalar; the function shape forces the other side to be the corresponding range.
            if (attribute.dataType() == DATETIME) {
                LongRangeBlockBuilder.LongRange r = (LongRangeBlockBuilder.LongRange) value;
                return new RangeQuery(source(), name, dateTimeToString(r.from()), true, dateTimeToString(r.to()), false, format, null);
            }
            DoubleRangeBlockBuilder.DoubleRange r = (DoubleRangeBlockBuilder.DoubleRange) value;
            return new RangeQuery(source(), name, finiteBound(r.from()), true, finiteBound(r.to()), false, null, null);
        }
        // The field is a range. Pick CONTAINS or WITHIN based on which side the field is on.
        ShapeRelation relation;
        Object lower;
        Object upper;
        boolean includeUpper;
        if (fieldIsLeft) {
            // RANGE_WITHIN(field_range, literal_range) — field_range is fully contained by literal_range.
            if (attribute.dataType() == DATE_RANGE) {
                LongRangeBlockBuilder.LongRange r = (LongRangeBlockBuilder.LongRange) value;
                lower = dateTimeToString(r.from());
                upper = dateTimeToString(r.to());
            } else {
                DoubleRangeBlockBuilder.DoubleRange r = (DoubleRangeBlockBuilder.DoubleRange) value;
                lower = finiteBound(r.from());
                upper = finiteBound(r.to());
            }
            includeUpper = false;
            relation = ShapeRelation.WITHIN;
        } else if (isRange(literalExp.dataType()) == false) {
            // RANGE_WITHIN(literal_point, field_range) — field_range contains the literal point.
            lower = literalExp.dataType() == DATETIME ? dateTimeToString((Long) value) : finiteBound((Double) value);
            upper = lower;
            includeUpper = true;
            relation = ShapeRelation.CONTAINS;
        } else {
            // RANGE_WITHIN(literal_range, field_range) — field_range fully contains the literal range.
            if (attribute.dataType() == DATE_RANGE) {
                LongRangeBlockBuilder.LongRange r = (LongRangeBlockBuilder.LongRange) value;
                lower = dateTimeToString(r.from());
                upper = dateTimeToString(r.to());
            } else {
                DoubleRangeBlockBuilder.DoubleRange r = (DoubleRangeBlockBuilder.DoubleRange) value;
                lower = finiteBound(r.from());
                upper = finiteBound(r.to());
            }
            includeUpper = false;
            relation = ShapeRelation.CONTAINS;
        }
        return new RangeQuery(source(), name, lower, true, upper, includeUpper, format, null, relation);
    }

    /**
     * Range queries on double fields reject non-finite bounds. An infinite bound means the range is
     * unbounded on that side, which a {@code null} query bound expresses — the same representation
     * the range mapper uses for open bounds. This can only widen the pushed query; the predicate is
     * pushed with {@link TranslationAware.Translatable#RECHECK}, so the retained filter still
     * enforces the exact semantics.
     */
    static Double finiteBound(double bound) {
        return Double.isFinite(bound) ? bound : null;
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new RangeWithin(source(), newChildren.get(0), newChildren.get(1));
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, RangeWithin::new, left, right);
    }
}
