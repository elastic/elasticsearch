/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.date;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.compute.data.LongRangeBlockBuilder;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.xpack.esql.capabilities.TranslationAware;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.querydsl.query.Query;
import org.elasticsearch.xpack.esql.core.querydsl.query.RangeQuery;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.util.Check;
import org.elasticsearch.xpack.esql.expression.function.Example;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.expression.function.scalar.EsqlScalarFunction;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.elasticsearch.xpack.esql.optimizer.rules.physical.local.LucenePushdownPredicates;
import org.elasticsearch.xpack.esql.planner.TranslatorHandler;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.FIRST;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.SECOND;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isType;
import static org.elasticsearch.xpack.esql.core.type.DataType.DATE_RANGE;

/**
 * InRange(date, date_range) -> boolean
 * Returns true if the provided date is within the given date_range.
 * Intended mostly for lookup joins and filtering, e.g. `where in_range(field, other_field)`.
 *
 * Supports Lucene pushdown when the date field is indexed and the range is a constant.
 */
public class InRange extends EsqlScalarFunction implements TranslationAware.SingleValueTranslationAware {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(Expression.class, "InRange", InRange::new);

    private final Expression date;
    private final Expression range;

    @FunctionInfo(
        returnType = "boolean",
        description = "Returns true if the provided date is contained in the provided date_range.",
        examples = @Example(file = "date_range", tag = "inRange", explanation = "Filter events within a specific date range")
    )
    public InRange(
        Source source,
        @Param(name = "date", type = { "date", "date_nanos" }, description = "Date to test.") Expression date,
        @Param(name = "range", type = { "date_range" }, description = "Range to test against.") Expression range
    ) {
        super(source, List.of(date, range));
        this.date = date;
        this.range = range;
    }

    private InRange(StreamInput in) throws IOException {
        this(Source.readFrom((PlanStreamInput) in), in.readNamedWriteable(Expression.class), in.readNamedWriteable(Expression.class));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        source().writeTo(out);
        out.writeNamedWriteable(date);
        out.writeNamedWriteable(range);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    public Expression date() {
        return date;
    }

    public Expression range() {
        return range;
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

        Object dateValue = date.fold(foldContext);
        Object rangeValue = range.fold(foldContext);

        if (dateValue == null || rangeValue == null) {
            return null;
        }

        // Extract date as long (millis or nanos)
        long dateMillisOrNanos = (Long) dateValue;

        // Extract range bounds
        LongRangeBlockBuilder.LongRange longRange = (LongRangeBlockBuilder.LongRange) rangeValue;
        long rangeFrom = longRange.from();
        long rangeTo = longRange.to();

        // Check if date is within range (inclusive)
        return dateMillisOrNanos >= rangeFrom && dateMillisOrNanos <= rangeTo;
    }

    @Override
    protected TypeResolution resolveType() {
        if (childrenResolved() == false) {
            return new TypeResolution("Unresolved children");
        }

        TypeResolution resolution = isType(date, DataType::isMillisOrNanos, sourceText(), FIRST, "date", "date_nanos").and(
            isType(range, dt -> dt == DATE_RANGE, sourceText(), SECOND, "date_range")
        );

        if (resolution.unresolved()) {
            return resolution;
        }

        return TypeResolution.TYPE_RESOLVED;
    }

    @Override
    public EvalOperator.ExpressionEvaluator.Factory toEvaluator(ToEvaluator toEvaluator) {
        var dateEvaluator = toEvaluator.apply(date);
        var rangeEvaluator = toEvaluator.apply(range);
        return new InRangeEvaluator.Factory(source(), dateEvaluator, rangeEvaluator);
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new InRange(source(), newChildren.get(0), newChildren.get(1));
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, InRange::new, date, range);
    }

    @Override
    public Translatable translatable(LucenePushdownPredicates pushdownPredicates) {
        // Can push down if:
        // 1. The date field is a pushable field attribute (indexed)
        // 2. The range is foldable (constant)
        return pushdownPredicates.isPushableFieldAttribute(date) && range.foldable() ? Translatable.YES : Translatable.NO;
    }

    @Override
    public Query asQuery(LucenePushdownPredicates pushdownPredicates, TranslatorHandler handler) {
        var fa = LucenePushdownPredicates.checkIsFieldAttribute(date);
        Check.isTrue(range.foldable(), "Expected foldable range, but got [{}]", range);

        String targetFieldName = handler.nameOf(fa.exactAttribute());

        var rangeValue = (LongRangeBlockBuilder.LongRange) range.fold(FoldContext.small());

        // Create a Lucene range query: field >= from AND field <= to
        // Range is half inclusive as we do in all ESQL ranges.
        return new RangeQuery(
            source(),
            targetFieldName,
            rangeValue.from(),  // lower bound
            true,               // include lower
            rangeValue.to(),    // upper bound
            true,               // include upper
            null                // zoneId (not needed for millis/nanos)
        );
    }

    @Override
    public Expression singleValueField() {
        return date;
    }
}
