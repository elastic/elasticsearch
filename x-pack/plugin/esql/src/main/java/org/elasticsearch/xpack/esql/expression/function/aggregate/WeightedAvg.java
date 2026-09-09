/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.aggregate;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.SurrogateExpression;
import org.elasticsearch.xpack.esql.expression.function.Example;
import org.elasticsearch.xpack.esql.expression.function.FunctionAppliesTo;
import org.elasticsearch.xpack.esql.expression.function.FunctionAppliesToLifecycle;
import org.elasticsearch.xpack.esql.expression.function.FunctionDefinition;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.FunctionType;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.expression.function.scalar.multivalue.MvAvg;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Div;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Mul;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.common.logging.LoggerMessageFormat.format;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.FIRST;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.SECOND;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isType;

public class WeightedAvg extends AggregateFunction implements SurrogateExpression {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Expression.class,
        "WeightedAvg",
        WeightedAvg::readFrom
    );
    public static final FunctionDefinition DEFINITION = FunctionDefinition.def(WeightedAvg.class)
        .binary(WeightedAvg::new)
        .name("weighted_avg");

    private static final String invalidWeightError = "{} argument of [{}] cannot be null or 0, received [{}]";

    @FunctionInfo(
        appliesTo = { @FunctionAppliesTo(lifeCycle = FunctionAppliesToLifecycle.GA) },
        returnType = "double",
        briefSummary = "Returns the weighted average of a numeric expression.",
        description = "The weighted average of a numeric expression.",
        type = FunctionType.AGGREGATE,
        examples = @Example(file = "stats", tag = "weighted-avg")
    )
    public WeightedAvg(
        Source source,
        @Param(name = "number", type = { "double", "integer", "long" }, description = "A numeric value.") Expression field,
        @Param(name = "weight", type = { "double", "integer", "long" }, description = "A numeric weight.") Expression weight
    ) {
        this(source, field, weight, Literal.TRUE, NO_WINDOW);
    }

    public WeightedAvg(Source source, Expression field, Expression weight, Expression filter, Expression window) {
        super(source, List.of(field, weight), filter, window, List.of());
    }

    private static WeightedAvg readFrom(StreamInput in) throws IOException {
        // Legacy serialization format for backwards compatibility
        Source source = Source.readFrom((PlanStreamInput) in);
        Expression field = in.readNamedWriteable(Expression.class);
        Expression filter = in.readNamedWriteable(Expression.class);
        Expression window = readWindow(in);
        Expression weight = in.readNamedWriteableCollectionAsList(Expression.class).get(0);
        return new WeightedAvg(source, field, weight, filter, window);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        // Legacy serialization format for backwards compatibility
        source().writeTo(out);
        out.writeNamedWriteable(field());
        out.writeNamedWriteable(filter());
        if (out.getTransportVersion().supports(WINDOW_INTERVAL)) {
            out.writeNamedWriteable(window());
        }
        out.writeNamedWriteableCollection(List.of(weight()));
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    protected Expression.TypeResolution resolveType() {
        if (childrenResolved() == false) {
            return new TypeResolution("Unresolved children");
        }

        TypeResolution resolution = isType(
            field(),
            dt -> dt.isNumeric() && dt != DataType.UNSIGNED_LONG,
            sourceText(),
            FIRST,
            "numeric except unsigned_long or counter types"
        );

        if (resolution.unresolved()) {
            return resolution;
        }

        resolution = isType(
            weight(),
            dt -> dt.isNumeric() && dt != DataType.UNSIGNED_LONG,
            sourceText(),
            SECOND,
            "numeric except unsigned_long or counter types"
        );

        if (resolution.unresolved()) {
            return resolution;
        }

        if (weight().dataType() == DataType.NULL) {
            return new TypeResolution(format(null, invalidWeightError, SECOND, sourceText(), null));
        }
        if (weight().foldable() == false) {
            return TypeResolution.TYPE_RESOLVED;
        }
        Object weightVal = weight().fold(FoldContext.small()/* TODO remove me*/);
        if (weightVal == null || weightVal.equals(0) || weightVal.equals(0.0)) {
            return new TypeResolution(format(null, invalidWeightError, SECOND, sourceText(), weightVal));
        }

        return TypeResolution.TYPE_RESOLVED;
    }

    @Override
    public DataType dataType() {
        return DataType.DOUBLE;
    }

    @Override
    protected NodeInfo<WeightedAvg> info() {
        return NodeInfo.create(this, WeightedAvg::new, field(), weight(), filter(), window());
    }

    @Override
    public WeightedAvg replaceChildren(List<Expression> newChildren) {
        return new WeightedAvg(source(), newChildren.get(0), newChildren.get(1), newChildren.get(2), newChildren.get(3));
    }

    @Override
    public WeightedAvg withFilter(Expression filter) {
        return new WeightedAvg(source(), field(), weight(), filter, window());
    }

    // TODO(jan): make OnlySurrogateExpression??
    @Override
    public Expression surrogate() {
        var s = source();
        var field = field();
        var weight = weight();

        if (field.foldable()) {
            return new MvAvg(s, field);
        }
        if (weight.foldable()) {
            return new Div(
                s,
                new Sum(s, field, filter(), window(), SummationMode.COMPENSATED_LITERAL),
                new Count(s, field, filter(), window()),
                dataType()
            );
        } else {
            return new Div(
                s,
                new Sum(s, new Mul(s, field, weight), filter(), window(), SummationMode.COMPENSATED_LITERAL),
                new Sum(s, weight, filter(), window(), SummationMode.COMPENSATED_LITERAL),
                dataType()
            );
        }
    }

    public Expression field() {
        return fields().get(0);
    }

    public Expression weight() {
        return fields().get(1);
    }
}
