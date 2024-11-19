/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.aggregate;

import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.esql.capabilities.Validatable;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.SurrogateExpression;
import org.elasticsearch.xpack.esql.expression.function.Example;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
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

public class WeightedAvg extends AggregateFunction implements SurrogateExpression, Validatable {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Expression.class,
        "WeightedAvg",
        WeightedAvg::new
    );

    private final Expression weight;

    private static final String invalidWeightError = "{} argument of [{}] cannot be null or 0, received [{}]";

    @FunctionInfo(
        returnType = "double",
        description = "The weighted average of a numeric expression.",
        isAggregation = true,
        examples = @Example(file = "stats", tag = "weighted-avg")
    )
    public WeightedAvg(
        Source source,
        @Param(name = "number", type = { "double", "integer", "long" }, description = "A numeric value.") Expression field,
        @Param(name = "weight", type = { "double", "integer", "long" }, description = "A numeric weight.") Expression weight
    ) {
        this(source, field, Literal.TRUE, weight);
    }

    public WeightedAvg(Source source, Expression field, Expression filter, Expression weight) {
        super(source, field, filter, List.of(weight));
        this.weight = weight;
    }

    private WeightedAvg(StreamInput in) throws IOException {
        this(
            Source.readFrom((PlanStreamInput) in),
            in.readNamedWriteable(Expression.class),
            in.getTransportVersion().onOrAfter(TransportVersions.ESQL_PER_AGGREGATE_FILTER)
                ? in.readNamedWriteable(Expression.class)
                : Literal.TRUE,
            in.getTransportVersion().onOrAfter(TransportVersions.ESQL_PER_AGGREGATE_FILTER)
                ? in.readNamedWriteableCollectionAsList(Expression.class).get(0)
                : in.readNamedWriteable(Expression.class)
        );
    }

    @Override
    protected void deprecatedWriteParams(StreamOutput out) throws IOException {
        out.writeNamedWriteable(weight);
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

        if (weight.dataType() == DataType.NULL
            || (weight.foldable() && (weight.fold() == null || weight.fold().equals(0) || weight.fold().equals(0.0)))) {
            return new TypeResolution(format(null, invalidWeightError, SECOND, sourceText(), weight.foldable() ? weight.fold() : null));
        }

        return TypeResolution.TYPE_RESOLVED;
    }

    @Override
    public DataType dataType() {
        return DataType.DOUBLE;
    }

    @Override
    protected NodeInfo<WeightedAvg> info() {
        return NodeInfo.create(this, WeightedAvg::new, field(), filter(), weight);
    }

    @Override
    public WeightedAvg replaceChildren(List<Expression> newChildren) {
        return new WeightedAvg(source(), newChildren.get(0), newChildren.get(1), newChildren.get(2));
    }

    @Override
    public WeightedAvg withFilter(Expression filter) {
        return new WeightedAvg(source(), field(), filter, weight());
    }

    @Override
    public Expression surrogate() {
        var s = source();
        var field = field();
        var weight = weight();

        if (field.foldable()) {
            return new MvAvg(s, field);
        }
        if (weight.foldable()) {
            return new Div(s, new Sum(s, field), new Count(s, field), dataType());
        } else {
            return new Div(s, new Sum(s, new Mul(s, field, weight)), new Sum(s, weight), dataType());
        }
    }

    public Expression weight() {
        return weight;
    }
}
