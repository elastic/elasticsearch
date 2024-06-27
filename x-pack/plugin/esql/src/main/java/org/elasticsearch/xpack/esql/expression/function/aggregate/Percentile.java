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
import org.elasticsearch.compute.aggregation.AggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.PercentileDoubleAggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.PercentileIntAggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.PercentileLongAggregatorFunctionSupplier;
import org.elasticsearch.xpack.esql.capabilities.Validatable;
import org.elasticsearch.xpack.esql.core.common.Failures;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamOutput;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.FIRST;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.SECOND;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isNumeric;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isType;
import static org.elasticsearch.xpack.esql.expression.Validations.isFoldable;

public class Percentile extends NumericAggregate implements Validatable {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Expression.class,
        "Percentile",
        Percentile::new
    );

    private final Expression percentile;

    @FunctionInfo(
        returnType = { "double", "integer", "long" },
        description = "The value at which a certain percentage of observed values occur.",
        isAggregation = true
    )
    public Percentile(
        Source source,
        @Param(name = "number", type = { "double", "integer", "long" }) Expression field,
        @Param(name = "percentile", type = { "double", "integer", "long" }) Expression percentile
    ) {
        super(source, field, List.of(percentile));
        this.percentile = percentile;
    }

    private Percentile(StreamInput in) throws IOException {
        this(Source.readFrom((PlanStreamInput) in), ((PlanStreamInput) in).readExpression(), ((PlanStreamInput) in).readExpression());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        Source.EMPTY.writeTo(out);
        ((PlanStreamOutput) out).writeExpression(children().get(0));
        ((PlanStreamOutput) out).writeExpression(children().get(1));
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    protected NodeInfo<Percentile> info() {
        return NodeInfo.create(this, Percentile::new, field(), percentile);
    }

    @Override
    public Percentile replaceChildren(List<Expression> newChildren) {
        return new Percentile(source(), newChildren.get(0), newChildren.get(1));
    }

    public Expression percentile() {
        return percentile;
    }

    @Override
    protected TypeResolution resolveType() {
        if (childrenResolved() == false) {
            return new TypeResolution("Unresolved children");
        }

        TypeResolution resolution = isType(
            field(),
            dt -> dt.isNumeric() && dt != DataType.UNSIGNED_LONG,
            sourceText(),
            FIRST,
            "numeric except unsigned_long"
        );
        if (resolution.unresolved()) {
            return resolution;
        }

        return isNumeric(percentile, sourceText(), SECOND);
    }

    @Override
    public void validate(Failures failures) {
        failures.add(isFoldable(percentile, sourceText(), SECOND));
    }

    @Override
    protected AggregatorFunctionSupplier longSupplier(List<Integer> inputChannels) {
        return new PercentileLongAggregatorFunctionSupplier(inputChannels, percentileValue());
    }

    @Override
    protected AggregatorFunctionSupplier intSupplier(List<Integer> inputChannels) {
        return new PercentileIntAggregatorFunctionSupplier(inputChannels, percentileValue());
    }

    @Override
    protected AggregatorFunctionSupplier doubleSupplier(List<Integer> inputChannels) {
        return new PercentileDoubleAggregatorFunctionSupplier(inputChannels, percentileValue());
    }

    private int percentileValue() {
        return ((Number) percentile.fold()).intValue();
    }
}
