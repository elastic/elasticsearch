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
import org.elasticsearch.compute.aggregation.AggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.StdDevDoubleAggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.StdDevIntAggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.StdDevLongAggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.StdDevStates;
import org.elasticsearch.xpack.esql.EsqlIllegalArgumentException;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.FunctionType;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.elasticsearch.xpack.esql.planner.ToAggregator;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.DEFAULT;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isType;
import static org.elasticsearch.xpack.esql.core.util.CollectionUtils.nullSafeList;

public class StdDev extends AggregateFunction implements ToAggregator {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(Expression.class, "StdDev", StdDev::new);

    private final Expression variation;

    @FunctionInfo(
        returnType = "double",
        description = "Internal function for calculating standard deviation/variance of fields",
        type = FunctionType.AGGREGATE
    )
    public StdDev(
        Source source,
        @Param(name = "number", type = { "double", "integer", "long" }) Expression field,
        @Param(name = "variation", type = "int", description = "index of stddev variation") Expression variation
    ) {
        this(source, field, Literal.TRUE, variation);
    }

    public StdDev(Source source, Expression field, Expression filter, Expression variation) {
        this(source, field, filter, variation != null ? List.of(variation) : List.of());
    }

    private StdDev(Source source, Expression field, Expression filter, List<Expression> params) {
        super(source, field, filter, params);
        this.variation = params.size() > 0 ? params.get(0) : null;
    }

    private StdDev(StreamInput in) throws IOException {
        this(
            Source.readFrom((PlanStreamInput) in),
            in.readNamedWriteable(Expression.class),
            in.getTransportVersion().onOrAfter(TransportVersions.V_8_16_0) ? in.readNamedWriteable(Expression.class) : Literal.TRUE,
            in.getTransportVersion().onOrAfter(TransportVersions.V_8_16_0)
                ? in.readNamedWriteableCollectionAsList(Expression.class)
                : nullSafeList(in.readOptionalNamedWriteable(Expression.class))
        );
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    public DataType dataType() {
        return DataType.DOUBLE;
    }

    @Override
    protected Expression.TypeResolution resolveType() {
        return isType(
            field(),
            dt -> dt.isNumeric() && dt != DataType.UNSIGNED_LONG,
            sourceText(),
            DEFAULT,
            "numeric except unsigned_long or counter types"
        );
    }

    @Override
    protected NodeInfo<StdDev> info() {
        return NodeInfo.create(this, StdDev::new, field(), filter(), variation);
    }

    @Override
    public StdDev replaceChildren(List<Expression> newChildren) {
        return new StdDev(source(), newChildren.get(0), newChildren.get(1), newChildren.size() > 2 ? newChildren.get(2) : null);
    }

    public StdDev withFilter(Expression filter) {
        return new StdDev(source(), field(), filter, variation);
    }

    @Override
    public final AggregatorFunctionSupplier supplier() {
        DataType type = field().dataType();
        int variation = this.variation == null
            ? StdDevStates.Variation.POPULATION.getIndex()
            : ((Number) (this.variation.fold(FoldContext.small() /* TODO remove me */))).intValue();
        if (type == DataType.LONG) {
            return new StdDevLongAggregatorFunctionSupplier(variation);
        }
        if (type == DataType.INTEGER) {
            return new StdDevIntAggregatorFunctionSupplier(variation);
        }
        if (type == DataType.DOUBLE) {
            return new StdDevDoubleAggregatorFunctionSupplier(variation);
        }
        throw EsqlIllegalArgumentException.illegalDataType(type);
    }
}
