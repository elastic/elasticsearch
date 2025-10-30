/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.aggregate;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.compute.aggregation.AggregatorFunction;
import org.elasticsearch.compute.aggregation.AggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.GroupingAggregatorFunction;
import org.elasticsearch.compute.aggregation.IntermediateStateDesc;
import org.elasticsearch.compute.aggregation.StdDevDoubleAggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.StdDevIntAggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.StdDevLongAggregatorFunctionSupplier;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.xpack.esql.EsqlIllegalArgumentException;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.Example;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.FunctionType;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.planner.ToAggregator;

import java.io.IOException;
import java.util.List;

import static java.util.Collections.emptyList;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.DEFAULT;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isType;

public class HistogramMerge extends AggregateFunction implements ToAggregator {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(Expression.class, "HistogramMerge", HistogramMerge::new);

    @FunctionInfo(
        returnType = "exponential_histogram",
        type = FunctionType.AGGREGATE
    )
    public HistogramMerge(Source source, @Param(name = "histogram", type = { "exponential_histogram" }) Expression field) {
        this(source, field, Literal.TRUE);
    }

    public HistogramMerge(Source source, Expression field, Expression filter) {
        super(source, field, filter, emptyList());
    }

    private HistogramMerge(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    public DataType dataType() {
        return DataType.EXPONENTIAL_HISTOGRAM;
    }

    @Override
    protected TypeResolution resolveType() {
        return isType(
            field(),
            dt -> dt == DataType.EXPONENTIAL_HISTOGRAM,
            sourceText(),
            DEFAULT,
            "exponential_histogram"
        );
    }

    @Override
    protected NodeInfo<HistogramMerge> info() {
        return NodeInfo.create(this, HistogramMerge::new, field(), filter());
    }

    @Override
    public HistogramMerge replaceChildren(List<Expression> newChildren) {
        return new HistogramMerge(source(), newChildren.get(0), newChildren.get(1));
    }

    public HistogramMerge withFilter(Expression filter) {
        return new HistogramMerge(source(), field(), filter);
    }

    @Override
    public final AggregatorFunctionSupplier supplier() {
        DataType type = field().dataType();
        if (type == DataType.LONG) {
            return new AggregatorFunctionSupplier() {

                @Override
                public String describe() {
                    return "merge of exponential histograms";
                }

                @Override
                public List<IntermediateStateDesc> nonGroupingIntermediateStateDesc() {
                    return List.of();
                }

                @Override
                public List<IntermediateStateDesc> groupingIntermediateStateDesc() {
                    return List.of();
                }

                @Override
                public AggregatorFunction aggregator(DriverContext driverContext, List<Integer> channels) {
                    return null;
                }

                @Override
                public GroupingAggregatorFunction groupingAggregator(DriverContext driverContext, List<Integer> channels) {
                    return null;
                }
            };
        }
        throw EsqlIllegalArgumentException.illegalDataType(type);
    }
}
