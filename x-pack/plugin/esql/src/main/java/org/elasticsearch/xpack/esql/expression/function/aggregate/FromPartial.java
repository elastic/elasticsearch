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
import org.elasticsearch.compute.aggregation.Aggregator;
import org.elasticsearch.compute.aggregation.AggregatorFunction;
import org.elasticsearch.compute.aggregation.AggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.AggregatorMode;
import org.elasticsearch.compute.aggregation.FromPartialAggregatorFunction;
import org.elasticsearch.compute.aggregation.FromPartialGroupingAggregatorFunction;
import org.elasticsearch.compute.aggregation.GroupingAggregator;
import org.elasticsearch.compute.aggregation.GroupingAggregatorFunction;
import org.elasticsearch.compute.aggregation.IntermediateStateDesc;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.xpack.esql.core.expression.AttributeSet;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.elasticsearch.xpack.esql.planner.ToAggregator;

import java.io.IOException;
import java.util.List;
import java.util.stream.IntStream;

/**
 * @see ToPartial
 */
public class FromPartial extends AggregateFunction implements ToAggregator {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Expression.class,
        "FromPartial",
        FromPartial::new
    );

    private final Expression function;

    public FromPartial(Source source, Expression field, Expression function) {
        this(source, field, Literal.TRUE, function);
    }

    public FromPartial(Source source, Expression field, Expression filter, Expression function) {
        super(source, field, filter, List.of(function));
        this.function = function;
    }

    private FromPartial(StreamInput in) throws IOException {
        this(
            Source.readFrom((PlanStreamInput) in),
            in.readNamedWriteable(Expression.class),
            in.getTransportVersion().onOrAfter(TransportVersions.V_8_16_0) ? in.readNamedWriteable(Expression.class) : Literal.TRUE,
            in.getTransportVersion().onOrAfter(TransportVersions.V_8_16_0)
                ? in.readNamedWriteableCollectionAsList(Expression.class).get(0)
                : in.readNamedWriteable(Expression.class)
        );
    }

    @Override
    protected void deprecatedWriteParams(StreamOutput out) throws IOException {
        out.writeNamedWriteable(function);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    public Expression function() {
        return function;
    }

    @Override
    public DataType dataType() {
        return function.dataType();
    }

    @Override
    protected TypeResolution resolveType() {
        return TypeResolution.TYPE_RESOLVED;
    }

    @Override
    public AttributeSet references() {
        return field().references(); // exclude the function and its argument
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new FromPartial(source(), newChildren.get(0), newChildren.get(1), newChildren.get(2));
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, FromPartial::new, field(), filter(), function);
    }

    @Override
    public FromPartial withFilter(Expression filter) {
        return new FromPartial(source(), field(), filter, function);
    }

    @Override
    public AggregatorFunctionSupplier supplier() {
        final AggregatorFunctionSupplier supplier = ((ToAggregator) function).supplier();
        return new AggregatorFunctionSupplier() {
            @Override
            public List<IntermediateStateDesc> nonGroupingIntermediateStateDesc() {
                return FromPartialAggregatorFunction.intermediateStateDesc();
            }

            @Override
            public List<IntermediateStateDesc> groupingIntermediateStateDesc() {
                return FromPartialGroupingAggregatorFunction.intermediateStateDesc();
            }

            @Override
            public AggregatorFunction aggregator(DriverContext driverContext, List<Integer> channels) {
                assert false : "aggregatorFactory() is override";
                throw new UnsupportedOperationException();
            }

            @Override
            public GroupingAggregatorFunction groupingAggregator(DriverContext driverContext, List<Integer> channels) {
                assert false : "groupingAggregatorFactory() is override";
                throw new UnsupportedOperationException();
            }

            @Override
            public Aggregator.Factory aggregatorFactory(AggregatorMode mode, List<Integer> channels) {
                if (channels.size() != 1) {
                    assert false : "from_partial aggregation requires exactly one input channel; got " + channels;
                    throw new IllegalArgumentException("from_partial aggregation requires exactly one input channel; got " + channels);
                }
                final int inputChannel = channels.get(0);
                var intermediateChannels = IntStream.range(0, supplier.nonGroupingIntermediateStateDesc().size()).boxed().toList();
                return new Aggregator.Factory() {
                    @Override
                    public Aggregator apply(DriverContext driverContext) {
                        // use groupingAggregator since we can receive intermediate output from a grouping aggregate
                        final var groupingAggregator = supplier.groupingAggregator(driverContext, intermediateChannels);
                        return new Aggregator(new FromPartialAggregatorFunction(driverContext, groupingAggregator, inputChannel), mode);
                    }

                    @Override
                    public String describe() {
                        return "from_partial(" + supplier.describe() + ")";
                    }
                };
            }

            @Override
            public GroupingAggregator.Factory groupingAggregatorFactory(AggregatorMode mode, List<Integer> channels) {
                if (channels.size() != 1) {
                    assert false : "from_partial aggregation requires exactly one input channel; got " + channels;
                    throw new IllegalArgumentException("from_partial aggregation requires exactly one input channel; got " + channels);
                }
                final int inputChannel = channels.get(0);
                var intermediateChannels = IntStream.range(0, supplier.nonGroupingIntermediateStateDesc().size()).boxed().toList();
                return new GroupingAggregator.Factory() {
                    @Override
                    public GroupingAggregator apply(DriverContext driverContext) {
                        final GroupingAggregatorFunction aggregator = supplier.groupingAggregator(driverContext, intermediateChannels);
                        return new GroupingAggregator(new FromPartialGroupingAggregatorFunction(aggregator, inputChannel), mode);
                    }

                    @Override
                    public String describe() {
                        return "from_partial(" + supplier.describe() + ")";
                    }
                };
            }

            @Override
            public String describe() {
                return "from_partial";
            }
        };
    }
}
