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
import org.elasticsearch.compute.aggregation.FromPartialGroupingAggregatorFunction;
import org.elasticsearch.compute.aggregation.GroupingAggregator;
import org.elasticsearch.compute.aggregation.GroupingAggregatorFunction;
import org.elasticsearch.compute.aggregation.ToPartialAggregatorFunction;
import org.elasticsearch.compute.aggregation.ToPartialGroupingAggregatorFunction;
import org.elasticsearch.compute.operator.DriverContext;
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
 * An internal aggregate function that always emits intermediate (or partial) output regardless
 * of the aggregate mode. The intermediate output should be consumed by {@link FromPartial},
 * which always receives the intermediate input. Since an intermediate aggregate output can
 * consist of multiple blocks, we wrap these output blocks in a single composite block.
 * The {@link FromPartial} then unwraps this input block into multiple primitive blocks and
 * passes them to the delegating GroupingAggregatorFunction.
 * <p>
 * Both of these commands yield the same result, except the second plan executes aggregates twice:
 * <pre>
 * ```
 * | ... before
 * | af(x) BY g
 * | ... after
 * ```
 * ```
 * | ... before
 * | $x = to_partial(af(x)) BY g
 * | from_partial($x, af(_)) BY g
 * | ...  after
 * </pre>
 * ```
 * @see ToPartialGroupingAggregatorFunction
 * @see FromPartialGroupingAggregatorFunction
 */
public class ToPartial extends AggregateFunction implements ToAggregator {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Expression.class,
        "ToPartial",
        ToPartial::new
    );

    private final Expression function;

    public ToPartial(Source source, Expression field, Expression function) {
        this(source, field, Literal.TRUE, function);
    }

    public ToPartial(Source source, Expression field, Expression filter, Expression function) {
        super(source, field, filter, List.of(function));
        this.function = function;
    }

    private ToPartial(StreamInput in) throws IOException {
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
        return DataType.PARTIAL_AGG;
    }

    @Override
    protected TypeResolution resolveType() {
        return function.typeResolved();
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new ToPartial(source(), newChildren.get(0), newChildren.get(1), newChildren.get(2));
    }

    @Override
    public ToPartial withFilter(Expression filter) {
        return new ToPartial(source(), field(), filter(), function);
    }

    @Override
    protected NodeInfo<ToPartial> info() {
        return NodeInfo.create(this, ToPartial::new, field(), filter(), function);
    }

    @Override
    public AggregatorFunctionSupplier supplier(List<Integer> inputChannels) {
        final ToAggregator toAggregator = (ToAggregator) function;
        return new AggregatorFunctionSupplier() {
            @Override
            public AggregatorFunction aggregator(DriverContext driverContext) {
                assert false : "aggregatorFactory() is override";
                throw new UnsupportedOperationException();
            }

            @Override
            public GroupingAggregatorFunction groupingAggregator(DriverContext driverContext) {
                assert false : "groupingAggregatorFactory() is override";
                throw new UnsupportedOperationException();
            }

            @Override
            public Aggregator.Factory aggregatorFactory(AggregatorMode mode) {
                final AggregatorFunctionSupplier supplier;
                if (mode.isInputPartial()) {
                    try (var dummy = toAggregator.supplier(inputChannels).aggregator(DriverContext.getLocalDriver())) {
                        var intermediateChannels = IntStream.range(0, dummy.intermediateBlockCount()).boxed().toList();
                        supplier = toAggregator.supplier(intermediateChannels);
                    }
                } else {
                    supplier = toAggregator.supplier(inputChannels);
                }
                return new Aggregator.Factory() {
                    @Override
                    public Aggregator apply(DriverContext driverContext) {
                        final AggregatorFunction aggregatorFunction = supplier.aggregator(driverContext);
                        return new Aggregator(new ToPartialAggregatorFunction(aggregatorFunction, inputChannels), mode);
                    }

                    @Override
                    public String describe() {
                        return "to_partial(" + supplier.describe() + ")";
                    }
                };
            }

            @Override
            public GroupingAggregator.Factory groupingAggregatorFactory(AggregatorMode mode) {
                final AggregatorFunctionSupplier supplier;
                if (mode.isInputPartial()) {
                    try (var dummy = toAggregator.supplier(inputChannels).aggregator(DriverContext.getLocalDriver())) {
                        var intermediateChannels = IntStream.range(0, dummy.intermediateBlockCount()).boxed().toList();
                        supplier = toAggregator.supplier(intermediateChannels);
                    }
                } else {
                    supplier = toAggregator.supplier(inputChannels);
                }
                return new GroupingAggregator.Factory() {
                    @Override
                    public GroupingAggregator apply(DriverContext driverContext) {
                        final GroupingAggregatorFunction aggregatorFunction = supplier.groupingAggregator(driverContext);
                        return new GroupingAggregator(new ToPartialGroupingAggregatorFunction(aggregatorFunction, inputChannels), mode);
                    }

                    @Override
                    public String describe() {
                        return "to_partial(" + supplier.describe() + ")";
                    }
                };
            }

            @Override
            public String describe() {
                return "to_partial";
            }
        };
    }
}
