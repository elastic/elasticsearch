/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.planner;

import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.aggregation.Aggregator;
import org.elasticsearch.compute.aggregation.AggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.AggregatorMode;
import org.elasticsearch.compute.aggregation.GroupingAggregator;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.operator.AggregationOperator;
import org.elasticsearch.compute.operator.HashAggregationOperator;
import org.elasticsearch.compute.operator.HashAggregationOperator.HashAggregationOperatorFactory;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.xpack.esql.EsqlIllegalArgumentException;
import org.elasticsearch.xpack.esql.plan.physical.AggregateExec;
import org.elasticsearch.xpack.esql.planner.LocalExecutionPlanner.LocalExecutionPlannerContext;
import org.elasticsearch.xpack.esql.planner.LocalExecutionPlanner.PhysicalOperation;
import org.elasticsearch.xpack.ql.expression.Alias;
import org.elasticsearch.xpack.ql.expression.Attribute;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.Expressions;
import org.elasticsearch.xpack.ql.expression.NameId;
import org.elasticsearch.xpack.ql.expression.NamedExpression;
import org.elasticsearch.xpack.ql.expression.function.aggregate.AggregateFunction;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;

abstract class AbstractPhysicalOperationProviders implements PhysicalOperationProviders {

    @Override
    public final PhysicalOperation groupingPhysicalOperation(
        AggregateExec aggregateExec,
        PhysicalOperation source,
        LocalExecutionPlannerContext context
    ) {
        Layout.Builder layout = new Layout.Builder();
        Operator.OperatorFactory operatorFactory = null;
        AggregateExec.Mode mode = aggregateExec.getMode();
        var aggregates = aggregateExec.aggregates();

        if (aggregateExec.groupings().isEmpty()) {
            // not grouping
            List<Aggregator.Factory> aggregatorFactories = new ArrayList<>();

            // append channels to the layout
            layout.appendChannels(aggregates);
            // create the agg factories
            aggregatesToFactory(
                aggregates,
                mode,
                source,
                context.bigArrays(),
                s -> aggregatorFactories.add(s.supplier.aggregatorFactory(s.mode))
            );

            if (aggregatorFactories.isEmpty() == false) {
                operatorFactory = new AggregationOperator.AggregationOperatorFactory(
                    aggregatorFactories,
                    mode == AggregateExec.Mode.FINAL ? AggregatorMode.FINAL : AggregatorMode.INITIAL
                );
            }
        } else {
            // grouping
            List<GroupingAggregator.Factory> aggregatorFactories = new ArrayList<>();
            List<GroupSpec> groupSpecs = new ArrayList<>(aggregateExec.groupings().size());
            for (Expression group : aggregateExec.groupings()) {
                var groupAttribute = Expressions.attribute(group);
                if (groupAttribute == null) {
                    throw new EsqlIllegalArgumentException("Unexpected non-named expression[{}] as grouping in [{}]", group, aggregateExec);
                }
                Set<NameId> grpAttribIds = new HashSet<>();
                grpAttribIds.add(groupAttribute.id());

                /*
                 * Check for aliasing in aggregates which occurs in two cases (due to combining project + stats):
                 *  - before stats (project x = a | stats by x) which requires the partial input to use a's channel
                 *  - after  stats (stats by a | project x = a) which causes the output layout to refer to the follow-up alias
                 */
                for (NamedExpression agg : aggregates) {
                    if (agg instanceof Alias a) {
                        if (a.child() instanceof Attribute attr) {
                            if (groupAttribute.id().equals(attr.id())) {
                                grpAttribIds.add(a.id());
                                // TODO: investigate whether a break could be used since it shouldn't be possible to have multiple
                                // attributes
                                // pointing to the same attribute
                            }
                            // partial mode only
                            // check if there's any alias used in grouping - no need for the final reduction since the intermediate data
                            // is in the output form
                            // if the group points to an alias declared in the aggregate, use the alias child as source
                            else if (mode == AggregateExec.Mode.PARTIAL) {
                                if (groupAttribute.semanticEquals(a.toAttribute())) {
                                    groupAttribute = attr;
                                    break;
                                }
                            }
                        }
                    }
                }
                layout.appendChannel(grpAttribIds);
                groupSpecs.add(new GroupSpec(source.layout.getChannel(groupAttribute.id()), groupAttribute));
            }

            for (var agg : aggregates) {
                if (agg instanceof Alias alias && alias.child() instanceof AggregateFunction) {
                    layout.appendChannel(alias.id());
                }
            }
            // create the agg factories
            aggregatesToFactory(
                aggregates,
                mode,
                source,
                context.bigArrays(),
                s -> aggregatorFactories.add(s.supplier.groupingAggregatorFactory(s.mode))
            );

            if (groupSpecs.size() == 1 && groupSpecs.get(0).channel == null) {
                operatorFactory = ordinalGroupingOperatorFactory(
                    source,
                    aggregateExec,
                    aggregatorFactories,
                    groupSpecs.get(0).attribute,
                    groupSpecs.get(0).elementType(),
                    context.bigArrays()
                );
            } else {
                operatorFactory = new HashAggregationOperatorFactory(
                    groupSpecs.stream().map(GroupSpec::toHashGroupSpec).toList(),
                    aggregatorFactories,
                    context.bigArrays()
                );
            }
        }
        if (operatorFactory != null) {
            return source.with(operatorFactory, layout.build());
        }
        throw new UnsupportedOperationException();
    }

    private record AggFunctionSupplierContext(AggregatorFunctionSupplier supplier, AggregatorMode mode) {}

    private void aggregatesToFactory(
        List<? extends NamedExpression> aggregates,
        AggregateExec.Mode mode,
        PhysicalOperation source,
        BigArrays bigArrays,
        Consumer<AggFunctionSupplierContext> consumer
    ) {
        for (NamedExpression ne : aggregates) {
            if (ne instanceof Alias alias) {
                var child = alias.child();
                if (child instanceof AggregateFunction aggregateFunction) {
                    AggregatorMode aggMode = null;
                    NamedExpression sourceAttr = null;

                    if (mode == AggregateExec.Mode.PARTIAL) {
                        aggMode = AggregatorMode.INITIAL;
                        // TODO: this needs to be made more reliable - use casting to blow up when dealing with expressions (e+1)
                        sourceAttr = Expressions.attribute(aggregateFunction.field());
                    } else if (mode == AggregateExec.Mode.FINAL) {
                        aggMode = AggregatorMode.FINAL;
                        sourceAttr = alias;
                    } else {
                        throw new UnsupportedOperationException();
                    }
                    var aggParams = aggregateFunction.parameters();
                    Object[] params = new Object[aggParams.size()];
                    for (int i = 0; i < params.length; i++) {
                        params[i] = aggParams.get(i).fold();
                    }

                    List<Integer> inputChannels = List.of(source.layout.getChannel(sourceAttr.id()));
                    assert inputChannels.size() > 0 && inputChannels.stream().allMatch(i -> i >= 0);
                    if (aggregateFunction instanceof ToAggregator agg) {
                        consumer.accept(new AggFunctionSupplierContext(agg.supplier(bigArrays, inputChannels), aggMode));
                    } else {
                        throw new UnsupportedOperationException("aggregate functions must extend ToAggregator");
                    }
                }
            }
        }
    }

    private record GroupSpec(Integer channel, Attribute attribute) {
        HashAggregationOperator.GroupSpec toHashGroupSpec() {
            if (channel == null) {
                throw new UnsupportedOperationException("planned to use ordinals but tried to use the hash instead");
            }
            return new HashAggregationOperator.GroupSpec(channel, elementType());
        }

        ElementType elementType() {
            return LocalExecutionPlanner.toElementType(attribute.dataType());
        }
    }

    /**
     * Build a grouping operator that operates on ordinals if possible.
     */
    public abstract Operator.OperatorFactory ordinalGroupingOperatorFactory(
        PhysicalOperation source,
        AggregateExec aggregateExec,
        List<GroupingAggregator.Factory> aggregatorFactories,
        Attribute attrSource,
        ElementType groupType,
        BigArrays bigArrays
    );
}
