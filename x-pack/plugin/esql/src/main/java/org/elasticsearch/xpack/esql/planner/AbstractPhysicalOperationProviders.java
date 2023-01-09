/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.planner;

import org.elasticsearch.compute.aggregation.Aggregator;
import org.elasticsearch.compute.aggregation.AggregatorMode;
import org.elasticsearch.compute.aggregation.BlockHash;
import org.elasticsearch.compute.aggregation.GroupingAggregator;
import org.elasticsearch.compute.operator.AggregationOperator;
import org.elasticsearch.compute.operator.HashAggregationOperator.HashAggregationOperatorFactory;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.xpack.esql.plan.physical.AggregateExec;
import org.elasticsearch.xpack.ql.expression.Alias;
import org.elasticsearch.xpack.ql.expression.Attribute;
import org.elasticsearch.xpack.ql.expression.AttributeSet;
import org.elasticsearch.xpack.ql.expression.Expressions;
import org.elasticsearch.xpack.ql.expression.NameId;
import org.elasticsearch.xpack.ql.expression.NamedExpression;
import org.elasticsearch.xpack.ql.expression.function.aggregate.AggregateFunction;
import org.elasticsearch.xpack.ql.type.DataTypes;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;

abstract class AbstractPhysicalOperationProviders implements PhysicalOperationProviders {

    @Override
    public final LocalExecutionPlanner.PhysicalOperation getGroupingPhysicalOperation(
        AggregateExec aggregateExec,
        LocalExecutionPlanner.PhysicalOperation source,
        LocalExecutionPlanner.LocalExecutionPlannerContext context
    ) {
        Layout.Builder layout = new Layout.Builder();
        Operator.OperatorFactory operatorFactory = null;
        AggregateExec.Mode mode = aggregateExec.getMode();

        if (aggregateExec.groupings().isEmpty()) {
            // not grouping
            List<Aggregator.AggregatorFactory> aggregatorFactories = new ArrayList<>();
            for (NamedExpression ne : aggregateExec.aggregates()) {
                // add the field to the layout
                layout.appendChannel(ne.id());

                if (ne instanceof Alias alias && alias.child()instanceof AggregateFunction aggregateFunction) {
                    AggregatorMode aggMode = null;
                    NamedExpression sourceAttr = null;

                    if (mode == AggregateExec.Mode.PARTIAL) {
                        aggMode = AggregatorMode.INITIAL;
                        // TODO: this needs to be made more reliable - use casting to blow up when dealing with expressions (e+1)
                        sourceAttr = (NamedExpression) aggregateFunction.field();
                    } else if (mode == AggregateExec.Mode.FINAL) {
                        aggMode = AggregatorMode.FINAL;
                        sourceAttr = alias;
                    } else {
                        throw new UnsupportedOperationException();
                    }
                    aggregatorFactories.add(
                        new Aggregator.AggregatorFactory(
                            AggregateMapper.mapToName(aggregateFunction),
                            AggregateMapper.mapToType(aggregateFunction),
                            aggMode,
                            source.layout.getChannel(sourceAttr.id())
                        )
                    );
                } else {
                    throw new UnsupportedOperationException();
                }
            }
            if (aggregatorFactories.isEmpty() == false) {
                operatorFactory = new AggregationOperator.AggregationOperatorFactory(
                    aggregatorFactories,
                    mode == AggregateExec.Mode.FINAL ? AggregatorMode.FINAL : AggregatorMode.INITIAL
                );
            }
        } else {
            // grouping
            List<GroupingAggregator.GroupingAggregatorFactory> aggregatorFactories = new ArrayList<>();
            AttributeSet groups = Expressions.references(aggregateExec.groupings());
            if (groups.size() != 1) {
                throw new UnsupportedOperationException("just one group, for now");
            }
            Attribute grpAttrib = groups.iterator().next();
            Set<NameId> grpAttribIds = new HashSet<>(List.of(grpAttrib.id()));
            // since the aggregate node can define aliases of the grouping column, there might be additional ids for the grouping column
            // e.g. in `... | stats c = count(a) by b | project c, bb = b`, the alias `bb = b` will be inlined in the resulting aggregation
            // node.
            for (NamedExpression agg : aggregateExec.aggregates()) {
                if (agg instanceof Alias a && a.child()instanceof Attribute attr && attr.id() == grpAttrib.id()) {
                    grpAttribIds.add(a.id());
                }
            }
            layout.appendChannel(grpAttribIds);

            final Supplier<BlockHash> blockHash;
            if (grpAttrib.dataType() == DataTypes.KEYWORD) {
                blockHash = () -> BlockHash.newBytesRefHash(context.bigArrays());
            } else {
                blockHash = () -> BlockHash.newLongHash(context.bigArrays());
            }

            for (NamedExpression ne : aggregateExec.aggregates()) {

                if (ne instanceof Alias alias && alias.child()instanceof AggregateFunction aggregateFunction) {
                    layout.appendChannel(alias.id());  // <<<< TODO: this one looks suspicious

                    AggregatorMode aggMode = null;
                    NamedExpression sourceAttr = null;

                    if (mode == AggregateExec.Mode.PARTIAL) {
                        aggMode = AggregatorMode.INITIAL;
                        sourceAttr = Expressions.attribute(aggregateFunction.field());
                    } else if (aggregateExec.getMode() == AggregateExec.Mode.FINAL) {
                        aggMode = AggregatorMode.FINAL;
                        sourceAttr = alias;
                    } else {
                        throw new UnsupportedOperationException();
                    }

                    aggregatorFactories.add(
                        new GroupingAggregator.GroupingAggregatorFactory(
                            context.bigArrays(),
                            AggregateMapper.mapToName(aggregateFunction),
                            AggregateMapper.mapToType(aggregateFunction),
                            aggMode,
                            source.layout.getChannel(sourceAttr.id())
                        )
                    );
                } else if (grpAttribIds.contains(ne.id()) == false && aggregateExec.groupings().contains(ne) == false) {
                    var u = ne instanceof Alias ? ((Alias) ne).child() : ne;
                    throw new UnsupportedOperationException(
                        "expected an aggregate function, but got [" + u + "] of type [" + u.nodeName() + "]"
                    );
                }
            }
            var attrSource = grpAttrib;

            final Integer inputChannel = source.layout.getChannel(attrSource.id());

            if (inputChannel == null) {
                operatorFactory = groupingOperatorFactory(source, aggregateExec, aggregatorFactories, attrSource, blockHash);
            } else {
                operatorFactory = new HashAggregationOperatorFactory(inputChannel, aggregatorFactories, blockHash);
            }
        }
        if (operatorFactory != null) {
            return source.with(operatorFactory, layout.build());
        }
        throw new UnsupportedOperationException();
    }

    public abstract Operator.OperatorFactory groupingOperatorFactory(
        LocalExecutionPlanner.PhysicalOperation source,
        AggregateExec aggregateExec,
        List<GroupingAggregator.GroupingAggregatorFactory> aggregatorFactories,
        Attribute attrSource,
        Supplier<BlockHash> blockHash
    );
}
