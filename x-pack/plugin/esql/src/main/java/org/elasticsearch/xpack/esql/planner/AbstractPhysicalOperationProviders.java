/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.planner;

import org.elasticsearch.compute.aggregation.Aggregator;
import org.elasticsearch.compute.aggregation.AggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.AggregatorMode;
import org.elasticsearch.compute.aggregation.FilteredAggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.GroupingAggregator;
import org.elasticsearch.compute.aggregation.blockhash.BlockHash;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.operator.AggregationOperator;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.compute.operator.HashAggregationOperator.HashAggregationOperatorFactory;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.xpack.esql.EsqlIllegalArgumentException;
import org.elasticsearch.xpack.esql.core.InvalidArgumentException;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.core.expression.NameId;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.evaluator.EvalMapper;
import org.elasticsearch.xpack.esql.expression.function.aggregate.AggregateFunction;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Count;
import org.elasticsearch.xpack.esql.plan.physical.AggregateExec;
import org.elasticsearch.xpack.esql.plan.physical.ExchangeSourceExec;
import org.elasticsearch.xpack.esql.planner.LocalExecutionPlanner.LocalExecutionPlannerContext;
import org.elasticsearch.xpack.esql.planner.LocalExecutionPlanner.PhysicalOperation;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;

import static java.util.Collections.emptyList;

public abstract class AbstractPhysicalOperationProviders implements PhysicalOperationProviders {

    private final AggregateMapper aggregateMapper = new AggregateMapper();

    @Override
    public final PhysicalOperation groupingPhysicalOperation(
        AggregateExec aggregateExec,
        PhysicalOperation source,
        LocalExecutionPlannerContext context
    ) {
        Layout.Builder layout = new Layout.Builder();
        Operator.OperatorFactory operatorFactory = null;
        AggregatorMode aggregatorMode = aggregateExec.getMode();
        var aggregates = aggregateExec.aggregates();

        var sourceLayout = source.layout;

        if (aggregatorMode != AggregatorMode.INITIAL && aggregatorMode != AggregatorMode.FINAL) {
            assert false : "Invalid aggregator mode [" + aggregatorMode + "]";
        }
        if (aggregatorMode == AggregatorMode.INITIAL && aggregateExec.child() instanceof ExchangeSourceExec) {
            // the reducer step at data node (local) level
            aggregatorMode = AggregatorMode.INTERMEDIATE;
        }

        if (aggregateExec.groupings().isEmpty()) {
            // not grouping
            List<Aggregator.Factory> aggregatorFactories = new ArrayList<>();

            // append channels to the layout
            if (aggregatorMode == AggregatorMode.FINAL) {
                layout.append(aggregates);
            } else {
                layout.append(aggregateMapper.mapNonGrouping(aggregates));
            }

            // create the agg factories
            aggregatesToFactory(
                aggregates,
                aggregatorMode,
                sourceLayout,
                false, // non-grouping
                s -> aggregatorFactories.add(s.supplier.aggregatorFactory(s.mode))
            );

            if (aggregatorFactories.isEmpty() == false) {
                operatorFactory = new AggregationOperator.AggregationOperatorFactory(aggregatorFactories, aggregatorMode);
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
                Layout.ChannelSet groupAttributeLayout = new Layout.ChannelSet(new HashSet<>(), groupAttribute.dataType());
                groupAttributeLayout.nameIds().add(groupAttribute.id());

                /*
                 * Check for aliasing in aggregates which occurs in two cases (due to combining project + stats):
                 *  - before stats (keep x = a | stats by x) which requires the partial input to use a's channel
                 *  - after  stats (stats by a | keep x = a) which causes the output layout to refer to the follow-up alias
                 */
                for (NamedExpression agg : aggregates) {
                    if (agg instanceof Alias a) {
                        if (a.child() instanceof Attribute attr) {
                            if (groupAttribute.id().equals(attr.id())) {
                                groupAttributeLayout.nameIds().add(a.id());
                                // TODO: investigate whether a break could be used since it shouldn't be possible to have multiple
                                // attributes pointing to the same attribute
                            }
                            // partial mode only
                            // check if there's any alias used in grouping - no need for the final reduction since the intermediate data
                            // is in the output form
                            // if the group points to an alias declared in the aggregate, use the alias child as source
                            else if (aggregatorMode == AggregatorMode.INITIAL || aggregatorMode == AggregatorMode.INTERMEDIATE) {
                                if (groupAttribute.semanticEquals(a.toAttribute())) {
                                    groupAttribute = attr;
                                    break;
                                }
                            }
                        }
                    }
                }
                layout.append(groupAttributeLayout);
                Layout.ChannelAndType groupInput = source.layout.get(groupAttribute.id());
                groupSpecs.add(new GroupSpec(groupInput == null ? null : groupInput.channel(), groupAttribute));
            }

            if (aggregatorMode == AggregatorMode.FINAL) {
                for (var agg : aggregates) {
                    if (Alias.unwrap(agg) instanceof AggregateFunction) {
                        layout.append(agg);
                    }
                }
            } else {
                layout.append(aggregateMapper.mapGrouping(aggregates));
            }

            // create the agg factories
            aggregatesToFactory(
                aggregates,
                aggregatorMode,
                sourceLayout,
                true, // grouping
                s -> aggregatorFactories.add(s.supplier.groupingAggregatorFactory(s.mode))
            );

            if (groupSpecs.size() == 1 && groupSpecs.get(0).channel == null) {
                operatorFactory = ordinalGroupingOperatorFactory(
                    source,
                    aggregateExec,
                    aggregatorFactories,
                    groupSpecs.get(0).attribute,
                    groupSpecs.get(0).elementType(),
                    context
                );
            } else {
                operatorFactory = new HashAggregationOperatorFactory(
                    groupSpecs.stream().map(GroupSpec::toHashGroupSpec).toList(),
                    aggregatorFactories,
                    context.pageSize(aggregateExec.estimatedRowSize())
                );
            }
        }
        if (operatorFactory != null) {
            return source.with(operatorFactory, layout.build());
        }
        throw new EsqlIllegalArgumentException("no operator factory");
    }

    /***
     * Creates a standard layout for intermediate aggregations, typically used across exchanges.
     * Puts the group first, followed by each aggregation.
     *
     * It's similar to the code above (groupingPhysicalOperation) but ignores the factory creation.
     */
    public static List<Attribute> intermediateAttributes(List<? extends NamedExpression> aggregates, List<? extends Expression> groupings) {
        var aggregateMapper = new AggregateMapper();

        List<Attribute> attrs = new ArrayList<>();

        // no groups
        if (groupings.isEmpty()) {
            attrs = Expressions.asAttributes(aggregateMapper.mapNonGrouping(aggregates));
        }
        // groups
        else {
            for (Expression group : groupings) {
                var groupAttribute = Expressions.attribute(group);
                if (groupAttribute == null) {
                    throw new EsqlIllegalArgumentException("Unexpected non-named expression[{}] as grouping", group);
                }
                Set<NameId> grpAttribIds = new HashSet<>();
                grpAttribIds.add(groupAttribute.id());

                /*
                 * Check for aliasing in aggregates which occurs in two cases (due to combining project + stats):
                 *  - before stats (keep x = a | stats by x) which requires the partial input to use a's channel
                 *  - after  stats (stats by a | keep x = a) which causes the output layout to refer to the follow-up alias
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
                        }
                    }
                }
                attrs.add(groupAttribute);
            }

            attrs.addAll(Expressions.asAttributes(aggregateMapper.mapGrouping(aggregates)));
        }
        return attrs;
    }

    private record AggFunctionSupplierContext(AggregatorFunctionSupplier supplier, AggregatorMode mode) {}

    private void aggregatesToFactory(
        List<? extends NamedExpression> aggregates,
        AggregatorMode mode,
        Layout layout,
        boolean grouping,
        Consumer<AggFunctionSupplierContext> consumer
    ) {
        // extract filtering channels - and wrap the aggregation with the new evaluator expression only during the init phase
        for (NamedExpression ne : aggregates) {
            // a filter can only appear on aggregate function, not on the grouping columns

            if (ne instanceof Alias alias) {
                var child = alias.child();
                if (child instanceof AggregateFunction aggregateFunction) {
                    List<NamedExpression> sourceAttr = new ArrayList<>();

                    if (mode == AggregatorMode.INITIAL) {
                        // TODO: this needs to be made more reliable - use casting to blow up when dealing with expressions (e+1)
                        Expression field = aggregateFunction.field();
                        // Only count can now support literals - all the other aggs should be optimized away
                        if (field.foldable()) {
                            if (aggregateFunction instanceof Count) {
                                sourceAttr = emptyList();
                            } else {
                                throw new InvalidArgumentException(
                                    "Does not support yet aggregations over constants - [{}]",
                                    aggregateFunction.sourceText()
                                );
                            }
                        } else {
                            // extra dependencies like TS ones (that require a timestamp)
                            for (Expression input : aggregateFunction.references()) {
                                Attribute attr = Expressions.attribute(input);
                                if (attr == null) {
                                    throw new EsqlIllegalArgumentException(
                                        "Cannot work with target field [{}] for agg [{}]",
                                        input.sourceText(),
                                        aggregateFunction.sourceText()
                                    );
                                }
                                sourceAttr.add(attr);
                            }
                        }
                    }
                    // coordinator/exchange phase
                    else if (mode == AggregatorMode.FINAL || mode == AggregatorMode.INTERMEDIATE) {
                        if (grouping) {
                            sourceAttr = aggregateMapper.mapGrouping(aggregateFunction);
                        } else {
                            sourceAttr = aggregateMapper.mapNonGrouping(aggregateFunction);
                        }
                    } else {
                        throw new EsqlIllegalArgumentException("illegal aggregation mode");
                    }
                    List<Integer> inputChannels = sourceAttr.stream().map(attr -> layout.get(attr.id()).channel()).toList();
                    assert inputChannels.stream().allMatch(i -> i >= 0) : inputChannels;

                    AggregatorFunctionSupplier aggSupplier = supplier(aggregateFunction, inputChannels);

                    // apply the filter only in the initial phase - as the rest of the data is already filtered
                    if (aggregateFunction.hasFilter() && mode.isInputPartial() == false) {
                        EvalOperator.ExpressionEvaluator.Factory evalFactory = EvalMapper.toEvaluator(aggregateFunction.filter(), layout);
                        aggSupplier = new FilteredAggregatorFunctionSupplier(aggSupplier, evalFactory);
                    }
                    consumer.accept(new AggFunctionSupplierContext(aggSupplier, mode));
                }
            }
        }
    }

    private static AggregatorFunctionSupplier supplier(AggregateFunction aggregateFunction, List<Integer> inputChannels) {
        if (aggregateFunction instanceof ToAggregator delegate) {
            return delegate.supplier(inputChannels);
        }
        throw new EsqlIllegalArgumentException("aggregate functions must extend ToAggregator");
    }

    private record GroupSpec(Integer channel, Attribute attribute) {
        BlockHash.GroupSpec toHashGroupSpec() {
            if (channel == null) {
                throw new EsqlIllegalArgumentException("planned to use ordinals but tried to use the hash instead");
            }
            return new BlockHash.GroupSpec(channel, elementType());
        }

        ElementType elementType() {
            return PlannerUtils.toElementType(attribute.dataType());
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
        LocalExecutionPlannerContext context
    );
}
