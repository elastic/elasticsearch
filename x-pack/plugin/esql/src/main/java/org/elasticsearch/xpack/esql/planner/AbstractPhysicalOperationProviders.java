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
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.analysis.AnalysisRegistry;
import org.elasticsearch.xpack.esql.EsqlIllegalArgumentException;
import org.elasticsearch.xpack.esql.core.InvalidArgumentException;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.AttributeMap;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.NameId;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.evaluator.EvalMapper;
import org.elasticsearch.xpack.esql.expression.Order;
import org.elasticsearch.xpack.esql.expression.function.aggregate.AggregateFunction;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Count;
import org.elasticsearch.xpack.esql.expression.function.grouping.Categorize;
import org.elasticsearch.xpack.esql.plan.physical.AbstractAggregateExec;
import org.elasticsearch.xpack.esql.plan.physical.TimeSeriesAggregateExec;
import org.elasticsearch.xpack.esql.plan.physical.TopNAggregateExec;
import org.elasticsearch.xpack.esql.planner.LocalExecutionPlanner.LocalExecutionPlannerContext;
import org.elasticsearch.xpack.esql.planner.LocalExecutionPlanner.PhysicalOperation;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;

import static java.util.Collections.emptyList;
import static org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter.stringToInt;

public abstract class AbstractPhysicalOperationProviders implements PhysicalOperationProviders {

    private final AggregateMapper aggregateMapper = new AggregateMapper();
    private final FoldContext foldContext;
    private final AnalysisRegistry analysisRegistry;

    AbstractPhysicalOperationProviders(FoldContext foldContext, AnalysisRegistry analysisRegistry) {
        this.foldContext = foldContext;
        this.analysisRegistry = analysisRegistry;
    }

    @Override
    public final PhysicalOperation groupingPhysicalOperation(
        AbstractAggregateExec aggregateExec,
        PhysicalOperation source,
        LocalExecutionPlannerContext context
    ) {
        // The layout this operation will produce.
        Layout.Builder layout = new Layout.Builder();
        Operator.OperatorFactory operatorFactory = null;
        AggregatorMode aggregatorMode = aggregateExec.getMode();
        var aggregates = aggregateExec.aggregates();

        var sourceLayout = source.layout;

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
                s -> aggregatorFactories.add(s.supplier.aggregatorFactory(s.mode, s.channels)),
                context
            );

            if (aggregatorFactories.isEmpty() == false) {
                operatorFactory = new AggregationOperator.AggregationOperatorFactory(aggregatorFactories, aggregatorMode);
            }
        } else {
            // grouping
            List<GroupingAggregator.Factory> aggregatorFactories = new ArrayList<>();
            List<GroupSpec> groupSpecs = new ArrayList<>(aggregateExec.groupings().size());
            AttributeMap<BlockHash.TopNDef> attributesToTopNDef = buildAttributesToTopNDefMap(aggregateExec);
            for (Expression group : aggregateExec.groupings()) {
                Attribute groupAttribute = Expressions.attribute(group);
                // In case of `... BY groupAttribute = CATEGORIZE(sourceGroupAttribute)` the actual source attribute is different.
                Attribute sourceGroupAttribute = (aggregatorMode.isInputPartial() == false
                    && group instanceof Alias as
                    && as.child() instanceof Categorize categorize) ? Expressions.attribute(categorize.field()) : groupAttribute;
                if (sourceGroupAttribute == null) {
                    throw new EsqlIllegalArgumentException("Unexpected non-named expression[{}] as grouping in [{}]", group, aggregateExec);
                }
                Layout.ChannelSet groupAttributeLayout = new Layout.ChannelSet(new HashSet<>(), sourceGroupAttribute.dataType());
                groupAttributeLayout.nameIds()
                    .add(group instanceof Alias as && as.child() instanceof Categorize ? groupAttribute.id() : sourceGroupAttribute.id());

                /*
                 * Check for aliasing in aggregates which occurs in two cases (due to combining project + stats):
                 *  - before stats (keep x = a | stats by x) which requires the partial input to use a's channel
                 *  - after  stats (stats by a | keep x = a) which causes the output layout to refer to the follow-up alias
                 */
                // TODO: This is likely required only for pre-8.14 node compatibility; confirm and remove if possible.
                // Since https://github.com/elastic/elasticsearch/pull/104958, it shouldn't be possible to have aliases in the aggregates
                // which the groupings refer to. Except for `BY CATEGORIZE(field)`, which remains as alias in the grouping, all aliases
                // should've become EVALs before or after the STATS.
                for (NamedExpression agg : aggregates) {
                    if (agg instanceof Alias a) {
                        if (a.child() instanceof Attribute attr) {
                            if (sourceGroupAttribute.id().equals(attr.id())) {
                                groupAttributeLayout.nameIds().add(a.id());
                                // TODO: investigate whether a break could be used since it shouldn't be possible to have multiple
                                // attributes pointing to the same attribute
                            }
                            // partial mode only
                            // check if there's any alias used in grouping - no need for the final reduction since the intermediate data
                            // is in the output form
                            // if the group points to an alias declared in the aggregate, use the alias child as source
                            else if (aggregatorMode.isOutputPartial()) {
                                if (sourceGroupAttribute.semanticEquals(a.toAttribute())) {
                                    sourceGroupAttribute = attr;
                                    break;
                                }
                            }
                        }
                    }
                }
                layout.append(groupAttributeLayout);
                Layout.ChannelAndType groupInput = source.layout.get(sourceGroupAttribute.id());
                BlockHash.TopNDef topNDef = attributesToTopNDef.get(sourceGroupAttribute);
                groupSpecs.add(new GroupSpec(groupInput == null ? null : groupInput.channel(), sourceGroupAttribute, group, topNDef));
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
                s -> aggregatorFactories.add(s.supplier.groupingAggregatorFactory(s.mode, s.channels)),
                context
            );
            // time-series aggregation
            if (aggregateExec instanceof TimeSeriesAggregateExec ts) {
                operatorFactory = timeSeriesAggregatorOperatorFactory(
                    ts,
                    aggregatorMode,
                    aggregatorFactories,
                    groupSpecs.stream().map(GroupSpec::toHashGroupSpec).toList(),
                    context
                );
                // ordinal grouping
            } else if (groupSpecs.size() == 1 && groupSpecs.get(0).channel == null) {
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
                    aggregatorMode,
                    aggregatorFactories,
                    context.pageSize(aggregateExec.estimatedRowSize()),
                    analysisRegistry
                );
            }
        }
        if (operatorFactory != null) {
            return source.with(operatorFactory, layout.build());
        }
        throw new EsqlIllegalArgumentException("no operator factory");
    }

    private AttributeMap<BlockHash.TopNDef> buildAttributesToTopNDefMap(AbstractAggregateExec aggregateExec) {
        if (aggregateExec instanceof TopNAggregateExec == false) {
            return AttributeMap.emptyAttributeMap();
        }

        TopNAggregateExec topNAggregateExec = (TopNAggregateExec) aggregateExec;
        List<Order> order = topNAggregateExec.order();
        Expression limit = topNAggregateExec.limit();

        if (order.isEmpty() || limit == null || order.size() != aggregateExec.groupings().size()) {
            return AttributeMap.emptyAttributeMap();
        }

        AttributeMap.Builder<BlockHash.TopNDef> builder = AttributeMap.builder(order.size());

        for (int i = 0; i < order.size(); i++) {
            Order orderEntry = order.get(i);

            if ((orderEntry.child() instanceof Attribute) == false) {
                throw new EsqlIllegalArgumentException("order by expression must be an attribute");
            }
            if ((limit instanceof Literal) == false) {
                throw new EsqlIllegalArgumentException("limit only supported with literal values");
            }

            Attribute attribute = (Attribute) orderEntry.child();
            int intLimit = stringToInt(((Literal) limit).value().toString());

            BlockHash.TopNDef topNDef = new BlockHash.TopNDef(
                i,
                orderEntry.direction().equals(Order.OrderDirection.ASC),
                orderEntry.nullsPosition().equals(Order.NullsPosition.FIRST),
                intLimit
            );
            builder.put(attribute, topNDef);
        }

        return builder.build();
    }

    /***
     * Creates a standard layout for intermediate aggregations, typically used across exchanges.
     * Puts the group first, followed by each aggregation.
     * <p>
     *     It's similar to the code above (groupingPhysicalOperation) but ignores the factory creation.
     * </p>
     */
    public static List<Attribute> intermediateAttributes(List<? extends NamedExpression> aggregates, List<? extends Expression> groupings) {
        // TODO: This should take CATEGORIZE into account:
        // it currently works because the CATEGORIZE intermediate state is just 1 block with the same type as the function return,
        // so the attribute generated here is the expected one
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

    private record AggFunctionSupplierContext(AggregatorFunctionSupplier supplier, List<Integer> channels, AggregatorMode mode) {}

    private void aggregatesToFactory(
        List<? extends NamedExpression> aggregates,
        AggregatorMode mode,
        Layout layout,
        boolean grouping,
        Consumer<AggFunctionSupplierContext> consumer,
        LocalExecutionPlannerContext context
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
                            sourceAttr = aggregateMapper.mapGrouping(ne);
                        } else {
                            sourceAttr = aggregateMapper.mapNonGrouping(ne);
                        }
                    } else {
                        throw new EsqlIllegalArgumentException("illegal aggregation mode");
                    }

                    AggregatorFunctionSupplier aggSupplier = supplier(aggregateFunction);

                    List<Integer> inputChannels = sourceAttr.stream().map(attr -> layout.get(attr.id()).channel()).toList();
                    assert inputChannels.stream().allMatch(i -> i >= 0) : inputChannels;

                    // apply the filter only in the initial phase - as the rest of the data is already filtered
                    if (aggregateFunction.hasFilter() && mode.isInputPartial() == false) {
                        EvalOperator.ExpressionEvaluator.Factory evalFactory = EvalMapper.toEvaluator(
                            foldContext,
                            aggregateFunction.filter(),
                            layout,
                            context.shardContexts()
                        );
                        aggSupplier = new FilteredAggregatorFunctionSupplier(aggSupplier, evalFactory);
                    }
                    consumer.accept(new AggFunctionSupplierContext(aggSupplier, inputChannels, mode));
                }
            }
        }
    }

    private static AggregatorFunctionSupplier supplier(AggregateFunction aggregateFunction) {
        if (aggregateFunction instanceof ToAggregator delegate) {
            return delegate.supplier();
        }
        throw new EsqlIllegalArgumentException("aggregate functions must extend ToAggregator");
    }

    /**
     * The input configuration of this group.
     *
     * @param channel The source channel of this group
     * @param attribute The attribute, source of this group
     * @param expression The expression being used to group
     */
    private record GroupSpec(Integer channel, Attribute attribute, Expression expression, @Nullable BlockHash.TopNDef topNDef) {
        BlockHash.GroupSpec toHashGroupSpec() {
            if (channel == null) {
                throw new EsqlIllegalArgumentException("planned to use ordinals but tried to use the hash instead");
            }

            return new BlockHash.GroupSpec(channel, elementType(), Alias.unwrap(expression) instanceof Categorize, topNDef);
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
        AbstractAggregateExec aggregateExec,
        List<GroupingAggregator.Factory> aggregatorFactories,
        Attribute attrSource,
        ElementType groupType,
        LocalExecutionPlannerContext context
    );

    public abstract Operator.OperatorFactory timeSeriesAggregatorOperatorFactory(
        TimeSeriesAggregateExec ts,
        AggregatorMode aggregatorMode,
        List<GroupingAggregator.Factory> aggregatorFactories,
        List<BlockHash.GroupSpec> groupSpecs,
        LocalExecutionPlannerContext context
    );
}
