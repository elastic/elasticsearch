/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.planner;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.compute.Describable;
import org.elasticsearch.compute.Experimental;
import org.elasticsearch.compute.aggregation.Aggregator.AggregatorFactory;
import org.elasticsearch.compute.aggregation.AggregatorMode;
import org.elasticsearch.compute.aggregation.BlockHash;
import org.elasticsearch.compute.aggregation.GroupingAggregator;
import org.elasticsearch.compute.aggregation.GroupingAggregatorFunction;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.lucene.DataPartitioning;
import org.elasticsearch.compute.lucene.LuceneSourceOperator.LuceneSourceOperatorFactory;
import org.elasticsearch.compute.lucene.ValuesSourceReaderOperator;
import org.elasticsearch.compute.operator.AggregationOperator.AggregationOperatorFactory;
import org.elasticsearch.compute.operator.Driver;
import org.elasticsearch.compute.operator.EvalOperator.EvalOperatorFactory;
import org.elasticsearch.compute.operator.EvalOperator.ExpressionEvaluator;
import org.elasticsearch.compute.operator.FilterOperator.FilterOperatorFactory;
import org.elasticsearch.compute.operator.HashAggregationOperator.HashAggregationOperatorFactory;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.compute.operator.Operator.OperatorFactory;
import org.elasticsearch.compute.operator.OutputOperator.OutputOperatorFactory;
import org.elasticsearch.compute.operator.RowOperator.RowOperatorFactory;
import org.elasticsearch.compute.operator.SinkOperator;
import org.elasticsearch.compute.operator.SinkOperator.SinkOperatorFactory;
import org.elasticsearch.compute.operator.SourceOperator;
import org.elasticsearch.compute.operator.SourceOperator.SourceOperatorFactory;
import org.elasticsearch.compute.operator.TopNOperator.TopNOperatorFactory;
import org.elasticsearch.compute.operator.exchange.Exchange;
import org.elasticsearch.compute.operator.exchange.ExchangeSinkOperator.ExchangeSinkOperatorFactory;
import org.elasticsearch.compute.operator.exchange.ExchangeSourceOperator.ExchangeSourceOperatorFactory;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.search.aggregations.support.FieldContext;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Avg;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Count;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.Round;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.Length;
import org.elasticsearch.xpack.esql.plan.physical.AggregateExec;
import org.elasticsearch.xpack.esql.plan.physical.EsQueryExec;
import org.elasticsearch.xpack.esql.plan.physical.EvalExec;
import org.elasticsearch.xpack.esql.plan.physical.ExchangeExec;
import org.elasticsearch.xpack.esql.plan.physical.FieldExtractExec;
import org.elasticsearch.xpack.esql.plan.physical.FilterExec;
import org.elasticsearch.xpack.esql.plan.physical.LimitExec;
import org.elasticsearch.xpack.esql.plan.physical.OutputExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.ProjectExec;
import org.elasticsearch.xpack.esql.plan.physical.RowExec;
import org.elasticsearch.xpack.esql.plan.physical.TopNExec;
import org.elasticsearch.xpack.esql.session.EsqlConfiguration;
import org.elasticsearch.xpack.ql.expression.Alias;
import org.elasticsearch.xpack.ql.expression.Attribute;
import org.elasticsearch.xpack.ql.expression.AttributeSet;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.Expressions;
import org.elasticsearch.xpack.ql.expression.Literal;
import org.elasticsearch.xpack.ql.expression.NameId;
import org.elasticsearch.xpack.ql.expression.NamedExpression;
import org.elasticsearch.xpack.ql.expression.Order;
import org.elasticsearch.xpack.ql.expression.function.aggregate.AggregateFunction;
import org.elasticsearch.xpack.ql.expression.predicate.operator.arithmetic.ArithmeticOperation;
import org.elasticsearch.xpack.ql.expression.predicate.operator.comparison.GreaterThan;
import org.elasticsearch.xpack.ql.type.DataTypes;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.stream.Collectors.joining;
import static org.elasticsearch.compute.operator.LimitOperator.LimitOperatorFactory;
import static org.elasticsearch.compute.operator.ProjectOperator.ProjectOperatorFactory;

/**
 * The local execution planner takes a plan (represented as PlanNode tree / digraph) as input and creates the corresponding
 * drivers that are used to execute the given plan.
 */
@Experimental
public class LocalExecutionPlanner {

    private static final Setting<Integer> TASK_CONCURRENCY = Setting.intSetting(
        "task_concurrency",
        ThreadPool.searchThreadPoolSize(EsExecutors.allocatedProcessors(Settings.EMPTY))
    );
    private static final Setting<Integer> BUFFER_MAX_PAGES = Setting.intSetting("buffer_max_pages", 500);
    private static final Setting<DataPartitioning> DATA_PARTITIONING = Setting.enumSetting(
        DataPartitioning.class,
        "data_partitioning",
        DataPartitioning.SEGMENT
    );

    private final BigArrays bigArrays;
    public final int taskConcurrency;
    private final int bufferMaxPages;
    private final DataPartitioning dataPartitioning;
    private final List<SearchContext> searchContexts;

    public LocalExecutionPlanner(BigArrays bigArrays, EsqlConfiguration configuration, List<SearchContext> searchContexts) {
        this.bigArrays = bigArrays;
        taskConcurrency = TASK_CONCURRENCY.get(configuration.pragmas());
        bufferMaxPages = BUFFER_MAX_PAGES.get(configuration.pragmas());
        dataPartitioning = DATA_PARTITIONING.get(configuration.pragmas());
        this.searchContexts = searchContexts;
    }

    /**
     * turn the given plan into a list of drivers to execute
     */
    public LocalExecutionPlan plan(PhysicalPlan node) {
        LocalExecutionPlanContext context = new LocalExecutionPlanContext();

        PhysicalOperation physicalOperation = plan(node, context);

        context.addDriverFactory(new DriverFactory(new DriverSupplier(bigArrays, physicalOperation), context.driverParallelism()));

        LocalExecutionPlan localExecutionPlan = new LocalExecutionPlan();
        localExecutionPlan.driverFactories.addAll(context.driverFactories);
        return localExecutionPlan;
    }

    public PhysicalOperation plan(PhysicalPlan node, LocalExecutionPlanContext context) {
        if (node instanceof AggregateExec aggregate) {
            PhysicalOperation source = plan(aggregate.child(), context);
            Layout.Builder layout = new Layout.Builder();
            OperatorFactory operatorFactory = null;

            if (aggregate.groupings().isEmpty()) {
                // not grouping
                for (NamedExpression e : aggregate.aggregates()) {
                    if (e instanceof Alias alias && alias.child()instanceof AggregateFunction aggregateFunction) {
                        var provider = AggregateMapper.map(aggregateFunction);

                        if (aggregate.getMode() == AggregateExec.Mode.PARTIAL) {
                            operatorFactory = new AggregationOperatorFactory(
                                List.of(
                                    new AggregatorFactory(
                                        provider,
                                        AggregatorMode.INITIAL,
                                        source.layout.getChannel(Expressions.attribute(aggregateFunction.field()).id())
                                    )
                                ),
                                AggregatorMode.INITIAL
                            );
                            layout.appendChannel(alias.id());
                        } else if (aggregate.getMode() == AggregateExec.Mode.FINAL) {
                            operatorFactory = new AggregationOperatorFactory(
                                List.of(new AggregatorFactory(provider, AggregatorMode.FINAL, source.layout.getChannel(alias.id()))),
                                AggregatorMode.FINAL
                            );
                            layout.appendChannel(alias.id());
                        } else {
                            throw new UnsupportedOperationException();
                        }
                    } else {
                        throw new UnsupportedOperationException();
                    }
                }
            } else {
                // grouping
                AttributeSet groups = Expressions.references(aggregate.groupings());
                if (groups.size() != 1) {
                    throw new UnsupportedOperationException("just one group, for now");
                }
                Attribute grpAttrib = groups.iterator().next();
                layout.appendChannel(grpAttrib.id());

                for (NamedExpression e : aggregate.aggregates()) {
                    if (e instanceof Alias alias && alias.child()instanceof AggregateFunction aggregateFunction) {
                        GroupingAggregatorFunction.GroupingAggregatorFunctionFactory aggregatorFunc;
                        if (aggregateFunction instanceof Avg) {
                            aggregatorFunc = GroupingAggregatorFunction.avg;
                        } else if (aggregateFunction instanceof Count) {
                            aggregatorFunc = GroupingAggregatorFunction.count;
                        } else {
                            throw new UnsupportedOperationException("unsupported aggregate function:" + aggregateFunction);
                        }

                        final Supplier<BlockHash> blockHash;
                        if (grpAttrib.dataType() == DataTypes.KEYWORD) {
                            blockHash = () -> BlockHash.newBytesRefHash(bigArrays);
                        } else {
                            blockHash = () -> BlockHash.newLongHash(bigArrays);
                        }
                        if (aggregate.getMode() == AggregateExec.Mode.PARTIAL) {
                            operatorFactory = new HashAggregationOperatorFactory(
                                source.layout.getChannel(grpAttrib.id()),
                                List.of(
                                    new GroupingAggregator.GroupingAggregatorFactory(
                                        bigArrays,
                                        aggregatorFunc,
                                        AggregatorMode.INITIAL,
                                        source.layout.getChannel(Expressions.attribute(aggregateFunction.field()).id())
                                    )
                                ),
                                blockHash,
                                AggregatorMode.INITIAL
                            );
                            layout.appendChannel(alias.id());  // <<<< TODO: this one looks suspicious
                        } else if (aggregate.getMode() == AggregateExec.Mode.FINAL) {
                            operatorFactory = new HashAggregationOperatorFactory(
                                source.layout.getChannel(grpAttrib.id()),
                                List.of(
                                    new GroupingAggregator.GroupingAggregatorFactory(
                                        bigArrays,
                                        aggregatorFunc,
                                        AggregatorMode.FINAL,
                                        source.layout.getChannel(alias.id())
                                    )
                                ),
                                blockHash,
                                AggregatorMode.FINAL
                            );
                            layout.appendChannel(alias.id());
                        } else {
                            throw new UnsupportedOperationException();
                        }
                    } else if (aggregate.groupings().contains(e) == false) {
                        var u = e instanceof Alias ? ((Alias) e).child() : e;
                        throw new UnsupportedOperationException(
                            "expected an aggregate function, but got [" + u + "] of type [" + u.nodeName() + "]"
                        );
                    }
                }

            }
            if (operatorFactory != null) {
                return source.with(operatorFactory, layout.build());
            }
            throw new UnsupportedOperationException();
        } else if (node instanceof EsQueryExec esQuery) {
            return planEsQueryNode(esQuery, context);
        } else if (node instanceof FieldExtractExec fieldExtractExec) {
            return planFieldExtractNode(context, fieldExtractExec);
        } else if (node instanceof OutputExec outputExec) {
            PhysicalOperation source = plan(outputExec.child(), context);
            var output = outputExec.output();
            if (output.size() != source.layout.numberOfIds()) {
                throw new IllegalStateException(
                    "expected layout:"
                        + output
                        + ": "
                        + output.stream().map(NamedExpression::id).toList()
                        + ", source.layout:"
                        + source.layout
                );
            }
            // align the page layout with the operator output
            // extraction order - the list ordinal is the same as the column one
            // while the value represents the position in the original page
            final int[] mappedPosition = new int[output.size()];
            int index = -1;
            boolean transformRequired = false;
            for (var attribute : output) {
                mappedPosition[++index] = source.layout.getChannel(attribute.id());
                if (transformRequired == false) {
                    transformRequired = mappedPosition[index] != index;
                }
            }
            Function<Page, Page> mapper = transformRequired ? p -> {
                var blocks = new Block[mappedPosition.length];
                for (int i = 0; i < blocks.length; i++) {
                    blocks[i] = p.getBlock(mappedPosition[i]);
                }
                return new Page(blocks);
            } : Function.identity();

            return source.withSink(
                new OutputOperatorFactory(Expressions.names(outputExec.output()), mapper, outputExec.getPageConsumer()),
                source.layout
            );
        } else if (node instanceof ExchangeExec exchangeExec) {
            DriverParallelism parallelism = exchangeExec.getType() == ExchangeExec.Type.GATHER
                ? DriverParallelism.SINGLE
                : new DriverParallelism(DriverParallelism.Type.TASK_LEVEL_PARALLELISM, taskConcurrency);
            context.driverParallelism(parallelism);
            Exchange ex = new Exchange(parallelism.instanceCount(), exchangeExec.getPartitioning().toExchange(), bufferMaxPages);

            LocalExecutionPlanContext subContext = context.createSubContext();
            PhysicalOperation source = plan(exchangeExec.child(), subContext);
            Layout layout = source.layout;
            PhysicalOperation sink = source.withSink(new ExchangeSinkOperatorFactory(ex), source.layout);
            context.addDriverFactory(new DriverFactory(new DriverSupplier(bigArrays, sink), subContext.driverParallelism()));
            return PhysicalOperation.fromSource(new ExchangeSourceOperatorFactory(ex), layout);
        } else if (node instanceof TopNExec topNExec) {
            PhysicalOperation source = plan(topNExec.child(), context);
            if (topNExec.order().size() != 1) {
                throw new UnsupportedOperationException();
            }
            Order order = topNExec.order().get(0);
            int sortByChannel;
            if (order.child()instanceof Attribute a) {
                sortByChannel = source.layout.getChannel(a.id());
            } else {
                throw new UnsupportedOperationException();
            }
            int limit;
            if (topNExec.getLimit()instanceof Literal literal) {
                limit = Integer.parseInt(literal.value().toString());
            } else {
                throw new UnsupportedOperationException();
            }

            return source.with(
                new TopNOperatorFactory(
                    sortByChannel,
                    order.direction() == Order.OrderDirection.ASC,
                    limit,
                    order.nullsPosition().equals(Order.NullsPosition.FIRST)
                ),
                source.layout
            );
        } else if (node instanceof EvalExec eval) {
            PhysicalOperation source = plan(eval.child(), context);
            if (eval.fields().size() != 1) {
                throw new UnsupportedOperationException();
            }
            NamedExpression namedExpression = eval.fields().get(0);
            ExpressionEvaluator evaluator;
            if (namedExpression instanceof Alias alias) {
                evaluator = toEvaluator(alias.child(), source.layout);
            } else {
                throw new UnsupportedOperationException();
            }
            Layout.Builder layout = source.layout.builder();
            layout.appendChannel(namedExpression.toAttribute().id());
            return source.with(
                new EvalOperatorFactory(evaluator, namedExpression.dataType().isRational() ? Double.TYPE : Long.TYPE),
                layout.build()
            );
        } else if (node instanceof RowExec row) {
            List<Object> obj = row.fields().stream().map(f -> {
                if (f instanceof Alias) {
                    return ((Alias) f).child().fold();
                } else {
                    return f.fold();
                }
            }).toList();
            Layout.Builder layout = new Layout.Builder();
            var output = row.output();
            for (int i = 0; i < output.size(); i++) {
                layout.appendChannel(output.get(i).id());
            }
            return PhysicalOperation.fromSource(new RowOperatorFactory(obj), layout.build());
        } else if (node instanceof ProjectExec project) {
            var source = plan(project.child(), context);

            Map<Integer, Set<NameId>> inputChannelToOutputIds = new HashMap<>();
            for (NamedExpression ne : project.projections()) {
                NameId inputId;
                if (ne instanceof Alias a) {
                    inputId = ((NamedExpression) a.child()).id();
                } else {
                    inputId = ne.id();
                }
                int inputChannel = source.layout.getChannel(inputId);
                inputChannelToOutputIds.computeIfAbsent(inputChannel, ignore -> new HashSet<>()).add(ne.id());
            }

            BitSet mask = new BitSet();
            Layout.Builder layout = new Layout.Builder();

            for (int inChannel = 0; inChannel < source.layout.numberOfChannels(); inChannel++) {
                Set<NameId> outputIds = inputChannelToOutputIds.get(inChannel);

                if (outputIds != null) {
                    mask.set(inChannel);
                    layout.appendChannel(outputIds);
                }
            }

            if (mask.cardinality() == source.layout.numberOfChannels()) {
                // all columns are retained, project operator is not needed but the layout needs to be updated
                return source.with(layout.build());
            } else {
                return source.with(new ProjectOperatorFactory(mask), layout.build());
            }
        } else if (node instanceof FilterExec filter) {
            PhysicalOperation source = plan(filter.child(), context);
            return source.with(new FilterOperatorFactory(toEvaluator(filter.condition(), source.layout)), source.layout);
        } else if (node instanceof LimitExec limit) {
            PhysicalOperation source = plan(limit.child(), context);
            return source.with(new LimitOperatorFactory((Integer) limit.limit().fold()), source.layout);
        }
        throw new UnsupportedOperationException(node.nodeName());
    }

    private PhysicalOperation planEsQueryNode(EsQueryExec esQuery, LocalExecutionPlanContext context) {
        Set<String> indices = Sets.newHashSet(esQuery.index().name());
        List<SearchExecutionContext> matchedSearchContexts = this.searchContexts.stream()
            .filter(ctx -> indices.contains(ctx.indexShard().shardId().getIndexName()))
            .map(SearchContext::getSearchExecutionContext)
            .toList();
        LuceneSourceOperatorFactory operatorFactory = new LuceneSourceOperatorFactory(
            matchedSearchContexts,
            ctx -> ctx.toQuery(esQuery.query()).query(),
            dataPartitioning,
            taskConcurrency
        );
        context.driverParallelism(new DriverParallelism(DriverParallelism.Type.DATA_PARALLELISM, operatorFactory.size()));
        Layout.Builder layout = new Layout.Builder();
        for (int i = 0; i < esQuery.output().size(); i++) {
            layout.appendChannel(esQuery.output().get(i).id());
        }
        return PhysicalOperation.fromSource(operatorFactory, layout.build());
    }

    private PhysicalOperation planFieldExtractNode(LocalExecutionPlanContext context, FieldExtractExec fieldExtractExec) {
        PhysicalOperation source = plan(fieldExtractExec.child(), context);
        Layout.Builder layout = source.layout.builder();

        var sourceAttrs = fieldExtractExec.sourceAttributes();

        PhysicalOperation op = source;
        for (Attribute attr : fieldExtractExec.attributesToExtract()) {
            layout.appendChannel(attr.id());
            Layout previousLayout = op.layout;

            // Create ValuesSource object for the field to extract its values
            final List<Tuple<ValuesSourceType, ValuesSource>> valuesSources = searchContexts.stream()
                .map(SearchContext::getSearchExecutionContext)
                .map(ctx -> {
                    MappedFieldType fieldType = ctx.getFieldType(attr.name());
                    IndexFieldData<?> fieldData = ctx.getForField(fieldType, MappedFieldType.FielddataOperation.SEARCH);
                    FieldContext fieldContext = new FieldContext(attr.name(), fieldData, fieldType);
                    ValuesSourceType vstype = fieldData.getValuesSourceType();
                    ValuesSource vs = vstype.getField(fieldContext, null);
                    return Tuple.tuple(vstype, vs);
                })
                .collect(Collectors.toList());

            final List<IndexReader> indexReaders = searchContexts.stream()
                .map(ctx -> ctx.getSearchExecutionContext().getIndexReader())
                .collect(Collectors.toList());

            op = op.with(
                new ValuesSourceReaderOperator.ValuesSourceReaderOperatorFactory(
                    valuesSources.stream().map(Tuple::v1).collect(Collectors.toList()),
                    valuesSources.stream().map(Tuple::v2).collect(Collectors.toList()),
                    indexReaders,
                    previousLayout.getChannel(sourceAttrs.get(0).id()),
                    previousLayout.getChannel(sourceAttrs.get(1).id()),
                    previousLayout.getChannel(sourceAttrs.get(2).id()),
                    attr.name()
                ),
                layout.build()
            );
        }
        return op;
    }

    private ExpressionEvaluator toEvaluator(Expression exp, Layout layout) {
        if (exp instanceof ArithmeticOperation ao) {
            ExpressionEvaluator leftEval = toEvaluator(ao.left(), layout);
            ExpressionEvaluator rightEval = toEvaluator(ao.right(), layout);
            return (page, pos) -> {
                Number left = (Number) leftEval.computeRow(page, pos);
                Number right = (Number) rightEval.computeRow(page, pos);
                return ao.function().apply(left, right);
            };
        } else if (exp instanceof GreaterThan gt) {
            ExpressionEvaluator e1 = toEvaluator(gt.left(), layout);
            ExpressionEvaluator e2 = toEvaluator(gt.right(), layout);
            if (gt.left().dataType().isRational()) {
                return (page, pos) -> ((Number) e1.computeRow(page, pos)).doubleValue() > ((Number) e2.computeRow(page, pos)).doubleValue();
            } else {
                return (page, pos) -> ((Number) e1.computeRow(page, pos)).longValue() > ((Number) e2.computeRow(page, pos)).longValue();
            }
        } else if (exp instanceof Attribute attr) {
            int channel = layout.getChannel(attr.id());
            return (page, pos) -> page.getBlock(channel).getObject(pos);
        } else if (exp instanceof Literal lit) {
            if (lit.value() == null) { // NULL, the literal
                return (page, pos) -> null;
            } else if (exp.dataType().isRational()) {
                double d = Double.parseDouble(lit.value().toString());
                return (page, pos) -> d;
            } else {
                long l = Long.parseLong(lit.value().toString());
                return (page, pos) -> l;
            }
        } else if (exp instanceof Round round) {
            ExpressionEvaluator fieldEvaluator = toEvaluator(round.field(), layout);
            // round.decimals() == null means that decimals were not provided (it's an optional parameter of the Round function)
            ExpressionEvaluator decimalsEvaluator = round.decimals() != null ? toEvaluator(round.decimals(), layout) : null;
            if (round.field().dataType().isRational()) {
                return (page, pos) -> {
                    // decimals could be null
                    // it's not the same null as round.decimals() being null
                    Object decimals = decimalsEvaluator != null ? decimalsEvaluator.computeRow(page, pos) : null;
                    return Round.process(fieldEvaluator.computeRow(page, pos), decimals);
                };
            } else {
                return (page, pos) -> fieldEvaluator.computeRow(page, pos);
            }
        } else if (exp instanceof Length length) {
            ExpressionEvaluator e1 = toEvaluator(length.field(), layout);
            return (page, pos) -> Length.process(((BytesRef) e1.computeRow(page, pos)).utf8ToString());
        } else {
            throw new UnsupportedOperationException(exp.nodeName());
        }
    }

    /**
     * Immutable physical operation.
     */
    static class PhysicalOperation implements Describable {
        private final SourceOperatorFactory sourceOperatorFactory;
        private final List<OperatorFactory> intermediateOperatorFactories;
        private final SinkOperatorFactory sinkOperatorFactory;

        private final Layout layout; // maps field names to channels

        /** Creates a new physical operation with the given source and layout. */
        static PhysicalOperation fromSource(SourceOperatorFactory sourceOperatorFactory, Layout layout) {
            return new PhysicalOperation(sourceOperatorFactory, layout);
        }

        /** Creates a new physical operation from this operation with the given layout. */
        PhysicalOperation with(Layout layout) {
            return new PhysicalOperation(this, Optional.empty(), Optional.empty(), layout);
        }

        /** Creates a new physical operation from this operation with the given intermediate operator and layout. */
        PhysicalOperation with(OperatorFactory operatorFactory, Layout layout) {
            return new PhysicalOperation(this, Optional.of(operatorFactory), Optional.empty(), layout);
        }

        /** Creates a new physical operation from this operation with the given sink and layout. */
        PhysicalOperation withSink(SinkOperatorFactory sink, Layout layout) {
            return new PhysicalOperation(this, Optional.empty(), Optional.of(sink), layout);
        }

        private PhysicalOperation(SourceOperatorFactory sourceOperatorFactory, Layout layout) {
            this.sourceOperatorFactory = sourceOperatorFactory;
            this.intermediateOperatorFactories = List.of();
            this.sinkOperatorFactory = null;
            this.layout = layout;
        }

        private PhysicalOperation(
            PhysicalOperation physicalOperation,
            Optional<OperatorFactory> intermediateOperatorFactory,
            Optional<SinkOperatorFactory> sinkOperatorFactory,
            Layout layout
        ) {
            sourceOperatorFactory = physicalOperation.sourceOperatorFactory;
            intermediateOperatorFactories = new ArrayList<>();
            intermediateOperatorFactories.addAll(physicalOperation.intermediateOperatorFactories);
            intermediateOperatorFactory.ifPresent(intermediateOperatorFactories::add);
            this.sinkOperatorFactory = sinkOperatorFactory.isPresent() ? sinkOperatorFactory.get() : null;
            this.layout = layout;
        }

        public SourceOperator source() {
            return sourceOperatorFactory.get();
        }

        public void operators(List<Operator> operators) {
            intermediateOperatorFactories.stream().map(OperatorFactory::get).forEach(operators::add);
        }

        public SinkOperator sink() {
            return sinkOperatorFactory.get();
        }

        @Override
        public String describe() {
            return Stream.concat(
                Stream.concat(Stream.of(sourceOperatorFactory), intermediateOperatorFactories.stream()),
                Stream.of(sinkOperatorFactory)
            ).map(Describable::describe).collect(joining("\n\\_", "\\_", ""));
        }
    }

    /**
     * The count and type of driver parallelism.
     */
    record DriverParallelism(Type type, int instanceCount) {

        static final DriverParallelism SINGLE = new DriverParallelism(Type.SINGLETON, 1);

        enum Type {
            SINGLETON,
            DATA_PARALLELISM,
            TASK_LEVEL_PARALLELISM
        }
    }

    /**
     * Context object used while generating a local plan. Currently only collects the driver factories as well as
     * maintains information how many driver instances should be created for a given driver.
     */
    public static class LocalExecutionPlanContext {
        final List<DriverFactory> driverFactories;

        private DriverParallelism driverParallelism = DriverParallelism.SINGLE;

        LocalExecutionPlanContext() {
            driverFactories = new ArrayList<>();
        }

        LocalExecutionPlanContext(List<DriverFactory> driverFactories) {
            this.driverFactories = driverFactories;
        }

        void addDriverFactory(DriverFactory driverFactory) {
            driverFactories.add(driverFactory);
        }

        public LocalExecutionPlanContext createSubContext() {
            LocalExecutionPlanContext subContext = new LocalExecutionPlanContext(driverFactories);
            return subContext;
        }

        public DriverParallelism driverParallelism() {
            return driverParallelism;
        }

        public void driverParallelism(DriverParallelism driverParallelism) {
            this.driverParallelism = driverParallelism;
        }
    }

    record DriverSupplier(BigArrays bigArrays, PhysicalOperation physicalOperation) implements Supplier<Driver>, Describable {

        @Override
        public Driver get() {
            SourceOperator source = null;
            List<Operator> operators = new ArrayList<>();
            SinkOperator sink = null;
            boolean success = false;
            try {
                source = physicalOperation.source();
                physicalOperation.operators(operators);
                sink = physicalOperation.sink();
                success = true;
                return new Driver(source, operators, sink, () -> {});
            } finally {
                if (false == success) {
                    Releasables.close(source, () -> Releasables.close(operators), sink);
                }
            }
        }

        @Override
        public String describe() {
            return physicalOperation.describe();
        }
    }

    record DriverFactory(DriverSupplier driverSupplier, DriverParallelism driverParallelism) implements Describable {
        @Override
        public String describe() {
            return "DriverFactory(instances = "
                + driverParallelism.instanceCount()
                + ", type = "
                + driverParallelism.type()
                + ")\n"
                + driverSupplier.describe();
        }
    }

    /**
     * Plan representation that is geared towards execution on a single node
     */
    public static class LocalExecutionPlan implements Describable {
        final List<DriverFactory> driverFactories = new ArrayList<>();

        public void createDrivers(List<Driver> drivers) {
            for (DriverFactory df : driverFactories) {
                for (int i = 0; i < df.driverParallelism.instanceCount; i++) {
                    drivers.add(df.driverSupplier.get());
                }
            }
        }

        @Override
        public String describe() {
            StringBuilder sb = new StringBuilder();
            sb.append(driverFactories.stream().map(DriverFactory::describe).collect(joining("\n")));
            return sb.toString();
        }
    }
}
