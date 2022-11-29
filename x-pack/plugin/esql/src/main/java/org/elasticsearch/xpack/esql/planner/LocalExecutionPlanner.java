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
import org.elasticsearch.compute.aggregation.AggregatorFunction;
import org.elasticsearch.compute.aggregation.AggregatorFunction.AggregatorFunctionFactory;
import org.elasticsearch.compute.aggregation.AggregatorMode;
import org.elasticsearch.compute.aggregation.BlockHash;
import org.elasticsearch.compute.aggregation.GroupingAggregator.GroupingAggregatorFactory;
import org.elasticsearch.compute.aggregation.GroupingAggregatorFunction;
import org.elasticsearch.compute.aggregation.GroupingAggregatorFunction.GroupingAggregatorFunctionFactory;
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
import org.elasticsearch.compute.operator.OperatorFactory;
import org.elasticsearch.compute.operator.OutputOperator.OutputOperatorFactory;
import org.elasticsearch.compute.operator.RowOperator.RowOperatorFactory;
import org.elasticsearch.compute.operator.TopNOperator.TopNOperatorFactory;
import org.elasticsearch.compute.operator.exchange.Exchange;
import org.elasticsearch.compute.operator.exchange.ExchangeSinkOperator.ExchangeSinkOperatorFactory;
import org.elasticsearch.compute.operator.exchange.ExchangeSourceOperator.ExchangeSourceOperatorFactory;
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
import org.elasticsearch.xpack.ql.expression.NamedExpression;
import org.elasticsearch.xpack.ql.expression.Order;
import org.elasticsearch.xpack.ql.expression.function.aggregate.AggregateFunction;
import org.elasticsearch.xpack.ql.expression.predicate.operator.arithmetic.Add;
import org.elasticsearch.xpack.ql.expression.predicate.operator.arithmetic.Div;
import org.elasticsearch.xpack.ql.expression.predicate.operator.comparison.GreaterThan;
import org.elasticsearch.xpack.ql.type.DataTypes;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static java.util.stream.Collectors.joining;
import static org.elasticsearch.compute.operator.ProjectOperator.ProjectOperatorFactory;

/**
 * The local execution planner takes a plan (represented as PlanNode tree / digraph) as input and creates the corresponding
 * drivers that are used to execute the given plan.
 */
@Experimental
public class LocalExecutionPlanner {

    private final List<SearchContext> searchContexts;
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

    public final int taskConcurrency;
    private final int bufferMaxPages;
    private final DataPartitioning dataPartitioning;

    public LocalExecutionPlanner(EsqlConfiguration configuration, List<SearchContext> searchContexts) {
        this.searchContexts = searchContexts;
        taskConcurrency = TASK_CONCURRENCY.get(configuration.pragmas());
        bufferMaxPages = BUFFER_MAX_PAGES.get(configuration.pragmas());
        dataPartitioning = DATA_PARTITIONING.get(configuration.pragmas());
    }

    /**
     * turn the given plan into a list of drivers to execute
     */
    public LocalExecutionPlan plan(PhysicalPlan node) {
        LocalExecutionPlanContext context = new LocalExecutionPlanContext();

        PhysicalOperation physicalOperation = plan(node, context);

        context.addDriverFactory(new DriverFactory(new DriverSupplier(physicalOperation), context.driverParallelism()));

        LocalExecutionPlan localExecutionPlan = new LocalExecutionPlan();
        localExecutionPlan.driverFactories.addAll(context.driverFactories);
        return localExecutionPlan;
    }

    public PhysicalOperation plan(PhysicalPlan node, LocalExecutionPlanContext context) {
        if (node instanceof AggregateExec aggregate) {
            PhysicalOperation source = plan(aggregate.child(), context);
            Map<Object, Integer> layout = new HashMap<>();
            OperatorFactory operatorFactory = null;

            if (aggregate.groupings().isEmpty()) {
                // not grouping
                for (NamedExpression e : aggregate.aggregates()) {
                    if (e instanceof Alias alias && alias.child()instanceof AggregateFunction aggregateFunction) {
                        AggregatorFunctionFactory aggregatorFunc;
                        if (aggregateFunction instanceof Avg avg) {
                            aggregatorFunc = avg.dataType().isRational() ? AggregatorFunction.doubleAvg : AggregatorFunction.longAvg;
                        } else if (aggregateFunction instanceof Count) {
                            aggregatorFunc = AggregatorFunction.count;
                        } else {
                            throw new UnsupportedOperationException("unsupported aggregate function:" + aggregateFunction);
                        }

                        if (aggregate.getMode() == AggregateExec.Mode.PARTIAL) {
                            operatorFactory = new AggregationOperatorFactory(
                                List.of(
                                    new AggregatorFactory(
                                        aggregatorFunc,
                                        AggregatorMode.INITIAL,
                                        source.layout.get(Expressions.attribute(aggregateFunction.field()).id())
                                    )
                                ),
                                AggregatorMode.INITIAL
                            );
                            layout.put(alias.id(), 0);
                        } else if (aggregate.getMode() == AggregateExec.Mode.FINAL) {
                            operatorFactory = new AggregationOperatorFactory(
                                List.of(new AggregatorFactory(aggregatorFunc, AggregatorMode.FINAL, source.layout.get(alias.id()))),
                                AggregatorMode.FINAL
                            );
                            layout.put(alias.id(), 0);
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
                layout.put(grpAttrib.id(), 0);

                for (NamedExpression e : aggregate.aggregates()) {
                    if (e instanceof Alias alias && alias.child()instanceof AggregateFunction aggregateFunction) {
                        GroupingAggregatorFunctionFactory aggregatorFunc;
                        if (aggregateFunction instanceof Avg) {
                            aggregatorFunc = GroupingAggregatorFunction.avg;
                        } else if (aggregateFunction instanceof Count) {
                            aggregatorFunc = GroupingAggregatorFunction.count;
                        } else {
                            throw new UnsupportedOperationException("unsupported aggregate function:" + aggregateFunction);
                        }

                        final Supplier<BlockHash> blockHash;
                        if (grpAttrib.dataType() == DataTypes.KEYWORD) {
                            blockHash = () -> BlockHash.newBytesRefHash(BigArrays.NON_RECYCLING_INSTANCE);
                        } else {
                            blockHash = () -> BlockHash.newLongHash(BigArrays.NON_RECYCLING_INSTANCE);
                        }
                        if (aggregate.getMode() == AggregateExec.Mode.PARTIAL) {
                            operatorFactory = new HashAggregationOperatorFactory(
                                source.layout.get(grpAttrib.id()),
                                List.of(
                                    new GroupingAggregatorFactory(
                                        aggregatorFunc,
                                        AggregatorMode.INITIAL,
                                        source.layout.get(Expressions.attribute(aggregateFunction.field()).id())
                                    )
                                ),
                                blockHash,
                                AggregatorMode.INITIAL
                            );
                            layout.put(alias.id(), 1);  // <<<< TODO: this one looks suspicious
                        } else if (aggregate.getMode() == AggregateExec.Mode.FINAL) {
                            operatorFactory = new HashAggregationOperatorFactory(
                                source.layout.get(grpAttrib.id()),
                                List.of(new GroupingAggregatorFactory(aggregatorFunc, AggregatorMode.FINAL, source.layout.get(alias.id()))),
                                blockHash,
                                AggregatorMode.FINAL
                            );
                            layout.put(alias.id(), 1);
                        } else {
                            throw new UnsupportedOperationException();
                        }
                    } else if (aggregate.groupings().contains(e) == false) {
                        throw new UnsupportedOperationException(
                            "expected an aggregate function, but got [" + e + "] of type [" + e.getClass().getSimpleName() + "]"
                        );
                    }
                }

            }
            if (operatorFactory != null) {
                return new PhysicalOperation(operatorFactory, layout, source);
            }
            throw new UnsupportedOperationException();
        } else if (node instanceof EsQueryExec esQuery) {
            return planEsQueryNode(esQuery, context);
        } else if (node instanceof FieldExtractExec fieldExtractExec) {
            return planFieldExtractNode(context, fieldExtractExec);
        } else if (node instanceof OutputExec outputExec) {
            PhysicalOperation source = plan(outputExec.child(), context);
            var output = outputExec.output();
            if (output.size() != source.layout.size()) {
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
                mappedPosition[++index] = source.layout.get(attribute.id());
                if (transformRequired == false) {
                    transformRequired = mappedPosition[index] != index;
                }
            }
            Function<Page, Page> mapper = transformRequired ? p -> {
                var blocks = new Block[p.getBlockCount()];
                for (int i = 0; i < blocks.length; i++) {
                    blocks[i] = p.getBlock(mappedPosition[i]);
                }
                return new Page(blocks);
            } : Function.identity();

            return new PhysicalOperation(
                new OutputOperatorFactory(Expressions.names(outputExec.output()), mapper, outputExec.getPageConsumer()),
                source.layout,
                source
            );
        } else if (node instanceof ExchangeExec exchangeExec) {
            DriverParallelism parallelism = exchangeExec.getType() == ExchangeExec.Type.GATHER
                ? DriverParallelism.SINGLE
                : new DriverParallelism(DriverParallelism.Type.TASK_LEVEL_PARALLELISM, taskConcurrency);
            context.driverParallelism(parallelism);
            Exchange ex = new Exchange(parallelism.instanceCount(), exchangeExec.getPartitioning().toExchange(), bufferMaxPages);

            LocalExecutionPlanContext subContext = context.createSubContext();
            PhysicalOperation source = plan(exchangeExec.child(), subContext);
            Map<Object, Integer> layout = source.layout;
            PhysicalOperation physicalOperation = new PhysicalOperation(new ExchangeSinkOperatorFactory(ex), source.layout, source);
            context.addDriverFactory(new DriverFactory(new DriverSupplier(physicalOperation), subContext.driverParallelism()));
            return new PhysicalOperation(new ExchangeSourceOperatorFactory(ex), layout);
        } else if (node instanceof TopNExec topNExec) {
            PhysicalOperation source = plan(topNExec.child(), context);
            if (topNExec.order().size() != 1) {
                throw new UnsupportedOperationException();
            }
            Order order = topNExec.order().get(0);
            int sortByChannel;
            if (order.child()instanceof Attribute a) {
                sortByChannel = source.layout.get(a.id());
            } else {
                throw new UnsupportedOperationException();
            }
            int limit;
            if (topNExec.getLimit()instanceof Literal literal) {
                limit = Integer.parseInt(literal.value().toString());
            } else {
                throw new UnsupportedOperationException();
            }

            return new PhysicalOperation(
                new TopNOperatorFactory(sortByChannel, order.direction() == Order.OrderDirection.ASC, limit),
                source.layout,
                source
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
            Map<Object, Integer> layout = new HashMap<>();
            layout.putAll(source.layout);
            layout.put(namedExpression.toAttribute().id(), layout.size());
            return new PhysicalOperation(
                new EvalOperatorFactory(evaluator, namedExpression.dataType().isRational() ? Double.TYPE : Long.TYPE),
                layout,
                source
            );
        } else if (node instanceof RowExec row) {
            List<Object> obj = row.fields().stream().map(f -> {
                if (f instanceof Alias) {
                    return ((Alias) f).child().fold();
                } else {
                    return f.fold();
                }
            }).toList();
            Map<Object, Integer> layout = new HashMap<>();
            var output = row.output();
            for (int i = 0; i < output.size(); i++) {
                layout.put(output.get(i).id(), i);
            }
            return new PhysicalOperation(new RowOperatorFactory(obj), layout);
        } else if (node instanceof ProjectExec project) {
            var source = plan(project.child(), context);
            Map<Object, Integer> layout = new HashMap<>();

            var outputSet = project.outputSet();
            var input = project.child().output();
            var mask = new BitSet(input.size());
            int layoutPos = 0;
            for (Attribute element : input) {
                var id = element.id();
                var maskPosition = source.layout.get(id);
                var keepColumn = outputSet.contains(element);
                mask.set(maskPosition, keepColumn);
                if (keepColumn) {
                    layout.put(id, layoutPos++);
                }
            }
            return new PhysicalOperation(new ProjectOperatorFactory(mask), layout, source);
        } else if (node instanceof FilterExec filter) {
            PhysicalOperation source = plan(filter.child(), context);
            return new PhysicalOperation(new FilterOperatorFactory(toEvaluator(filter.condition(), source.layout)), source.layout, source);
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
        Map<Object, Integer> layout = new HashMap<>();
        for (int i = 0; i < esQuery.output().size(); i++) {
            layout.put(esQuery.output().get(i).id(), i);
        }
        return new PhysicalOperation(operatorFactory, layout);
    }

    private PhysicalOperation planFieldExtractNode(LocalExecutionPlanContext context, FieldExtractExec fieldExtractExec) {
        PhysicalOperation source = plan(fieldExtractExec.child(), context);
        Map<Object, Integer> layout = new HashMap<>();
        layout.putAll(source.layout);

        var sourceAttrs = fieldExtractExec.sourceAttributes();

        PhysicalOperation op = source;
        for (Attribute attr : fieldExtractExec.attributesToExtract()) {
            layout = new HashMap<>(layout);
            layout.put(attr.id(), layout.size());
            Map<Object, Integer> previousLayout = op.layout;

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

            op = new PhysicalOperation(
                new ValuesSourceReaderOperator.ValuesSourceReaderOperatorFactory(
                    valuesSources.stream().map(Tuple::v1).collect(Collectors.toList()),
                    valuesSources.stream().map(Tuple::v2).collect(Collectors.toList()),
                    indexReaders,
                    previousLayout.get(sourceAttrs.get(0).id()),
                    previousLayout.get(sourceAttrs.get(1).id()),
                    previousLayout.get(sourceAttrs.get(2).id()),
                    attr.name()
                ),
                layout,
                op
            );
        }
        return op;
    }

    private ExpressionEvaluator toEvaluator(Expression exp, Map<Object, Integer> layout) {
        if (exp instanceof Add add) {
            ExpressionEvaluator e1 = toEvaluator(add.left(), layout);
            ExpressionEvaluator e2 = toEvaluator(add.right(), layout);
            if (add.dataType().isRational()) {
                return (page, pos) -> ((Number) e1.computeRow(page, pos)).doubleValue() + ((Number) e2.computeRow(page, pos)).doubleValue();
            } else {
                return (page, pos) -> ((Number) e1.computeRow(page, pos)).longValue() + ((Number) e2.computeRow(page, pos)).longValue();
            }
        } else if (exp instanceof Div div) {
            ExpressionEvaluator e1 = toEvaluator(div.left(), layout);
            ExpressionEvaluator e2 = toEvaluator(div.right(), layout);
            if (div.dataType().isRational()) {
                return (page, pos) -> ((Number) e1.computeRow(page, pos)).doubleValue() / ((Number) e2.computeRow(page, pos)).doubleValue();
            } else {
                return (page, pos) -> ((Number) e1.computeRow(page, pos)).longValue() / ((Number) e2.computeRow(page, pos)).longValue();
            }
        } else if (exp instanceof GreaterThan gt) {
            ExpressionEvaluator e1 = toEvaluator(gt.left(), layout);
            ExpressionEvaluator e2 = toEvaluator(gt.right(), layout);
            if (gt.left().dataType().isRational()) {
                return (page, pos) -> ((Number) e1.computeRow(page, pos)).doubleValue() > ((Number) e2.computeRow(page, pos)).doubleValue();
            } else {
                return (page, pos) -> ((Number) e1.computeRow(page, pos)).longValue() > ((Number) e2.computeRow(page, pos)).longValue();
            }
        } else if (exp instanceof Attribute attr) {
            int channel = layout.get(attr.id());
            return (page, pos) -> page.getBlock(channel).getObject(pos);
        } else if (exp instanceof Literal lit) {
            if (exp.dataType().isRational()) {
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

    public static class PhysicalOperation implements Describable {
        private final List<OperatorFactory> operatorFactories = new ArrayList<>();
        private final Map<Object, Integer> layout; // maps field names to channels

        PhysicalOperation(OperatorFactory operatorFactory, Map<Object, Integer> layout) {
            this.operatorFactories.add(operatorFactory);
            this.layout = layout;
        }

        PhysicalOperation(OperatorFactory operatorFactory, Map<Object, Integer> layout, PhysicalOperation source) {
            this.operatorFactories.addAll(source.operatorFactories);
            this.operatorFactories.add(operatorFactory);
            this.layout = layout;
        }

        public List<Operator> operators() {
            return operatorFactories.stream().map(OperatorFactory::get).collect(Collectors.toList());
        }

        @Override
        public String describe() {
            return operatorFactories.stream().map(Describable::describe).collect(joining("\n\\_", "\\_", ""));
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

    record DriverSupplier(PhysicalOperation physicalOperation) implements Supplier<Driver>, Describable {

        @Override
        public Driver get() {
            return new Driver(physicalOperation.operators(), () -> {});
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

        public List<Driver> createDrivers() {
            return driverFactories.stream()
                .flatMap(df -> IntStream.range(0, df.driverParallelism().instanceCount()).mapToObj(i -> df.driverSupplier.get()))
                .collect(Collectors.toList());
        }

        public List<DriverFactory> getDriverFactories() {
            return driverFactories;
        }

        @Override
        public String describe() {
            StringBuilder sb = new StringBuilder();
            sb.append(driverFactories.stream().map(DriverFactory::describe).collect(joining("\n")));
            return sb.toString();
        }
    }
}
