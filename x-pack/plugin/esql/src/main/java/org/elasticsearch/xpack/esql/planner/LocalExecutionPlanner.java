/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.planner;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.NumericUtils;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.compute.Experimental;
import org.elasticsearch.compute.aggregation.Aggregator;
import org.elasticsearch.compute.aggregation.AggregatorFunction;
import org.elasticsearch.compute.aggregation.AggregatorMode;
import org.elasticsearch.compute.aggregation.GroupingAggregator;
import org.elasticsearch.compute.aggregation.GroupingAggregatorFunction;
import org.elasticsearch.compute.lucene.LuceneSourceOperator;
import org.elasticsearch.compute.lucene.NumericDocValuesExtractor;
import org.elasticsearch.compute.operator.AggregationOperator;
import org.elasticsearch.compute.operator.DoubleTransformerOperator;
import org.elasticsearch.compute.operator.Driver;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.compute.operator.EvalOperator.ExpressionEvaluator;
import org.elasticsearch.compute.operator.HashAggregationOperator;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.compute.operator.OutputOperator;
import org.elasticsearch.compute.operator.RowOperator;
import org.elasticsearch.compute.operator.TopNOperator;
import org.elasticsearch.compute.operator.exchange.Exchange;
import org.elasticsearch.compute.operator.exchange.ExchangeSinkOperator;
import org.elasticsearch.compute.operator.exchange.ExchangeSourceOperator;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Avg;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Count;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.Round;
import org.elasticsearch.xpack.esql.plan.physical.AggregateExec;
import org.elasticsearch.xpack.esql.plan.physical.EsQueryExec;
import org.elasticsearch.xpack.esql.plan.physical.EvalExec;
import org.elasticsearch.xpack.esql.plan.physical.ExchangeExec;
import org.elasticsearch.xpack.esql.plan.physical.FieldExtractExec;
import org.elasticsearch.xpack.esql.plan.physical.OutputExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
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
import org.elasticsearch.xpack.ql.expression.predicate.operator.arithmetic.Add;
import org.elasticsearch.xpack.ql.expression.predicate.operator.arithmetic.Div;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * The local execution planner takes a plan (represented as PlanNode tree / digraph) as input and creates the corresponding
 * drivers that are used to execute the given plan.
 */
@Experimental
public class LocalExecutionPlanner {

    private final List<IndexReaderReference> indexReaders;
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

    public LocalExecutionPlanner(EsqlConfiguration configuration, List<IndexReaderReference> indexReaders) {
        this.indexReaders = indexReaders;
        taskConcurrency = TASK_CONCURRENCY.get(configuration.pragmas());
        bufferMaxPages = BUFFER_MAX_PAGES.get(configuration.pragmas());
        dataPartitioning = DATA_PARTITIONING.get(configuration.pragmas());
    }

    public enum DataPartitioning {
        SHARD,
        SEGMENT,
        DOC,
    }

    public record IndexReaderReference(IndexReader indexReader, ShardId shardId) {

    }

    /**
     * turn the given plan into a list of drivers to execute
     */
    public LocalExecutionPlan plan(PhysicalPlan node) {
        LocalExecutionPlanContext context = new LocalExecutionPlanContext();

        PhysicalOperation physicalOperation = plan(node, context);

        context.addDriverFactory(
            new DriverFactory(() -> new Driver(physicalOperation.operators(), () -> {}), context.getDriverInstanceCount())
        );

        LocalExecutionPlan localExecutionPlan = new LocalExecutionPlan();
        localExecutionPlan.driverFactories.addAll(context.driverFactories);
        return localExecutionPlan;
    }

    public PhysicalOperation plan(PhysicalPlan node, LocalExecutionPlanContext context) {
        if (node instanceof AggregateExec aggregate) {
            PhysicalOperation source = plan(aggregate.child(), context);
            Map<Object, Integer> layout = new HashMap<>();
            Supplier<Operator> operatorFactory = null;

            if (aggregate.groupings().isEmpty()) {
                // not grouping
                for (NamedExpression e : aggregate.aggregates()) {
                    if (e instanceof Alias alias && alias.child()instanceof AggregateFunction aggregateFunction) {
                        BiFunction<AggregatorMode, Integer, AggregatorFunction> aggregatorFunc;
                        if (aggregateFunction instanceof Avg avg) {
                            aggregatorFunc = avg.dataType().isRational() ? AggregatorFunction.doubleAvg : AggregatorFunction.longAvg;
                        } else if (aggregateFunction instanceof Count) {
                            aggregatorFunc = AggregatorFunction.count;
                        } else {
                            throw new UnsupportedOperationException("unsupported aggregate function:" + aggregateFunction);
                        }

                        if (aggregate.getMode() == AggregateExec.Mode.PARTIAL) {
                            operatorFactory = () -> new AggregationOperator(
                                List.of(
                                    new Aggregator(
                                        aggregatorFunc,
                                        AggregatorMode.INITIAL,
                                        source.layout.get(Expressions.attribute(aggregateFunction.field()).id())
                                    )
                                )
                            );
                            layout.put(alias.id(), 0);
                        } else if (aggregate.getMode() == AggregateExec.Mode.FINAL) {
                            operatorFactory = () -> new AggregationOperator(
                                // TODO: use intermediate name
                                List.of(new Aggregator(aggregatorFunc, AggregatorMode.FINAL, source.layout.get(alias.id())))
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
                        BiFunction<AggregatorMode, Integer, GroupingAggregatorFunction> aggregatorFunc;
                        if (aggregateFunction instanceof Avg) {
                            aggregatorFunc = GroupingAggregatorFunction.avg;
                        } else if (aggregateFunction instanceof Count) {
                            aggregatorFunc = GroupingAggregatorFunction.count;
                        } else {
                            throw new UnsupportedOperationException("unsupported aggregate function:" + aggregateFunction);
                        }

                        if (aggregate.getMode() == AggregateExec.Mode.PARTIAL) {
                            operatorFactory = () -> new HashAggregationOperator(
                                source.layout.get(grpAttrib.id()),
                                List.of(
                                    new GroupingAggregator(
                                        aggregatorFunc,
                                        AggregatorMode.INITIAL,
                                        source.layout.get(Expressions.attribute(aggregateFunction.field()).id())
                                    )
                                ),
                                BigArrays.NON_RECYCLING_INSTANCE
                            );
                            layout.put(alias.id(), 1);  // <<<< TODO: this one looks suspicious
                        } else if (aggregate.getMode() == AggregateExec.Mode.FINAL) {
                            operatorFactory = () -> new HashAggregationOperator(
                                source.layout.get(grpAttrib.id()),
                                List.of(new GroupingAggregator(aggregatorFunc, AggregatorMode.FINAL, source.layout.get(alias.id()))),
                                BigArrays.NON_RECYCLING_INSTANCE
                            );
                            layout.put(alias.id(), 1);
                        } else {
                            throw new UnsupportedOperationException();
                        }
                    } else {
                        throw new UnsupportedOperationException();
                    }
                }

            }
            if (operatorFactory != null) {
                return new PhysicalOperation(operatorFactory, layout, source);
            }
            throw new UnsupportedOperationException();
        } else if (node instanceof EsQueryExec esQuery) {
            Supplier<Operator> operatorFactory;
            Set<String> indices = Sets.newHashSet(esQuery.index().name());
            Query query = new MatchAllDocsQuery(); // TODO: esQuery.query
            if (dataPartitioning == DataPartitioning.SHARD) {
                context.setDriverInstanceCount(
                    Math.toIntExact(indexReaders.stream().filter(iRR -> indices.contains(iRR.shardId().getIndexName())).count())
                );
                operatorFactory = IntStream.range(0, indexReaders.size())
                    .mapToObj(i -> Tuple.tuple(i, indexReaders.get(i)))
                    .filter(tup -> indices.contains(tup.v2().shardId().getIndexName()))
                    .map(tuple -> new LuceneSourceOperator(tuple.v2().indexReader(), tuple.v1(), query))
                    .iterator()::next;
            } else if (dataPartitioning == DataPartitioning.SEGMENT) {
                context.setDriverInstanceCount(
                    indexReaders.stream()
                        .filter(iRR -> indices.contains(iRR.shardId().getIndexName()))
                        .mapToInt(indexReader -> LuceneSourceOperator.numSegmentSlices(indexReader.indexReader()))
                        .sum()
                );
                operatorFactory = IntStream.range(0, indexReaders.size())
                    .mapToObj(i -> Tuple.tuple(i, indexReaders.get(i)))
                    .filter(tup -> indices.contains(tup.v2().shardId().getIndexName()))
                    .flatMap(tuple -> new LuceneSourceOperator(tuple.v2().indexReader(), tuple.v1(), query).segmentSlice().stream())
                    .iterator()::next;
            } else if (dataPartitioning == DataPartitioning.DOC) {
                context.setDriverInstanceCount(
                    indexReaders.stream()
                        .filter(iRR -> indices.contains(iRR.shardId().getIndexName()))
                        .mapToInt(indexReader -> LuceneSourceOperator.numDocSlices(indexReader.indexReader(), taskConcurrency))
                        .sum()
                );
                operatorFactory = IntStream.range(0, indexReaders.size())
                    .mapToObj(i -> Tuple.tuple(i, indexReaders.get(i)))
                    .filter(tup -> indices.contains(tup.v2().shardId().getIndexName()))
                    .flatMap(
                        tuple -> new LuceneSourceOperator(tuple.v2().indexReader(), tuple.v1(), query).docSlice(taskConcurrency).stream()
                    )
                    .iterator()::next;
            } else {
                throw new UnsupportedOperationException();
            }
            Map<Object, Integer> layout = new HashMap<>();
            for (int i = 0; i < esQuery.output().size(); i++) {
                layout.put(esQuery.output().get(i).id(), i);
            }
            return new PhysicalOperation(operatorFactory, layout);
        } else if (node instanceof FieldExtractExec fieldExtractExec) {
            PhysicalOperation source = plan(fieldExtractExec.child(), context);
            Map<Object, Integer> layout = new HashMap<>();
            layout.putAll(source.layout);

            var souceAttributes = fieldExtractExec.sourceAttributes().toArray(new Attribute[3]);

            PhysicalOperation op = source;
            for (Attribute attr : fieldExtractExec.attributesToExtract()) {
                layout = new HashMap<>(layout);
                layout.put(attr.id(), layout.size());
                Map<Object, Integer> previousLayout = op.layout;
                op = new PhysicalOperation(
                    () -> new NumericDocValuesExtractor(
                        indexReaders.stream().map(IndexReaderReference::indexReader).collect(Collectors.toList()),
                        previousLayout.get(souceAttributes[0].id()),
                        previousLayout.get(souceAttributes[1].id()),
                        previousLayout.get(souceAttributes[2].id()),
                        attr.name()
                    ),
                    layout,
                    op
                );
                if (attr.dataType().isRational()) {
                    layout = new HashMap<>(layout);
                    int channel = layout.get(attr.id());
                    layout.put(new NameId(), channel);
                    layout.remove(attr.id());
                    layout.put(attr.id(), layout.size());
                    op = new PhysicalOperation(
                        () -> new DoubleTransformerOperator(channel, NumericUtils::sortableLongToDouble),
                        layout,
                        op
                    );
                }
            }
            return op;
        } else if (node instanceof OutputExec outputExec) {
            PhysicalOperation source = plan(outputExec.child(), context);
            if (outputExec.output().size() != source.layout.size()) {
                throw new IllegalStateException("expected layout:" + outputExec.output() + ", source.layout:" + source.layout);
            }
            return new PhysicalOperation(
                () -> new OutputOperator(
                    outputExec.output().stream().map(NamedExpression::name).collect(Collectors.toList()),
                    outputExec.getPageConsumer()
                ),
                source.layout,
                source
            );
        } else if (node instanceof ExchangeExec exchangeExec) {
            int driverInstances = exchangeExec.getType() == ExchangeExec.Type.GATHER ? 1 : taskConcurrency;
            context.setDriverInstanceCount(driverInstances);
            Exchange ex = new Exchange(driverInstances, exchangeExec.getPartitioning().toExchange(), bufferMaxPages);

            LocalExecutionPlanContext subContext = context.createSubContext();
            PhysicalOperation source = plan(exchangeExec.child(), subContext);
            Map<Object, Integer> layout = source.layout;
            PhysicalOperation physicalOperation = new PhysicalOperation(
                () -> new ExchangeSinkOperator(ex.createSink()),
                source.layout,
                source
            );
            context.addDriverFactory(
                new DriverFactory(() -> new Driver(physicalOperation.operators(), () -> {}), subContext.getDriverInstanceCount())
            );
            return new PhysicalOperation(() -> new ExchangeSourceOperator(ex.getNextSource()), layout);
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
                () -> new TopNOperator(sortByChannel, order.direction() == Order.OrderDirection.ASC, limit),
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
                () -> new EvalOperator(evaluator, namedExpression.dataType().isRational() ? Double.TYPE : Long.TYPE),
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
            for (int i = 0; i < row.output().size(); i++) {
                layout.put(row.output().get(i).id(), i);
            }
            return new PhysicalOperation(() -> new RowOperator(obj), layout);
        }
        throw new UnsupportedOperationException(node.nodeName());
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
        } else if (exp instanceof Attribute attr) {
            int channel = layout.get(attr.id());
            if (attr.dataType().isRational()) {
                return (page, pos) -> page.getBlock(channel).getDouble(pos);
            } else {
                return (page, pos) -> page.getBlock(channel).getLong(pos);
            }
        } else if (exp instanceof Literal lit) {
            if (exp.dataType().isRational()) {
                double d = Double.parseDouble(lit.value().toString());
                return (page, pos) -> d;
            } else {
                long l = Long.parseLong(lit.value().toString());
                return (page, pos) -> l;
            }
        } else if (exp instanceof Round round) {
            ExpressionEvaluator e = toEvaluator(round.field(), layout);
            if (round.field().dataType().isRational()) {
                return (page, pos) -> Math.round(((Number) e.computeRow(page, pos)).doubleValue());
            } else {
                return (page, pos) -> ((Number) e.computeRow(page, pos)).longValue();
            }
        } else {
            throw new UnsupportedOperationException(exp.nodeName());
        }
    }

    public static class PhysicalOperation {
        private final List<Supplier<Operator>> operatorFactories = new ArrayList<>();
        private final Map<Object, Integer> layout; // maps field names to channels

        PhysicalOperation(Supplier<Operator> operatorFactory, Map<Object, Integer> layout) {
            this.operatorFactories.add(operatorFactory);
            this.layout = layout;
        }

        PhysicalOperation(Supplier<Operator> operatorFactory, Map<Object, Integer> layout, PhysicalOperation source) {
            this.operatorFactories.addAll(source.operatorFactories);
            this.operatorFactories.add(operatorFactory);
            this.layout = layout;
        }

        public List<Operator> operators() {
            return operatorFactories.stream().map(Supplier::get).collect(Collectors.toList());
        }
    }

    /**
     * Context object used while generating a local plan. Currently only collects the driver factories as well as
     * maintains information how many driver instances should be created for a given driver.
     */
    public static class LocalExecutionPlanContext {
        final List<DriverFactory> driverFactories;
        int driverInstanceCount = 1;

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

        public int getDriverInstanceCount() {
            return driverInstanceCount;
        }

        public void setDriverInstanceCount(int driverInstanceCount) {
            this.driverInstanceCount = driverInstanceCount;
        }
    }

    public record DriverFactory(Supplier<Driver> driverSupplier, int driverInstances) {

    }

    /**
     * Plan representation that is geared towards execution on a single node
     */
    public static class LocalExecutionPlan {
        final List<DriverFactory> driverFactories = new ArrayList<>();

        public List<Driver> createDrivers() {
            return driverFactories.stream()
                .flatMap(df -> IntStream.range(0, df.driverInstances).mapToObj(i -> df.driverSupplier.get()))
                .collect(Collectors.toList());
        }

        public List<DriverFactory> getDriverFactories() {
            return driverFactories;
        }
    }
}
