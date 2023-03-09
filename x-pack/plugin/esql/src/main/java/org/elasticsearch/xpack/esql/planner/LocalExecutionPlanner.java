/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.planner;

import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.compute.Describable;
import org.elasticsearch.compute.ann.Experimental;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.lucene.DataPartitioning;
import org.elasticsearch.compute.operator.Driver;
import org.elasticsearch.compute.operator.EvalOperator.EvalOperatorFactory;
import org.elasticsearch.compute.operator.EvalOperator.ExpressionEvaluator;
import org.elasticsearch.compute.operator.FilterOperator.FilterOperatorFactory;
import org.elasticsearch.compute.operator.LocalSourceOperator;
import org.elasticsearch.compute.operator.LocalSourceOperator.LocalSourceFactory;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.compute.operator.Operator.OperatorFactory;
import org.elasticsearch.compute.operator.OutputOperator.OutputOperatorFactory;
import org.elasticsearch.compute.operator.RowOperator.RowOperatorFactory;
import org.elasticsearch.compute.operator.SinkOperator;
import org.elasticsearch.compute.operator.SinkOperator.SinkOperatorFactory;
import org.elasticsearch.compute.operator.SourceOperator;
import org.elasticsearch.compute.operator.SourceOperator.SourceOperatorFactory;
import org.elasticsearch.compute.operator.TopNOperator;
import org.elasticsearch.compute.operator.TopNOperator.TopNOperatorFactory;
import org.elasticsearch.compute.operator.exchange.ExchangeSinkOperator.ExchangeSinkOperatorFactory;
import org.elasticsearch.compute.operator.exchange.ExchangeSourceOperator.ExchangeSourceOperatorFactory;
import org.elasticsearch.compute.operator.exchange.LocalExchanger;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.esql.plan.physical.AggregateExec;
import org.elasticsearch.xpack.esql.plan.physical.EsQueryExec;
import org.elasticsearch.xpack.esql.plan.physical.EvalExec;
import org.elasticsearch.xpack.esql.plan.physical.ExchangeExec;
import org.elasticsearch.xpack.esql.plan.physical.FieldExtractExec;
import org.elasticsearch.xpack.esql.plan.physical.FilterExec;
import org.elasticsearch.xpack.esql.plan.physical.LimitExec;
import org.elasticsearch.xpack.esql.plan.physical.LocalSourceExec;
import org.elasticsearch.xpack.esql.plan.physical.OutputExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.ProjectExec;
import org.elasticsearch.xpack.esql.plan.physical.RowExec;
import org.elasticsearch.xpack.esql.plan.physical.TopNExec;
import org.elasticsearch.xpack.esql.session.EsqlConfiguration;
import org.elasticsearch.xpack.ql.expression.Alias;
import org.elasticsearch.xpack.ql.expression.Attribute;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.Expressions;
import org.elasticsearch.xpack.ql.expression.Literal;
import org.elasticsearch.xpack.ql.expression.NameId;
import org.elasticsearch.xpack.ql.expression.NamedExpression;
import org.elasticsearch.xpack.ql.expression.Order;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.DataTypes;
import org.elasticsearch.xpack.ql.util.Holder;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;
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
        ThreadPool.searchOrGetThreadPoolSize(EsExecutors.allocatedProcessors(Settings.EMPTY))
    );
    private static final Setting<Integer> BUFFER_MAX_PAGES = Setting.intSetting("buffer_max_pages", 500);
    private static final Setting<DataPartitioning> DATA_PARTITIONING = Setting.enumSetting(
        DataPartitioning.class,
        "data_partitioning",
        DataPartitioning.SEGMENT
    );

    private final BigArrays bigArrays;
    private final int taskConcurrency;
    private final int bufferMaxPages;
    private final DataPartitioning dataPartitioning;
    private final PhysicalOperationProviders physicalOperationProviders;

    public LocalExecutionPlanner(
        BigArrays bigArrays,
        EsqlConfiguration configuration,
        PhysicalOperationProviders physicalOperationProviders
    ) {
        this.bigArrays = bigArrays;
        this.physicalOperationProviders = physicalOperationProviders;
        taskConcurrency = TASK_CONCURRENCY.get(configuration.pragmas());
        bufferMaxPages = BUFFER_MAX_PAGES.get(configuration.pragmas());
        dataPartitioning = DATA_PARTITIONING.get(configuration.pragmas());
    }

    /**
     * turn the given plan into a list of drivers to execute
     */
    public LocalExecutionPlan plan(PhysicalPlan node) {

        var context = new LocalExecutionPlannerContext(
            new ArrayList<>(),
            new Holder<>(DriverParallelism.SINGLE),
            taskConcurrency,
            bufferMaxPages,
            dataPartitioning,
            bigArrays
        );

        PhysicalOperation physicalOperation = plan(node, context);

        context.addDriverFactory(
            new DriverFactory(new DriverSupplier(context.bigArrays, physicalOperation), context.driverParallelism().get())
        );

        return new LocalExecutionPlan(context.driverFactories);
    }

    private PhysicalOperation plan(PhysicalPlan node, LocalExecutionPlannerContext context) {
        if (node instanceof AggregateExec aggregate) {
            return planAggregation(aggregate, context);
        } else if (node instanceof FieldExtractExec fieldExtractExec) {
            return planFieldExtractNode(context, fieldExtractExec);
        } else if (node instanceof ExchangeExec exchangeExec) {
            return planExchange(exchangeExec, context);
        } else if (node instanceof TopNExec topNExec) {
            return planTopN(topNExec, context);
        } else if (node instanceof EvalExec eval) {
            return planEval(eval, context);
        } else if (node instanceof ProjectExec project) {
            return planProject(project, context);
        } else if (node instanceof FilterExec filter) {
            return planFilter(filter, context);
        } else if (node instanceof LimitExec limit) {
            return planLimit(limit, context);
        }
        // source nodes
        else if (node instanceof EsQueryExec esQuery) {
            return planEsQueryNode(esQuery, context);
        } else if (node instanceof RowExec row) {
            return planRow(row, context);
        } else if (node instanceof LocalSourceExec localSource) {
            return planLocal(localSource, context);
        }
        // output
        else if (node instanceof OutputExec outputExec) {
            return planOutput(outputExec, context);
        }

        throw new UnsupportedOperationException(node.nodeName());
    }

    private PhysicalOperation planAggregation(AggregateExec aggregate, LocalExecutionPlannerContext context) {
        return physicalOperationProviders.groupingPhysicalOperation(aggregate, plan(aggregate.child(), context), context);
    }

    private PhysicalOperation planEsQueryNode(EsQueryExec esQuery, LocalExecutionPlannerContext context) {
        return physicalOperationProviders.sourcePhysicalOperation(esQuery, context);
    }

    private PhysicalOperation planFieldExtractNode(LocalExecutionPlannerContext context, FieldExtractExec fieldExtractExec) {
        return physicalOperationProviders.fieldExtractPhysicalOperation(fieldExtractExec, plan(fieldExtractExec.child(), context));
    }

    /**
     * Map QL's {@link DataType} to the compute engine's {@link ElementType}.
     */
    static ElementType toElementType(DataType dataType) {
        if (dataType == DataTypes.LONG || dataType == DataTypes.DATETIME) {
            return ElementType.LONG;
        }
        if (dataType == DataTypes.INTEGER) {
            return ElementType.INT;
        }
        if (dataType == DataTypes.DOUBLE) {
            return ElementType.DOUBLE;
        }
        // unsupported fields are passed through as a BytesRef
        if (dataType == DataTypes.KEYWORD || dataType == DataTypes.UNSUPPORTED) {
            return ElementType.BYTES_REF;
        }
        if (dataType == DataTypes.BOOLEAN) {
            return ElementType.BOOLEAN;
        }
        if (dataType == DataTypes.NULL) {
            return ElementType.NULL;
        }
        if (dataType == DataTypes.BOOLEAN) {
            return ElementType.BOOLEAN;
        }
        throw new UnsupportedOperationException("unsupported data type [" + dataType + "]");
    }

    private PhysicalOperation planOutput(OutputExec outputExec, LocalExecutionPlannerContext context) {
        PhysicalOperation source = plan(outputExec.child(), context);
        var output = outputExec.output();

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
    }

    private PhysicalOperation planExchange(ExchangeExec exchangeExec, LocalExecutionPlannerContext context) {
        DriverParallelism parallelism = DriverParallelism.SINGLE;
        context.driverParallelism(parallelism);
        LocalExchanger exchanger = new LocalExchanger(bufferMaxPages);
        LocalExecutionPlannerContext subContext = context.createSubContext();
        PhysicalOperation source = plan(exchangeExec.child(), subContext);
        Layout layout = source.layout;
        PhysicalOperation sink = source.withSink(new ExchangeSinkOperatorFactory(exchanger::createExchangeSink), source.layout);
        context.addDriverFactory(new DriverFactory(new DriverSupplier(context.bigArrays, sink), subContext.driverParallelism().get()));
        return PhysicalOperation.fromSource(new ExchangeSourceOperatorFactory(exchanger::createExchangeSource), layout);
    }

    private PhysicalOperation planTopN(TopNExec topNExec, LocalExecutionPlannerContext context) {
        PhysicalOperation source = plan(topNExec.child(), context);

        List<TopNOperator.SortOrder> orders = topNExec.order().stream().map(order -> {
            int sortByChannel;
            if (order.child()instanceof Attribute a) {
                sortByChannel = source.layout.getChannel(a.id());
            } else {
                throw new UnsupportedOperationException();
            }

            return new TopNOperator.SortOrder(
                sortByChannel,
                order.direction().equals(Order.OrderDirection.ASC),
                order.nullsPosition().equals(Order.NullsPosition.FIRST)
            );
        }).toList();

        int limit;
        if (topNExec.limit()instanceof Literal literal) {
            limit = Integer.parseInt(literal.value().toString());
        } else {
            throw new UnsupportedOperationException();
        }

        return source.with(new TopNOperatorFactory(limit, orders), source.layout);
    }

    private PhysicalOperation planEval(EvalExec eval, LocalExecutionPlannerContext context) {
        PhysicalOperation source = plan(eval.child(), context);

        for (NamedExpression namedExpression : eval.fields()) {
            Supplier<ExpressionEvaluator> evaluatorSupplier;
            if (namedExpression instanceof Alias alias) {
                evaluatorSupplier = EvalMapper.toEvaluator(alias.child(), source.layout);
            } else {
                throw new UnsupportedOperationException();
            }
            Layout.Builder layout = source.layout.builder();
            layout.appendChannel(namedExpression.toAttribute().id());
            source = source.with(new EvalOperatorFactory(evaluatorSupplier, toElementType(namedExpression.dataType())), layout.build());
        }
        return source;
    }

    private Supplier<ExpressionEvaluator> toEvaluator(Expression exp, Layout layout) {
        return EvalMapper.toEvaluator(exp, layout);
    }

    private PhysicalOperation planRow(RowExec row, LocalExecutionPlannerContext context) {
        List<Object> obj = row.fields().stream().map(f -> {
            if (f instanceof Alias) {
                return ((Alias) f).child().fold();
            } else {
                return f.fold();
            }
        }).toList();
        Layout.Builder layout = new Layout.Builder();
        var output = row.output();
        for (Attribute attribute : output) {
            layout.appendChannel(attribute.id());
        }
        return PhysicalOperation.fromSource(new RowOperatorFactory(obj), layout.build());
    }

    private PhysicalOperation planLocal(LocalSourceExec localSourceExec, LocalExecutionPlannerContext context) {

        Layout.Builder layout = new Layout.Builder();
        var output = localSourceExec.output();
        for (Attribute attribute : output) {
            layout.appendChannel(attribute.id());
        }
        LocalSourceOperator.ObjectSupplier supplier = Collections::emptyList;
        return PhysicalOperation.fromSource(new LocalSourceFactory(() -> new LocalSourceOperator(supplier)), layout.build());
    }

    private PhysicalOperation planProject(ProjectExec project, LocalExecutionPlannerContext context) {
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
    }

    private PhysicalOperation planFilter(FilterExec filter, LocalExecutionPlannerContext context) {
        PhysicalOperation source = plan(filter.child(), context);
        // TODO: should this be extracted into a separate eval block?
        return source.with(new FilterOperatorFactory(toEvaluator(filter.condition(), source.layout)), source.layout);
    }

    private PhysicalOperation planLimit(LimitExec limit, LocalExecutionPlannerContext context) {
        PhysicalOperation source = plan(limit.child(), context);
        return source.with(new LimitOperatorFactory((Integer) limit.limit().fold()), source.layout);
    }

    /**
     * Immutable physical operation.
     */
    public static class PhysicalOperation implements Describable {
        private final SourceOperatorFactory sourceOperatorFactory;
        private final List<OperatorFactory> intermediateOperatorFactories;
        private final SinkOperatorFactory sinkOperatorFactory;

        final Layout layout; // maps field names to channels

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

        DriverParallelism {
            if (instanceCount <= 0) {
                throw new IllegalArgumentException("instance count must be greater than zero; got: " + instanceCount);
            }
        }

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
    public record LocalExecutionPlannerContext(
        List<DriverFactory> driverFactories,
        Holder<DriverParallelism> driverParallelism,
        int taskConcurrency,
        int bufferMaxPages,
        DataPartitioning dataPartitioning,
        BigArrays bigArrays
    ) {
        void addDriverFactory(DriverFactory driverFactory) {
            driverFactories.add(driverFactory);
        }

        void driverParallelism(DriverParallelism parallelism) {
            driverParallelism.set(parallelism);
        }

        public LocalExecutionPlannerContext createSubContext() {
            return new LocalExecutionPlannerContext(
                driverFactories,
                new Holder<>(DriverParallelism.SINGLE),
                taskConcurrency,
                bufferMaxPages,
                dataPartitioning,
                bigArrays
            );
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
                return new Driver(physicalOperation::describe, source, operators, sink, () -> {});
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
        final List<DriverFactory> driverFactories;

        LocalExecutionPlan(List<DriverFactory> driverFactories) {
            this.driverFactories = driverFactories;
        }

        public List<Driver> createDrivers() {
            List<Driver> drivers = new ArrayList<>();
            for (DriverFactory df : driverFactories) {
                for (int i = 0; i < df.driverParallelism.instanceCount; i++) {
                    drivers.add(df.driverSupplier.get());
                }
            }
            return drivers;
        }

        @Override
        public String describe() {
            StringBuilder sb = new StringBuilder();
            sb.append(driverFactories.stream().map(DriverFactory::describe).collect(joining("\n")));
            return sb.toString();
        }
    }
}
