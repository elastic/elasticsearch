/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.planner;

import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.iterable.Iterables;
import org.elasticsearch.compute.Describable;
import org.elasticsearch.compute.ann.Experimental;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.lucene.DataPartitioning;
import org.elasticsearch.compute.operator.ColumnExtractOperator;
import org.elasticsearch.compute.operator.Driver;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.EvalOperator.EvalOperatorFactory;
import org.elasticsearch.compute.operator.EvalOperator.ExpressionEvaluator;
import org.elasticsearch.compute.operator.FilterOperator.FilterOperatorFactory;
import org.elasticsearch.compute.operator.LocalSourceOperator;
import org.elasticsearch.compute.operator.LocalSourceOperator.LocalSourceFactory;
import org.elasticsearch.compute.operator.MvExpandOperator;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.compute.operator.Operator.OperatorFactory;
import org.elasticsearch.compute.operator.OutputOperator.OutputOperatorFactory;
import org.elasticsearch.compute.operator.RowOperator.RowOperatorFactory;
import org.elasticsearch.compute.operator.ShowOperator;
import org.elasticsearch.compute.operator.SinkOperator;
import org.elasticsearch.compute.operator.SinkOperator.SinkOperatorFactory;
import org.elasticsearch.compute.operator.SourceOperator;
import org.elasticsearch.compute.operator.SourceOperator.SourceOperatorFactory;
import org.elasticsearch.compute.operator.StringExtractOperator;
import org.elasticsearch.compute.operator.TopNOperator;
import org.elasticsearch.compute.operator.TopNOperator.TopNOperatorFactory;
import org.elasticsearch.compute.operator.exchange.ExchangeSinkHandler;
import org.elasticsearch.compute.operator.exchange.ExchangeSinkOperator.ExchangeSinkOperatorFactory;
import org.elasticsearch.compute.operator.exchange.ExchangeSourceHandler;
import org.elasticsearch.compute.operator.exchange.ExchangeSourceOperator.ExchangeSourceOperatorFactory;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.xpack.esql.EsqlIllegalArgumentException;
import org.elasticsearch.xpack.esql.enrich.EnrichLookupOperator;
import org.elasticsearch.xpack.esql.enrich.EnrichLookupService;
import org.elasticsearch.xpack.esql.plan.physical.AggregateExec;
import org.elasticsearch.xpack.esql.plan.physical.DissectExec;
import org.elasticsearch.xpack.esql.plan.physical.EnrichExec;
import org.elasticsearch.xpack.esql.plan.physical.EsQueryExec;
import org.elasticsearch.xpack.esql.plan.physical.EvalExec;
import org.elasticsearch.xpack.esql.plan.physical.ExchangeExec;
import org.elasticsearch.xpack.esql.plan.physical.ExchangeSinkExec;
import org.elasticsearch.xpack.esql.plan.physical.ExchangeSourceExec;
import org.elasticsearch.xpack.esql.plan.physical.FieldExtractExec;
import org.elasticsearch.xpack.esql.plan.physical.FilterExec;
import org.elasticsearch.xpack.esql.plan.physical.GrokExec;
import org.elasticsearch.xpack.esql.plan.physical.LimitExec;
import org.elasticsearch.xpack.esql.plan.physical.LocalSourceExec;
import org.elasticsearch.xpack.esql.plan.physical.MvExpandExec;
import org.elasticsearch.xpack.esql.plan.physical.OutputExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.ProjectExec;
import org.elasticsearch.xpack.esql.plan.physical.RowExec;
import org.elasticsearch.xpack.esql.plan.physical.ShowExec;
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
import java.util.Arrays;
import java.util.BitSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
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

    private final String sessionId;
    private final CancellableTask parentTask;
    private final BigArrays bigArrays;
    private final EsqlConfiguration configuration;
    private final ExchangeSourceHandler exchangeSourceHandler;
    private final ExchangeSinkHandler exchangeSinkHandler;
    private final EnrichLookupService enrichLookupService;
    private final PhysicalOperationProviders physicalOperationProviders;

    public LocalExecutionPlanner(
        String sessionId,
        CancellableTask parentTask,
        BigArrays bigArrays,
        EsqlConfiguration configuration,
        ExchangeSourceHandler exchangeSourceHandler,
        ExchangeSinkHandler exchangeSinkHandler,
        EnrichLookupService enrichLookupService,
        PhysicalOperationProviders physicalOperationProviders
    ) {
        this.sessionId = sessionId;
        this.parentTask = parentTask;
        this.bigArrays = bigArrays;
        this.exchangeSourceHandler = exchangeSourceHandler;
        this.exchangeSinkHandler = exchangeSinkHandler;
        this.enrichLookupService = enrichLookupService;
        this.physicalOperationProviders = physicalOperationProviders;
        this.configuration = configuration;
    }

    /**
     * turn the given plan into a list of drivers to execute
     */
    public LocalExecutionPlan plan(PhysicalPlan node) {
        var context = new LocalExecutionPlannerContext(
            new ArrayList<>(),
            new Holder<>(DriverParallelism.SINGLE),
            configuration.pragmas().taskConcurrency(),
            configuration.pragmas().dataPartitioning(),
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
        } else if (node instanceof DissectExec dissect) {
            return planDissect(dissect, context);
        } else if (node instanceof GrokExec grok) {
            return planGrok(grok, context);
        } else if (node instanceof ProjectExec project) {
            return planProject(project, context);
        } else if (node instanceof FilterExec filter) {
            return planFilter(filter, context);
        } else if (node instanceof LimitExec limit) {
            return planLimit(limit, context);
        } else if (node instanceof MvExpandExec mvExpand) {
            return planMvExpand(mvExpand, context);
        }
        // source nodes
        else if (node instanceof EsQueryExec esQuery) {
            return planEsQueryNode(esQuery, context);
        } else if (node instanceof RowExec row) {
            return planRow(row, context);
        } else if (node instanceof LocalSourceExec localSource) {
            return planLocal(localSource, context);
        } else if (node instanceof ShowExec show) {
            return planShow(show);
        } else if (node instanceof ExchangeSourceExec exchangeSource) {
            return planExchangeSource(exchangeSource);
        }
        // lookups and joins
        else if (node instanceof EnrichExec enrich) {
            return planEnrich(enrich, context);
        }
        // output
        else if (node instanceof OutputExec outputExec) {
            return planOutput(outputExec, context);
        } else if (node instanceof ExchangeSinkExec exchangeSink) {
            return planExchangeSink(exchangeSink, context);
        }

        throw new UnsupportedOperationException(node.nodeName());
    }

    private PhysicalOperation planAggregation(AggregateExec aggregate, LocalExecutionPlannerContext context) {
        return physicalOperationProviders.groupingPhysicalOperation(aggregate, plan(aggregate.child(), context), context);
    }

    private PhysicalOperation planEsQueryNode(EsQueryExec esQuery, LocalExecutionPlannerContext context) {
        if (esQuery.query() == null) {
            esQuery = new EsQueryExec(
                esQuery.source(),
                esQuery.index(),
                esQuery.output(),
                new MatchAllQueryBuilder(),
                esQuery.limit(),
                esQuery.sorts()
            );
        }
        return physicalOperationProviders.sourcePhysicalOperation(esQuery, context);
    }

    private PhysicalOperation planFieldExtractNode(LocalExecutionPlannerContext context, FieldExtractExec fieldExtractExec) {
        return physicalOperationProviders.fieldExtractPhysicalOperation(fieldExtractExec, plan(fieldExtractExec.child(), context));
    }

    /**
     * Map QL's {@link DataType} to the compute engine's {@link ElementType}.
     */
    public static ElementType toElementType(DataType dataType) {
        if (dataType == DataTypes.LONG || dataType == DataTypes.DATETIME || dataType == DataTypes.UNSIGNED_LONG) {
            return ElementType.LONG;
        }
        if (dataType == DataTypes.INTEGER) {
            return ElementType.INT;
        }
        if (dataType == DataTypes.DOUBLE) {
            return ElementType.DOUBLE;
        }
        // unsupported fields are passed through as a BytesRef
        if (dataType == DataTypes.KEYWORD
            || dataType == DataTypes.TEXT
            || dataType == DataTypes.IP
            || dataType == DataTypes.VERSION
            || dataType == DataTypes.UNSUPPORTED) {
            return ElementType.BYTES_REF;
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

        return source.withSink(new OutputOperatorFactory(Expressions.names(output), mapper, outputExec.getPageConsumer()), source.layout);
    }

    private PhysicalOperation planExchange(ExchangeExec exchangeExec, LocalExecutionPlannerContext context) {
        throw new EsqlIllegalArgumentException("Exchange needs to be replaced with a sink/source");
    }

    private PhysicalOperation planExchangeSink(ExchangeSinkExec exchangeSink, LocalExecutionPlannerContext context) {
        Objects.requireNonNull(exchangeSinkHandler, "ExchangeSinkHandler wasn't provided");
        PhysicalOperation source = plan(exchangeSink.child(), context);
        return source.withSink(new ExchangeSinkOperatorFactory(exchangeSinkHandler::createExchangeSink), source.layout);
    }

    private PhysicalOperation planExchangeSource(ExchangeSourceExec exchangeSource) {
        // TODO: ugly hack for now to get the same layout - need to properly support it and have it exposed in the plan and over the wire
        LocalExecutionPlannerContext dummyContext = new LocalExecutionPlannerContext(
            new ArrayList<>(),
            new Holder<>(DriverParallelism.SINGLE),
            1,
            DataPartitioning.SHARD,
            BigArrays.NON_RECYCLING_INSTANCE
        );

        var planToGetLayout = plan(exchangeSource.nodeLayout(), dummyContext);
        Objects.requireNonNull(exchangeSourceHandler, "ExchangeSourceHandler wasn't provided");
        return PhysicalOperation.fromSource(
            new ExchangeSourceOperatorFactory(exchangeSourceHandler::createExchangeSource),
            planToGetLayout.layout
        );
    }

    private PhysicalOperation planTopN(TopNExec topNExec, LocalExecutionPlannerContext context) {
        PhysicalOperation source = plan(topNExec.child(), context);

        List<TopNOperator.SortOrder> orders = topNExec.order().stream().map(order -> {
            int sortByChannel;
            if (order.child() instanceof Attribute a) {
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
        if (topNExec.limit() instanceof Literal literal) {
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
            source = source.with(new EvalOperatorFactory(evaluatorSupplier), layout.build());
        }
        return source;
    }

    private PhysicalOperation planDissect(DissectExec dissect, LocalExecutionPlannerContext context) {
        PhysicalOperation source = plan(dissect.child(), context);
        Layout.Builder layoutBuilder = source.layout.builder();
        for (Attribute attr : dissect.extractedFields()) {
            layoutBuilder.appendChannel(attr.id());
        }
        final Expression expr = dissect.inputExpression();
        String[] attributeNames = Expressions.names(dissect.extractedFields()).toArray(new String[0]);
        ElementType[] types = new ElementType[dissect.extractedFields().size()];
        Arrays.fill(types, ElementType.BYTES_REF);

        Layout layout = layoutBuilder.build();
        source = source.with(
            new StringExtractOperator.StringExtractOperatorFactory(
                attributeNames,
                EvalMapper.toEvaluator(expr, layout),
                () -> (input) -> dissect.parser().parser().parse(input)
            ),
            layout
        );
        return source;
    }

    private PhysicalOperation planGrok(GrokExec grok, LocalExecutionPlannerContext context) {
        PhysicalOperation source = plan(grok.child(), context);
        Layout.Builder layoutBuilder = source.layout.builder();
        List<Attribute> extractedFields = grok.extractedFields();
        for (Attribute attr : extractedFields) {
            layoutBuilder.appendChannel(attr.id());
        }

        Map<String, Integer> fieldToPos = new HashMap<>(extractedFields.size());
        Map<String, ElementType> fieldToType = new HashMap<>(extractedFields.size());
        ElementType[] types = new ElementType[extractedFields.size()];
        for (int i = 0; i < extractedFields.size(); i++) {
            Attribute extractedField = extractedFields.get(i);
            ElementType type = toElementType(extractedField.dataType());
            fieldToPos.put(extractedField.name(), i);
            fieldToType.put(extractedField.name(), type);
            types[i] = type;
        }

        Layout layout = layoutBuilder.build();
        source = source.with(
            new ColumnExtractOperator.Factory(
                types,
                EvalMapper.toEvaluator(grok.inputExpression(), layout),
                () -> new GrokEvaluatorExtracter(grok.pattern().grok(), grok.pattern().pattern(), fieldToPos, fieldToType)
            ),
            layout
        );
        return source;
    }

    private PhysicalOperation planEnrich(EnrichExec enrich, LocalExecutionPlannerContext context) {
        PhysicalOperation source = plan(enrich.child(), context);
        Layout.Builder layoutBuilder = source.layout.builder();
        List<NamedExpression> extractedFields = enrich.enrichFields();
        for (NamedExpression attr : extractedFields) {
            layoutBuilder.appendChannel(attr.id());
        }
        Layout layout = layoutBuilder.build();
        Set<String> indices = enrich.enrichIndex().concreteIndices();
        if (indices.size() != 1) {
            throw new EsqlIllegalArgumentException("Resolved enrich should have one concrete index; got " + indices);
        }
        String enrichIndex = Iterables.get(indices, 0);
        return source.with(
            new EnrichLookupOperator.Factory(
                sessionId,
                parentTask,
                1, // TODO: Add a concurrent setting for enrich - also support unordered mode
                source.layout.getChannel(enrich.matchField().id()),
                enrichLookupService,
                enrichIndex,
                "match", // TODO: enrich should also resolve the match_type
                enrich.policyMatchField(),
                enrich.enrichFields()
            ),
            layout
        );
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
        LocalSourceOperator.BlockSupplier supplier = () -> localSourceExec.supplier().get();
        var operator = new LocalSourceOperator(supplier);
        return PhysicalOperation.fromSource(new LocalSourceFactory(() -> operator), layout.build());
    }

    private PhysicalOperation planShow(ShowExec showExec) {
        Layout.Builder layout = new Layout.Builder();
        for (var attribute : showExec.output()) {
            layout.appendChannel(attribute.id());
        }
        return PhysicalOperation.fromSource(new ShowOperator.ShowOperatorFactory(showExec.values()), layout.build());
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

    private PhysicalOperation planMvExpand(MvExpandExec mvExpandExec, LocalExecutionPlannerContext context) {
        PhysicalOperation source = plan(mvExpandExec.child(), context);
        return source.with(new MvExpandOperator.Factory(source.layout.getChannel(mvExpandExec.target().id())), source.layout);
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

        public SourceOperator source(DriverContext driverContext) {
            return sourceOperatorFactory.get(driverContext);
        }

        public void operators(List<Operator> operators, DriverContext driverContext) {
            intermediateOperatorFactories.stream().map(opFactory -> opFactory.get(driverContext)).forEach(operators::add);
        }

        public SinkOperator sink(DriverContext driverContext) {
            return sinkOperatorFactory.get(driverContext);
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
                dataPartitioning,
                bigArrays
            );
        }
    }

    record DriverSupplier(BigArrays bigArrays, PhysicalOperation physicalOperation) implements Function<String, Driver>, Describable {

        @Override
        public Driver apply(String sessionId) {
            SourceOperator source = null;
            List<Operator> operators = new ArrayList<>();
            SinkOperator sink = null;
            boolean success = false;
            var driverContext = new DriverContext();
            try {
                source = physicalOperation.source(driverContext);
                physicalOperation.operators(operators, driverContext);
                sink = physicalOperation.sink(driverContext);
                success = true;
                return new Driver(sessionId, driverContext, physicalOperation::describe, source, operators, sink, () -> {});
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

        public List<Driver> createDrivers(String sessionId) {
            List<Driver> drivers = new ArrayList<>();
            for (DriverFactory df : driverFactories) {
                for (int i = 0; i < df.driverParallelism.instanceCount; i++) {
                    drivers.add(df.driverSupplier.apply(sessionId));
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
