/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.planner;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.Describable;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.LocalCircuitBreaker;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.lucene.LuceneOperator;
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
import org.elasticsearch.compute.operator.exchange.ExchangeSinkHandler;
import org.elasticsearch.compute.operator.exchange.ExchangeSinkOperator.ExchangeSinkOperatorFactory;
import org.elasticsearch.compute.operator.exchange.ExchangeSourceHandler;
import org.elasticsearch.compute.operator.exchange.ExchangeSourceOperator.ExchangeSourceOperatorFactory;
import org.elasticsearch.compute.operator.topn.TopNEncoder;
import org.elasticsearch.compute.operator.topn.TopNOperator;
import org.elasticsearch.compute.operator.topn.TopNOperator.TopNOperatorFactory;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.xpack.esql.EsqlIllegalArgumentException;
import org.elasticsearch.xpack.esql.enrich.EnrichLookupOperator;
import org.elasticsearch.xpack.esql.enrich.EnrichLookupService;
import org.elasticsearch.xpack.esql.evaluator.EvalMapper;
import org.elasticsearch.xpack.esql.evaluator.command.GrokEvaluatorExtracter;
import org.elasticsearch.xpack.esql.plan.physical.AggregateExec;
import org.elasticsearch.xpack.esql.plan.physical.DissectExec;
import org.elasticsearch.xpack.esql.plan.physical.EnrichExec;
import org.elasticsearch.xpack.esql.plan.physical.EsQueryExec;
import org.elasticsearch.xpack.esql.plan.physical.EsStatsQueryExec;
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
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;
import org.elasticsearch.xpack.esql.session.EsqlConfiguration;
import org.elasticsearch.xpack.ql.expression.Alias;
import org.elasticsearch.xpack.ql.expression.Attribute;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.Expressions;
import org.elasticsearch.xpack.ql.expression.Literal;
import org.elasticsearch.xpack.ql.expression.NameId;
import org.elasticsearch.xpack.ql.expression.NamedExpression;
import org.elasticsearch.xpack.ql.expression.Order;
import org.elasticsearch.xpack.ql.util.Holder;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Stream;

import static java.util.Arrays.asList;
import static java.util.stream.Collectors.joining;
import static org.elasticsearch.compute.operator.LimitOperator.Factory;
import static org.elasticsearch.compute.operator.ProjectOperator.ProjectOperatorFactory;
import static org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter.stringToInt;

/**
 * The local execution planner takes a plan (represented as PlanNode tree / digraph) as input and creates the corresponding
 * drivers that are used to execute the given plan.
 */
public class LocalExecutionPlanner {
    private static final Logger logger = LogManager.getLogger(LocalExecutionPlanner.class);

    private final String sessionId;
    private final String clusterAlias;
    private final CancellableTask parentTask;
    private final BigArrays bigArrays;
    private final BlockFactory blockFactory;
    private final Settings settings;
    private final EsqlConfiguration configuration;
    private final ExchangeSourceHandler exchangeSourceHandler;
    private final ExchangeSinkHandler exchangeSinkHandler;
    private final EnrichLookupService enrichLookupService;
    private final PhysicalOperationProviders physicalOperationProviders;

    public LocalExecutionPlanner(
        String sessionId,
        String clusterAlias,
        CancellableTask parentTask,
        BigArrays bigArrays,
        BlockFactory blockFactory,
        Settings settings,
        EsqlConfiguration configuration,
        ExchangeSourceHandler exchangeSourceHandler,
        ExchangeSinkHandler exchangeSinkHandler,
        EnrichLookupService enrichLookupService,
        PhysicalOperationProviders physicalOperationProviders
    ) {
        this.sessionId = sessionId;
        this.clusterAlias = clusterAlias;
        this.parentTask = parentTask;
        this.bigArrays = bigArrays;
        this.blockFactory = blockFactory;
        this.settings = settings;
        this.exchangeSourceHandler = exchangeSourceHandler;
        this.exchangeSinkHandler = exchangeSinkHandler;
        this.enrichLookupService = enrichLookupService;
        this.physicalOperationProviders = physicalOperationProviders;
        this.configuration = configuration;
    }

    /**
     * turn the given plan into a list of drivers to execute
     */
    public LocalExecutionPlan plan(PhysicalPlan localPhysicalPlan) {
        var context = new LocalExecutionPlannerContext(
            new ArrayList<>(),
            new Holder<>(DriverParallelism.SINGLE),
            configuration.pragmas(),
            bigArrays,
            blockFactory,
            settings
        );

        // workaround for https://github.com/elastic/elasticsearch/issues/99782
        localPhysicalPlan = localPhysicalPlan.transformUp(
            AggregateExec.class,
            a -> a.getMode() == AggregateExec.Mode.FINAL ? new ProjectExec(a.source(), a, Expressions.asAttributes(a.aggregates())) : a
        );
        PhysicalOperation physicalOperation = plan(localPhysicalPlan, context);

        final TimeValue statusInterval = configuration.pragmas().statusInterval();
        context.addDriverFactory(
            new DriverFactory(
                new DriverSupplier(context.bigArrays, context.blockFactory, physicalOperation, statusInterval, settings),
                context.driverParallelism().get()
            )
        );

        return new LocalExecutionPlan(context.driverFactories);
    }

    private PhysicalOperation plan(PhysicalPlan node, LocalExecutionPlannerContext context) {
        if (node instanceof AggregateExec aggregate) {
            return planAggregation(aggregate, context);
        } else if (node instanceof FieldExtractExec fieldExtractExec) {
            return planFieldExtractNode(fieldExtractExec, context);
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
        } else if (node instanceof EsStatsQueryExec statsQuery) {
            return planEsStats(statsQuery, context);
        } else if (node instanceof RowExec row) {
            return planRow(row, context);
        } else if (node instanceof LocalSourceExec localSource) {
            return planLocal(localSource, context);
        } else if (node instanceof ShowExec show) {
            return planShow(show);
        } else if (node instanceof ExchangeSourceExec exchangeSource) {
            return planExchangeSource(exchangeSource, context);
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

        throw new EsqlIllegalArgumentException("unknown physical plan node [" + node.nodeName() + "]");
    }

    private PhysicalOperation planAggregation(AggregateExec aggregate, LocalExecutionPlannerContext context) {
        var source = plan(aggregate.child(), context);
        return physicalOperationProviders.groupingPhysicalOperation(aggregate, source, context);
    }

    private PhysicalOperation planEsQueryNode(EsQueryExec esQueryExec, LocalExecutionPlannerContext context) {
        return physicalOperationProviders.sourcePhysicalOperation(esQueryExec, context);
    }

    private PhysicalOperation planEsStats(EsStatsQueryExec statsQuery, LocalExecutionPlannerContext context) {
        if (physicalOperationProviders instanceof EsPhysicalOperationProviders == false) {
            throw new EsqlIllegalArgumentException("EsStatsQuery should only occur against a Lucene backend");
        }
        if (statsQuery.stats().size() > 1) {
            throw new EsqlIllegalArgumentException("EsStatsQuery currently supports only one field statistic");
        }

        // for now only one stat is supported
        EsStatsQueryExec.Stat stat = statsQuery.stats().get(0);

        EsPhysicalOperationProviders esProvider = (EsPhysicalOperationProviders) physicalOperationProviders;
        final LuceneOperator.Factory luceneFactory = esProvider.countSource(context, stat.filter(statsQuery.query()), statsQuery.limit());

        Layout.Builder layout = new Layout.Builder();
        layout.append(statsQuery.outputSet());
        int instanceCount = Math.max(1, luceneFactory.taskConcurrency());
        context.driverParallelism(new DriverParallelism(DriverParallelism.Type.DATA_PARALLELISM, instanceCount));
        return PhysicalOperation.fromSource(luceneFactory, layout.build());
    }

    private PhysicalOperation planFieldExtractNode(FieldExtractExec fieldExtractExec, LocalExecutionPlannerContext context) {
        return physicalOperationProviders.fieldExtractPhysicalOperation(fieldExtractExec, plan(fieldExtractExec.child(), context));
    }

    private PhysicalOperation planOutput(OutputExec outputExec, LocalExecutionPlannerContext context) {
        PhysicalOperation source = plan(outputExec.child(), context);
        var output = outputExec.output();

        return source.withSink(
            new OutputOperatorFactory(
                Expressions.names(output),
                alignPageToAttributes(output, source.layout),
                outputExec.getPageConsumer()
            ),
            source.layout
        );
    }

    private static Function<Page, Page> alignPageToAttributes(List<Attribute> attrs, Layout layout) {
        // align the page layout with the operator output
        // extraction order - the list ordinal is the same as the column one
        // while the value represents the position in the original page
        final int[] mappedPosition = new int[attrs.size()];
        int index = -1;
        boolean transformRequired = false;
        for (var attribute : attrs) {
            mappedPosition[++index] = layout.get(attribute.id()).channel();
            transformRequired |= mappedPosition[index] != index;
        }
        Function<Page, Page> transformer = transformRequired ? p -> {
            var blocks = new Block[mappedPosition.length];
            for (int i = 0; i < blocks.length; i++) {
                blocks[i] = p.getBlock(mappedPosition[i]);
                blocks[i].incRef();
            }
            p.releaseBlocks();
            return new Page(blocks);
        } : Function.identity();

        return transformer;
    }

    private PhysicalOperation planExchange(ExchangeExec exchangeExec, LocalExecutionPlannerContext context) {
        throw new UnsupportedOperationException("Exchange needs to be replaced with a sink/source");
    }

    private PhysicalOperation planExchangeSink(ExchangeSinkExec exchangeSink, LocalExecutionPlannerContext context) {
        Objects.requireNonNull(exchangeSinkHandler, "ExchangeSinkHandler wasn't provided");
        var child = exchangeSink.child();

        PhysicalOperation source = plan(child, context);

        Function<Page, Page> transformer = exchangeSink.isIntermediateAgg()
            ? Function.identity()
            : alignPageToAttributes(exchangeSink.output(), source.layout);

        return source.withSink(new ExchangeSinkOperatorFactory(exchangeSinkHandler::createExchangeSink, transformer), source.layout);
    }

    private PhysicalOperation planExchangeSource(ExchangeSourceExec exchangeSource, LocalExecutionPlannerContext context) {
        Objects.requireNonNull(exchangeSourceHandler, "ExchangeSourceHandler wasn't provided");

        var builder = new Layout.Builder();
        builder.append(exchangeSource.output());
        // decorate the layout
        var l = builder.build();
        var layout = exchangeSource.isIntermediateAgg() ? new ExchangeLayout(l) : l;

        return PhysicalOperation.fromSource(new ExchangeSourceOperatorFactory(exchangeSourceHandler::createExchangeSource), layout);
    }

    private PhysicalOperation planTopN(TopNExec topNExec, LocalExecutionPlannerContext context) {
        PhysicalOperation source = plan(topNExec.child(), context);

        ElementType[] elementTypes = new ElementType[source.layout.numberOfChannels()];
        TopNEncoder[] encoders = new TopNEncoder[source.layout.numberOfChannels()];
        List<Layout.ChannelSet> inverse = source.layout.inverse();
        for (int channel = 0; channel < inverse.size(); channel++) {
            elementTypes[channel] = PlannerUtils.toElementType(inverse.get(channel).type());
            encoders[channel] = switch (inverse.get(channel).type().typeName()) {
                case "ip" -> TopNEncoder.IP;
                case "text", "keyword" -> TopNEncoder.UTF8;
                case "version" -> TopNEncoder.VERSION;
                case "boolean", "null", "byte", "short", "integer", "long", "double", "float", "half_float", "datetime", "date_period",
                    "time_duration", "object", "nested", "scaled_float", "unsigned_long", "_doc" -> TopNEncoder.DEFAULT_SORTABLE;
                case "geo_point", "cartesian_point", "geo_shape", "cartesian_shape" -> TopNEncoder.DEFAULT_UNSORTABLE;
                // unsupported fields are encoded as BytesRef, we'll use the same encoder; all values should be null at this point
                case "unsupported" -> TopNEncoder.UNSUPPORTED;
                default -> throw new EsqlIllegalArgumentException("No TopN sorting encoder for type " + inverse.get(channel).type());
            };
        }
        List<TopNOperator.SortOrder> orders = topNExec.order().stream().map(order -> {
            int sortByChannel;
            if (order.child() instanceof Attribute a) {
                sortByChannel = source.layout.get(a.id()).channel();
            } else {
                throw new EsqlIllegalArgumentException("order by expression must be an attribute");
            }

            return new TopNOperator.SortOrder(
                sortByChannel,
                order.direction().equals(Order.OrderDirection.ASC),
                order.nullsPosition().equals(Order.NullsPosition.FIRST)
            );
        }).toList();

        int limit;
        if (topNExec.limit() instanceof Literal literal) {
            limit = stringToInt(literal.value().toString());
        } else {
            throw new EsqlIllegalArgumentException("limit only supported with literal values");
        }

        // TODO Replace page size with passing estimatedRowSize down
        /*
         * The 2000 below is a hack to account for incoming size and to make
         * sure the estimated row size is never 0 which'd cause a divide by 0.
         * But we should replace this with passing the estimate into the real
         * topn and letting it actually measure the size of rows it produces.
         * That'll be more accurate. And we don't have a path for estimating
         * incoming rows. And we don't need one because we can estimate.
         */
        return source.with(
            new TopNOperatorFactory(
                limit,
                asList(elementTypes),
                asList(encoders),
                orders,
                context.pageSize(2000 + topNExec.estimatedRowSize())
            ),
            source.layout
        );
    }

    private PhysicalOperation planEval(EvalExec eval, LocalExecutionPlannerContext context) {
        PhysicalOperation source = plan(eval.child(), context);

        for (Alias field : eval.fields()) {
            var evaluatorSupplier = EvalMapper.toEvaluator(field.child(), source.layout);
            Layout.Builder layout = source.layout.builder();
            layout.append(field.toAttribute());
            source = source.with(new EvalOperatorFactory(evaluatorSupplier), layout.build());
        }
        return source;
    }

    private PhysicalOperation planDissect(DissectExec dissect, LocalExecutionPlannerContext context) {
        PhysicalOperation source = plan(dissect.child(), context);
        Layout.Builder layoutBuilder = source.layout.builder();
        layoutBuilder.append(dissect.extractedFields());
        final Expression expr = dissect.inputExpression();
        String[] attributeNames = Expressions.names(dissect.extractedFields()).toArray(new String[0]);

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
        layoutBuilder.append(extractedFields);
        Map<String, Integer> fieldToPos = new HashMap<>(extractedFields.size());
        Map<String, ElementType> fieldToType = new HashMap<>(extractedFields.size());
        ElementType[] types = new ElementType[extractedFields.size()];
        for (int i = 0; i < extractedFields.size(); i++) {
            Attribute extractedField = extractedFields.get(i);
            ElementType type = PlannerUtils.toElementType(extractedField.dataType());
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
        layoutBuilder.append(enrich.enrichFields());
        Layout layout = layoutBuilder.build();
        String enrichIndex = enrich.concreteIndices().get(clusterAlias);
        if (enrichIndex == null) {
            throw new EsqlIllegalArgumentException("No concrete enrich index for cluster [" + clusterAlias + "]");
        }
        Layout.ChannelAndType input = source.layout.get(enrich.matchField().id());
        return source.with(
            new EnrichLookupOperator.Factory(
                sessionId,
                parentTask,
                context.queryPragmas().enrichMaxWorkers(),
                input.channel(),
                enrichLookupService,
                input.type(),
                enrichIndex,
                enrich.matchType(),
                enrich.policyMatchField(),
                enrich.enrichFields()
            ),
            layout
        );
    }

    private ExpressionEvaluator.Factory toEvaluator(Expression exp, Layout layout) {
        return EvalMapper.toEvaluator(exp, layout);
    }

    private PhysicalOperation planRow(RowExec row, LocalExecutionPlannerContext context) {
        List<Object> obj = row.fields().stream().map(f -> f.child().fold()).toList();
        Layout.Builder layout = new Layout.Builder();
        layout.append(row.output());
        return PhysicalOperation.fromSource(new RowOperatorFactory(obj), layout.build());
    }

    private PhysicalOperation planLocal(LocalSourceExec localSourceExec, LocalExecutionPlannerContext context) {
        Layout.Builder layout = new Layout.Builder();
        layout.append(localSourceExec.output());
        LocalSourceOperator.BlockSupplier supplier = () -> localSourceExec.supplier().get();
        var operator = new LocalSourceOperator(supplier);
        return PhysicalOperation.fromSource(new LocalSourceFactory(() -> operator), layout.build());
    }

    private PhysicalOperation planShow(ShowExec showExec) {
        Layout.Builder layout = new Layout.Builder();
        layout.append(showExec.output());
        return PhysicalOperation.fromSource(new ShowOperator.ShowOperatorFactory(showExec.values()), layout.build());
    }

    private PhysicalOperation planProject(ProjectExec project, LocalExecutionPlannerContext context) {
        var source = plan(project.child(), context);
        List<? extends NamedExpression> projections = project.projections();
        List<Integer> projectionList = new ArrayList<>(projections.size());

        Layout.Builder layout = new Layout.Builder();
        Map<Integer, Layout.ChannelSet> inputChannelToOutputIds = new HashMap<>();
        for (int index = 0, size = projections.size(); index < size; index++) {
            NamedExpression ne = projections.get(index);

            NameId inputId = null;
            if (ne instanceof Alias a) {
                inputId = ((NamedExpression) a.child()).id();
            } else {
                inputId = ne.id();
            }
            Layout.ChannelAndType input = source.layout.get(inputId);
            Layout.ChannelSet channelSet = inputChannelToOutputIds.get(input.channel());
            if (channelSet == null) {
                channelSet = new Layout.ChannelSet(new HashSet<>(), input.type());
                channelSet.nameIds().add(ne.id());
                layout.append(channelSet);
            } else {
                channelSet.nameIds().add(ne.id());
            }
            if (channelSet.type() != input.type()) {
                throw new IllegalArgumentException("type mismatch for aliases");
            }
            projectionList.add(input.channel());
        }

        return source.with(new ProjectOperatorFactory(projectionList), layout.build());
    }

    private PhysicalOperation planFilter(FilterExec filter, LocalExecutionPlannerContext context) {
        PhysicalOperation source = plan(filter.child(), context);
        // TODO: should this be extracted into a separate eval block?
        return source.with(new FilterOperatorFactory(toEvaluator(filter.condition(), source.layout)), source.layout);
    }

    private PhysicalOperation planLimit(LimitExec limit, LocalExecutionPlannerContext context) {
        PhysicalOperation source = plan(limit.child(), context);
        return source.with(new Factory((Integer) limit.limit().fold()), source.layout);
    }

    private PhysicalOperation planMvExpand(MvExpandExec mvExpandExec, LocalExecutionPlannerContext context) {
        PhysicalOperation source = plan(mvExpandExec.child(), context);
        int blockSize = 5000;// TODO estimate row size and use context.pageSize()
        Layout.Builder layout = source.layout.builder();
        layout.replace(mvExpandExec.target().id(), mvExpandExec.expanded().id());
        return source.with(
            new MvExpandOperator.Factory(source.layout.get(mvExpandExec.target().id()).channel(), blockSize),
            layout.build()
        );
    }

    /**
     * Immutable physical operation.
     */
    public static class PhysicalOperation implements Describable {
        final SourceOperatorFactory sourceOperatorFactory;
        final List<OperatorFactory> intermediateOperatorFactories;
        final SinkOperatorFactory sinkOperatorFactory;

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

        @Override
        public String toString() {
            return describe();
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
        QueryPragmas queryPragmas,
        BigArrays bigArrays,
        BlockFactory blockFactory,
        Settings settings
    ) {
        void addDriverFactory(DriverFactory driverFactory) {
            driverFactories.add(driverFactory);
        }

        void driverParallelism(DriverParallelism parallelism) {
            driverParallelism.set(parallelism);
        }

        int pageSize(Integer estimatedRowSize) {
            if (estimatedRowSize == null) {
                throw new IllegalStateException("estimated row size hasn't been set");
            }
            if (estimatedRowSize == 0) {
                throw new IllegalStateException("estimated row size can't be 0");
            }
            if (queryPragmas.pageSize() != 0) {
                return queryPragmas.pageSize();
            }
            return Math.max(SourceOperator.MIN_TARGET_PAGE_SIZE, SourceOperator.TARGET_PAGE_SIZE / estimatedRowSize);
        }
    }

    record DriverSupplier(
        BigArrays bigArrays,
        BlockFactory blockFactory,
        PhysicalOperation physicalOperation,
        TimeValue statusInterval,
        Settings settings
    ) implements Function<String, Driver>, Describable {
        @Override
        public Driver apply(String sessionId) {
            SourceOperator source = null;
            List<Operator> operators = new ArrayList<>();
            SinkOperator sink = null;
            boolean success = false;
            var localBreakerSettings = new LocalCircuitBreaker.SizeSettings(settings);
            final var localBreaker = new LocalCircuitBreaker(
                blockFactory.breaker(),
                localBreakerSettings.overReservedBytes(),
                localBreakerSettings.maxOverReservedBytes()
            );
            var driverContext = new DriverContext(bigArrays, blockFactory.newChildFactory(localBreaker));
            try {
                source = physicalOperation.source(driverContext);
                physicalOperation.operators(operators, driverContext);
                sink = physicalOperation.sink(driverContext);
                success = true;
                return new Driver(
                    sessionId,
                    System.currentTimeMillis(),
                    System.nanoTime(),
                    driverContext,
                    physicalOperation::describe,
                    source,
                    operators,
                    sink,
                    statusInterval,
                    localBreaker
                );
            } finally {
                if (false == success) {
                    Releasables.close(source, () -> Releasables.close(operators), sink, localBreaker);
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
            boolean success = false;
            try {
                for (DriverFactory df : driverFactories) {
                    for (int i = 0; i < df.driverParallelism.instanceCount; i++) {
                        logger.trace("building {} {}", i, df);
                        drivers.add(df.driverSupplier.apply(sessionId));
                    }
                }
                success = true;
                return drivers;
            } finally {
                if (success == false) {
                    Releasables.close(Releasables.wrap(drivers));
                }
            }
        }

        @Override
        public String describe() {
            return driverFactories.stream().map(DriverFactory::describe).collect(joining("\n"));
        }
    }
}
