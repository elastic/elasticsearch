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
import org.elasticsearch.compute.aggregation.AggregatorMode;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.LocalCircuitBreaker;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.lucene.LuceneOperator;
import org.elasticsearch.compute.operator.ChangePointOperator;
import org.elasticsearch.compute.operator.ColumnExtractOperator;
import org.elasticsearch.compute.operator.ColumnLoadOperator;
import org.elasticsearch.compute.operator.Driver;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.EvalOperator.EvalOperatorFactory;
import org.elasticsearch.compute.operator.FilterOperator.FilterOperatorFactory;
import org.elasticsearch.compute.operator.LimitOperator;
import org.elasticsearch.compute.operator.LocalSourceOperator;
import org.elasticsearch.compute.operator.LocalSourceOperator.LocalSourceFactory;
import org.elasticsearch.compute.operator.MvExpandOperator;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.compute.operator.Operator.OperatorFactory;
import org.elasticsearch.compute.operator.OutputOperator.OutputOperatorFactory;
import org.elasticsearch.compute.operator.RowInTableLookupOperator;
import org.elasticsearch.compute.operator.ScoreOperator;
import org.elasticsearch.compute.operator.ShowOperator;
import org.elasticsearch.compute.operator.SinkOperator;
import org.elasticsearch.compute.operator.SinkOperator.SinkOperatorFactory;
import org.elasticsearch.compute.operator.SourceOperator;
import org.elasticsearch.compute.operator.SourceOperator.SourceOperatorFactory;
import org.elasticsearch.compute.operator.StringExtractOperator;
import org.elasticsearch.compute.operator.exchange.ExchangeSink;
import org.elasticsearch.compute.operator.exchange.ExchangeSinkOperator.ExchangeSinkOperatorFactory;
import org.elasticsearch.compute.operator.exchange.ExchangeSource;
import org.elasticsearch.compute.operator.exchange.ExchangeSourceOperator.ExchangeSourceOperatorFactory;
import org.elasticsearch.compute.operator.topn.TopNEncoder;
import org.elasticsearch.compute.operator.topn.TopNOperator;
import org.elasticsearch.compute.operator.topn.TopNOperator.TopNOperatorFactory;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.xpack.esql.EsqlIllegalArgumentException;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.core.expression.NameId;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.expression.TypedAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.util.Holder;
import org.elasticsearch.xpack.esql.enrich.EnrichLookupOperator;
import org.elasticsearch.xpack.esql.enrich.EnrichLookupService;
import org.elasticsearch.xpack.esql.enrich.LookupFromIndexOperator;
import org.elasticsearch.xpack.esql.enrich.LookupFromIndexService;
import org.elasticsearch.xpack.esql.evaluator.EvalMapper;
import org.elasticsearch.xpack.esql.evaluator.command.GrokEvaluatorExtracter;
import org.elasticsearch.xpack.esql.expression.Order;
import org.elasticsearch.xpack.esql.plan.physical.AggregateExec;
import org.elasticsearch.xpack.esql.plan.physical.ChangePointExec;
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
import org.elasticsearch.xpack.esql.plan.physical.HashJoinExec;
import org.elasticsearch.xpack.esql.plan.physical.LimitExec;
import org.elasticsearch.xpack.esql.plan.physical.LocalSourceExec;
import org.elasticsearch.xpack.esql.plan.physical.LookupJoinExec;
import org.elasticsearch.xpack.esql.plan.physical.MvExpandExec;
import org.elasticsearch.xpack.esql.plan.physical.OutputExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.ProjectExec;
import org.elasticsearch.xpack.esql.plan.physical.ShowExec;
import org.elasticsearch.xpack.esql.plan.physical.TopNExec;
import org.elasticsearch.xpack.esql.planner.EsPhysicalOperationProviders.ShardContext;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;
import org.elasticsearch.xpack.esql.score.ScoreMapper;
import org.elasticsearch.xpack.esql.session.Configuration;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static java.util.Arrays.asList;
import static java.util.stream.Collectors.joining;
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
    private final Configuration configuration;
    private final Supplier<ExchangeSource> exchangeSourceSupplier;
    private final Supplier<ExchangeSink> exchangeSinkSupplier;
    private final EnrichLookupService enrichLookupService;
    private final LookupFromIndexService lookupFromIndexService;
    private final PhysicalOperationProviders physicalOperationProviders;
    private final List<ShardContext> shardContexts;

    public LocalExecutionPlanner(
        String sessionId,
        String clusterAlias,
        CancellableTask parentTask,
        BigArrays bigArrays,
        BlockFactory blockFactory,
        Settings settings,
        Configuration configuration,
        Supplier<ExchangeSource> exchangeSourceSupplier,
        Supplier<ExchangeSink> exchangeSinkSupplier,
        EnrichLookupService enrichLookupService,
        LookupFromIndexService lookupFromIndexService,
        PhysicalOperationProviders physicalOperationProviders,
        List<ShardContext> shardContexts
    ) {

        this.sessionId = sessionId;
        this.clusterAlias = clusterAlias;
        this.parentTask = parentTask;
        this.bigArrays = bigArrays;
        this.blockFactory = blockFactory;
        this.settings = settings;
        this.exchangeSourceSupplier = exchangeSourceSupplier;
        this.exchangeSinkSupplier = exchangeSinkSupplier;
        this.enrichLookupService = enrichLookupService;
        this.lookupFromIndexService = lookupFromIndexService;
        this.physicalOperationProviders = physicalOperationProviders;
        this.configuration = configuration;
        this.shardContexts = shardContexts;
    }

    /**
     * turn the given plan into a list of drivers to execute
     */
    public LocalExecutionPlan plan(String taskDescription, FoldContext foldCtx, PhysicalPlan localPhysicalPlan) {
        var context = new LocalExecutionPlannerContext(
            new ArrayList<>(),
            new Holder<>(DriverParallelism.SINGLE),
            configuration.pragmas(),
            bigArrays,
            blockFactory,
            foldCtx,
            settings,
            shardContexts
        );

        // workaround for https://github.com/elastic/elasticsearch/issues/99782
        localPhysicalPlan = localPhysicalPlan.transformUp(
            AggregateExec.class,
            a -> a.getMode() == AggregatorMode.FINAL ? new ProjectExec(a.source(), a, Expressions.asAttributes(a.aggregates())) : a
        );
        PhysicalOperation physicalOperation = plan(localPhysicalPlan, context);

        final TimeValue statusInterval = configuration.pragmas().statusInterval();
        context.addDriverFactory(
            new DriverFactory(
                new DriverSupplier(taskDescription, context.bigArrays, context.blockFactory, physicalOperation, statusInterval, settings),
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
        } else if (node instanceof ChangePointExec changePoint) {
            return planChangePoint(changePoint, context);
        }
        // source nodes
        else if (node instanceof EsQueryExec esQuery) {
            return planEsQueryNode(esQuery, context);
        } else if (node instanceof EsStatsQueryExec statsQuery) {
            return planEsStats(statsQuery, context);
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
        } else if (node instanceof HashJoinExec join) {
            return planHashJoin(join, context);
        } else if (node instanceof LookupJoinExec join) {
            return planLookupJoin(join, context);
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
        Objects.requireNonNull(exchangeSinkSupplier, "ExchangeSinkHandler wasn't provided");
        var child = exchangeSink.child();

        PhysicalOperation source = plan(child, context);

        Function<Page, Page> transformer = exchangeSink.isIntermediateAgg()
            ? Function.identity()
            : alignPageToAttributes(exchangeSink.output(), source.layout);

        return source.withSink(new ExchangeSinkOperatorFactory(exchangeSinkSupplier, transformer), source.layout);
    }

    private PhysicalOperation planExchangeSource(ExchangeSourceExec exchangeSource, LocalExecutionPlannerContext context) {
        Objects.requireNonNull(exchangeSourceSupplier, "ExchangeSourceHandler wasn't provided");

        var builder = new Layout.Builder();
        builder.append(exchangeSource.output());
        // decorate the layout
        var l = builder.build();
        var layout = exchangeSource.isIntermediateAgg() ? new ExchangeLayout(l) : l;

        return PhysicalOperation.fromSource(new ExchangeSourceOperatorFactory(exchangeSourceSupplier), layout);
    }

    private PhysicalOperation planTopN(TopNExec topNExec, LocalExecutionPlannerContext context) {
        final Integer rowSize = topNExec.estimatedRowSize();
        assert rowSize != null && rowSize > 0 : "estimated row size [" + rowSize + "] wasn't set";
        PhysicalOperation source = plan(topNExec.child(), context);

        ElementType[] elementTypes = new ElementType[source.layout.numberOfChannels()];
        TopNEncoder[] encoders = new TopNEncoder[source.layout.numberOfChannels()];
        List<Layout.ChannelSet> inverse = source.layout.inverse();
        for (int channel = 0; channel < inverse.size(); channel++) {
            elementTypes[channel] = PlannerUtils.toElementType(inverse.get(channel).type());
            encoders[channel] = switch (inverse.get(channel).type()) {
                case IP -> TopNEncoder.IP;
                case TEXT, KEYWORD, SEMANTIC_TEXT -> TopNEncoder.UTF8;
                case VERSION -> TopNEncoder.VERSION;
                case BOOLEAN, NULL, BYTE, SHORT, INTEGER, LONG, DOUBLE, FLOAT, HALF_FLOAT, DATETIME, DATE_NANOS, DATE_PERIOD, TIME_DURATION,
                    OBJECT, SCALED_FLOAT, UNSIGNED_LONG, DOC_DATA_TYPE, TSID_DATA_TYPE -> TopNEncoder.DEFAULT_SORTABLE;
                case GEO_POINT, CARTESIAN_POINT, GEO_SHAPE, CARTESIAN_SHAPE, COUNTER_LONG, COUNTER_INTEGER, COUNTER_DOUBLE, SOURCE,
                    AGGREGATE_METRIC_DOUBLE -> TopNEncoder.DEFAULT_UNSORTABLE;
                // unsupported fields are encoded as BytesRef, we'll use the same encoder; all values should be null at this point
                case PARTIAL_AGG, UNSUPPORTED -> TopNEncoder.UNSUPPORTED;
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
        return source.with(
            new TopNOperatorFactory(limit, asList(elementTypes), asList(encoders), orders, context.pageSize(rowSize)),
            source.layout
        );
    }

    private PhysicalOperation planEval(EvalExec eval, LocalExecutionPlannerContext context) {
        PhysicalOperation source = plan(eval.child(), context);

        for (Alias field : eval.fields()) {
            var evaluatorSupplier = EvalMapper.toEvaluator(context.foldCtx(), field.child(), source.layout);
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
        // Names in the pattern and layout can differ.
        // Attributes need to be rename-able to avoid problems with shadowing - see GeneratingPlan resp. PushDownRegexExtract.
        String[] patternNames = Expressions.names(dissect.parser().keyAttributes(Source.EMPTY)).toArray(new String[0]);

        Layout layout = layoutBuilder.build();
        source = source.with(
            new StringExtractOperator.StringExtractOperatorFactory(
                patternNames,
                EvalMapper.toEvaluator(context.foldCtx(), expr, layout),
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
        List<Attribute> extractedFieldsFromPattern = grok.pattern().extractedFields();
        for (int i = 0; i < extractedFields.size(); i++) {
            DataType extractedFieldType = extractedFields.get(i).dataType();
            // Names in pattern and layout can differ.
            // Attributes need to be rename-able to avoid problems with shadowing - see GeneratingPlan resp. PushDownRegexExtract.
            String patternName = extractedFieldsFromPattern.get(i).name();
            ElementType type = PlannerUtils.toElementType(extractedFieldType);
            fieldToPos.put(patternName, i);
            fieldToType.put(patternName, type);
            types[i] = type;
        }

        Layout layout = layoutBuilder.build();
        source = source.with(
            new ColumnExtractOperator.Factory(
                types,
                EvalMapper.toEvaluator(context.foldCtx(), grok.inputExpression(), layout),
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
                enrich.enrichFields(),
                enrich.source()
            ),
            layout
        );
    }

    private PhysicalOperation planHashJoin(HashJoinExec join, LocalExecutionPlannerContext context) {
        PhysicalOperation source = plan(join.left(), context);
        int positionsChannel = source.layout.numberOfChannels();

        Layout.Builder layoutBuilder = source.layout.builder();
        for (Attribute f : join.output()) {
            if (join.left().outputSet().contains(f)) {
                continue;
            }
            layoutBuilder.append(f);
        }
        Layout layout = layoutBuilder.build();
        LocalSourceExec localSourceExec = (LocalSourceExec) join.joinData();
        Block[] localData = localSourceExec.supplier().get();

        RowInTableLookupOperator.Key[] keys = new RowInTableLookupOperator.Key[join.leftFields().size()];
        int[] blockMapping = new int[join.leftFields().size()];
        for (int k = 0; k < join.leftFields().size(); k++) {
            Attribute left = join.leftFields().get(k);
            Attribute right = join.rightFields().get(k);
            Block localField = null;
            List<Attribute> output = join.joinData().output();
            for (int l = 0; l < output.size(); l++) {
                if (output.get(l).name().equals(right.name())) {
                    localField = localData[l];
                }
            }
            if (localField == null) {
                throw new IllegalArgumentException("can't find local data for [" + right + "]");
            }

            keys[k] = new RowInTableLookupOperator.Key(left.name(), localField);
            Layout.ChannelAndType input = source.layout.get(left.id());
            blockMapping[k] = input.channel();
        }

        // Load the "positions" of each match
        source = source.with(new RowInTableLookupOperator.Factory(keys, blockMapping), layout);

        // Load the "values" from each match
        for (Attribute f : join.addedFields()) {
            Block localField = null;
            for (int l = 0; l < join.joinData().output().size(); l++) {
                if (join.joinData().output().get(l).name().equals(f.name())) {
                    localField = localData[l];
                }
            }
            if (localField == null) {
                throw new IllegalArgumentException("can't find local data for [" + f + "]");
            }
            source = source.with(
                new ColumnLoadOperator.Factory(new ColumnLoadOperator.Values(f.name(), localField), positionsChannel),
                layout
            );
        }

        // Drop the "positions" of the match
        List<Integer> projection = new ArrayList<>();
        IntStream.range(0, positionsChannel).boxed().forEach(projection::add);
        IntStream.range(positionsChannel + 1, positionsChannel + 1 + join.addedFields().size()).boxed().forEach(projection::add);
        return source.with(new ProjectOperatorFactory(projection), layout);
    }

    private PhysicalOperation planLookupJoin(LookupJoinExec join, LocalExecutionPlannerContext context) {
        PhysicalOperation source = plan(join.left(), context);
        Layout.Builder layoutBuilder = source.layout.builder();
        for (Attribute f : join.addedFields()) {
            layoutBuilder.append(f);
        }
        Layout layout = layoutBuilder.build();

        EsQueryExec localSourceExec = (EsQueryExec) join.lookup();
        if (localSourceExec.indexMode() != IndexMode.LOOKUP) {
            throw new IllegalArgumentException("can't plan [" + join + "]");
        }
        Map<String, IndexMode> indicesWithModes = localSourceExec.indexNameWithModes();
        if (indicesWithModes.size() != 1) {
            throw new IllegalArgumentException("can't plan [" + join + "], found more than 1 index");
        }
        var entry = indicesWithModes.entrySet().iterator().next();
        if (entry.getValue() != IndexMode.LOOKUP) {
            throw new IllegalArgumentException("can't plan [" + join + "], found index with mode [" + entry.getValue() + "]");
        }
        String indexName = entry.getKey();
        if (join.leftFields().size() != join.rightFields().size()) {
            throw new IllegalArgumentException("can't plan [" + join + "]: mismatching left and right field count");
        }
        List<MatchConfig> matchFields = new ArrayList<>(join.leftFields().size());
        for (int i = 0; i < join.leftFields().size(); i++) {
            TypedAttribute left = (TypedAttribute) join.leftFields().get(i);
            FieldAttribute right = (FieldAttribute) join.rightFields().get(i);
            Layout.ChannelAndType input = source.layout.get(left.id());
            if (input == null) {
                throw new IllegalArgumentException("can't plan [" + join + "][" + left + "]");
            }
            matchFields.add(new MatchConfig(right, input));
        }
        if (matchFields.size() != 1) {
            throw new IllegalArgumentException("can't plan [" + join + "]: multiple join predicates are not supported");
        }
        // TODO support multiple match fields, and support more than equality predicates
        MatchConfig matchConfig = matchFields.get(0);

        return source.with(
            new LookupFromIndexOperator.Factory(
                sessionId,
                parentTask,
                context.queryPragmas().enrichMaxWorkers(),
                matchConfig.channel(),
                ctx -> lookupFromIndexService,
                matchConfig.type(),
                indexName,
                matchConfig.fieldName(),
                join.addedFields().stream().map(f -> (NamedExpression) f).toList(),
                join.source()
            ),
            layout
        );
    }

    private record MatchConfig(String fieldName, int channel, DataType type) {
        private MatchConfig(FieldAttribute match, Layout.ChannelAndType input) {
            // Note, this handles TEXT fields with KEYWORD subfields
            this(match.exactAttribute().name(), input.channel(), input.type());
        }
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
            if (input == null) {
                throw new IllegalStateException("can't find input for [" + ne + "]");
            }
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
        PhysicalOperation filterOperation = source.with(
            new FilterOperatorFactory(EvalMapper.toEvaluator(context.foldCtx(), filter.condition(), source.layout, shardContexts)),
            source.layout
        );
        if (PlannerUtils.usesScoring(filter)) {
            // Add scorer operator to add the filter expression scores to the overall scores
            int scoreBlock = 0;
            for (Attribute attribute : filter.output()) {
                if (MetadataAttribute.SCORE.equals(attribute.name())) {
                    break;
                }
                scoreBlock++;
            }
            if (scoreBlock == filter.output().size()) {
                throw new IllegalStateException("Couldn't find _score attribute in a WHERE clause");
            }

            filterOperation = filterOperation.with(
                new ScoreOperator.ScoreOperatorFactory(ScoreMapper.toScorer(filter.condition(), shardContexts), scoreBlock),
                filterOperation.layout
            );
        }
        return filterOperation;
    }

    private PhysicalOperation planLimit(LimitExec limit, LocalExecutionPlannerContext context) {
        PhysicalOperation source = plan(limit.child(), context);
        return source.with(new LimitOperator.Factory((Integer) limit.limit().fold(context.foldCtx)), source.layout);
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

    private PhysicalOperation planChangePoint(ChangePointExec changePoint, LocalExecutionPlannerContext context) {
        PhysicalOperation source = plan(changePoint.child(), context);
        Layout layout = source.layout.builder().append(changePoint.targetType()).append(changePoint.targetPvalue()).build();
        return source.with(
            new ChangePointOperator.Factory(
                layout.get(changePoint.value().id()).channel(),
                changePoint.sourceText(),
                changePoint.sourceLocation().getLineNumber(),
                changePoint.sourceLocation().getColumnNumber()
            ),
            layout
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
            ).map(describable -> describable == null ? "null" : describable.describe()).collect(joining("\n\\_", "\\_", ""));
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
        FoldContext foldCtx,
        Settings settings,
        List<EsPhysicalOperationProviders.ShardContext> shardContexts
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
        String taskDescription,
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
                    taskDescription,
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
