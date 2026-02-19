/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.planner;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.compute.Describable;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.LocalCircuitBreaker;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.lucene.IndexedByShardId;
import org.elasticsearch.compute.lucene.query.DataPartitioning;
import org.elasticsearch.compute.lucene.query.LuceneOperator;
import org.elasticsearch.compute.lucene.query.TimeSeriesSourceOperator;
import org.elasticsearch.compute.operator.ChangePointOperator;
import org.elasticsearch.compute.operator.ColumnExtractOperator;
import org.elasticsearch.compute.operator.ColumnLoadOperator;
import org.elasticsearch.compute.operator.Driver;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.compute.operator.EvalOperator.EvalOperatorFactory;
import org.elasticsearch.compute.operator.FilterOperator.FilterOperatorFactory;
import org.elasticsearch.compute.operator.LimitOperator;
import org.elasticsearch.compute.operator.LocalSourceOperator;
import org.elasticsearch.compute.operator.LocalSourceOperator.LocalSourceFactory;
import org.elasticsearch.compute.operator.MMROperator;
import org.elasticsearch.compute.operator.MvExpandOperator;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.compute.operator.Operator.OperatorFactory;
import org.elasticsearch.compute.operator.OutputOperator.OutputOperatorFactory;
import org.elasticsearch.compute.operator.RowInTableLookupOperator;
import org.elasticsearch.compute.operator.SampleOperator;
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
import org.elasticsearch.compute.operator.fuse.LinearConfig;
import org.elasticsearch.compute.operator.fuse.LinearScoreEvalOperator;
import org.elasticsearch.compute.operator.fuse.RrfConfig;
import org.elasticsearch.compute.operator.fuse.RrfScoreEvalOperator;
import org.elasticsearch.compute.operator.topn.DocVectorEncoder;
import org.elasticsearch.compute.operator.topn.TopNEncoder;
import org.elasticsearch.compute.operator.topn.TopNOperator;
import org.elasticsearch.compute.operator.topn.TopNOperator.TopNOperatorFactory;
import org.elasticsearch.core.Assertions;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.node.Node;
import org.elasticsearch.search.vectors.VectorData;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.transport.RemoteClusterAware;
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
import org.elasticsearch.xpack.esql.enrich.MatchConfig;
import org.elasticsearch.xpack.esql.evaluator.EvalMapper;
import org.elasticsearch.xpack.esql.evaluator.command.CompoundOutputEvaluator;
import org.elasticsearch.xpack.esql.evaluator.command.GrokEvaluatorExtracter;
import org.elasticsearch.xpack.esql.expression.Foldables;
import org.elasticsearch.xpack.esql.expression.Order;
import org.elasticsearch.xpack.esql.inference.InferenceService;
import org.elasticsearch.xpack.esql.inference.completion.CompletionOperator;
import org.elasticsearch.xpack.esql.inference.rerank.RerankOperator;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.AggregateExec;
import org.elasticsearch.xpack.esql.plan.physical.ChangePointExec;
import org.elasticsearch.xpack.esql.plan.physical.CompoundOutputEvalExec;
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
import org.elasticsearch.xpack.esql.plan.physical.FragmentExec;
import org.elasticsearch.xpack.esql.plan.physical.FuseScoreEvalExec;
import org.elasticsearch.xpack.esql.plan.physical.GrokExec;
import org.elasticsearch.xpack.esql.plan.physical.HashJoinExec;
import org.elasticsearch.xpack.esql.plan.physical.LimitExec;
import org.elasticsearch.xpack.esql.plan.physical.LocalSourceExec;
import org.elasticsearch.xpack.esql.plan.physical.LookupJoinExec;
import org.elasticsearch.xpack.esql.plan.physical.MMRExec;
import org.elasticsearch.xpack.esql.plan.physical.MvExpandExec;
import org.elasticsearch.xpack.esql.plan.physical.OutputExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.ProjectExec;
import org.elasticsearch.xpack.esql.plan.physical.SampleExec;
import org.elasticsearch.xpack.esql.plan.physical.ShowExec;
import org.elasticsearch.xpack.esql.plan.physical.TimeSeriesAggregateExec;
import org.elasticsearch.xpack.esql.plan.physical.TopNExec;
import org.elasticsearch.xpack.esql.plan.physical.inference.CompletionExec;
import org.elasticsearch.xpack.esql.plan.physical.inference.RerankExec;
import org.elasticsearch.xpack.esql.planner.EsPhysicalOperationProviders.ShardContext;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;
import org.elasticsearch.xpack.esql.score.ScoreMapper;
import org.elasticsearch.xpack.esql.session.Configuration;
import org.elasticsearch.xpack.esql.session.EsqlCCSUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static java.util.Arrays.asList;
import static java.util.stream.Collectors.joining;
import static org.elasticsearch.compute.operator.ProjectOperator.ProjectOperatorFactory;
import static org.elasticsearch.xpack.esql.plan.logical.MMR.getMMRLimitValue;
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
    private final InferenceService inferenceService;
    private final PhysicalOperationProviders physicalOperationProviders;

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
        InferenceService inferenceService,
        PhysicalOperationProviders physicalOperationProviders
    ) {

        this.sessionId = sessionId;
        this.clusterAlias = clusterAlias;
        this.parentTask = parentTask;
        this.bigArrays = bigArrays;
        this.blockFactory = blockFactory;
        this.settings = settings;
        this.configuration = configuration;
        this.exchangeSourceSupplier = exchangeSourceSupplier;
        this.exchangeSinkSupplier = exchangeSinkSupplier;
        this.enrichLookupService = enrichLookupService;
        this.lookupFromIndexService = lookupFromIndexService;
        this.inferenceService = inferenceService;
        this.physicalOperationProviders = physicalOperationProviders;
    }

    /**
     * turn the given plan into a list of drivers to execute
     */
    public LocalExecutionPlan plan(
        String description,
        FoldContext foldCtx,
        PlannerSettings plannerSettings,
        PhysicalPlan localPhysicalPlan,
        IndexedByShardId<? extends ShardContext> shardContexts
    ) {
        final boolean timeSeries = localPhysicalPlan.anyMatch(p -> p instanceof TimeSeriesAggregateExec);
        var context = new LocalExecutionPlannerContext(
            description,
            new ArrayList<>(),
            new Holder<>(DriverParallelism.SINGLE),
            configuration.pragmas(),
            bigArrays,
            blockFactory,
            foldCtx,
            plannerSettings,
            timeSeries,
            shardContexts
        );

        // workaround for https://github.com/elastic/elasticsearch/issues/99782
        localPhysicalPlan = localPhysicalPlan.transformUp(
            AggregateExec.class,
            a -> a.getMode().isOutputPartial() ? a : new ProjectExec(a.source(), a, Expressions.asAttributes(a.aggregates()))
        );
        PhysicalOperation physicalOperation = plan(localPhysicalPlan, context);

        final TimeValue statusInterval = configuration.pragmas().statusInterval();
        context.addDriverFactory(
            new DriverFactory(
                new DriverSupplier(
                    description,
                    ClusterName.CLUSTER_NAME_SETTING.get(settings).value(),
                    Node.NODE_NAME_SETTING.get(settings),
                    context.bigArrays,
                    context.blockFactory,
                    context.shardContexts,
                    physicalOperation,
                    statusInterval,
                    settings
                ),
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
        } else if (node instanceof RerankExec rerank) {
            return planRerank(rerank, context);
        } else if (node instanceof ChangePointExec changePoint) {
            return planChangePoint(changePoint, context);
        } else if (node instanceof CompletionExec completion) {
            return planCompletion(completion, context);
        } else if (node instanceof SampleExec Sample) {
            return planSample(Sample, context);
        } else if (node instanceof CompoundOutputEvalExec coe) {
            return planCompoundOutputEval(coe, context);
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
            return planExchangeSource(exchangeSource, exchangeSourceSupplier);
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
        } else if (node instanceof FuseScoreEvalExec fuse) {
            return planFuseScoreEvalExec(fuse, context);
        } else if (node instanceof MMRExec mmr) {
            return planMMR(mmr, context);
        }

        throw new EsqlIllegalArgumentException("unknown physical plan node [" + node.nodeName() + "]");
    }

    private PhysicalOperation planMMR(MMRExec mmr, LocalExecutionPlannerContext context) {
        PhysicalOperation source = plan(mmr.child(), context);

        assert (mmr.diversifyField() != null) : "diversifyField is required for the MMROperator";

        int limit = getMMRLimitValue(mmr.limit());
        Float lambdaValue = mmr.lambda();
        VectorData queryVector = mmr.queryVector();

        int diversifyFieldChannel = source.layout.get(mmr.diversifyField().id()).channel();
        String diversifyField = mmr.diversifyField().qualifiedName();

        return source.with(new MMROperator.Factory(diversifyField, diversifyFieldChannel, limit, queryVector, lambdaValue), source.layout);
    }

    private PhysicalOperation planCompoundOutputEval(final CompoundOutputEvalExec coe, LocalExecutionPlannerContext context) {
        PhysicalOperation source = plan(coe.child(), context);
        Layout.Builder layoutBuilder = source.layout.builder();
        layoutBuilder.append(coe.outputFieldAttributes());

        ElementType[] types = new ElementType[coe.outputFieldAttributes().size()];
        for (int i = 0; i < coe.outputFieldAttributes().size(); i++) {
            types[i] = PlannerUtils.toElementType(coe.outputFieldAttributes().get(i).dataType());
        }

        Layout layout = layoutBuilder.build();

        source = source.with(
            new ColumnExtractOperator.Factory(
                types,
                EvalMapper.toEvaluator(context.foldCtx(), coe.input(), layout),
                new CompoundOutputEvaluator.Factory(coe.input().dataType(), coe.source(), coe)
            ),
            layout
        );
        return source;
    }

    private PhysicalOperation planCompletion(CompletionExec completion, LocalExecutionPlannerContext context) {
        PhysicalOperation source = plan(completion.child(), context);
        String inferenceId = BytesRefs.toString(completion.inferenceId().fold(context.foldCtx()));
        Map<String, Object> taskSettings = completion.taskSettings().toFoldedMap(context.foldCtx());
        Layout outputLayout = source.layout.builder().append(completion.targetField()).build();
        EvalOperator.ExpressionEvaluator.Factory promptEvaluatorFactory = EvalMapper.toEvaluator(
            context.foldCtx(),
            completion.prompt(),
            source.layout
        );

        return source.with(
            new CompletionOperator.Factory(inferenceService, inferenceId, promptEvaluatorFactory, taskSettings),
            outputLayout
        );
    }

    private PhysicalOperation planFuseScoreEvalExec(FuseScoreEvalExec fuse, LocalExecutionPlannerContext context) {
        PhysicalOperation source = plan(fuse.child(), context);
        Layout layout = source.layout;

        int scorePosition = layout.get(fuse.score().id()).channel();
        int discriminatorPosition = layout.get(fuse.discriminator().id()).channel();

        if (fuse.fuseConfig() instanceof RrfConfig rrfConfig) {
            return source.with(
                new RrfScoreEvalOperator.Factory(discriminatorPosition, scorePosition, rrfConfig, fuse.source()),
                source.layout
            );
        } else if (fuse.fuseConfig() instanceof LinearConfig linearConfig) {
            return source.with(
                new LinearScoreEvalOperator.Factory(discriminatorPosition, scorePosition, linearConfig, fuse.source()),
                source.layout
            );
        }

        throw new EsqlIllegalArgumentException("unknown FUSE score method [" + fuse.fuseConfig() + "]");
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
        // for now only one stat is supported
        EsStatsQueryExec.Stat stat = statsQuery.stat();

        EsPhysicalOperationProviders esProvider = (EsPhysicalOperationProviders) physicalOperationProviders;
        var queryFunction = switch (stat) {
            case EsStatsQueryExec.BasicStat basic -> esProvider.querySupplier(basic.filter(statsQuery.query()));
            case EsStatsQueryExec.ByStat byStat -> esProvider.querySupplier(byStat.queryBuilderAndTags());
        };
        final LuceneOperator.Factory luceneFactory = esProvider.countSource(context, queryFunction, stat.tagTypes(), statsQuery.limit());

        Layout.Builder layout = new Layout.Builder();
        layout.append(statsQuery.outputSet());
        int instanceCount = Math.max(1, luceneFactory.taskConcurrency());
        context.driverParallelism(new DriverParallelism(DriverParallelism.Type.DATA_PARALLELISM, instanceCount));
        return PhysicalOperation.fromSource(luceneFactory, layout.build());
    }

    private PhysicalOperation planFieldExtractNode(FieldExtractExec fieldExtractExec, LocalExecutionPlannerContext context) {
        return physicalOperationProviders.fieldExtractPhysicalOperation(fieldExtractExec, plan(fieldExtractExec.child(), context), context);
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
        if (Assertions.ENABLED) {
            List<Attribute> inputAttributes = exchangeSink.child().output();
            for (Attribute attr : inputAttributes) {
                assert source.layout.get(attr.id()) != null
                    : "input attribute [" + attr + "] does not exist in the source layout [" + source.layout + "]";
            }
        }
        return source.withSink(new ExchangeSinkOperatorFactory(exchangeSinkSupplier), source.layout);
    }

    private PhysicalOperation planExchangeSource(ExchangeSourceExec exchangeSource, Supplier<ExchangeSource> exchangeSourceSupplier) {
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
            var fieldExtractPreference = fieldExtractPreference(topNExec, inverse.get(channel).nameIds());
            elementTypes[channel] = PlannerUtils.toElementType(inverse.get(channel).type(), fieldExtractPreference);
            encoders[channel] = switch (inverse.get(channel).type()) {
                case IP -> TopNEncoder.IP;
                case TEXT, KEYWORD -> TopNEncoder.UTF8;
                case VERSION -> TopNEncoder.VERSION;
                case DOC_DATA_TYPE -> new DocVectorEncoder(context.shardContexts);
                case BOOLEAN, NULL, BYTE, SHORT, INTEGER, LONG, DOUBLE, FLOAT, HALF_FLOAT, DATETIME, DATE_NANOS, DATE_PERIOD, TIME_DURATION,
                    OBJECT, SCALED_FLOAT, UNSIGNED_LONG -> TopNEncoder.DEFAULT_SORTABLE;
                case GEO_POINT, CARTESIAN_POINT, GEO_SHAPE, CARTESIAN_SHAPE, COUNTER_LONG, COUNTER_INTEGER, COUNTER_DOUBLE, SOURCE,
                    AGGREGATE_METRIC_DOUBLE, DENSE_VECTOR, GEOHASH, GEOTILE, GEOHEX, EXPONENTIAL_HISTOGRAM, TDIGEST, HISTOGRAM,
                    TSID_DATA_TYPE, DATE_RANGE -> TopNEncoder.DEFAULT_UNSORTABLE;
                // unsupported fields are encoded as BytesRef, we'll use the same encoder; all values should be null at this point
                case UNSUPPORTED -> TopNEncoder.UNSUPPORTED;
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
            Object val = literal.value() instanceof BytesRef br ? BytesRefs.toString(br) : literal.value();
            limit = stringToInt(val.toString());
        } else {
            throw new EsqlIllegalArgumentException("limit only supported with literal values");
        }
        return source.with(
            new TopNOperatorFactory(
                limit,
                asList(elementTypes),
                asList(encoders),
                orders,
                context.pageSize(topNExec, rowSize),
                topNExec.inputOrdering()
            ),
            source.layout
        );
    }

    private static MappedFieldType.FieldExtractPreference fieldExtractPreference(TopNExec topNExec, Set<NameId> nameIds) {
        MappedFieldType.FieldExtractPreference fieldExtractPreference = MappedFieldType.FieldExtractPreference.NONE;
        // See if any of the NameIds is marked as having been loaded with doc-values preferences, which will affect the ElementType chosen.
        for (NameId nameId : nameIds) {
            for (Attribute withDocValues : topNExec.docValuesAttributes()) {
                if (nameId.equals(withDocValues.id())) {
                    fieldExtractPreference = MappedFieldType.FieldExtractPreference.DOC_VALUES;
                    break;
                }
            }
        }
        return fieldExtractPreference;
    }

    private PhysicalOperation planEval(EvalExec eval, LocalExecutionPlannerContext context) {
        PhysicalOperation source = plan(eval.child(), context);

        for (Alias field : eval.fields()) {
            var evaluatorSupplier = EvalMapper.toEvaluator(context.foldCtx(), field.child(), source.layout, context.shardContexts);
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
        final Map<String, Integer> fieldToPos = Maps.newHashMapWithExpectedSize(extractedFields.size());
        final Map<String, ElementType> fieldToType = Maps.newHashMapWithExpectedSize(extractedFields.size());
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
                new GrokEvaluatorExtracter.Factory(grok.pattern().grok(), grok.pattern().pattern(), fieldToPos, fieldToType)
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

    private PhysicalOperation planRerank(RerankExec rerank, LocalExecutionPlannerContext context) {
        PhysicalOperation source = plan(rerank.child(), context);

        List<EvalOperator.ExpressionEvaluator.Factory> rerankFieldsEvaluators = rerank.rerankFields()
            .stream()
            .map(rerankField -> EvalMapper.toEvaluator(context.foldCtx(), rerankField.child(), source.layout))
            .toList();

        assert rerankFieldsEvaluators.size() > 0 : "rerank expression evaluators must not be empty";

        String inferenceId = BytesRefs.toString(rerank.inferenceId().fold(context.foldCtx));
        String queryText = BytesRefs.toString(rerank.queryText().fold(context.foldCtx));

        Layout outputLayout = source.layout;
        if (source.layout.get(rerank.scoreAttribute().id()) == null) {
            outputLayout = source.layout.builder().append(rerank.scoreAttribute()).build();
        }

        int scoreChannel = outputLayout.get(rerank.scoreAttribute().id()).channel();

        return source.with(
            new RerankOperator.Factory(
                inferenceService,
                inferenceId,
                queryText,
                rerankFieldsEvaluators,
                scoreChannel,
                RerankOperator.DEFAULT_BATCH_SIZE
            ),
            outputLayout
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
        Page localData = localSourceExec.supplier().get();

        RowInTableLookupOperator.Key[] keys = new RowInTableLookupOperator.Key[join.leftFields().size()];
        int[] blockMapping = new int[join.leftFields().size()];
        for (int k = 0; k < join.leftFields().size(); k++) {
            Attribute left = join.leftFields().get(k);
            Attribute right = join.rightFields().get(k);
            Block localField = null;
            List<Attribute> output = join.joinData().output();
            for (int l = 0; l < output.size(); l++) {
                if (output.get(l).name().equals(right.name())) {
                    localField = localData.getBlock(l);
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
        var joinDataOutput = join.joinData().output();
        for (Attribute f : join.addedFields()) {
            Block localField = null;
            for (int l = 0; l < joinDataOutput.size(); l++) {
                if (joinDataOutput.get(l).name().equals(f.name())) {
                    localField = localData.getBlock(l);
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

        EsRelation esRelation = findEsRelation(join.lookup());
        if (esRelation == null || esRelation.indexMode() != IndexMode.LOOKUP) {
            throw new IllegalArgumentException("can't plan [" + join + "]");
        }

        // After enabling remote joins, we can have one of the two situations here:
        // 1. We've just got one entry - this should be the one relevant to the join, and it should be for this cluster
        // 2. We have got multiple entries - this means each cluster has its own one, and we should extract one relevant for this cluster
        Map.Entry<String, IndexMode> entry;
        if (esRelation.indexNameWithModes().size() == 1) {
            entry = esRelation.indexNameWithModes().entrySet().iterator().next();
        } else {
            var maybeEntry = esRelation.indexNameWithModes()
                .entrySet()
                .stream()
                .filter(e -> RemoteClusterAware.parseClusterAlias(e.getKey()).equals(clusterAlias))
                .findFirst();
            entry = maybeEntry.orElseThrow(
                () -> new IllegalStateException(
                    "can't plan [" + join + "]: no matching index found " + EsqlCCSUtils.inClusterName(clusterAlias)
                )
            );
        }

        if (entry.getValue() != IndexMode.LOOKUP) {
            throw new IllegalStateException("can't plan [" + join + "], found index with mode [" + entry.getValue() + "]");
        }
        String[] indexSplit = RemoteClusterAware.splitIndexName(entry.getKey());
        // No prefix is ok, prefix with this cluster is ok, something else is not
        if (indexSplit[0] != null && clusterAlias.equals(indexSplit[0]) == false) {
            throw new IllegalStateException(
                "can't plan [" + join + "]: no matching index found " + EsqlCCSUtils.inClusterName(clusterAlias)
            );
        }
        String indexName = indexSplit[1];
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

            // TODO: Using exactAttribute was supposed to handle TEXT fields with KEYWORD subfields - but we don't allow these in lookup
            // indices, so the call to exactAttribute looks redundant now.
            String fieldName = right.exactAttribute().fieldName().string();

            // we support 2 types of joins: Field name joins and Expression joins
            // for Field name join, we do not ship any join on expression.
            // we built the Lucene query on the field name that is passed in the MatchConfig.fieldName
            // so for Field name we need to pass the attribute name from the right side, because that is needed to build the query
            // For expression joins, we pass an expression such as left_id > right_id.
            // So in this case we pass in left_id as the field name, because that is what we are shipping to the lookup node
            // The lookup node will replace that name, with the actual values for each row and perform the lookup join
            // We need to pass the left name, because we need to know what data we have shipped.
            // It is not acceptable to just use the left or right side of the operator because the same field can be joined multiple times
            // e.g. LOOKUP JOIN ON left_id < right_id_1 and left_id >= right_id_2
            // we want to be able to optimize this in the future and only ship the left_id once
            if (join.isOnJoinExpression()) {
                fieldName = left.name();
            }
            matchFields.add(new MatchConfig(fieldName, input));
        }
        return source.with(
            new LookupFromIndexOperator.Factory(
                matchFields,
                sessionId,
                parentTask,
                context.queryPragmas().enrichMaxWorkers(),
                ctx -> lookupFromIndexService,
                esRelation.indexPattern(),
                indexName,
                join.addedFields().stream().map(f -> (NamedExpression) f).toList(),
                join.source(),
                join.right(),
                join.joinOnConditions()
            ),
            layout
        );
    }

    private static EsRelation findEsRelation(PhysicalPlan node) {
        if (node instanceof FragmentExec fragmentExec) {
            List<LogicalPlan> esRelations = fragmentExec.fragment().collectFirstChildren(x -> x instanceof EsRelation);
            if (esRelations.size() == 1) {
                return (EsRelation) esRelations.get(0);
            }
        }
        return null;
    }

    private PhysicalOperation planLocal(LocalSourceExec localSourceExec, LocalExecutionPlannerContext context) {
        Layout.Builder layout = new Layout.Builder();
        layout.append(localSourceExec.output());
        LocalSourceOperator.PageSupplier supplier = () -> localSourceExec.supplier().get();
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
        return planProject(project, source);
    }

    public static PhysicalOperation planProject(ProjectExec project, PhysicalOperation source) {
        List<? extends NamedExpression> projections = project.projections();
        List<Integer> projectionList = new ArrayList<>(projections.size());

        Layout.Builder layout = new Layout.Builder();
        for (NamedExpression ne : projections) {
            NameId inputId = ne instanceof Alias a ? ((NamedExpression) a.child()).id() : ne.id();
            Layout.ChannelAndType input = source.layout.get(inputId);
            if (input == null) {
                throw new IllegalStateException("can't find input for [" + ne + "]");
            }
            layout.append(ne);
            projectionList.add(input.channel());
        }

        return source.with(new ProjectOperatorFactory(projectionList), layout.build());
    }

    private PhysicalOperation planFilter(FilterExec filter, LocalExecutionPlannerContext context) {
        PhysicalOperation source = plan(filter.child(), context);
        // TODO: should this be extracted into a separate eval block?
        PhysicalOperation filterOperation = source.with(
            new FilterOperatorFactory(EvalMapper.toEvaluator(context.foldCtx(), filter.condition(), source.layout, context.shardContexts)),
            source.layout
        );
        // Add ScoreOperator only on data nodes. Data nodes are able to calculate scores running queries on the resulting docs.
        if (context.shardContexts.isEmpty() == false && PlannerUtils.usesScoring(filter)) {
            // Add scorer operator to add the filter expression scores to the overall scores
            Attribute scoreAttribute = null;

            for (Attribute attribute : filter.output()) {
                if (attribute instanceof MetadataAttribute && MetadataAttribute.SCORE.equals(attribute.name())) {
                    scoreAttribute = attribute;
                }
            }
            assert scoreAttribute != null : "Couldn't find _score attribute in a WHERE clause";

            int scoreBlock = filterOperation.layout.get(scoreAttribute.id()).channel();
            filterOperation = filterOperation.with(
                new ScoreOperator.ScoreOperatorFactory(ScoreMapper.toScorer(filter.condition(), context.shardContexts), scoreBlock),
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
        return source.with(new ChangePointOperator.Factory(layout.get(changePoint.value().id()).channel(), changePoint.source()), layout);
    }

    private PhysicalOperation planSample(SampleExec rsx, LocalExecutionPlannerContext context) {
        PhysicalOperation source = plan(rsx.child(), context);
        var probability = (double) Foldables.valueOf(context.foldCtx(), rsx.probability());
        return source.with(new SampleOperator.Factory(probability), source.layout);
    }

    /**
     * Immutable physical operation.
     */
    public static class PhysicalOperation {
        final SourceOperatorFactory sourceOperatorFactory;
        final List<OperatorFactory> intermediateOperatorFactories;
        final SinkOperatorFactory sinkOperatorFactory;

        final Layout layout; // maps field names to channels

        /**
         * Creates a new physical operation with the given source and layout.
         */
        public static PhysicalOperation fromSource(SourceOperatorFactory sourceOperatorFactory, Layout layout) {
            return new PhysicalOperation(sourceOperatorFactory, layout);
        }

        /**
         * Creates a new physical operation from this operation with the given layout.
         */
        public PhysicalOperation with(Layout layout) {
            return new PhysicalOperation(this, Optional.empty(), Optional.empty(), layout);
        }

        /**
         * Creates a new physical operation from this operation with the given intermediate operator and layout.
         */
        public PhysicalOperation with(OperatorFactory operatorFactory, Layout layout) {
            return new PhysicalOperation(this, Optional.of(operatorFactory), Optional.empty(), layout);
        }

        /**
         * Creates a new physical operation from this operation with the given sink and layout.
         */
        public PhysicalOperation withSink(SinkOperatorFactory sink, Layout layout) {
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

        public Layout layout() {
            return layout;
        }

        public Supplier<String> longDescription() {
            return new LongDescription(sourceOperatorFactory, intermediateOperatorFactories, sinkOperatorFactory);
        }

        @Override
        public String toString() {
            return longDescription().get();
        }
    }

    /**
     * Closure that builds the description. This is a subset of {@link PhysicalOperation}
     * that we pass to {@link Driver} that does not contain the quite large
     * {@link PhysicalOperation#layout} member.
     */
    private record LongDescription(
        SourceOperatorFactory sourceOperatorFactory,
        List<OperatorFactory> intermediateOperatorFactories,
        SinkOperatorFactory sinkOperatorFactory
    ) implements Supplier<String> {
        @Override
        public String get() {
            return Stream.concat(
                Stream.concat(Stream.of(sourceOperatorFactory), intermediateOperatorFactories.stream()),
                Stream.of(sinkOperatorFactory)
            ).map(describable -> describable == null ? "null" : describable.describe()).collect(joining("\n\\_", "\\_", ""));
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
        String description,
        List<DriverFactory> driverFactories,
        Holder<DriverParallelism> driverParallelism,
        QueryPragmas queryPragmas,
        BigArrays bigArrays,
        BlockFactory blockFactory,
        FoldContext foldCtx,
        PlannerSettings plannerSettings,
        boolean timeSeries,
        IndexedByShardId<? extends ShardContext> shardContexts
    ) {
        void addDriverFactory(DriverFactory driverFactory) {
            driverFactories.add(driverFactory);
        }

        void driverParallelism(DriverParallelism parallelism) {
            driverParallelism.set(parallelism);
        }

        DataPartitioning.AutoStrategy autoPartitioningStrategy() {
            return timeSeries ? DataPartitioning.AutoStrategy.DEFAULT_TIME_SERIES : DataPartitioning.AutoStrategy.DEFAULT;
        }

        int pageSize(PhysicalPlan node, Integer estimatedRowSize) {
            if (estimatedRowSize == null) {
                throw new IllegalStateException("estimated row size hasn't been set");
            }
            if (estimatedRowSize == 0) {
                throw new IllegalStateException("estimated row size can't be 0");
            }
            if (queryPragmas.pageSize() != 0) {
                return queryPragmas.pageSize();
            }
            if (timeSeries && node instanceof EsQueryExec) {
                return TimeSeriesSourceOperator.pageSize(estimatedRowSize, plannerSettings.valuesLoadingJumboSize().getBytes());
            } else {
                return Math.max(SourceOperator.MIN_TARGET_PAGE_SIZE, SourceOperator.TARGET_PAGE_SIZE / estimatedRowSize);
            }
        }
    }

    record DriverSupplier(
        String description,
        String clusterName,
        String nodeName,
        BigArrays bigArrays,
        BlockFactory blockFactory,
        IndexedByShardId<? extends ShardContext> shardContexts,
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
            var driverContext = new DriverContext(bigArrays, blockFactory.newChildFactory(localBreaker), localBreakerSettings, description);
            try {
                source = physicalOperation.source(driverContext);
                physicalOperation.operators(operators, driverContext);
                sink = physicalOperation.sink(driverContext);
                success = true;
                return new Driver(
                    sessionId,
                    description,
                    clusterName,
                    nodeName,
                    System.currentTimeMillis(),
                    System.nanoTime(),
                    driverContext,
                    physicalOperation.longDescription(),
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
            return physicalOperation.toString();
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
