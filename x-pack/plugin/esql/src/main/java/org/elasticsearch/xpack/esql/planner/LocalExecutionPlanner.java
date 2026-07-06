/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.planner;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.routing.ShardIterator;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.compute.Describable;
import org.elasticsearch.compute.aggregation.blockhash.BlockHash;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.LocalCircuitBreaker;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.compute.lucene.IndexedByShardId;
import org.elasticsearch.compute.lucene.query.DataPartitioning;
import org.elasticsearch.compute.lucene.query.LuceneOperator;
import org.elasticsearch.compute.lucene.query.TimeSeriesSourceOperator;
import org.elasticsearch.compute.operator.ChangePointOperator;
import org.elasticsearch.compute.operator.ColumnExtractOperator;
import org.elasticsearch.compute.operator.ColumnLoadOperator;
import org.elasticsearch.compute.operator.DistinctByOperator;
import org.elasticsearch.compute.operator.Driver;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.EvalOperator.EvalOperatorFactory;
import org.elasticsearch.compute.operator.FilterOperator.FilterOperatorFactory;
import org.elasticsearch.compute.operator.GroupedLimitOperator;
import org.elasticsearch.compute.operator.HighlightConfig;
import org.elasticsearch.compute.operator.HighlightOperator;
import org.elasticsearch.compute.operator.LimitOperator;
import org.elasticsearch.compute.operator.LocalSourceOperator;
import org.elasticsearch.compute.operator.LocalSourceOperator.LocalSourceFactory;
import org.elasticsearch.compute.operator.MMROperator;
import org.elasticsearch.compute.operator.MetricFieldInfo;
import org.elasticsearch.compute.operator.MetricsInfoOperator;
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
import org.elasticsearch.compute.operator.SparklineGenerateEmptyBucketsOperator;
import org.elasticsearch.compute.operator.StringExtractOperator;
import org.elasticsearch.compute.operator.TimeSeriesCollapseOperator;
import org.elasticsearch.compute.operator.TsInfoOperator;
import org.elasticsearch.compute.operator.exchange.ExchangeSink;
import org.elasticsearch.compute.operator.exchange.ExchangeSinkOperator.ExchangeSinkOperatorFactory;
import org.elasticsearch.compute.operator.exchange.ExchangeSource;
import org.elasticsearch.compute.operator.exchange.ExchangeSourceOperator.ExchangeSourceOperatorFactory;
import org.elasticsearch.compute.operator.fuse.LinearConfig;
import org.elasticsearch.compute.operator.fuse.LinearScoreEvalOperator;
import org.elasticsearch.compute.operator.fuse.RrfConfig;
import org.elasticsearch.compute.operator.fuse.RrfScoreEvalOperator;
import org.elasticsearch.compute.operator.topn.GroupedTopNOperator;
import org.elasticsearch.compute.operator.topn.NumericTopNOperator;
import org.elasticsearch.compute.operator.topn.SharedMinCompetitive;
import org.elasticsearch.compute.operator.topn.SharedNumericThreshold;
import org.elasticsearch.compute.operator.topn.TopNEncoder;
import org.elasticsearch.compute.operator.topn.TopNOperator;
import org.elasticsearch.compute.operator.topn.TopNOperator.TopNOperatorFactory;
import org.elasticsearch.core.Assertions;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.grok.MatcherWatchdog;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.analysis.AnalysisRegistry;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MappingLookup;
import org.elasticsearch.index.mapper.SourceFieldMapper;
import org.elasticsearch.index.mapper.TimeSeriesParams;
import org.elasticsearch.index.mapper.blockloader.BlockLoaderFunctionConfig;
import org.elasticsearch.iplocation.api.IpDataLookup;
import org.elasticsearch.iplocation.api.IpLocationConsumer;
import org.elasticsearch.iplocation.api.IpLocationService;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.node.Node;
import org.elasticsearch.search.vectors.VectorData;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.transport.RemoteClusterAware;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.useragent.api.UserAgentParser;
import org.elasticsearch.useragent.api.UserAgentParserRegistry;
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
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.core.type.FunctionEsField;
import org.elasticsearch.xpack.esql.core.util.Holder;
import org.elasticsearch.xpack.esql.datasources.AsyncExternalSourceOperatorFactory;
import org.elasticsearch.xpack.esql.datasources.DeferredExtractionCapable;
import org.elasticsearch.xpack.esql.datasources.ExternalFieldExtractOperator;
import org.elasticsearch.xpack.esql.datasources.ExternalSliceQueue;
import org.elasticsearch.xpack.esql.datasources.FileMetadataColumns;
import org.elasticsearch.xpack.esql.datasources.OperatorFactoryRegistry;
import org.elasticsearch.xpack.esql.datasources.PartitionMetadata;
import org.elasticsearch.xpack.esql.datasources.spi.ColumnExtractor;
import org.elasticsearch.xpack.esql.datasources.spi.FileList;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReader;
import org.elasticsearch.xpack.esql.datasources.spi.SourceOperatorContext;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.enrich.EnrichLookupOperator;
import org.elasticsearch.xpack.esql.enrich.EnrichLookupService;
import org.elasticsearch.xpack.esql.enrich.LookupFromIndexOperator;
import org.elasticsearch.xpack.esql.enrich.LookupFromIndexService;
import org.elasticsearch.xpack.esql.enrich.MatchConfig;
import org.elasticsearch.xpack.esql.evaluator.EvalMapper;
import org.elasticsearch.xpack.esql.evaluator.command.CompoundOutputEvaluator;
import org.elasticsearch.xpack.esql.evaluator.command.GrokEvaluatorExtracter;
import org.elasticsearch.xpack.esql.evaluator.command.IpLocationFunctionBridge;
import org.elasticsearch.xpack.esql.evaluator.command.UserAgentFunctionBridge;
import org.elasticsearch.xpack.esql.expression.Foldables;
import org.elasticsearch.xpack.esql.expression.Order;
import org.elasticsearch.xpack.esql.inference.InferenceService;
import org.elasticsearch.xpack.esql.inference.completion.CompletionOperator;
import org.elasticsearch.xpack.esql.inference.rerank.RerankOperator;
import org.elasticsearch.xpack.esql.optimizer.rules.physical.ProjectAwayColumns;
import org.elasticsearch.xpack.esql.optimizer.rules.physical.local.ExternalSourceAggregatePushdown;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.Grok;
import org.elasticsearch.xpack.esql.plan.logical.HighlightOptions;
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
import org.elasticsearch.xpack.esql.plan.physical.ExternalFieldExtractExec;
import org.elasticsearch.xpack.esql.plan.physical.ExternalSourceExec;
import org.elasticsearch.xpack.esql.plan.physical.FieldExtractExec;
import org.elasticsearch.xpack.esql.plan.physical.FilterExec;
import org.elasticsearch.xpack.esql.plan.physical.FragmentExec;
import org.elasticsearch.xpack.esql.plan.physical.FuseScoreEvalExec;
import org.elasticsearch.xpack.esql.plan.physical.GrokExec;
import org.elasticsearch.xpack.esql.plan.physical.HashJoinExec;
import org.elasticsearch.xpack.esql.plan.physical.HighlightExec;
import org.elasticsearch.xpack.esql.plan.physical.IpLocationExec;
import org.elasticsearch.xpack.esql.plan.physical.LimitByExec;
import org.elasticsearch.xpack.esql.plan.physical.LimitExec;
import org.elasticsearch.xpack.esql.plan.physical.LocalSourceExec;
import org.elasticsearch.xpack.esql.plan.physical.LookupJoinExec;
import org.elasticsearch.xpack.esql.plan.physical.MMRExec;
import org.elasticsearch.xpack.esql.plan.physical.MetricsInfoExec;
import org.elasticsearch.xpack.esql.plan.physical.MvExpandExec;
import org.elasticsearch.xpack.esql.plan.physical.OutputExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.ProjectExec;
import org.elasticsearch.xpack.esql.plan.physical.RegisteredDomainExec;
import org.elasticsearch.xpack.esql.plan.physical.SampleExec;
import org.elasticsearch.xpack.esql.plan.physical.ShowExec;
import org.elasticsearch.xpack.esql.plan.physical.SparklineGenerateEmptyBucketsExec;
import org.elasticsearch.xpack.esql.plan.physical.TimeSeriesAggregateExec;
import org.elasticsearch.xpack.esql.plan.physical.TimeSeriesCollapseExec;
import org.elasticsearch.xpack.esql.plan.physical.TopNByExec;
import org.elasticsearch.xpack.esql.plan.physical.TopNExec;
import org.elasticsearch.xpack.esql.plan.physical.TsInfoExec;
import org.elasticsearch.xpack.esql.plan.physical.UnaryExec;
import org.elasticsearch.xpack.esql.plan.physical.UriPartsExec;
import org.elasticsearch.xpack.esql.plan.physical.UserAgentExec;
import org.elasticsearch.xpack.esql.plan.physical.inference.CompletionExec;
import org.elasticsearch.xpack.esql.plan.physical.inference.RerankExec;
import org.elasticsearch.xpack.esql.planner.EsPhysicalOperationProviders.ShardContext;
import org.elasticsearch.xpack.esql.plugin.EsqlPlugin;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;
import org.elasticsearch.xpack.esql.score.ScoreMapper;
import org.elasticsearch.xpack.esql.session.Configuration;
import org.elasticsearch.xpack.esql.session.EsqlCCSUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Executor;
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

    /**
     * Default rows per page for external file sources when {@link ExternalSourceExec#estimatedRowSize()} is unknown
     * or non-positive. Used by {@link #planExternalSource} as the batch size passed to format readers (including NDJSON).
     */
    public static final int DEFAULT_EXTERNAL_SOURCE_PAGE_SIZE_ROWS = 1000;

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
    private final UserAgentParserRegistry userAgentParserRegistry;
    private final IpLocationService ipLocationService;
    private final ProjectResolver projectResolver;
    private final AbstractPhysicalOperationProviders physicalOperationProviders;
    private final OperatorFactoryRegistry operatorFactoryRegistry;
    @Nullable
    private final Executor parallelWorkerExecutor;
    private final int esqlWorkerPoolSize;
    private final MatcherWatchdog grokMatcherWatchdog;

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
        UserAgentParserRegistry userAgentParserRegistry,
        IpLocationService ipLocationService,
        ProjectResolver projectResolver,
        AbstractPhysicalOperationProviders physicalOperationProviders,
        OperatorFactoryRegistry operatorFactoryRegistry,
        @Nullable Executor parallelWorkerExecutor,
        int esqlWorkerPoolSize,
        MatcherWatchdog grokMatcherWatchdog
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
        this.userAgentParserRegistry = userAgentParserRegistry;
        this.ipLocationService = ipLocationService;
        this.projectResolver = projectResolver;
        this.physicalOperationProviders = physicalOperationProviders;
        this.operatorFactoryRegistry = operatorFactoryRegistry;
        this.parallelWorkerExecutor = parallelWorkerExecutor;
        this.esqlWorkerPoolSize = esqlWorkerPoolSize;
        // Resolved once by the caller from the live ClusterSettings (the setting is dynamic), then shared
        // by every GROK matcher this planner builds — MatcherWatchdog.Default is a stateless, immutable
        // wrapper around a single timeout value.
        this.grokMatcherWatchdog = grokMatcherWatchdog;
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
            settings,
            shardContexts,
            physicalOperationProviders.analysisRegistry()
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
        } else if (node instanceof ExternalFieldExtractExec extExtract) {
            return planExternalFieldExtract(extExtract, context);
        } else if (node instanceof ExchangeExec exchangeExec) {
            return planExchange(exchangeExec, context);
        } else if (node instanceof TopNExec topNExec) {
            return planTopN(topNExec, context);
        } else if (node instanceof TopNByExec topNByExec) {
            return planTopNBy(topNByExec, context);
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
        } else if (node instanceof LimitByExec limitBy) {
            return planLimitBy(limitBy, context);
        } else if (node instanceof LimitExec limit) {
            return planLimit(limit, context);
        } else if (node instanceof MvExpandExec mvExpand) {
            return planMvExpand(mvExpand, context);
        } else if (node instanceof TimeSeriesCollapseExec tsCollapse) {
            return planTimeSeriesCollapse(tsCollapse, context);
        } else if (node instanceof HighlightExec highlight) {
            return planHighlight(highlight, context);
        } else if (node instanceof RerankExec rerank) {
            return planRerank(rerank, context);
        } else if (node instanceof ChangePointExec changePoint) {
            return planChangePoint(changePoint, context);
        } else if (node instanceof CompletionExec completion) {
            return planCompletion(completion, context);
        } else if (node instanceof SampleExec Sample) {
            return planSample(Sample, context);
        } else if (node instanceof IpLocationExec ipLoc) {
            return planIpLocation(ipLoc, context);
        } else if (node instanceof UserAgentExec userAgent) {
            return planUserAgent(userAgent, context);
        } else if (node instanceof UriPartsExec uriParts) {
            return planUriParts(uriParts, context);
        } else if (node instanceof RegisteredDomainExec rd) {
            return planRegisteredDomain(rd, context);
        } else if (node instanceof MetricsInfoExec metricsInfo) {
            return planMetricsInfo(metricsInfo, context);
        } else if (node instanceof TsInfoExec tsInfo) {
            return planTsInfo(tsInfo, context);
        } else if (node instanceof SparklineGenerateEmptyBucketsExec sparkline) {
            return planSparklineGenerateEmptyBuckets(sparkline, context);
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
        } else if (node instanceof ExternalSourceExec externalSource) {
            return planExternalSource(externalSource, context);
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

        int limit = mmr.limitValue();
        float lambdaValue = mmr.lambda();
        VectorData queryVector = mmr.queryVector();

        int diversifyFieldChannel = source.layout.get(mmr.diversifyField().id()).channel();
        String diversifyField = mmr.diversifyField().qualifiedName();

        return source.with(new MMROperator.Factory(diversifyField, diversifyFieldChannel, limit, queryVector, lambdaValue), source.layout);
    }

    private PhysicalOperation planIpLocation(IpLocationExec exec, LocalExecutionPlannerContext context) {
        String projectId = projectResolver.getProjectId().id();
        ipLocationService.requestDownloads(projectId, IpLocationConsumer.ESQL);

        IpDataLookup lookup = ipLocationService.createIpDataLookup(projectId, exec.databaseFile(), exec.outputFieldNames());
        CompoundOutputEvaluator.OutputFieldsCollectorProvider provider = new CompoundOutputEvaluator.OutputFieldsCollectorProvider() {
            @Override
            public CompoundOutputEvaluator.OutputFieldsCollector createOutputFieldsCollector() {
                return new IpLocationFunctionBridge.IpLocationCollectorImpl(exec.outputFieldNames(), lookup, exec.databaseFile());
            }

            @Override
            public String collectorSimpleName() {
                return IpLocationFunctionBridge.IpLocationCollectorImpl.class.getSimpleName();
            }
        };
        CompoundOutputEvaluator.MultiValueStrategy strategy = exec.firstOnly()
            ? CompoundOutputEvaluator.MultiValueStrategy.TAKE_FIRST
            : CompoundOutputEvaluator.MultiValueStrategy.REJECT;
        return planCompoundOutputEval(exec, provider, strategy, context);
    }

    private PhysicalOperation planUserAgent(UserAgentExec exec, LocalExecutionPlannerContext context) {
        UserAgentParser parser = userAgentParserRegistry.getParser(exec.regexFile());
        if (parser == null) {
            throw new EsqlIllegalArgumentException("Unknown user-agent regex file [" + exec.regexFile() + "]");
        }
        CompoundOutputEvaluator.OutputFieldsCollectorProvider provider = new CompoundOutputEvaluator.OutputFieldsCollectorProvider() {
            @Override
            public CompoundOutputEvaluator.OutputFieldsCollector createOutputFieldsCollector() {
                return new UserAgentFunctionBridge.UserAgentCollectorImpl(exec.outputFieldNames(), parser, exec.extractDeviceType());
            }

            @Override
            public String collectorSimpleName() {
                return UserAgentFunctionBridge.UserAgentCollectorImpl.class.getSimpleName();
            }
        };
        return planCompoundOutputEval(exec, provider, CompoundOutputEvaluator.MultiValueStrategy.REJECT, context);
    }

    private PhysicalOperation planUriParts(UriPartsExec uriParts, LocalExecutionPlannerContext context) {
        return planCompoundOutputEval(uriParts, uriParts, CompoundOutputEvaluator.MultiValueStrategy.REJECT, context);
    }

    private PhysicalOperation planRegisteredDomain(RegisteredDomainExec rd, LocalExecutionPlannerContext context) {
        return planCompoundOutputEval(rd, rd, CompoundOutputEvaluator.MultiValueStrategy.REJECT, context);
    }

    private PhysicalOperation planCompoundOutputEval(
        final CompoundOutputEvalExec coe,
        CompoundOutputEvaluator.OutputFieldsCollectorProvider provider,
        CompoundOutputEvaluator.MultiValueStrategy multiValueStrategy,
        LocalExecutionPlannerContext context
    ) {
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
                EvalMapper.toEvaluator(context.foldCtx(), coe.input(), layout, context.analysisRegistry()),
                new CompoundOutputEvaluator.Factory(coe.input().dataType(), coe.source(), provider, multiValueStrategy)
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
        ExpressionEvaluator.Factory promptEvaluatorFactory = EvalMapper.toEvaluator(
            context.foldCtx(),
            completion.prompt(),
            source.layout,
            context.analysisRegistry()
        );

        return source.with(
            new CompletionOperator.Factory(inferenceService, inferenceId, promptEvaluatorFactory, taskSettings, completion.timeout()),
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

    /**
     * Plan an {@link ExternalFieldExtractExec} (inserted by {@code InsertExternalFieldExtraction})
     * by appending an {@link ExternalFieldExtractOperator} above the upstream operator chain.
     * <p>
     * The upstream source factory must implement {@link DeferredExtractionCapable} so the new
     * operator can resolve the per-driver {@link org.elasticsearch.xpack.esql.datasources.SourceExtractors
     * SourceExtractors} registry; otherwise we fail loudly because the optimizer rule should never
     * have inserted us above an incapable source. The pairing is invariant because
     * {@code FileSourceFactory} flips the deferred-extraction switch on the underlying
     * {@code AsyncExternalSourceOperatorFactory} based on the very same
     * {@link org.elasticsearch.xpack.esql.datasources.spi.ColumnExtractor#ROW_POSITION_COLUMN
     * _rowPosition} column the rule injected.
     * <p>
     * Layout: the upstream layout is rewritten to drop the {@code _rowPosition} channel and
     * append channels for every {@code attributesToExtract}. The
     * {@link ExternalFieldExtractOperator} mirrors that channel reshaping at runtime.
     */
    private PhysicalOperation planExternalFieldExtract(ExternalFieldExtractExec exec, LocalExecutionPlannerContext context) {
        PhysicalOperation source = plan(exec.child(), context);

        if ((source.sourceOperatorFactory instanceof DeferredExtractionCapable) == false) {
            throw new IllegalStateException(
                "ExternalFieldExtractExec planned above source factory ["
                    + source.sourceOperatorFactory.getClass().getName()
                    + "] which does not implement DeferredExtractionCapable; "
                    + "InsertExternalFieldExtraction must only fire above ColumnExtractorAware sources"
            );
        }
        DeferredExtractionCapable capable = (DeferredExtractionCapable) source.sourceOperatorFactory;

        Attribute rowPosition = exec.rowPositionAttribute();
        Layout.ChannelAndType rpEntry = source.layout.get(rowPosition.id());
        if (rpEntry == null) {
            throw new IllegalStateException(
                "_rowPosition attribute ["
                    + rowPosition
                    + "] is not present in upstream layout; "
                    + "InsertExternalFieldExtraction must include it in the narrowed source projection"
            );
        }
        int rowPositionChannel = rpEntry.channel();

        // Pass-through channels: every channel from the upstream layout except the row-position
        // channel, in increasing channel order. The output layout below is built from the
        // upstream layout's inverse list using the same skip-rule, then has the deferred
        // attributes appended; channel indices line up with the operator's output assembly.
        List<Layout.ChannelSet> inverse = source.layout.inverse();
        int upstreamChannels = source.layout.numberOfChannels();
        List<Integer> passThroughChannels = new ArrayList<>(upstreamChannels - 1);
        Layout.Builder layoutBuilder = new Layout.Builder();
        for (int ch = 0; ch < upstreamChannels; ch++) {
            if (ch == rowPositionChannel) {
                continue;
            }
            passThroughChannels.add(ch);
            layoutBuilder.append(inverse.get(ch));
        }
        layoutBuilder.append(exec.attributesToExtract());
        Layout newLayout = layoutBuilder.build();

        List<String> deferredColumnNames = new ArrayList<>(exec.attributesToExtract().size());
        for (Attribute a : exec.attributesToExtract()) {
            deferredColumnNames.add(a.name());
        }

        ExternalFieldExtractOperator.Factory factory = new ExternalFieldExtractOperator.Factory(
            rowPositionChannel,
            passThroughChannels,
            deferredColumnNames,
            capable::sourceExtractorsFor
        );
        return source.with(factory, newLayout);
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
        PhysicalOperation source = plan(topNExec.child(), context);
        // Specialisation: a single-key sort over an ExternalSourceExec narrowed by
        // InsertExternalFieldExtraction to {@code [sortKey, _rowPosition]} can run on the
        // primitive {@link NumericTopNOperator} instead of the generic byte-encoding one. We
        // make the decision here — rather than as a separate plan node + optimizer rule — because
        // the choice is purely an implementation detail (same TopN semantics, different operator)
        // and every input we need is already on hand at translation time. If the predicate
        // doesn't match we fall through to the generic factory below; the rule predicate and the
        // generic fallback share the same plan node.
        NumericTopNOperator.NumericTopNOperatorFactory numericFactory = tryBuildNumericTopN(topNExec, source, context);
        if (numericFactory != null) {
            return source.with(numericFactory, source.layout);
        }
        var common = topNCommon(rowSize, topNExec.order(), topNExec.limit(), topNExec.docValuesAttributes(), source, context);
        TopNOperator.ParallelWorkerConfig parallelWorkerConfig = null;
        if (parallelWorkerExecutor != null && TopNOperator.PARALLEL_TOPN_FEATURE_FLAG.isEnabled()) {
            int workerCount = Math.max(1, Math.min(context.plannerSettings.parallelTopNMaxWorkers(), esqlWorkerPoolSize / 2));
            parallelWorkerConfig = new TopNOperator.ParallelWorkerConfig(
                parallelWorkerExecutor,
                workerCount,
                2 * workerCount,
                context.plannerSettings.parallelTopNPromotionThresholdRows()
            );
        }
        // For a single keyword/text sort key directly over an external source, publish the generic
        // TopNOperator's competitive BytesRef bound to DynamicThresholdAware format readers so they
        // can skip row groups/stripes that cannot contain a globally competitive row. This is the
        // BYTES_REF counterpart to the numeric NumericTopNOperator + SharedNumericThreshold path.
        // Wiring the readers and obtaining the supplier are done together so a pre-set supplier on
        // the TopNExec can never reach the operator without the readers also being wired to it.
        SharedMinCompetitive.Supplier minCompetitive = tryBuildExternalMinCompetitive(topNExec, source, topNExec.minCompetitive());
        return source.with(
            new TopNOperatorFactory(
                common.limit,
                asList(common.elementTypes),
                asList(common.encoders),
                common.orders,
                context.pageSize(topNExec, rowSize),
                context.plannerSettings.valuesLoadingJumboSize().getBytes(),
                topNExec.inputOrdering(),
                minCompetitive,
                parallelWorkerConfig
            ),
            source.layout
        );
    }

    /**
     * Builds and wires a {@link SharedMinCompetitive} side-channel so the generic
     * {@code TopNOperator} can publish a competitive {@code BYTES_REF} bound to the external source's
     * {@code DynamicThresholdAware} format readers, or returns {@code null} when the plan shape does
     * not qualify. The same supplier is handed back to the caller so the operator and the readers
     * share one channel.
     *
     * <p>Preconditions, all checked here:
     * <ul>
     *     <li>Exactly one sort {@link Order} (the channel exposes a single comparable bound).</li>
     *     <li>The sort attribute is a plain keyword/text {@link Attribute} that is a real column of
     *         the external source (same id in {@link ExternalSourceExec#output()}). A computed sort
     *         key would publish a bound that does not line up with the file column statistics the
     *         reader compares against, so it is rejected.</li>
     *     <li>The source operator factory is an {@link AsyncExternalSourceOperatorFactory}.</li>
     *     <li>The source carries no {@code pushedTopN} hint (the source already prunes; layering a
     *         threshold on top would be redundant).</li>
     * </ul>
     *
     * <p>When {@code preexisting} is non-null it is reused (e.g. a supplier already carried on the
     * {@link TopNExec}) instead of building a fresh one, but the readers are always wired to whichever
     * supplier is returned. Wiring and supplier creation are intentionally kept together so a supplier
     * can never reach the operator without the format readers being wired to the same channel.
     *
     * @param preexisting a supplier already attached to the plan node, or {@code null} to build one
     */
    @Nullable
    private SharedMinCompetitive.Supplier tryBuildExternalMinCompetitive(
        TopNExec topNExec,
        PhysicalOperation source,
        @Nullable SharedMinCompetitive.Supplier preexisting
    ) {
        List<Order> orders = topNExec.order();
        if (orders.size() != 1) {
            return null;
        }
        Order order = orders.get(0);
        Attribute sortAttribute = Expressions.attribute(order.child());
        if (sortAttribute == null) {
            return null;
        }
        if (isBytesRefThresholdSupportedSortType(sortAttribute.dataType()) == false) {
            return null;
        }
        if (source.sourceOperatorFactory instanceof AsyncExternalSourceOperatorFactory == false) {
            return null;
        }
        AsyncExternalSourceOperatorFactory externalSourceFactory = (AsyncExternalSourceOperatorFactory) source.sourceOperatorFactory;
        ExternalSourceExec externalSource = findExternalSourceBelow(topNExec.child());
        if (externalSource == null || externalSource.pushedTopN() != null) {
            return null;
        }
        // The sort key must be a column the source reads straight from the file (matched by id), so
        // the published bound and the reader's column statistics speak about the same values.
        boolean sortIsSourceColumn = false;
        for (Attribute attribute : externalSource.output()) {
            if (attribute.id().equals(sortAttribute.id())) {
                sortIsSourceColumn = true;
                break;
            }
        }
        if (sortIsSourceColumn == false) {
            return null;
        }
        boolean asc = order.direction() == Order.OrderDirection.ASC;
        boolean nullsFirst = order.nullsPosition() == Order.NullsPosition.FIRST;
        SharedMinCompetitive.Supplier supplier = preexisting != null
            ? preexisting
            : new SharedMinCompetitive.Supplier(blockFactory.breaker(), topNExec.minCompetitiveKeyConfig());
        externalSourceFactory.setMinCompetitiveSupplier(supplier, sortAttribute.name(), asc, nullsFirst);
        return supplier;
    }

    /**
     * Sort-key data types eligible for the {@code BYTES_REF} external threshold. Scoped to
     * keyword/text, whose {@code UTF8} TopN encoding decodes back to the same raw UTF-8 bytes that
     * Parquet/ORC publish as string min/max statistics, so the reader's lexicographic comparison is
     * exact. IP and VERSION encode to a different byte form than the file stats and are deferred.
     */
    private static boolean isBytesRefThresholdSupportedSortType(DataType dataType) {
        return dataType == DataType.KEYWORD || dataType == DataType.TEXT;
    }

    /**
     * Decide whether the {@code TopNExec} qualifies for the specialised {@link NumericTopNOperator}
     * and, if so, return its factory; otherwise {@code null} and the caller falls back to the
     * generic operator. The predicate is intentionally narrow — every check has a documented
     * reason — so any plan that fails to qualify gets the functionally-correct generic path.
     *
     * <p>Preconditions, all checked here:
     * <ul>
     *     <li>Exactly one sort {@link Order} (Tier 1 is single-key).</li>
     *     <li>The sort attribute is a plain {@link Attribute} (no expressions over the field) of
     *         a fixed-width numeric type — currently LONG, INTEGER, DOUBLE, BOOLEAN, DATETIME, or
     *         DATE_NANOS. FLOAT, UNSIGNED_LONG, HALF_FLOAT, and SCALED_FLOAT are deferred to a
     *         follow-up PR; they need a tiny encoding addition but no operator surface change.</li>
     *     <li>The limit is a literal foldable to a positive {@code int}. Non-literal limits go
     *         to the generic operator (the bytes-encoding path doesn't need a literal).</li>
     *     <li>The {@code TopNExec} sits directly above (or via a {@link UnaryExec} spine ending
     *         in) an {@link ExternalSourceExec}.</li>
     *     <li>The source's narrowed output is exactly {@code [sortKey, _rowPosition]} — two
     *         channels, sort key at channel 0, synthetic row-position column at channel 1.
     *         {@code InsertExternalFieldExtraction} produces this shape; any extra eager column
     *         (pushed-filter input, virtual {@code _file.*}) leaves more than two channels and
     *         disqualifies the substitution because the specialised operator's 2-channel layout
     *         cannot pass extra columns through.</li>
     *     <li>The source carries no {@code pushedTopN} hint. If
     *         {@code PushTopNIntoExternalSource} already annotated the source, the BlockHash will
     *         prune during aggregation and the generic TopN above must remain as the safety net
     *         (replacing it would double-count the budget).</li>
     *     <li>If the source operator factory is an {@link AsyncExternalSourceOperatorFactory},
     *         the planner shares the live threshold with external format readers.</li>
     * </ul>
     *
     * <p>No plan-time multi-value exclude: the operator supports multi-valued sort keys natively
     * via {@code NumericSortKeyExtractor} (MV-min for ASC, MV-max for DESC, empty MV slot treated
     * as null). This matches the generic {@code TopNOperator}'s behaviour through its
     * {@code KeyExtractorForX} family, so the substitution is semantics-preserving on MV input.
     */
    private NumericTopNOperator.NumericTopNOperatorFactory tryBuildNumericTopN(
        TopNExec topNExec,
        PhysicalOperation source,
        LocalExecutionPlannerContext context
    ) {
        List<Order> orders = topNExec.order();
        if (orders.size() != 1) {
            return null;
        }
        Order sortOrder = orders.get(0);
        Attribute sortAttribute = Expressions.attribute(sortOrder.child());
        if (sortAttribute == null) {
            return null;
        }
        if (isNumericTopNSupportedSortType(sortAttribute.dataType()) == false) {
            return null;
        }
        // Two guards, working at different layers. The first one (the layout check) is the real
        // gate on what {@link NumericTopNOperator#addInput} actually sees: if any intervening
        // {@link UnaryExec} on the spine — pushed filter, residual evaluator, future plan-node
        // insertion — adds a column, it shows up in {@code source.layout} and disqualifies the
        // rewrite. The second one (the source-output check below) is a belt-and-braces guard
        // against an ExternalSourceExec that was never narrowed in the first place (e.g. the
        // rule didn't run, or kept extras). Together they reject everything that isn't the exact
        // narrowed shape the operator expects: [sortKey, _rowPosition] with sortKey at channel 0.
        if (source.layout.numberOfChannels() != 2) {
            return null;
        }
        Layout.ChannelAndType sortEntry = source.layout.get(sortAttribute.id());
        if (sortEntry == null || sortEntry.channel() != NumericTopNOperator.SORT_KEY_CHANNEL) {
            return null;
        }
        // Walk down the UnaryExec spine to confirm we're sitting over a narrowed ExternalSourceExec
        // and to inspect its pushedTopN hint. Same traversal {@code InsertExternalFieldExtraction}
        // uses (inlined here to keep the planner from depending on an optimizer-rule class).
        ExternalSourceExec externalSource = findExternalSourceBelow(topNExec.child());
        if (externalSource == null) {
            return null;
        }
        List<Attribute> sourceOutput = externalSource.output();
        if (sourceOutput.size() != 2) {
            return null;
        }
        if (ColumnExtractor.ROW_POSITION_COLUMN.equals(sourceOutput.get(1).name()) == false) {
            return null;
        }
        if (externalSource.pushedTopN() != null) {
            return null;
        }
        // Limit must be a positive integer literal. We re-fold here (rather than carrying a
        // rule-folded value) so the planner remains the single source of truth for the literal's
        // primitive form.
        Expression limitExpr = topNExec.limit();
        if (limitExpr.foldable() == false) {
            return null;
        }
        Object folded = limitExpr.fold(context.foldCtx());
        Integer limit = numericTopNFoldedLimit(folded);
        if (limit == null || limit <= 0) {
            return null;
        }
        ElementType keyElementType = PlannerUtils.toElementType(sortAttribute.dataType());
        boolean asc = sortOrder.direction() == Order.OrderDirection.ASC;
        boolean nullsFirst = sortOrder.nullsPosition() == Order.NullsPosition.FIRST;
        SharedNumericThreshold.Supplier thresholdSupplier = null;
        if (source.sourceOperatorFactory instanceof AsyncExternalSourceOperatorFactory externalSourceFactory) {
            thresholdSupplier = new SharedNumericThreshold.Supplier(asc, nullsFirst);
            externalSourceFactory.setNumericThresholdSupplier(thresholdSupplier, sortAttribute.name(), keyElementType, asc, nullsFirst);
        }
        return new NumericTopNOperator.NumericTopNOperatorFactory(limit, keyElementType, asc, nullsFirst, thresholdSupplier);
    }

    /**
     * Sort-key element types the specialised {@link NumericTopNOperator} can rank. Mirrors the
     * operator's own {@code assertSupportedType} — DATETIME and DATE_NANOS collapse to
     * {@link ElementType#LONG} at planning time (see {@link PlannerUtils#toElementType}), so they
     * go through the LONG path. Keeping the predicate here rather than on the operator lets the
     * planner cleanly skip the optimisation without instantiating the factory.
     *
     * <p>Deliberately excluded:
     * <ul>
     *     <li>{@code UNSIGNED_LONG}: maps to {@link ElementType#LONG} but the operator's
     *         {@code ~raw} encoding does not preserve unsigned ordering. Supporting it needs a
     *         different encoding ({@code raw ^ Long.MIN_VALUE}, sign-bit flip) and is parked for
     *         a follow-up.</li>
     *     <li>{@code FLOAT}, {@code HALF_FLOAT}, {@code SCALED_FLOAT}: ESQL widens these to
     *         {@link DataType#DOUBLE} at load time, so a sort attribute with one of these data
     *         types never reaches this predicate in practice — {@link PlannerUtils#toElementType}
     *         throws on them outright. Listing them here as "rejected" would be misleading; the
     *         load-time widening already routes them through the DOUBLE branch.</li>
     * </ul>
     */
    private static boolean isNumericTopNSupportedSortType(DataType dataType) {
        return dataType == DataType.LONG
            || dataType == DataType.INTEGER
            || dataType == DataType.DOUBLE
            || dataType == DataType.BOOLEAN
            || dataType == DataType.DATETIME
            || dataType == DataType.DATE_NANOS;
    }

    /**
     * Folds a TopN limit expression to a positive {@link Integer}, returning {@code null} when
     * the limit is non-integral, negative, or out of {@code int} range.
     */
    private static Integer numericTopNFoldedLimit(Object folded) {
        if (folded instanceof Integer i) {
            return i;
        }
        if (folded instanceof Number n) {
            long l = n.longValue();
            if (l < 0 || l > Integer.MAX_VALUE) {
                return null;
            }
            return (int) l;
        }
        return null;
    }

    /**
     * Walk down a {@link UnaryExec} spine looking for an {@link ExternalSourceExec}; returns
     * {@code null} when the spine ends in any other leaf. Mirrors the traversal used by
     * {@code InsertExternalFieldExtraction#findExternalSource} (inlined here so the planner does
     * not depend on an optimizer-rule class).
     */
    private static ExternalSourceExec findExternalSourceBelow(PhysicalPlan start) {
        PhysicalPlan p = start;
        while (true) {
            if (p instanceof ExternalSourceExec es) {
                return es;
            }
            if (p instanceof UnaryExec u) {
                p = u.child();
                continue;
            }
            return null;
        }
    }

    private PhysicalOperation planTopNBy(TopNByExec topNByExec, LocalExecutionPlannerContext context) {
        final Integer rowSize = topNByExec.estimatedRowSize();
        PhysicalOperation source = plan(topNByExec.child(), context);
        var common = topNCommon(rowSize, topNByExec.order(), topNByExec.limitPerGroup(), topNByExec.docValuesAttributes(), source, context);
        List<Integer> groupKeys = topNByExec.groupings()
            .stream()
            .map(grouping -> getAttributeChannel(grouping, source.layout, "LIMIT BY expression must be an attribute"))
            .toList();
        if (groupKeys.isEmpty()) {
            throw new EsqlIllegalArgumentException("TopNBy groupings cannot be empty at runtime");
        }
        return source.with(
            new GroupedTopNOperator.GroupedTopNOperatorFactory(
                common.limit,
                asList(common.elementTypes),
                asList(common.encoders),
                common.orders,
                groupKeys,
                context.pageSize(topNByExec, rowSize),
                context.plannerSettings.valuesLoadingJumboSize().getBytes()
            ),
            source.layout
        );
    }

    private record TopNCommon(ElementType[] elementTypes, TopNEncoder[] encoders, List<TopNOperator.SortOrder> orders, int limit) {}

    private TopNCommon topNCommon(
        Integer rowSize,
        List<Order> order,
        Expression limitExpr,
        Set<Attribute> docValuesAttributes,
        PhysicalOperation source,
        LocalExecutionPlannerContext context
    ) {
        assert rowSize != null && rowSize > 0 : "estimated row size [" + rowSize + "] wasn't set";

        ElementType[] elementTypes = new ElementType[source.layout.numberOfChannels()];
        TopNEncoder[] encoders = new TopNEncoder[source.layout.numberOfChannels()];
        List<Layout.ChannelSet> inverse = source.layout.inverse();
        for (int channel = 0; channel < inverse.size(); channel++) {
            var fieldExtractPreference = fieldExtractPreference(docValuesAttributes, inverse.get(channel).nameIds());
            elementTypes[channel] = PlannerUtils.toElementType(inverse.get(channel).type(), fieldExtractPreference);
            encoders[channel] = TopNExec.encoder(inverse.get(channel).type(), context.shardContexts);
        }
        List<TopNOperator.SortOrder> orders = order.stream().map(o -> {
            int sortByChannel = getAttributeChannel(o.child(), source.layout, "order by expression must be an attribute");
            return new TopNOperator.SortOrder(
                sortByChannel,
                o.direction().equals(Order.OrderDirection.ASC),
                o.nullsPosition().equals(Order.NullsPosition.FIRST)
            );
        }).toList();

        int limit;
        if (limitExpr instanceof Literal literal) {
            Object val = literal.value() instanceof BytesRef br ? BytesRefs.toString(br) : literal.value();
            limit = stringToInt(val.toString());
        } else {
            throw new EsqlIllegalArgumentException("limit only supported with literal values");
        }
        return new TopNCommon(elementTypes, encoders, orders, limit);
    }

    private static int getAttributeChannel(Expression expression, Layout layout, String errMessage) {
        if (expression instanceof Attribute a) {
            return layout.get(a.id()).channel();
        } else {
            throw new EsqlIllegalArgumentException(errMessage);
        }
    }

    private static MappedFieldType.FieldExtractPreference fieldExtractPreference(Set<Attribute> docValuesAttributes, Set<NameId> nameIds) {
        // See if any of the NameIds is marked as having been loaded with doc-values preferences, which will affect the ElementType chosen.
        for (NameId nameId : nameIds) {
            for (Attribute withDocValues : docValuesAttributes) {
                if (nameId.equals(withDocValues.id())) {
                    return MappedFieldType.FieldExtractPreference.DOC_VALUES;
                }
            }
        }
        return MappedFieldType.FieldExtractPreference.NONE;
    }

    private PhysicalOperation planEval(EvalExec eval, LocalExecutionPlannerContext context) {
        PhysicalOperation source = plan(eval.child(), context);

        for (Alias field : eval.fields()) {
            var evaluatorSupplier = EvalMapper.toEvaluator(
                context.foldCtx(),
                field.child(),
                source.layout,
                context.shardContexts,
                context.analysisRegistry()
            );
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
                EvalMapper.toEvaluator(context.foldCtx(), expr, layout, context.analysisRegistry()),
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
        // Rebind the matcher to this node's own grok.watchdog.max_execution_time setting instead of the
        // no-op watchdog it was parsed/deserialized with, since the pattern is about to run against real data.
        org.elasticsearch.grok.Grok watchdogGrok = Grok.pattern(grok.source(), grok.pattern().pattern(), grokMatcherWatchdog).grok();
        source = source.with(
            new ColumnExtractOperator.Factory(
                types,
                EvalMapper.toEvaluator(context.foldCtx(), grok.inputExpression(), layout, context.analysisRegistry()),
                new GrokEvaluatorExtracter.Factory(watchdogGrok, grok.pattern().pattern(), fieldToPos, fieldToType)
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

        List<ExpressionEvaluator.Factory> rerankFieldsEvaluators = rerank.rerankFields()
            .stream()
            .map(rerankField -> EvalMapper.toEvaluator(context.foldCtx(), rerankField.child(), source.layout, context.analysisRegistry()))
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
                RerankOperator.DEFAULT_BATCH_SIZE,
                rerank.timeout()
            ),
            outputLayout
        );
    }

    // TODO: when highlighting can run directly against shard data, use real index offsets and per-field analyzers
    // instead of re-analyzing each row in a MemoryIndex.
    private PhysicalOperation planHighlight(HighlightExec highlight, LocalExecutionPlannerContext context) {
        PhysicalOperation source = plan(highlight.child(), context);

        Expression queryExpr = highlight.query();
        if (queryExpr == null) {
            throw new EsqlIllegalArgumentException("HIGHLIGHT requires an explicit query string");
        }
        String queryText = BytesRefs.toString(queryExpr.fold(context.foldCtx));
        // TODO: honour boundary_scanner*, order, max_analyzed_offset, and phrase_limit once HighlightOptions exposes them.
        HighlightOptions options = HighlightOptions.from(highlight.options(), context.foldCtx());
        HighlightConfig config = new HighlightConfig(
            queryText,
            options.preTag(),
            options.postTag(),
            options.encoder(),
            options.numberOfFragments(),
            options.fragmentSize(),
            options.noMatchSize()
        );

        List<ExpressionEvaluator.Factory> fieldEvaluators = highlight.fields()
            .stream()
            .map(field -> EvalMapper.toEvaluator(context.foldCtx(), field, source.layout, context.analysisRegistry()))
            .toList();

        // Append one keyword column per highlighted field.
        // The generated attributes are appended in the same order as the ON fields,
        // so the operator's appended blocks line up with these layout channels.
        Layout.Builder layoutBuilder = source.layout.builder();
        layoutBuilder.append(highlight.generatedFields());

        return source.with(new HighlightOperator.Factory(config, fieldEvaluators), layoutBuilder.build());
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
                .filter(e -> RemoteClusterAware.splitIndexName(e.getKey()).getClusterGroupingKey().equals(clusterAlias))
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
        var indexSplit = RemoteClusterAware.splitIndexName(entry.getKey());
        // No prefix is ok, prefix with this cluster is ok, something else is not
        if (indexSplit.clusterAlias() != null && clusterAlias.equals(indexSplit.clusterAlias()) == false) {
            throw new IllegalStateException(
                "can't plan [" + join + "]: no matching index found " + EsqlCCSUtils.inClusterName(clusterAlias)
            );
        }
        String indexName = indexSplit.indexExpression();
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
        boolean useStreamingOperator = shouldUseStreamingOperator(lookupFromIndexService, indexName);
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
                join.joinOnConditions(),
                useStreamingOperator,
                context.queryPragmas().exchangeBufferSize(),
                configuration.profile(),
                configuration
            ),
            layout
        );
    }

    private static final TransportVersion ESQL_LOOKUP_PLANNING = TransportVersion.fromName("esql_lookup_planning");

    /**
     * Determines whether streaming lookup should be used based on the {@link EsqlPlugin#LOOKUP_JOIN_STREAMING}
     * setting and the target nodes' transport versions.
     * Streaming lookup requires all target nodes to support the streaming protocol.
     */
    private boolean shouldUseStreamingOperator(LookupFromIndexService service, String indexName) {
        try {
            if (service.getClusterService().getClusterSettings().get(EsqlPlugin.LOOKUP_JOIN_STREAMING) == false) {
                return false;
            }

            ClusterState clusterState = service.getClusterService().state();

            // Resolve target nodes for the lookup index
            var projectState = service.getProjectResolver().getProjectState(clusterState);
            var shardIterators = service.getClusterService()
                .operationRouting()
                .searchShards(projectState, new String[] { indexName }, Map.of(), "_local");

            // Check ALL shard routings (primary + replicas) to ensure every node
            // that could potentially handle the lookup supports streaming
            for (ShardIterator shardIt : shardIterators) {
                for (ShardRouting shardRouting : shardIt) {
                    DiscoveryNode node = clusterState.nodes().get(shardRouting.currentNodeId());
                    Transport.Connection connection = service.getTransportService().getConnection(node);
                    TransportVersion nodeVersion = connection.getTransportVersion();
                    if (nodeVersion.supports(ESQL_LOOKUP_PLANNING) == false) {
                        logger.debug(
                            "Using non-streaming lookup operator: node [{}] has transport version [{}] which does not support [{}]",
                            node.getId(),
                            nodeVersion,
                            ESQL_LOOKUP_PLANNING
                        );
                        return false;
                    }
                }
            }
            return true;
        } catch (Exception e) {
            // If we can't determine the version, fall back to non-streaming for safety
            logger.debug("Failed to determine target node version for lookup, using non-streaming operator", e);
            return false;
        }
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

    private PhysicalOperation planMetricsInfo(MetricsInfoExec metricsInfoExec, LocalExecutionPlannerContext context) {
        if (metricsInfoExec.mode() == MetricsInfoExec.Mode.FINAL || metricsInfoExec.mode() == MetricsInfoExec.Mode.INTERMEDIATE) {
            return planMetricsInfoFinal(metricsInfoExec, context);
        }
        // INITIAL mode: extraction on data nodes.
        if (FieldExtractExec.extractSourceAttributesFrom(metricsInfoExec.child()) == null) {
            if (logger.isDebugEnabled()) {
                logger.debug(
                    "planMetricsInfo: no _doc attribute found in child [{}], outputSet [{}]; falling back to empty source",
                    metricsInfoExec.child().nodeName(),
                    metricsInfoExec.child().outputSet()
                );
            }
            return emptySourceForAttributes(metricsInfoExec.output());
        }
        // Step 1: Extract _tsid only
        FieldAttribute tsidAttr = new FieldAttribute(
            metricsInfoExec.source(),
            null,
            null,
            MetadataAttribute.TSID_FIELD,
            new EsField(MetadataAttribute.TSID_FIELD, DataType.TSID_DATA_TYPE, Map.of(), false, EsField.TimeSeriesFieldType.NONE),
            true
        );

        FieldExtractExec tsidExtractExec = new FieldExtractExec(
            metricsInfoExec.source(),
            metricsInfoExec.child(),
            List.of(tsidAttr),
            MappedFieldType.FieldExtractPreference.DOC_VALUES
        );

        PhysicalOperation tsidSource = planFieldExtractNode(tsidExtractExec, context);

        // Step 2: Dedup by _tsid
        int tsidChannel = tsidSource.layout.get(tsidAttr.id()).channel();
        PhysicalOperation dedupedSource = tsidSource.with(new DistinctByOperator.Factory(tsidChannel), tsidSource.layout);

        // Step 3: Extract _timeseries metadata (dimensions + metrics) from synthetic source
        FieldAttribute metadataSourceAttr = new FieldAttribute(
            metricsInfoExec.source(),
            null,
            null,
            "_timeseries_metadata",
            new FunctionEsField(
                new EsField(SourceFieldMapper.NAME, DataType.KEYWORD, Map.of(), false, EsField.TimeSeriesFieldType.DIMENSION),
                DataType.KEYWORD,
                new BlockLoaderFunctionConfig.TimeSeriesMetadata(true, Set.of())
            ),
            true
        );

        FieldAttribute indexAttr = new FieldAttribute(
            metricsInfoExec.source(),
            null,
            null,
            MetadataAttribute.INDEX,
            new EsField(MetadataAttribute.INDEX, DataType.KEYWORD, Map.of(), false, EsField.TimeSeriesFieldType.NONE),
            true
        );

        FieldExtractExec metadataExtractExec = new FieldExtractExec(
            metricsInfoExec.source(),
            tsidExtractExec,
            List.of(metadataSourceAttr, indexAttr),
            MappedFieldType.FieldExtractPreference.NONE
        );

        PhysicalOperation sourceWithMetadata = physicalOperationProviders.fieldExtractPhysicalOperation(
            metadataExtractExec,
            dedupedSource,
            context
        );

        int metadataSourceChannel = sourceWithMetadata.layout.get(metadataSourceAttr.id()).channel();
        int indexChannel = sourceWithMetadata.layout.get(indexAttr.id()).channel();

        Layout.Builder layoutBuilder = new Layout.Builder();
        layoutBuilder.append(metricsInfoExec.output());

        MetricsInfoOperator.MetricFieldLookup fieldLookup = createMetricFieldLookup(context.shardContexts);

        return sourceWithMetadata.with(
            new MetricsInfoOperator.Factory(fieldLookup, metadataSourceChannel, indexChannel),
            layoutBuilder.build()
        );
    }

    /**
     * FINAL mode: runs on the coordinator. Reads the 6-column MetricsInfo output from the
     * exchange (produced by data-node INITIAL phases) and merges rows by metric signature.
     */
    private PhysicalOperation planMetricsInfoFinal(MetricsInfoExec metricsInfoExec, LocalExecutionPlannerContext context) {
        PhysicalOperation source = plan(metricsInfoExec.child(), context);

        List<Attribute> outputAttrs = metricsInfoExec.output();
        int[] channels = new int[outputAttrs.size()];
        for (int i = 0; i < outputAttrs.size(); i++) {
            channels[i] = source.layout.get(outputAttrs.get(i).id()).channel();
        }

        Layout.Builder layoutBuilder = new Layout.Builder();
        layoutBuilder.append(outputAttrs);

        return source.with(new MetricsInfoOperator.FinalFactory(channels), layoutBuilder.build());
    }

    private PhysicalOperation planTsInfo(TsInfoExec tsInfoExec, LocalExecutionPlannerContext context) {
        if (tsInfoExec.mode() == TsInfoExec.Mode.FINAL || tsInfoExec.mode() == TsInfoExec.Mode.INTERMEDIATE) {
            return planTsInfoFinal(tsInfoExec, context);
        }
        // INITIAL mode: extraction on data nodes.
        if (FieldExtractExec.extractSourceAttributesFrom(tsInfoExec.child()) == null) {
            if (logger.isDebugEnabled()) {
                logger.debug(
                    "planTsInfo: no _doc attribute found in child [{}], outputSet [{}]; falling back to empty source",
                    tsInfoExec.child().nodeName(),
                    tsInfoExec.child().outputSet()
                );
            }
            return emptySourceForAttributes(tsInfoExec.output());
        }
        // Step 1: Extract _tsid only
        FieldAttribute tsidAttr = new FieldAttribute(
            tsInfoExec.source(),
            null,
            null,
            MetadataAttribute.TSID_FIELD,
            new EsField(MetadataAttribute.TSID_FIELD, DataType.TSID_DATA_TYPE, Map.of(), false, EsField.TimeSeriesFieldType.NONE),
            true
        );

        FieldExtractExec tsidExtractExec = new FieldExtractExec(
            tsInfoExec.source(),
            tsInfoExec.child(),
            List.of(tsidAttr),
            MappedFieldType.FieldExtractPreference.DOC_VALUES
        );

        PhysicalOperation tsidSource = planFieldExtractNode(tsidExtractExec, context);

        // Step 2: Dedup by _tsid
        int tsidChannel = tsidSource.layout.get(tsidAttr.id()).channel();
        PhysicalOperation dedupedSource = tsidSource.with(new DistinctByOperator.Factory(tsidChannel), tsidSource.layout);

        // Step 3: Extract _timeseries metadata and _index
        FieldAttribute metadataSourceAttr = new FieldAttribute(
            tsInfoExec.source(),
            null,
            null,
            "_timeseries_metadata",
            new FunctionEsField(
                new EsField(SourceFieldMapper.NAME, DataType.KEYWORD, Map.of(), false, EsField.TimeSeriesFieldType.DIMENSION),
                DataType.KEYWORD,
                new BlockLoaderFunctionConfig.TimeSeriesMetadata(true, Set.of())
            ),
            true
        );

        FieldAttribute indexAttr = new FieldAttribute(
            tsInfoExec.source(),
            null,
            null,
            MetadataAttribute.INDEX,
            new EsField(MetadataAttribute.INDEX, DataType.KEYWORD, Map.of(), false, EsField.TimeSeriesFieldType.NONE),
            true
        );

        FieldExtractExec metadataExtractExec = new FieldExtractExec(
            tsInfoExec.source(),
            tsidExtractExec,
            List.of(metadataSourceAttr, indexAttr),
            MappedFieldType.FieldExtractPreference.NONE
        );

        PhysicalOperation sourceWithMetadata = physicalOperationProviders.fieldExtractPhysicalOperation(
            metadataExtractExec,
            dedupedSource,
            context
        );

        int metadataSourceChannel = sourceWithMetadata.layout.get(metadataSourceAttr.id()).channel();
        int indexChannel = sourceWithMetadata.layout.get(indexAttr.id()).channel();

        Layout.Builder layoutBuilder = new Layout.Builder();
        layoutBuilder.append(tsInfoExec.output());

        MetricsInfoOperator.MetricFieldLookup fieldLookup = createMetricFieldLookup(context.shardContexts);

        return sourceWithMetadata.with(new TsInfoOperator.Factory(fieldLookup, metadataSourceChannel, indexChannel), layoutBuilder.build());
    }

    /**
     * FINAL mode: runs on the coordinator. Reads the 7-column TsInfo output from the
     * exchange (produced by data-node INITIAL phases) and merges rows by ts signature.
     */
    private PhysicalOperation planTsInfoFinal(TsInfoExec tsInfoExec, LocalExecutionPlannerContext context) {
        PhysicalOperation source = plan(tsInfoExec.child(), context);

        List<Attribute> outputAttrs = tsInfoExec.output();
        int[] channels = new int[outputAttrs.size()];
        for (int i = 0; i < outputAttrs.size(); i++) {
            channels[i] = source.layout.get(outputAttrs.get(i).id()).channel();
        }

        Layout.Builder layoutBuilder = new Layout.Builder();
        layoutBuilder.append(outputAttrs);

        return source.with(new TsInfoOperator.FinalFactory(channels), layoutBuilder.build());
    }

    private PhysicalOperation emptySourceForAttributes(List<Attribute> attributes) {
        Layout.Builder layout = new Layout.Builder();
        layout.append(attributes);
        LocalSourceOperator.PageSupplier empty = () -> null;
        return PhysicalOperation.fromSource(new LocalSourceFactory(() -> new LocalSourceOperator(empty)), layout.build());
    }

    private MetricsInfoOperator.MetricFieldLookup createMetricFieldLookup(IndexedByShardId<? extends ShardContext> shardContexts) {
        Map<String, MappingLookup> mappingsByIndex = new HashMap<>();
        for (ShardContext shard : shardContexts.iterable()) {
            if (shard.indexSettings().getMode().isTsdb()) {
                mappingsByIndex.putIfAbsent(shard.indexSettings().getIndex().getName(), shard.mappingLookup());
            }
        }

        return (indexName, fieldName) -> {
            String localIndexName = RemoteClusterAware.splitIndexName(indexName).indexExpression();
            MappingLookup mappingLookup = mappingsByIndex.get(localIndexName);
            if (mappingLookup == null) {
                return null;
            }
            MappedFieldType fieldType = mappingLookup.getFieldType(fieldName);
            if (fieldType == null) {
                return null;
            }
            TimeSeriesParams.MetricType tsMetricType = fieldType.getMetricType();
            if (tsMetricType == null) {
                return null;
            }
            String unit = fieldType.meta().get("unit");
            if (unit != null && unit.isBlank()) {
                unit = null;
            }
            return new MetricFieldInfo(fieldName, indexName, fieldType.typeName(), tsMetricType.toString(), unit);
        };
    }

    /**
     * Plans a generic external source using the OperatorFactoryRegistry.
     *
     * <p>This method uses the registry to create the appropriate operator factory based on
     * the source type and path. The registry will:
     * <ol>
     *   <li>Check if a plugin has registered a custom factory for the source type</li>
     *   <li>Fall back to the generic AsyncExternalSourceOperatorFactory using
     *       storage and format registries</li>
     * </ol>
     *
     * @param externalSource the external source physical plan node
     * @param context the planner context
     * @return the physical operation
     */
    private PhysicalOperation planExternalSource(ExternalSourceExec externalSource, LocalExecutionPlannerContext context) {
        Layout.Builder layout = new Layout.Builder();
        layout.append(externalSource.output());

        Integer estimatedRowSize = externalSource.estimatedRowSize();
        int pageSize = (estimatedRowSize != null && estimatedRowSize > 0)
            ? Math.max(SourceOperator.MIN_TARGET_PAGE_SIZE, SourceOperator.TARGET_PAGE_SIZE / estimatedRowSize)
            : DEFAULT_EXTERNAL_SOURCE_PAGE_SIZE_ROWS;

        if (operatorFactoryRegistry == null) {
            throw new IllegalStateException("OperatorFactoryRegistry is required for external sources");
        }

        StoragePath path = StoragePath.of(externalSource.sourcePath());
        List<String> projectedColumns = new ArrayList<>();
        for (Attribute attr : externalSource.output()) {
            projectedColumns.add(attr.name());
        }

        // ProjectAwayColumns inserts a single synthetic attribute when all real columns are pruned.
        // Treat it the same as empty, so the decoder takes the row-count-only fast path.
        if (projectedColumns.size() == 1 && ProjectAwayColumns.ALL_FIELDS_PROJECTED.equals(projectedColumns.getFirst())) {
            projectedColumns = List.of();
        }

        int pushedLimit = externalSource.pushedLimit();

        // Shrink buffer for small limits
        int effectiveBufferSize = 10;
        if (pushedLimit != FormatReader.NO_LIMIT) {
            effectiveBufferSize = Math.min(10, (pushedLimit + pageSize - 1) / pageSize + 1);
        }

        FileList fileList = externalSource.fileList();
        int splitCount = externalSource.splits().size();
        ExternalSliceQueue sliceQueue = null;
        int instanceCount = 1;

        /*
         * Whenever explicit splits are assigned to this instance, route execution through the slice queue so
         * the operator reads exactly those splits (expanding a single coalesced split into its leaf
         * FileSplits). This must hold even when a resolved FileList is also present — the coordinator keeps
         * one, but a data node does not (it isn't serialized). Falling through to the resolved-FileList
         * multi-file read when splits are assigned would re-read the entire glob behind the assigned splits,
         * double-counting the rows the slice-queue instances also read. The multi-file read path is therefore
         * only for the no-splits case, where this instance owns the whole resolved FileList.
         */
        boolean useSliceQueue = splitCount > 0;
        if (useSliceQueue) {
            sliceQueue = new ExternalSliceQueue(externalSource.splits());
        }
        if (splitCount > 1) {
            int maxParallelism = context.queryPragmas().taskConcurrency();
            if (pushedLimit != FormatReader.NO_LIMIT && pushedLimit <= pageSize) {
                instanceCount = 1;
            } else if (pushedLimit != FormatReader.NO_LIMIT) {
                int pagesNeeded = Math.max(1, (pushedLimit + pageSize - 1) / pageSize);
                instanceCount = Math.min(pagesNeeded, Math.min(splitCount, maxParallelism));
            } else {
                instanceCount = Math.min(splitCount, maxParallelism);
            }
        }
        // Carries every name VirtualColumnIterator should materialise: Hive-style partition columns
        // plus the _file.* metadata columns the user actually requested (these reach the relation
        // output only via METADATA, or the temporary EXTERNAL shim — they are no longer auto-attached
        // to every external schema). Passed through SourceOperatorContext.partitionColumnNames
        // (legacy method name kept to avoid an SPI rename on this PR).
        Set<String> virtualColumnNames = new LinkedHashSet<>();
        if (fileList != null) {
            PartitionMetadata pm = fileList.partitionMetadata();
            if (pm != null && pm.isEmpty() == false) {
                virtualColumnNames.addAll(pm.partitionColumns().keySet());
            }
        }
        // On a data node the resolved FileList is not serialized (see the slice-queue note above), so the
        // partition columns above are absent there. Their names ARE serialized via the PARTITION_COLUMNS_KEY
        // stamp in sourceMetadata — the same stamp the aggregate fold reads
        // (ExternalSourceAggregatePushdown.partitionColumnNames). Union them in so VirtualColumnIterator
        // materialises the partition column as a constant block even when ONLY a partition column is projected
        // (e.g. COUNT(p) that safe-missed to a scan): otherwise the operator treats it as a data column, the
        // reader emits a 0-block page, and the downstream aggregator reads a non-existent block.
        virtualColumnNames.addAll(ExternalSourceAggregatePushdown.partitionColumnNames(externalSource.sourceMetadata()));
        for (Attribute attr : externalSource.output()) {
            if (FileMetadataColumns.isFileMetadataColumn(attr.name())) {
                virtualColumnNames.add(attr.name());
            }
        }

        SourceOperatorContext operatorContext = SourceOperatorContext.builder()
            .sourceType(externalSource.sourceType())
            .path(path)
            .projectedColumns(projectedColumns)
            .attributes(externalSource.output())
            .batchSize(pageSize)
            .maxBufferSize(effectiveBufferSize)
            .rowLimit(pushedLimit)
            .executor(operatorFactoryRegistry.executor())
            .fileReadExecutor(operatorFactoryRegistry.fileReadExecutor())
            .config(externalSource.config())
            .sourceMetadata(externalSource.sourceMetadata())
            .pushedFilter(externalSource.pushedFilter())
            .pushedExpressions(externalSource.pushedExpressions())
            .fileList(fileList)
            .schemaMap(externalSource.schemaMap())
            .partitionColumnNames(virtualColumnNames)
            .sliceQueue(sliceQueue)
            .parsingParallelism(context.queryPragmas().parsingParallelism())
            .maxConcurrentOpenSegments(context.queryPragmas().maxConcurrentOpenSegments())
            .maxRecordBytes(Math.toIntExact(context.queryPragmas().maxRecordSize().getBytes()))
            .parallelism(instanceCount)
            .datasetName(externalSource.datasetName())
            .deferredExtraction(externalSource.deferredExtraction())
            .build();

        SourceOperator.SourceOperatorFactory factory = operatorFactoryRegistry.factory(operatorContext);
        context.driverParallelism(new DriverParallelism(DriverParallelism.Type.DATA_PARALLELISM, instanceCount));
        return PhysicalOperation.fromSource(factory, layout.build());
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
            new FilterOperatorFactory(
                EvalMapper.toEvaluator(
                    context.foldCtx(),
                    filter.condition(),
                    source.layout,
                    context.shardContexts,
                    context.analysisRegistry()
                )
            ),
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

    private PhysicalOperation planLimitBy(LimitByExec limitBy, LocalExecutionPlannerContext context) {
        PhysicalOperation source = plan(limitBy.child(), context);
        int limitValue = (Integer) limitBy.limitPerGroup().fold(context.foldCtx);
        Layout layout = source.layout;
        List<Integer> groupKeys = limitBy.groupings()
            .stream()
            .map(g -> getAttributeChannel(g, layout, "LIMIT BY expression must be an attribute"))
            .toList();
        List<Layout.ChannelSet> inverse = layout.inverse();
        List<ElementType> elementTypes = new ArrayList<>(layout.numberOfChannels());
        for (int channel = 0; channel < inverse.size(); channel++) {
            elementTypes.add(PlannerUtils.toElementType(inverse.get(channel).type()));
        }
        return source.with(new GroupedLimitOperator.Factory(limitValue, groupKeys, elementTypes), source.layout);
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

    private PhysicalOperation planTimeSeriesCollapse(TimeSeriesCollapseExec collapse, LocalExecutionPlannerContext context) {
        PhysicalOperation source = plan(collapse.child(), context);
        Layout layout = source.layout;

        List<BlockHash.GroupSpec> groups = collapse.dimensions().stream().map(attribute -> {
            Layout.ChannelAndType input = layout.get(attribute.id());
            return new BlockHash.GroupSpec(input.channel(), PlannerUtils.toElementType(input.type()));
        }).toList();
        int valueChannel = layout.get(collapse.value().id()).channel();
        int stepChannel = layout.get(collapse.step().id()).channel();

        return source.with(
            new TimeSeriesCollapseOperator.Factory(
                groups,
                valueChannel,
                stepChannel,
                collapse.start(),
                collapse.end(),
                collapse.stepMillis()
            ),
            layout
        );
    }

    private PhysicalOperation planChangePoint(ChangePointExec changePoint, LocalExecutionPlannerContext context) {
        PhysicalOperation source = plan(changePoint.child(), context);
        Layout layout = source.layout.builder().append(changePoint.targetType()).append(changePoint.targetPvalue()).build();
        int valueChannel = layout.get(changePoint.value().id()).channel();
        List<Integer> groupingChannels = changePoint.groupings()
            .stream()
            .map(g -> getAttributeChannel(g, layout, "CHANGE_POINT BY expression must be an attribute"))
            .toList();
        return source.with(new ChangePointOperator.Factory(valueChannel, groupingChannels, changePoint.source()), layout);
    }

    private PhysicalOperation planSample(SampleExec rsx, LocalExecutionPlannerContext context) {
        PhysicalOperation source = plan(rsx.child(), context);
        var probability = (double) Foldables.valueOf(context.foldCtx(), rsx.probability());
        return source.with(new SampleOperator.Factory(probability), source.layout);
    }

    private PhysicalOperation planSparklineGenerateEmptyBuckets(
        SparklineGenerateEmptyBucketsExec sparkline,
        LocalExecutionPlannerContext context
    ) {
        PhysicalOperation source = plan(sparkline.child(), context);
        Layout intermediateLayout = new Layout.Builder().append(sparkline.values())
            .append(sparkline.passthroughAttributes())
            .append(sparkline.groupings().stream().map(Expressions::attribute).toList())
            .build();

        PhysicalOperation withOperator = source.with(
            new SparklineGenerateEmptyBucketsOperator.Factory(
                sparkline.values().size(),
                sparkline.dateBucketRounding(),
                sparkline.minDate(),
                sparkline.maxDate()
            ),
            intermediateLayout
        );

        List<Integer> projection = new ArrayList<>();
        Layout.Builder finalLayoutBuilder = new Layout.Builder();
        for (Attribute attr : sparkline.output()) {
            Layout.ChannelAndType input = intermediateLayout.get(attr.id());
            projection.add(input.channel());
            finalLayoutBuilder.append(attr);
        }
        return withOperator.with(new ProjectOperatorFactory(projection), finalLayoutBuilder.build());

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
        Settings settings,
        IndexedByShardId<? extends ShardContext> shardContexts,
        @Nullable AnalysisRegistry analysisRegistry
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
