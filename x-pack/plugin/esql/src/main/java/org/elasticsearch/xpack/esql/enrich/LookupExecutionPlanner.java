/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.enrich;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.LocalCircuitBreaker;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.lucene.IndexedByShardId;
import org.elasticsearch.compute.lucene.IndexedByShardIdFromSingleton;
import org.elasticsearch.compute.lucene.ShardContext;
import org.elasticsearch.compute.lucene.read.ValuesSourceReaderOperator;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.EvalOperator.EvalOperatorFactory;
import org.elasticsearch.compute.operator.FilterOperator;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.compute.operator.Operator.OperatorFactory;
import org.elasticsearch.compute.operator.OutputOperator.CollectedPagesProvider;
import org.elasticsearch.compute.operator.OutputOperator.OutputOperatorFactory;
import org.elasticsearch.compute.operator.SourceOperator.SourceOperatorFactory;
import org.elasticsearch.compute.operator.Warnings;
import org.elasticsearch.compute.operator.lookup.BlockOptimization;
import org.elasticsearch.compute.operator.lookup.LookupEnrichQueryGenerator;
import org.elasticsearch.compute.operator.lookup.LookupQueryOperator;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.index.mapper.BlockLoader;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.Rewriteable;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.search.internal.AliasFilter;
import org.elasticsearch.xpack.esql.EsqlIllegalArgumentException;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.evaluator.EvalMapper;
import org.elasticsearch.xpack.esql.plan.physical.EvalExec;
import org.elasticsearch.xpack.esql.plan.physical.FieldExtractExec;
import org.elasticsearch.xpack.esql.plan.physical.FilterExec;
import org.elasticsearch.xpack.esql.plan.physical.OutputExec;
import org.elasticsearch.xpack.esql.plan.physical.ParameterizedQueryExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.ProjectExec;
import org.elasticsearch.xpack.esql.plan.physical.UnaryExec;
import org.elasticsearch.xpack.esql.planner.EsPhysicalOperationProviders;
import org.elasticsearch.xpack.esql.planner.Layout;
import org.elasticsearch.xpack.esql.planner.LocalExecutionPlanner;
import org.elasticsearch.xpack.esql.planner.LocalExecutionPlanner.PhysicalOperation;
import org.elasticsearch.xpack.esql.planner.PlannerSettings;
import org.elasticsearch.xpack.esql.planner.PlannerUtils;
import org.elasticsearch.xpack.esql.plugin.EsqlPlugin;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;

/**
 * Plans the execution of a lookup physical plan
 * Converts PhysicalPlan nodes into PhysicalOperation with operator factories
 * Also converts PhysicalOperation into actual Operators
 */
public class LookupExecutionPlanner {
    /**
     * Functional interface for creating a {@link LookupEnrichQueryGenerator} from plan data.
     * Matches the signature of {@code LookupFromIndexService.queryListFromPlan}.
     */
    @FunctionalInterface
    public interface QueryListFromPlanFactory {
        LookupEnrichQueryGenerator create(
            List<MatchConfig> matchFields,
            @Nullable Expression joinOnConditions,
            @Nullable QueryBuilder pushedQuery,
            SearchExecutionContext context,
            AliasFilter aliasFilter,
            Warnings warnings
        );
    }

    /**
     * Extended DriverContext that provides access to ShardContext, LookupShardContext, collectedPages, and inputPage.
     * This allows factories to retrieve ShardContext, SearchExecutionContext (from LookupShardContext),
     * collectedPages, and inputPage at operator creation time.
     * SearchExecutionContext is retrieved dynamically from LookupShardContext to avoid caching stale references.
     */
    static class LookupDriverContext extends DriverContext implements CollectedPagesProvider {
        private final ShardContext shardContext;
        private final AbstractLookupService.LookupShardContext lookupShardContext;
        private final List<Page> collectedPages;
        private final Page inputPage;
        private final AliasFilter aliasFilter;

        LookupDriverContext(
            BigArrays bigArrays,
            BlockFactory blockFactory,
            ShardContext shardContext,
            AbstractLookupService.LookupShardContext lookupShardContext,
            List<Page> collectedPages,
            Page inputPage,
            AliasFilter aliasFilter,
            LocalCircuitBreaker.SizeSettings localBreakerSettings
        ) {
            super(bigArrays, blockFactory, localBreakerSettings);
            this.shardContext = shardContext;
            this.lookupShardContext = lookupShardContext;
            this.collectedPages = collectedPages;
            this.inputPage = inputPage;
            this.aliasFilter = aliasFilter;
        }

        ShardContext shardContext() {
            return shardContext;
        }

        AbstractLookupService.LookupShardContext lookupShardContext() {
            return lookupShardContext;
        }

        /**
         * Get SearchExecutionContext dynamically from LookupShardContext to ensure it's current and not stale.
         */
        SearchExecutionContext searchExecutionContext() {
            return lookupShardContext.executionContext();
        }

        @Override
        public List<Page> collectedPages() {
            return collectedPages;
        }

        Page inputPage() {
            return inputPage;
        }

        AliasFilter aliasFilter() {
            return aliasFilter;
        }
    }

    private final BlockFactory blockFactory;
    private final BigArrays bigArrays;
    private final LocalCircuitBreaker.SizeSettings localBreakerSettings;

    public LookupExecutionPlanner(BlockFactory blockFactory, BigArrays bigArrays, LocalCircuitBreaker.SizeSettings localBreakerSettings) {
        this.blockFactory = blockFactory;
        this.bigArrays = bigArrays;
        this.localBreakerSettings = localBreakerSettings;
    }

    /**
     * Creates a PhysicalOperation with operator factories, matching LocalExecutionPlanner's pattern.
     * @param lookupSource the Source to use for warning messages (from the original LOOKUP JOIN expression),
     *                     passed separately because plan nodes conventionally discard Source during serialization.
     */
    public PhysicalOperation buildOperatorFactories(
        PlannerSettings plannerSettings,
        PhysicalPlan physicalPlan,
        BlockOptimization blockOptimization,
        SourceOperatorFactory sourceFactory,
        FoldContext foldCtx,
        QueryListFromPlanFactory queryListFromPlanFactory,
        Source lookupSource
    ) {
        return planLookupNode(
            plannerSettings,
            physicalPlan,
            blockOptimization,
            sourceFactory,
            foldCtx,
            queryListFromPlanFactory,
            lookupSource
        );
    }

    /**
     * Creates the actual operators from the PhysicalOperation using DriverContext.
     * The collectedPages list is created here and stored in LookupDriverContext.
     */
    public LookupFromIndexService.LookupQueryPlan buildOperators(
        PhysicalOperation physicalOperation,
        AbstractLookupService.LookupShardContext shardContext,
        List<Releasable> releasables,
        Page inputPage,
        AliasFilter aliasFilter
    ) {

        final LocalCircuitBreaker localBreaker = new LocalCircuitBreaker(
            blockFactory.breaker(),
            localBreakerSettings.overReservedBytes(),
            localBreakerSettings.maxOverReservedBytes()
        );
        releasables.add(localBreaker);

        // Create collectedPages list that will be used by OutputOperatorFactory
        List<Page> collectedPages = Collections.synchronizedList(new ArrayList<>());

        LookupDriverContext driverContext = new LookupDriverContext(
            bigArrays,
            blockFactory.newChildFactory(localBreaker),
            shardContext.context(),
            shardContext,
            collectedPages,
            inputPage,
            aliasFilter,
            localBreakerSettings
        );

        // In streaming mode, BidirectionalBatchExchangeServer provides:
        // - ExchangeSourceOperator (source) - receives pages from client
        // - ExchangeSinkOperator (sink) - sends results back to client
        // So we only need to create the intermediate operators that process the pages
        // The operator chain on server is: LookupQueryOperator -> ValuesSourceReaderOperator -> ProjectOperator
        List<Operator> intermediateOperators = new ArrayList<>();

        physicalOperation.operators(intermediateOperators, driverContext);
        for (Operator op : intermediateOperators) {
            releasables.add(op);
        }

        return new LookupFromIndexService.LookupQueryPlan(shardContext, localBreaker, driverContext, intermediateOperators);
    }

    /**
     * Recursively plans a PhysicalPlan node into a PhysicalOperation, processing children first.
     */
    private PhysicalOperation planLookupNode(
        PlannerSettings plannerSettings,
        PhysicalPlan node,
        BlockOptimization optimizationState,
        SourceOperatorFactory sourceFactory,
        FoldContext foldCtx,
        QueryListFromPlanFactory queryListFromPlanFactory,
        Source lookupSource
    ) {
        PhysicalOperation source;
        if (node instanceof UnaryExec unaryExec) {
            source = planLookupNode(
                plannerSettings,
                unaryExec.child(),
                optimizationState,
                sourceFactory,
                foldCtx,
                queryListFromPlanFactory,
                lookupSource
            );
        } else {
            // there could be a leaf node such as ParameterizedQueryExec
            source = null;
        }

        // Plan this node based on its type
        if (node instanceof ParameterizedQueryExec parameterizedQueryExec) {
            return planParameterizedQueryExec(
                parameterizedQueryExec,
                optimizationState,
                sourceFactory,
                queryListFromPlanFactory,
                lookupSource
            );
        } else if (node instanceof FieldExtractExec fieldExtractExec) {
            return planFieldExtractExec(plannerSettings, fieldExtractExec, source);
        } else if (node instanceof EvalExec evalExec) {
            return planEvalExec(evalExec, source, foldCtx);
        } else if (node instanceof FilterExec filterExec) {
            return planFilterExec(filterExec, source, foldCtx);
        } else if (node instanceof ProjectExec projectExec) {
            return planProjectExec(projectExec, source);
        } else if (node instanceof OutputExec outputExec) {
            return planOutputExec(outputExec, source);
        } else {
            throw new EsqlIllegalArgumentException("unknown physical plan node [" + node.nodeName() + "]");
        }
    }

    private PhysicalOperation planParameterizedQueryExec(
        ParameterizedQueryExec parameterizedQueryExec,
        BlockOptimization optimizationState,
        SourceOperatorFactory sourceFactory,
        QueryListFromPlanFactory queryListFromPlanFactory,
        Source lookupSource
    ) {
        Layout.Builder layoutBuilder = new Layout.Builder();
        List<Attribute> output = parameterizedQueryExec.output();
        for (Attribute attr : output) {
            layoutBuilder.append(attr);
        }
        Layout layout = layoutBuilder.build();

        OperatorFactory enrichQueryFactory = new LookupQueryOperatorFactory(
            LookupQueryOperator.DEFAULT_MAX_PAGE_SIZE,
            optimizationState,
            0,
            parameterizedQueryExec.matchFields(),
            parameterizedQueryExec.joinOnConditions(),
            parameterizedQueryExec.query(),
            lookupSource,
            queryListFromPlanFactory,
            parameterizedQueryExec.emptyResult()
        );

        return PhysicalOperation.fromSource(sourceFactory, layout).with(enrichQueryFactory, layout);
    }

    private PhysicalOperation planFieldExtractExec(
        PlannerSettings plannerSettings,
        FieldExtractExec fieldExtractExec,
        PhysicalOperation source
    ) {
        List<Attribute> attributesToExtract = fieldExtractExec.attributesToExtract();

        Layout.Builder layoutBuilder = new Layout.Builder();
        for (Attribute attr : fieldExtractExec.output()) {
            layoutBuilder.append(attr);
        }
        Layout layout = layoutBuilder.build();

        // Determine the doc channel from the source layout
        Attribute sourceAttr = fieldExtractExec.sourceAttribute();
        int docChannel = source.layout().get(sourceAttr.id()).channel();

        // Create a factory that builds ShardContext and BlockLoader dynamically from LookupDriverContext
        // to avoid caching stale IndexReader references when PhysicalOperation is cached
        ByteSizeValue jumboSize = plannerSettings.valuesLoadingJumboSize();
        return source.with(new OperatorFactory() {
            @Override
            public Operator get(DriverContext driverContext) {
                // In lookup execution path, driverContext is always LookupDriverContext
                LookupDriverContext lookupDriverContext = (LookupDriverContext) driverContext;
                EsPhysicalOperationProviders.ShardContext esShardContext = lookupDriverContext.lookupShardContext().context();

                // Create BlockLoader dynamically using fresh shardContext from LookupDriverContext
                List<ValuesSourceReaderOperator.FieldInfo> fields = new ArrayList<>(attributesToExtract.size());
                for (Attribute attr : attributesToExtract) {
                    NamedExpression extractField = (NamedExpression) attr;
                    BlockLoader loader = esShardContext.blockLoader(
                        AbstractLookupService.extractFieldName(extractField),
                        extractField.dataType() == DataType.UNSUPPORTED,
                        MappedFieldType.FieldExtractPreference.NONE,
                        null,
                        null,
                        plannerSettings.blockLoaderSizeOrdinals(),
                        plannerSettings.blockLoaderSizeScript()

                    );
                    fields.add(
                        new ValuesSourceReaderOperator.FieldInfo(
                            extractField.name(),
                            PlannerUtils.toElementType(extractField.dataType()),
                            false,
                            (ctx, shardIdx) -> {
                                if (shardIdx != 0) {
                                    throw new IllegalStateException("only one shard");
                                }
                                return ValuesSourceReaderOperator.load(loader);
                            }
                        )
                    );
                }

                IndexedByShardId<ValuesSourceReaderOperator.ShardContext> shardContexts = new IndexedByShardIdFromSingleton<>(
                    new ValuesSourceReaderOperator.ShardContext(
                        esShardContext.searcher().getIndexReader(),
                        esShardContext::newSourceLoader,
                        EsqlPlugin.STORED_FIELDS_SEQUENTIAL_PROPORTION.getDefault(org.elasticsearch.common.settings.Settings.EMPTY)
                    )
                );
                return new ValuesSourceReaderOperator(
                    driverContext,
                    jumboSize.getBytes(),
                    fields,
                    shardContexts,
                    true,
                    docChannel,
                    PlannerSettings.SOURCE_RESERVATION_FACTOR.get(Settings.EMPTY),
                    PlannerSettings.DOC_SEQUENCE_BYTES_REF_FIELD_THRESHOLD.getDefault(Settings.EMPTY)
                );
            }

            @Override
            public String describe() {
                StringBuilder sb = new StringBuilder();
                sb.append("ValuesSourceReaderOperator[fields = [");
                if (attributesToExtract.size() < 10) {
                    boolean first = true;
                    for (Attribute attr : attributesToExtract) {
                        if (first) {
                            first = false;
                        } else {
                            sb.append(", ");
                        }
                        sb.append(attr.name());
                    }
                } else {
                    sb.append(attributesToExtract.size()).append(" fields");
                }
                sb.append("]]");
                return sb.toString();
            }
        }, layout);
    }

    private PhysicalOperation planEvalExec(EvalExec evalExec, PhysicalOperation source, FoldContext foldCtx) {
        for (Alias field : evalExec.fields()) {
            var evaluatorSupplier = EvalMapper.toEvaluator(foldCtx, field.child(), source.layout());
            Layout.Builder layout = source.layout().builder();
            layout.append(field.toAttribute());
            source = source.with(new EvalOperatorFactory(evaluatorSupplier), layout.build());
        }
        return source;
    }

    private PhysicalOperation planFilterExec(FilterExec filterExec, PhysicalOperation source, FoldContext foldCtx) {
        return source.with(
            new FilterOperator.FilterOperatorFactory(EvalMapper.toEvaluator(foldCtx, filterExec.condition(), source.layout())),
            source.layout()
        );
    }

    private PhysicalOperation planProjectExec(ProjectExec projectExec, PhysicalOperation source) {
        return LocalExecutionPlanner.planProject(projectExec, source);
    }

    /**
     * Plans OutputExec into a PhysicalOperation with OutputOperatorFactory.
     * The OutputOperatorFactory will get collectedPages from LookupDriverContext via CollectedPagesProvider.
     */
    private PhysicalOperation planOutputExec(OutputExec outputExec, PhysicalOperation source) {
        var output = outputExec.output();
        return source.withSink(new OutputOperatorFactory(Expressions.names(output), Function.identity(), page -> {}), source.layout());
    }

    /**
     * Factory for LookupQueryOperator.
     * Creates an intermediate operator that processes match field pages from ExchangeSourceOperator
     * and generates queries to lookup document IDs.
     */
    private record LookupQueryOperatorFactory(
        int maxPageSize,
        BlockOptimization blockOptimization,
        int shardId,
        List<MatchConfig> matchFields,
        @Nullable Expression joinOnConditions,
        @Nullable QueryBuilder query,
        Source planSource,
        QueryListFromPlanFactory queryListFromPlanFactory,
        boolean emptyResult
    ) implements OperatorFactory {
        @Override
        public Operator get(DriverContext driverContext) {
            LookupDriverContext lookupDriverContext = (LookupDriverContext) driverContext;
            ShardContext shardContext = lookupDriverContext.shardContext();
            SearchExecutionContext searchExecutionContext = lookupDriverContext.searchExecutionContext();
            IndexedByShardId<? extends ShardContext> shardContexts = new IndexedByShardIdFromSingleton<>(shardContext, shardId);

            Warnings warnings = Warnings.createWarnings(DriverContext.WarningsMode.COLLECT, planSource);
            QueryBuilder rewrittenQuery = rewriteQuery(query, searchExecutionContext);
            LookupEnrichQueryGenerator queryList = queryListFromPlanFactory.create(
                matchFields,
                joinOnConditions,
                rewrittenQuery,
                searchExecutionContext,
                lookupDriverContext.aliasFilter(),
                warnings
            );

            return new LookupQueryOperator(
                driverContext.blockFactory(),
                maxPageSize,
                queryList,
                shardContexts,
                shardId,
                searchExecutionContext,
                warnings,
                emptyResult
            );
        }

        @Override
        public String describe() {
            return "LookupQueryOperator[maxPageSize=" + maxPageSize + ", emptyResult=" + emptyResult + "]";
        }
    }

    private static QueryBuilder rewriteQuery(@Nullable QueryBuilder query, SearchExecutionContext searchExecutionContext) {
        if (query == null) {
            return null;
        }
        try {
            return Rewriteable.rewrite(query, searchExecutionContext, true);
        } catch (IOException e) {
            throw new UncheckedIOException("Error while rewriting pushed query for lookup", e);
        }
    }

}
