/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.enrich;

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
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.compute.operator.Operator.OperatorFactory;
import org.elasticsearch.compute.operator.OutputOperator;
import org.elasticsearch.compute.operator.OutputOperator.CollectedPagesProvider;
import org.elasticsearch.compute.operator.OutputOperator.OutputOperatorFactory;
import org.elasticsearch.compute.operator.SinkOperator;
import org.elasticsearch.compute.operator.SourceOperator;
import org.elasticsearch.compute.operator.SourceOperator.SourceOperatorFactory;
import org.elasticsearch.compute.operator.Warnings;
import org.elasticsearch.compute.operator.lookup.BlockOptimization;
import org.elasticsearch.compute.operator.lookup.EnrichQuerySourceOperator;
import org.elasticsearch.compute.operator.lookup.LookupEnrichQueryGenerator;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.index.mapper.BlockLoader;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.search.internal.AliasFilter;
import org.elasticsearch.xpack.esql.EsqlIllegalArgumentException;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.plan.physical.FieldExtractExec;
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
     * Functional interface for creating a LookupEnrichQueryGenerator from the necessary parameters.
     */
    @FunctionalInterface
    public interface QueryListFactory {
        LookupEnrichQueryGenerator create(
            AbstractLookupService.TransportRequest request,
            SearchExecutionContext searchExecutionContext,
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
        private final AbstractLookupService.TransportRequest request;
        private final AliasFilter aliasFilter;
        private final QueryListFactory queryListFactory;

        LookupDriverContext(
            BigArrays bigArrays,
            BlockFactory blockFactory,
            ShardContext shardContext,
            AbstractLookupService.LookupShardContext lookupShardContext,
            List<Page> collectedPages,
            Page inputPage,
            AbstractLookupService.TransportRequest request,
            AliasFilter aliasFilter,
            QueryListFactory queryListFactory,
            LocalCircuitBreaker.SizeSettings localBreakerSettings
        ) {
            super(bigArrays, blockFactory, localBreakerSettings);
            this.shardContext = shardContext;
            this.lookupShardContext = lookupShardContext;
            this.collectedPages = collectedPages;
            this.inputPage = inputPage;
            this.request = request;
            this.aliasFilter = aliasFilter;
            this.queryListFactory = queryListFactory;
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

        AbstractLookupService.TransportRequest request() {
            return request;
        }

        AliasFilter aliasFilter() {
            return aliasFilter;
        }

        QueryListFactory queryListFactory() {
            return queryListFactory;
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
     */
    public PhysicalOperation buildOperatorFactories(
        PlannerSettings plannerSettings,
        AbstractLookupService.TransportRequest request,
        PhysicalPlan physicalPlan,
        BlockOptimization blockOptimization
    ) throws IOException {
        return planLookupNode(plannerSettings, physicalPlan, request, blockOptimization);
    }

    /**
     * Creates the actual operators from the PhysicalOperation using DriverContext.
     * The collectedPages list is created here and stored in LookupDriverContext.
     */
    public LookupFromIndexService.LookupQueryPlan buildOperators(
        PhysicalOperation physicalOperation,
        AbstractLookupService.LookupShardContext shardContext,
        List<Releasable> releasables,
        AbstractLookupService.TransportRequest request,
        AliasFilter aliasFilter,
        QueryListFactory queryListFactory
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
            request.inputPage,
            request,
            aliasFilter,
            queryListFactory,
            localBreakerSettings
        );

        // Create operators from factories
        SourceOperator sourceOperator = physicalOperation.source(driverContext);
        releasables.add(sourceOperator);

        List<Operator> intermediateOperators = new ArrayList<>();
        physicalOperation.operators(intermediateOperators, driverContext);
        for (Operator op : intermediateOperators) {
            releasables.add(op);
        }

        SinkOperator sinkOperator = physicalOperation.sink(driverContext);
        releasables.add(sinkOperator);

        return new LookupFromIndexService.LookupQueryPlan(
            shardContext,
            localBreaker,
            driverContext,
            sourceOperator,
            intermediateOperators,
            collectedPages,
            (OutputOperator) sinkOperator
        );
    }

    /**
     * Recursively plans a PhysicalPlan node into a PhysicalOperation, processing children first.
     */
    private PhysicalOperation planLookupNode(
        PlannerSettings plannerSettings,
        PhysicalPlan node,
        AbstractLookupService.TransportRequest request,
        BlockOptimization optimizationState
    ) throws IOException {
        PhysicalOperation source;
        if (node instanceof UnaryExec unaryExec) {
            source = planLookupNode(plannerSettings, unaryExec.child(), request, optimizationState);
        } else {
            // there could be a leaf node such as ParameterizedQueryExec
            source = null;
        }

        // Plan this node based on its type
        if (node instanceof ParameterizedQueryExec parameterizedQueryExec) {
            return planParameterizedQueryExec(parameterizedQueryExec, optimizationState);
        } else if (node instanceof FieldExtractExec fieldExtractExec) {
            return planFieldExtractExec(plannerSettings, fieldExtractExec, source);
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
        BlockOptimization optimizationState
    ) {
        Layout.Builder layoutBuilder = new Layout.Builder();
        List<Attribute> output = parameterizedQueryExec.output();
        for (Attribute attr : output) {
            layoutBuilder.append(attr);
        }
        Layout layout = layoutBuilder.build();

        return PhysicalOperation.fromSource(
            new EnrichQuerySourceOperatorFactory(EnrichQuerySourceOperator.DEFAULT_MAX_PAGE_SIZE, optimizationState, 0),
            layout
        );
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
        ByteSizeValue jumboSize = ByteSizeValue.ofBytes(Long.MAX_VALUE);
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
                        plannerSettings.blockLoaderSizeOrdinals(),
                        plannerSettings.blockLoaderSizeScript()

                    );
                    fields.add(
                        new ValuesSourceReaderOperator.FieldInfo(
                            extractField.name(),
                            PlannerUtils.toElementType(extractField.dataType()),
                            false,
                            shardIdx -> {
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
                return new ValuesSourceReaderOperator(driverContext, jumboSize.getBytes(), fields, shardContexts, true, docChannel);
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

    private record EnrichQuerySourceOperatorFactory(int maxPageSize, BlockOptimization blockOptimization, int shardId)
        implements
            SourceOperatorFactory {
        @Override
        public SourceOperator get(DriverContext driverContext) {
            // In lookup execution path, driverContext is always LookupDriverContext
            LookupDriverContext lookupDriverContext = (LookupDriverContext) driverContext;
            ShardContext shardContext = lookupDriverContext.shardContext();
            SearchExecutionContext searchExecutionContext = lookupDriverContext.searchExecutionContext();
            Page inputPage = lookupDriverContext.inputPage();
            IndexedByShardId<? extends ShardContext> shardContexts = new IndexedByShardIdFromSingleton<>(shardContext, shardId);

            Warnings warnings = Warnings.createWarnings(driverContext.warningsMode(), lookupDriverContext.request().source);

            LookupEnrichQueryGenerator queryList = lookupDriverContext.queryListFactory()
                .create(lookupDriverContext.request(), searchExecutionContext, lookupDriverContext.aliasFilter(), warnings);

            return new EnrichQuerySourceOperator(
                driverContext.blockFactory(),
                maxPageSize,
                queryList,
                inputPage,
                blockOptimization,
                shardContexts,
                shardId,
                searchExecutionContext,
                warnings
            );
        }

        @Override
        public String describe() {
            return "EnrichQuerySourceOperator[maxPageSize=" + maxPageSize + "]";
        }
    }

}
