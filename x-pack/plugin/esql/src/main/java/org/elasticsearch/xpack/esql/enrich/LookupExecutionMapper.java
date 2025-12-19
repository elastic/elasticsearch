/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.enrich;

import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.ElementType;
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
import org.elasticsearch.compute.operator.ProjectOperator;
import org.elasticsearch.compute.operator.SinkOperator;
import org.elasticsearch.compute.operator.SourceOperator;
import org.elasticsearch.compute.operator.SourceOperator.SourceOperatorFactory;
import org.elasticsearch.compute.operator.Warnings;
import org.elasticsearch.compute.operator.lookup.BlockOptimization;
import org.elasticsearch.compute.operator.lookup.EnrichQuerySourceOperator;
import org.elasticsearch.compute.operator.lookup.LookupEnrichQueryGenerator;
import org.elasticsearch.compute.operator.lookup.MergePositionsOperator;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.index.mapper.BlockLoader;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.xpack.esql.EsqlIllegalArgumentException;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.plan.physical.FieldExtractExec;
import org.elasticsearch.xpack.esql.plan.physical.LookupMergeDropExec;
import org.elasticsearch.xpack.esql.plan.physical.OutputExec;
import org.elasticsearch.xpack.esql.plan.physical.ParameterizedQueryExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.UnaryExec;
import org.elasticsearch.xpack.esql.planner.EsPhysicalOperationProviders;
import org.elasticsearch.xpack.esql.planner.Layout;
import org.elasticsearch.xpack.esql.planner.LocalExecutionPlanner.PhysicalOperation;
import org.elasticsearch.xpack.esql.planner.PlannerUtils;
import org.elasticsearch.xpack.esql.plugin.EsqlPlugin;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;

/**
 * Plans the execution of a lookup physical plan
 * Converts PhysicalPlan nodes into PhysicalOperation with operator factories
 * Also converts PhysicalOperation into actual Operators
 */
public class LookupExecutionMapper {
    private final BlockFactory blockFactory;
    private final BigArrays bigArrays;
    private final boolean mergePages;

    /**
     * Extended DriverContext that provides access to ShardContext, LookupShardContext, collectedPages, and inputPage.
     * This allows factories to retrieve ShardContext, SearchExecutionContext (from LookupShardContext), collectedPages, and inputPage at operator creation time.
     * Used by both LookupExecutionMapper and AbstractLookupService.
     * SearchExecutionContext is retrieved dynamically from LookupShardContext to avoid caching stale references.
     */
    static class LookupDriverContext extends DriverContext implements CollectedPagesProvider {
        private final ShardContext shardContext;
        private final AbstractLookupService.LookupShardContext lookupShardContext;
        private final List<Page> collectedPages;
        private final Page inputPage;

        LookupDriverContext(
            BigArrays bigArrays,
            BlockFactory blockFactory,
            ShardContext shardContext,
            AbstractLookupService.LookupShardContext lookupShardContext,
            List<Page> collectedPages,
            Page inputPage
        ) {
            super(bigArrays, blockFactory);
            this.shardContext = shardContext;
            this.lookupShardContext = lookupShardContext;
            this.collectedPages = collectedPages;
            this.inputPage = inputPage;
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
    }

    public LookupExecutionMapper(
        BlockFactory blockFactory,
        BigArrays bigArrays,
        LocalCircuitBreaker.SizeSettings localBreakerSettings,
        boolean mergePages
    ) {
        this.blockFactory = blockFactory;
        this.bigArrays = bigArrays;
        this.mergePages = mergePages;
    }

    /**
     * Creates a PhysicalOperation with operator factories, matching LocalExecutionPlanner's pattern.
     */
    public PhysicalOperation buildOperatorFactories(
        AbstractLookupService.TransportRequest request,
        PhysicalPlan physicalPlan,
        AbstractLookupService.LookupShardContext shardContext,
        Warnings warnings,
        BlockOptimization blockOptimization
    ) throws IOException {
        return planLookupNode(physicalPlan, request, shardContext, warnings, blockOptimization);
    }

    /**
     * Creates the actual operators from the PhysicalOperation using DriverContext.
     * The collectedPages list is created here and stored in LookupDriverContext.
     */
    public AbstractLookupService.LookupQueryPlan buildOperators(
        PhysicalOperation physicalOperation,
        AbstractLookupService.LookupShardContext shardContext,
        LocalCircuitBreaker localBreaker,
        List<Releasable> releasables,
        Page inputPage
    ) {
        // Create collectedPages list that will be used by OutputOperatorFactory
        List<Page> collectedPages = Collections.synchronizedList(new ArrayList<>());

        // Create extended driver context that provides access to ShardContext, LookupShardContext, collectedPages, and inputPage
        // Note: inputPage and lookupShardContext are from the current request (not cached). The request lifecycle (request.incRef/decRef)
        // keeps inputPage blocks alive until the driver finishes and listener completes.
        // SearchExecutionContext is retrieved dynamically from LookupShardContext to avoid caching stale references.
        LookupDriverContext driverContext = new LookupDriverContext(
            bigArrays,
            blockFactory.newChildFactory(localBreaker),
            shardContext.context(),
            shardContext,
            collectedPages,
            inputPage
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

        return new AbstractLookupService.LookupQueryPlan(
            shardContext,
            localBreaker,
            driverContext,
            (EnrichQuerySourceOperator) sourceOperator,
            intermediateOperators,
            collectedPages,
            (OutputOperator) sinkOperator
        );
    }

    /**
     * Recursively plans a PhysicalPlan node into a PhysicalOperation, processing children first.
     */
    private PhysicalOperation planLookupNode(
        PhysicalPlan node,
        AbstractLookupService.TransportRequest request,
        AbstractLookupService.LookupShardContext shardContext,
        Warnings warnings,
        BlockOptimization optimizationState
    ) throws IOException {
        PhysicalOperation source;
        if (node instanceof UnaryExec unaryExec) {
            source = planLookupNode(unaryExec.child(), request, shardContext, warnings, optimizationState);
        } else {
            source = null;
        }

        // Plan this node based on its type
        if (node instanceof ParameterizedQueryExec parameterizedQueryExec) {
            return planParameterizedQueryExec(parameterizedQueryExec, request, shardContext, warnings, optimizationState);
        } else if (node instanceof FieldExtractExec fieldExtractExec) {
            return planFieldExtractExec(fieldExtractExec, source, shardContext);
        } else if (node instanceof LookupMergeDropExec lookupMergeDropExec) {
            return planLookupMergeDropExec(lookupMergeDropExec, source, optimizationState);
        } else if (node instanceof OutputExec outputExec) {
            return planOutputExec(outputExec, source);
        } else {
            throw new EsqlIllegalArgumentException("unknown physical plan node [" + node.nodeName() + "]");
        }
    }

    /**
     * Plans ParameterizedQueryExec into a PhysicalOperation with EnrichQuerySourceOperatorFactory.
     */
    private PhysicalOperation planParameterizedQueryExec(
        ParameterizedQueryExec parameterizedQueryExec,
        AbstractLookupService.TransportRequest request,
        AbstractLookupService.LookupShardContext shardContext,
        Warnings warnings,
        BlockOptimization optimizationState
    ) {
        LookupEnrichQueryGenerator queryList = parameterizedQueryExec.queryList();

        Layout.Builder layoutBuilder = new Layout.Builder();
        List<Attribute> output = parameterizedQueryExec.output();
        for (Attribute attr : output) {
            layoutBuilder.append(attr);
        }
        Layout layout = layoutBuilder.build();

        return PhysicalOperation.fromSource(
            new EnrichQuerySourceOperatorFactory(
                EnrichQuerySourceOperator.DEFAULT_MAX_PAGE_SIZE,
                queryList,
                optimizationState,
                0,
                warnings
            ),
            layout
        );
    }

    /**
     * Plans FieldExtractExec into a PhysicalOperation with ValuesSourceReaderOperatorFactory.
     */
    private PhysicalOperation planFieldExtractExec(
        FieldExtractExec fieldExtractExec,
        PhysicalOperation source,
        AbstractLookupService.LookupShardContext shardContext
    ) {
        EsPhysicalOperationProviders.ShardContext esShardContext = shardContext.context();
        List<ValuesSourceReaderOperator.FieldInfo> fields = new ArrayList<>(fieldExtractExec.attributesToExtract().size());
        for (NamedExpression extractField : fieldExtractExec.attributesToExtract()) {
            String fieldName = extractFieldName(extractField);
            BlockLoader loader = esShardContext.blockLoader(
                fieldName,
                extractField.dataType() == DataType.UNSUPPORTED,
                MappedFieldType.FieldExtractPreference.NONE,
                null
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
                        return loader;
                    }
                )
            );
        }

        Layout.Builder layoutBuilder = new Layout.Builder();
        for (NamedExpression extractField : fieldExtractExec.attributesToExtract()) {
            layoutBuilder.append(extractField.toAttribute());
        }
        Layout layout = layoutBuilder.build();

        // Create a factory that will build ShardContext dynamically from LookupDriverContext
        // to avoid caching stale IndexReader references when PhysicalOperation is cached
        return source.with(
            new ValuesSourceReaderOperatorFactoryWithDynamicContext(
                org.elasticsearch.common.unit.ByteSizeValue.ofBytes(Long.MAX_VALUE),
                fields,
                0
            ),
            layout
        );
    }

    /**
     * Factory for ValuesSourceReaderOperator that creates ShardContext dynamically from LookupDriverContext
     * to avoid caching stale IndexReader references when PhysicalOperation is cached.
     */
    private record ValuesSourceReaderOperatorFactoryWithDynamicContext(
        org.elasticsearch.common.unit.ByteSizeValue jumboSize,
        List<ValuesSourceReaderOperator.FieldInfo> fields,
        int docChannel
    ) implements OperatorFactory {
        @Override
        public Operator get(DriverContext driverContext) {
            // Get ShardContext from LookupDriverContext to ensure we use the current, valid IndexReader
            if (driverContext instanceof LookupDriverContext lookupDriverContext) {
                EsPhysicalOperationProviders.ShardContext esShardContext = lookupDriverContext.lookupShardContext().context();
                IndexedByShardId<ValuesSourceReaderOperator.ShardContext> shardContexts = new IndexedByShardIdFromSingleton<>(
                    new ValuesSourceReaderOperator.ShardContext(
                        esShardContext.searcher().getIndexReader(),
                        esShardContext::newSourceLoader,
                        EsqlPlugin.STORED_FIELDS_SEQUENTIAL_PROPORTION.getDefault(org.elasticsearch.common.settings.Settings.EMPTY)
                    )
                );
                return new ValuesSourceReaderOperator(
                    driverContext.blockFactory(),
                    jumboSize.getBytes(),
                    fields,
                    shardContexts,
                    docChannel
                );
            } else {
                throw new IllegalStateException("Expected LookupDriverContext but got " + driverContext.getClass().getName());
            }
        }

        @Override
        public String describe() {
            StringBuilder sb = new StringBuilder();
            sb.append("ValuesSourceReaderOperator[fields = [");
            if (fields.size() < 10) {
                boolean first = true;
                for (ValuesSourceReaderOperator.FieldInfo f : fields) {
                    if (first) {
                        first = false;
                    } else {
                        sb.append(", ");
                    }
                    sb.append(f.name());
                }
            } else {
                sb.append(fields.size()).append(" fields");
            }
            sb.append("]]");
            return sb.toString();
        }
    }

    /**
     * Plans LookupMergeDropExec into a PhysicalOperation with MergePositionsOperatorFactory or ProjectOperatorFactory.
     */
    private PhysicalOperation planLookupMergeDropExec(
        LookupMergeDropExec lookupMergeDropExec,
        PhysicalOperation source,
        BlockOptimization blockOptimization
    ) {
        // Build layout with just the extract fields (output of this operation)
        Layout.Builder layoutBuilder = new Layout.Builder();
        for (NamedExpression extractField : lookupMergeDropExec.extractFields()) {
            layoutBuilder.append(extractField.toAttribute());
        }
        Layout layout = layoutBuilder.build();

        if (mergePages && lookupMergeDropExec.mergingChannels().length > 0 && blockOptimization != BlockOptimization.NONE) {
            return source.with(
                new MergePositionsOperatorFactory(
                    1,
                    lookupMergeDropExec.mergingChannels(),
                    lookupMergeDropExec.mergingTypes(),
                    blockOptimization
                ),
                layout
            );
        }
        // No merge: just drop doc block
        int end = lookupMergeDropExec.extractFields().size() + 1;
        List<Integer> projection = new ArrayList<>(end);
        for (int i = 1; i <= end; i++) {
            projection.add(i);
        }
        return source.with(new ProjectOperator.ProjectOperatorFactory(projection), layout);
    }

    /**
     * Plans OutputExec into a PhysicalOperation with OutputOperatorFactory.
     * The factory will get collectedPages from LookupDriverContext.
     */
    private PhysicalOperation planOutputExec(OutputExec outputExec, PhysicalOperation source) {
        var output = outputExec.output();
        return source.withSink(new OutputOperatorFactory(Expressions.names(output), Function.identity(), null), source.layout());
    }

    /**
     * Factory for EnrichQuerySourceOperator.
     * Creates optimized page when needed (DICTIONARY state) during operator creation.
     * Retrieves ShardContext from LookupDriverContext at operator creation time to avoid capturing closed resources.
     */
    private record EnrichQuerySourceOperatorFactory(
        int maxPageSize,
        LookupEnrichQueryGenerator queryList,
        BlockOptimization blockOptimization,
        int shardId,
        Warnings warnings
    ) implements SourceOperatorFactory {
        @Override
        public SourceOperator get(DriverContext driverContext) {
            // Get ShardContext, SearchExecutionContext, and inputPage from LookupDriverContext
            if (driverContext instanceof LookupDriverContext lookupDriverContext) {
                ShardContext shardContext = lookupDriverContext.shardContext();
                SearchExecutionContext searchExecutionContext = lookupDriverContext.searchExecutionContext();
                Page inputPage = lookupDriverContext.inputPage();
                IndexedByShardId<? extends ShardContext> shardContexts = new IndexedByShardIdFromSingleton<>(shardContext, shardId);

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
            } else {
                throw new IllegalStateException("Expected LookupDriverContext but got " + driverContext.getClass().getName());
            }
        }

        @Override
        public String describe() {
            return "EnrichQuerySourceOperator[maxPageSize=" + maxPageSize + "]";
        }
    }

    /**
     * Factory for MergePositionsOperator.
     */
    private record MergePositionsOperatorFactory(
        int positionChannel,
        int[] mergingChannels,
        ElementType[] mergingTypes,
        BlockOptimization blockOptimization
    ) implements OperatorFactory {
        @Override
        public Operator get(DriverContext driverContext) {
            // Get inputPage from LookupDriverContext
            if (driverContext instanceof LookupDriverContext lookupDriverContext) {
                Page inputPage = lookupDriverContext.inputPage();
                return new MergePositionsOperator(
                    positionChannel,
                    mergingChannels,
                    mergingTypes,
                    blockOptimization,
                    inputPage,
                    driverContext.blockFactory()
                );
            } else {
                throw new IllegalStateException("Expected LookupDriverContext but got " + driverContext.getClass().getName());
            }
        }

        @Override
        public String describe() {
            return "MergePositionsOperator[positionChannel="
                + positionChannel
                + ", mergingChannels="
                + Arrays.toString(mergingChannels)
                + "]";
        }
    }

    /**
     * Determines the optimization applicable for the input page.
     * Returns the state that indicates what optimization can be applied:
     * - DICTIONARY: input block has ordinals, can use dictionary block and ordinals
     * - RANGE: need to create range block for selected positions
     * - NONE: no optimization needed
     */
    public BlockOptimization determineOptimization(Page inputPage) {
        if (mergePages == false) {
            return BlockOptimization.NONE;
        }
        Block inputBlock = inputPage.getBlock(0);
        if (inputBlock instanceof BytesRefBlock bytesRefBlock && bytesRefBlock.asOrdinals() != null) {
            return BlockOptimization.DICTIONARY;
        }
        return BlockOptimization.RANGE;
    }

    /**
     * Extracts field name from a NamedExpression, handling FieldAttribute and Alias cases.
     */
    private String extractFieldName(NamedExpression extractField) {
        return extractField instanceof FieldAttribute fa ? fa.fieldName().string()
            : extractField instanceof Alias a ? ((NamedExpression) a.child()).name()
            : extractField.name();
    }

}
