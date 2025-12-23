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
import org.elasticsearch.compute.lucene.IndexedByShardIdFromSingleton;
import org.elasticsearch.compute.lucene.ShardContext;
import org.elasticsearch.compute.lucene.read.ValuesSourceReaderOperator;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.compute.operator.Operator.OperatorFactory;
import org.elasticsearch.compute.operator.OutputOperator;
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
        BlockOptimization blockOptimization,
        List<Page> collectedPages
    ) throws IOException {
        return planLookupNode(physicalPlan, request, shardContext, warnings, blockOptimization, collectedPages);
    }

    /**
     * Creates the actual operators from the PhysicalOperation using DriverContext.
     */
    public AbstractLookupService.LookupQueryPlan buildOperators(
        PhysicalOperation physicalOperation,
        AbstractLookupService.LookupShardContext shardContext,
        LocalCircuitBreaker localBreaker,
        List<Page> collectedPages,
        List<Releasable> releasables
    ) {
        // Create driver context
        DriverContext driverContext = new DriverContext(bigArrays, blockFactory.newChildFactory(localBreaker));

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
        BlockOptimization optimizationState,
        List<Page> collectedPages
    ) throws IOException {
        PhysicalOperation source;
        if (node instanceof UnaryExec unaryExec) {
            source = planLookupNode(unaryExec.child(), request, shardContext, warnings, optimizationState, collectedPages);
        } else {
            source = null;
        }

        // Plan this node based on its type
        if (node instanceof ParameterizedQueryExec parameterizedQueryExec) {
            return planParameterizedQueryExec(parameterizedQueryExec, request, shardContext, warnings, optimizationState);
        } else if (node instanceof FieldExtractExec fieldExtractExec) {
            return planFieldExtractExec(fieldExtractExec, source, shardContext);
        } else if (node instanceof LookupMergeDropExec lookupMergeDropExec) {
            return planLookupMergeDropExec(lookupMergeDropExec, source, optimizationState, request.inputPage);
        } else if (node instanceof OutputExec outputExec) {
            return planOutputExec(outputExec, source, collectedPages);
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
                request.inputPage,
                optimizationState,
                new IndexedByShardIdFromSingleton<>(shardContext.context()),
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

        return source.with(
            new ValuesSourceReaderOperator.Factory(
                org.elasticsearch.common.unit.ByteSizeValue.ofBytes(Long.MAX_VALUE),
                fields,
                new IndexedByShardIdFromSingleton<>(
                    new ValuesSourceReaderOperator.ShardContext(
                        esShardContext.searcher().getIndexReader(),
                        esShardContext::newSourceLoader,
                        EsqlPlugin.STORED_FIELDS_SEQUENTIAL_PROPORTION.getDefault(org.elasticsearch.common.settings.Settings.EMPTY)
                    )
                ),
                0
            ),
            layout
        );
    }

    /**
     * Plans LookupMergeDropExec into a PhysicalOperation with MergePositionsOperatorFactory or ProjectOperatorFactory.
     */
    private PhysicalOperation planLookupMergeDropExec(
        LookupMergeDropExec lookupMergeDropExec,
        PhysicalOperation source,
        BlockOptimization blockOptimization,
        Page inputPage
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
                    blockOptimization,
                    inputPage
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
     */
    private PhysicalOperation planOutputExec(OutputExec outputExec, PhysicalOperation source, List<Page> collectedPages) {
        var output = outputExec.output();
        return source.withSink(
            new OutputOperatorFactory(Expressions.names(output), Function.identity(), collectedPages::add),
            source.layout()
        );
    }

    /**
     * Factory for EnrichQuerySourceOperator.
     * Creates optimized page when needed (DICTIONARY state) during operator creation.
     */
    private record EnrichQuerySourceOperatorFactory(
        int maxPageSize,
        LookupEnrichQueryGenerator queryList,
        Page inputPage,
        BlockOptimization blockOptimization,
        IndexedByShardIdFromSingleton<? extends ShardContext> shardContexts,
        int shardId,
        Warnings warnings
    ) implements SourceOperatorFactory {
        @Override
        public SourceOperator get(DriverContext driverContext) {
            return new EnrichQuerySourceOperator(
                driverContext.blockFactory(),
                maxPageSize,
                queryList,
                inputPage,
                blockOptimization,
                shardContexts,
                shardId,
                warnings
            );
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
        BlockOptimization blockOptimization,
        Page inputPage
    ) implements OperatorFactory {
        @Override
        public Operator get(DriverContext driverContext) {
            return new MergePositionsOperator(
                positionChannel,
                mergingChannels,
                mergingTypes,
                blockOptimization,
                inputPage,
                driverContext.blockFactory()
            );
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
