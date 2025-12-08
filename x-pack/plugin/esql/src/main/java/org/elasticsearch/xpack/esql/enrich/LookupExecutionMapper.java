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
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.LocalCircuitBreaker;
import org.elasticsearch.compute.data.OrdinalBytesRefBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.lucene.IndexedByShardIdFromSingleton;
import org.elasticsearch.compute.lucene.read.ValuesSourceReaderOperator;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.compute.operator.OutputOperator;
import org.elasticsearch.compute.operator.ProjectOperator;
import org.elasticsearch.compute.operator.SinkOperator;
import org.elasticsearch.compute.operator.SourceOperator;
import org.elasticsearch.compute.operator.Warnings;
import org.elasticsearch.compute.operator.lookup.EnrichQuerySourceOperator;
import org.elasticsearch.compute.operator.lookup.LookupEnrichQueryGenerator;
import org.elasticsearch.compute.operator.lookup.MergePositionsOperator;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.index.mapper.BlockLoader;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.xpack.esql.EsqlIllegalArgumentException;
import org.elasticsearch.xpack.esql.core.expression.Alias;
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
import org.elasticsearch.xpack.esql.planner.PlannerUtils;
import org.elasticsearch.xpack.esql.plugin.EsqlPlugin;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;

/**
 * Plans the execution of a lookup physical plan by converting it to operators.
 */
public class LookupExecutionMapper {
    private final BlockFactory blockFactory;
    private final BigArrays bigArrays;
    private final LocalCircuitBreaker.SizeSettings localBreakerSettings;
    private final boolean mergePages;

    public LookupExecutionMapper(
        BlockFactory blockFactory,
        BigArrays bigArrays,
        LocalCircuitBreaker.SizeSettings localBreakerSettings,
        boolean mergePages
    ) {
        this.blockFactory = blockFactory;
        this.bigArrays = bigArrays;
        this.localBreakerSettings = localBreakerSettings;
        this.mergePages = mergePages;
    }

    /**
     * Result of block optimization contains optimized page and finish operator details.
     * Implements Releasable to manage ownership of selectedPositions when it's not from dictionary.
     */
    private static final class BlockOptimization implements Releasable {
        private final Page optimizedPage;
        private IntBlock selectedPositions;  // null if no merge, otherwise the selectedPositions block for MergePositionsOperator
        private final boolean selectedPositionsFromDictionary;  // true if selectedPositions comes from ordinals/dictionary

        BlockOptimization(Page optimizedPage, IntBlock selectedPositions, boolean selectedPositionsFromDictionary) {
            this.optimizedPage = optimizedPage;
            this.selectedPositions = selectedPositions;
            this.selectedPositionsFromDictionary = selectedPositionsFromDictionary;
        }

        Page optimizedPage() {
            return optimizedPage;
        }

        IntBlock selectedPositions() {
            return selectedPositions;
        }

        /**
         * Releases ownership of selectedPositions if we own it.
         * Call this when transferring ownership to another component (e.g., MergePositionsOperator).
         * Safe to call multiple times - subsequent calls are no-ops.
         */
        void releaseSelectedPositions() {
            // Only close selectedPositions if we own it (not from dictionary)
            // When selectedPositionsFromDictionary is true, the block is owned by the original block
            if (selectedPositionsFromDictionary == false && selectedPositions != null) {
                Releasables.close(selectedPositions);
                selectedPositions = null;  // Mark as released to prevent double-close
            }
        }

        @Override
        public void close() {
            // Close if we own it. Safe to call even if releaseSelectedPositions() was already called,
            // as setting selectedPositions to null prevents double-close.
            releaseSelectedPositions();
        }
    }

    /**
     * Maps a physical plan to operators for execution.
     */
    public AbstractLookupService.LookupQueryPlan map(
        AbstractLookupService.TransportRequest request,
        PhysicalPlan physicalPlan,
        AbstractLookupService.LookupShardContext shardContext,
        List<Releasable> releasables
    ) throws IOException {
        // Create driver context
        final LocalCircuitBreaker localBreaker = new LocalCircuitBreaker(
            blockFactory.breaker(),
            localBreakerSettings.overReservedBytes(),
            localBreakerSettings.maxOverReservedBytes()
        );
        releasables.add(localBreaker);
        final DriverContext driverContext = new DriverContext(bigArrays, blockFactory.newChildFactory(localBreaker));

        // Create warnings
        var warnings = Warnings.createWarnings(
            DriverContext.WarningsMode.COLLECT,
            request.source.source().getLineNumber(),
            request.source.source().getColumnNumber(),
            request.source.text()
        );

        // Optimize input block and create optimized page
        // Use try-with-resources to ensure optimization is always closed
        try (BlockOptimization optimization = optimizeBlockAndCreatePage(request.inputPage)) {
            List<Page> collectedPages = Collections.synchronizedList(new ArrayList<>());
            List<Operator> operators = new ArrayList<>();
            mapLookupNode(
                physicalPlan,
                shardContext,
                driverContext,
                warnings,
                request,
                optimization.optimizedPage(),
                optimization,
                releasables,
                collectedPages,
                operators
            );

            // Extract source (first), sink (last), and intermediate operators
            if (operators.isEmpty()) {
                throw new IllegalStateException("No operators planned");
            }
            SourceOperator sourceOperator = (SourceOperator) operators.get(0);
            SinkOperator sinkOperator = operators.size() > 1 ? (SinkOperator) operators.get(operators.size() - 1) : null;
            List<Operator> intermediateOperators = operators.size() > 2 ? operators.subList(1, operators.size() - 1) : List.of();

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
    }

    /**
     * Optimizes the input block and creates the optimized page (matches main branch logic).
     * Also determines the selectedPositions for MergePositionsOperator.
     * Structure matches main branch: if ordinals optimization applies, use dictionary block and ordinals;
     * else if mergePages, create range block; else no selectedPositions (dropDocBlockOperator case).
     */
    private BlockOptimization optimizeBlockAndCreatePage(Page inputPage) {
        final OrdinalBytesRefBlock ordinalsBytesRefBlock;
        Block inputBlock = inputPage.getBlock(0);
        Block optimizedBlock = inputBlock;
        IntBlock selectedPositions = null;
        boolean selectedPositionsFromDictionary = false;

        if (mergePages  // TODO fix this optimization for Lookup.
            && inputBlock instanceof BytesRefBlock bytesRefBlock
            && (ordinalsBytesRefBlock = bytesRefBlock.asOrdinals()) != null) {

            optimizedBlock = ordinalsBytesRefBlock.getDictionaryVector().asBlock();
            selectedPositions = ordinalsBytesRefBlock.getOrdinalsBlock();
            selectedPositionsFromDictionary = true;
        } else {
            if (mergePages) {
                selectedPositions = IntVector.range(0, inputBlock.getPositionCount(), blockFactory).asBlock();
            }
        }

        Page optimizedPage = (optimizedBlock != inputBlock) ? createPageWithOptimizedBlock(inputPage, 0, optimizedBlock) : inputPage;

        return new BlockOptimization(optimizedPage, selectedPositions, selectedPositionsFromDictionary);
    }

    /**
     * Recursively maps a PhysicalPlan node to operators, processing children first.
     * Appends operators to the provided list where first is source, last is sink, and middle are intermediate operators.
     */
    private void mapLookupNode(
        PhysicalPlan node,
        AbstractLookupService.LookupShardContext shardContext,
        DriverContext driverContext,
        Warnings warnings,
        AbstractLookupService.TransportRequest request,
        Page inputPage,
        BlockOptimization optimization,
        List<Releasable> releasables,
        List<Page> collectedPages,
        List<Operator> operators
    ) throws IOException {
        if (node instanceof UnaryExec unaryExec) {
            mapLookupNode(
                unaryExec.child(),
                shardContext,
                driverContext,
                warnings,
                request,
                inputPage,
                optimization,
                releasables,
                collectedPages,
                operators
            );
        }

        // Map this node based on its type
        if (node instanceof OutputExec) {
            mapOutputExec(operators, collectedPages, releasables);
        } else if (node instanceof LookupMergeDropExec lookupMergeDropExec) {
            mapLookupMergeDropExec(lookupMergeDropExec, operators, driverContext, optimization, releasables);
        } else if (node instanceof FieldExtractExec fieldExtractExec) {
            mapFieldExtractExec(fieldExtractExec, operators, shardContext, driverContext, releasables);
        } else if (node instanceof ParameterizedQueryExec parameterizedQueryExec) {
            mapParameterizedQueryExec(parameterizedQueryExec, shardContext, driverContext, warnings, inputPage, releasables, operators);
        } else {
            throw new EsqlIllegalArgumentException("unknown physical plan node [" + node.nodeName() + "]");
        }
    }

    private void mapOutputExec(List<Operator> operators, List<Page> collectedPages, List<Releasable> releasables) {
        OutputOperator outputOperator = new OutputOperator(List.of(), Function.identity(), collectedPages::add);
        releasables.add(outputOperator);

        if (operators == null || operators.isEmpty()) {
            throw new IllegalStateException("Operators cannot be null or empty");
        }
        operators.add(outputOperator);
    }

    private void mapLookupMergeDropExec(
        LookupMergeDropExec lookupMergeDropExec,
        List<Operator> operators,
        DriverContext driverContext,
        BlockOptimization optimization,
        List<Releasable> releasables
    ) {
        // Use pre-computed optimization result to create finish operator
        Operator finishOperator = createFinishOperator(lookupMergeDropExec, driverContext, optimization, releasables);
        operators.add(finishOperator);
    }

    private void mapFieldExtractExec(
        FieldExtractExec fieldExtractExec,
        List<Operator> operators,
        AbstractLookupService.LookupShardContext shardContext,
        DriverContext driverContext,
        List<Releasable> releasables
    ) {

        EsPhysicalOperationProviders.ShardContext esShardContext = shardContext.context();
        List<ValuesSourceReaderOperator.FieldInfo> fields = new ArrayList<>(fieldExtractExec.attributesToExtract().size());
        for (NamedExpression extractField : fieldExtractExec.attributesToExtract()) {
            String fieldName = extractField instanceof FieldAttribute fa ? fa.fieldName().string()
                : extractField instanceof Alias a ? ((NamedExpression) a.child()).name()
                : extractField.name();
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
        ValuesSourceReaderOperator operator = new ValuesSourceReaderOperator(
            driverContext.blockFactory(),
            Long.MAX_VALUE,
            fields,
            new IndexedByShardIdFromSingleton<>(
                new ValuesSourceReaderOperator.ShardContext(
                    esShardContext.searcher().getIndexReader(),
                    esShardContext::newSourceLoader,
                    EsqlPlugin.STORED_FIELDS_SEQUENTIAL_PROPORTION.getDefault(org.elasticsearch.common.settings.Settings.EMPTY)
                )
            ),
            0
        );
        releasables.add(operator);
        operators.add(operator);
    }

    private void mapParameterizedQueryExec(
        ParameterizedQueryExec parameterizedQueryExec,
        AbstractLookupService.LookupShardContext shardContext,
        DriverContext driverContext,
        Warnings warnings,
        Page inputPage,
        List<Releasable> releasables,
        List<Operator> operators
    ) {
        // inputPage is already optimized (from BlockOptimization)
        LookupEnrichQueryGenerator queryList = parameterizedQueryExec.queryList();

        EnrichQuerySourceOperator sourceOperator = new EnrichQuerySourceOperator(
            driverContext.blockFactory(),
            EnrichQuerySourceOperator.DEFAULT_MAX_PAGE_SIZE,
            queryList,
            inputPage,  // Use optimized page (already optimized in planExecution)
            new IndexedByShardIdFromSingleton<>(shardContext.context()),
            0,
            warnings
        );
        releasables.add(sourceOperator);

        // Add source operator: [source]
        operators.add(sourceOperator);
    }

    /**
     * Creates a new Page with the optimized block replacing the block at the specified channelOffset.
     */
    private Page createPageWithOptimizedBlock(Page originalPage, int channelOffset, Block optimizedBlock) {
        Block[] blocks = new Block[originalPage.getBlockCount()];
        for (int i = 0; i < blocks.length; i++) {
            blocks[i] = (i == channelOffset) ? optimizedBlock : originalPage.getBlock(i);
        }
        return new Page(blocks);
    }

    /**
     * Creates the finish operator (MergePositionsOperator or dropDocBlockOperator) using pre-computed optimization.
     */
    private Operator createFinishOperator(
        LookupMergeDropExec lookupMergeDropExec,
        DriverContext driverContext,
        BlockOptimization optimization,
        List<Releasable> releasables
    ) {
        if (mergePages) {
            // Use pre-computed selectedPositions from optimization
            IntBlock selectedPositions = optimization.selectedPositions();
            if (selectedPositions != null && lookupMergeDropExec.mergingChannels().length > 0) {
                Operator finishOperator = new MergePositionsOperator(
                    1,
                    lookupMergeDropExec.mergingChannels(),
                    lookupMergeDropExec.mergingTypes(),
                    selectedPositions,
                    driverContext.blockFactory()
                );
                // MergePositionsOperator calls mustIncRef() on selectedPositions, taking ownership
                // Release our reference to selectedPositions if we own it
                optimization.releaseSelectedPositions();
                releasables.add(finishOperator);
                return finishOperator;
            }
        }
        // No merge: just drop doc block
        Operator finishOperator = dropDocBlockOperator(lookupMergeDropExec.extractFields());
        releasables.add(finishOperator);
        return finishOperator;
    }

    /**
     * Drop just the first block, keeping the remaining.
     */
    private Operator dropDocBlockOperator(List<NamedExpression> extractFields) {
        int end = extractFields.size() + 1;
        List<Integer> projection = new ArrayList<>(end);
        for (int i = 1; i <= end; i++) {
            projection.add(i);
        }
        return new ProjectOperator(projection);
    }
}
