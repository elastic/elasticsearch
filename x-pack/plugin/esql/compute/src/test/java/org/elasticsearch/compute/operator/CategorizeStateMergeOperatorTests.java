/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.analysis.common.CommonAnalysisPlugin;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.compute.aggregation.blockhash.BlockHash;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.test.CannedSourceOperator;
import org.elasticsearch.compute.test.OperatorTestCase;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.index.analysis.AnalysisRegistry;
import org.elasticsearch.indices.analysis.AnalysisModule;
import org.elasticsearch.plugins.scanners.StablePluginsRegistry;
import org.elasticsearch.xpack.ml.MachineLearning;
import org.hamcrest.Matcher;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.not;

/**
 * Tests for {@link CategorizeStateMergeOperator}: state-channel removal, ID remapping via empty
 * and real categorizer states, multivalued positions, and end-to-end pipeline correctness.
 */
public class CategorizeStateMergeOperatorTests extends OperatorTestCase {

    private static final BlockHash.CategorizeDef CATEGORIZE_DEF = new BlockHash.CategorizeDef(
        null,
        BlockHash.CategorizeDef.OutputFormat.TOKENS,
        70
    );

    /** Two-byte serialization of an empty categorizer: seenNull=false (0x00), count=0 (0x00). */
    private static final BytesRef EMPTY_STATE = new BytesRef(new byte[] { 0, 0 });

    private AnalysisRegistry analysisRegistry;

    @Before
    public void initAnalysisRegistry() throws IOException {
        analysisRegistry = new AnalysisModule(
            TestEnvironment.newEnvironment(
                Settings.builder().put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString()).build()
            ),
            List.of(new MachineLearning(Settings.EMPTY), new CommonAnalysisPlugin()),
            new StablePluginsRegistry()
        ).getAnalysisRegistry();
    }

    /**
     * Input pages have two blocks: channel 0 = IntBlock (local cat IDs), channel 1 = BytesRefBlock (empty state).
     * With an empty state every local ID maps to NULL_ORD (0).
     */
    @Override
    protected SourceOperator simpleInput(BlockFactory blockFactory, int size) {
        List<Page> pages = new ArrayList<>();
        for (int p = 0; p < 2; p++) {
            IntBlock catIds;
            try (IntBlock.Builder builder = blockFactory.newIntBlockBuilder(size)) {
                for (int i = 0; i < size; i++) {
                    builder.appendInt(i % 3 + 1); // positive IDs; empty state will remap these to 0
                }
                catIds = builder.build();
            }
            BytesRefBlock state = blockFactory.newConstantBytesRefBlockWith(EMPTY_STATE, size);
            pages.add(new Page(catIds, state));
        }
        return new CannedSourceOperator(pages.iterator());
    }

    @Override
    protected void assertSimpleOutput(List<Page> input, List<Page> results) {
        assertThat(results.size(), equalTo(input.size()));
        for (int i = 0; i < results.size(); i++) {
            Page inputPage = input.get(i);
            Page resultPage = results.get(i);
            assertThat(resultPage.getPositionCount(), equalTo(inputPage.getPositionCount()));
            // State channel is dropped: block count decreases by one
            assertThat("state channel dropped", resultPage.getBlockCount(), equalTo(inputPage.getBlockCount() - 1));
            // Empty state maps all IDs to NULL_ORD (0)
            IntBlock remapped = resultPage.getBlock(0);
            for (int p = 0; p < resultPage.getPositionCount(); p++) {
                assertThat("local ID remapped to NULL_ORD via empty state", remapped.getInt(remapped.getFirstValueIndex(p)), equalTo(0));
            }
        }
    }

    @Override
    protected Operator.OperatorFactory simple(SimpleOptions options) {
        return new CategorizeStateMergeOperator.Factory(0, 1, CATEGORIZE_DEF);
    }

    @Override
    protected Matcher<String> expectedDescriptionOfSimple() {
        return equalTo("CategorizeStateMergeOperator[catIdChannel=0, stateChannel=1]");
    }

    @Override
    protected Matcher<String> expectedToStringOfSimple() {
        return equalTo("CategorizeStateMergeOperator[catIdChannel=0, stateChannel=1]");
    }

    /** NULL_ORD (0) in local IDs maps to NULL_ORD (0) in global IDs regardless of state. */
    public void testNullOrdPassesThroughUnchanged() {
        DriverContext ctx = driverContext();
        BlockFactory blockFactory = ctx.blockFactory();
        try (CategorizeStateMergeOperator op = new CategorizeStateMergeOperator.Factory(0, 1, CATEGORIZE_DEF).get(ctx)) {
            IntBlock catIds = blockFactory.newConstantIntBlockWith(0, 3);
            BytesRefBlock state = blockFactory.newConstantBytesRefBlockWith(EMPTY_STATE, 3);
            op.addInput(new Page(catIds, state));
            op.finish();
            Page result = op.getOutput();
            assertNotNull(result);
            try {
                IntBlock remapped = result.getBlock(0);
                for (int p = 0; p < 3; p++) {
                    assertThat("NULL_ORD stays 0", remapped.getInt(remapped.getFirstValueIndex(p)), equalTo(0));
                }
            } finally {
                result.releaseBlocks();
            }
        }
    }

    /** State channel (index 1) is absent from the output page. */
    public void testStateChannelDropped() {
        DriverContext ctx = driverContext();
        BlockFactory blockFactory = ctx.blockFactory();
        try (CategorizeStateMergeOperator op = new CategorizeStateMergeOperator.Factory(0, 1, CATEGORIZE_DEF).get(ctx)) {
            IntBlock catIds = blockFactory.newConstantIntBlockWith(1, 2);
            BytesRefBlock state = blockFactory.newConstantBytesRefBlockWith(EMPTY_STATE, 2);
            op.addInput(new Page(catIds, state));
            op.finish();
            Page result = op.getOutput();
            assertNotNull(result);
            try {
                assertThat("output has one block (state dropped)", result.getBlockCount(), equalTo(1));
            } finally {
                result.releaseBlocks();
            }
        }
    }

    /**
     * A real categorizer state: two messages produce two distinct local IDs, the merge operator
     * remaps them to positive global IDs, and identical messages from two separate states share
     * the same global ID.
     */
    public void testRealStateRemapsIdsToGlobalIds() {
        DriverContext ctx = driverContext();
        BlockFactory blockFactory = ctx.blockFactory();

        // Build the categorizer state for ["Connection error", "Disconnected"]
        CategorizeEvalOperator[] holder = new CategorizeEvalOperator[1];
        new CategorizeEvalOperator.Factory(0, CATEGORIZE_DEF, analysisRegistry, holder).get(ctx);
        CategorizeEvalOperator evalOp = holder[0];

        Page evalPage;
        try (BytesRefBlock.Builder builder = blockFactory.newBytesRefBlockBuilder(2)) {
            builder.appendBytesRef(new BytesRef("Connection error"));
            builder.appendBytesRef(new BytesRef("Disconnected"));
            evalOp.addInput(new Page(builder.build()));
            evalOp.finish();
            evalPage = evalOp.getOutput();
        }
        assertNotNull(evalPage);

        // Serialize the state via the emit operator
        CategorizeStateEmitOperator emitOp = new CategorizeStateEmitOperator.Factory(holder).get(ctx);
        IntBlock localCatIds = evalPage.getBlock(1);
        evalPage.getBlock(0).close(); // release the original text block
        emitOp.addInput(new Page(localCatIds));
        emitOp.finish();
        Page emitPage = emitOp.getOutput();
        assertNotNull(emitPage);

        // Feed the emit output through the merge operator
        try (CategorizeStateMergeOperator mergeOp = new CategorizeStateMergeOperator.Factory(0, 1, CATEGORIZE_DEF).get(ctx)) {
            mergeOp.addInput(emitPage);
            mergeOp.finish();
            Page mergeResult = mergeOp.getOutput();
            assertNotNull(mergeResult);
            try {
                assertThat("state channel dropped", mergeResult.getBlockCount(), equalTo(1));
                IntBlock globalIds = mergeResult.getBlock(0);
                int id0 = globalIds.getInt(globalIds.getFirstValueIndex(0));
                int id1 = globalIds.getInt(globalIds.getFirstValueIndex(1));
                assertThat("Connection error gets a positive global ID", id0, greaterThan(0));
                assertThat("Disconnected gets a positive global ID", id1, greaterThan(0));
                assertThat("Connection error and Disconnected have different global IDs", id0 == id1, equalTo(false));
            } finally {
                mergeResult.releaseBlocks();
            }
        }
        evalOp.close();
    }

    /**
     * Regression for the distributed correctness bug where, before state merging, the coordinator
     * re-categorized the sparse survivor rows from scratch. Two shards independently assign
     * local ordinal 1 to different logical categories; without state merging a coordinator using
     * local ordinals directly would treat both rows as "group 1" and a LIMIT 1 would discard one.
     *
     * <p>Concretely:
     * <ul>
     *   <li>Shard 1 sees many "Error …" messages → local cat 1 = {@code error *}</li>
     *   <li>Shard 2 sees many "Info …" messages → local cat 1 = {@code info *} (independent counter)</li>
     *   <li>Both shards keep one row (local cat 1) and pass it, together with their categorizer state,
     *       through the exchange.</li>
     *   <li>State merging must map shard 1's local cat 1 and shard 2's local cat 1 to <em>different</em>
     *       global ordinals so that the coordinator's LIMIT BY correctly retains one row per category.</li>
     * </ul>
     */
    public void testCrossShardSameLocalOrdinalMappedToDifferentGlobalIds() throws IOException {
        DriverContext ctx = driverContext();
        BlockFactory blockFactory = ctx.blockFactory();

        // Shard 1: "Error" messages → local cat 1 = "error *"
        Page shard1Page = buildShardPage(ctx, blockFactory, List.of("Error alpha timeout", "Error beta timeout", "Error gamma timeout"), 0);
        // Shard 2: "Info" messages → also local cat 1 (independent ordinal counter)
        Page shard2Page = buildShardPage(ctx, blockFactory, List.of("Info alpha success", "Info beta success", "Info gamma success"), 0);

        try (CategorizeStateMergeOperator mergeOp = new CategorizeStateMergeOperator.Factory(0, 1, CATEGORIZE_DEF).get(ctx)) {
            mergeOp.addInput(shard1Page);
            Page merged1 = mergeOp.getOutput();
            assertNotNull(merged1);

            mergeOp.addInput(shard2Page);
            mergeOp.finish();
            Page merged2 = mergeOp.getOutput();
            assertNotNull(merged2);

            try {
                IntBlock globalIds1 = merged1.getBlock(0);
                IntBlock globalIds2 = merged2.getBlock(0);
                int globalId1 = globalIds1.getInt(globalIds1.getFirstValueIndex(0));
                int globalId2 = globalIds2.getInt(globalIds2.getFirstValueIndex(0));

                assertThat("shard 1 row gets a positive global ID", globalId1, greaterThan(0));
                assertThat("shard 2 row gets a positive global ID", globalId2, greaterThan(0));
                assertThat("same local ordinal from different shards maps to distinct global ordinals", globalId1, not(equalTo(globalId2)));
            } finally {
                merged1.releaseBlocks();
                merged2.releaseBlocks();
            }
        }
    }

    /**
     * Simulates what one data-node shard emits through the exchange for a single kept row.
     * Runs all {@code messages} through a fresh {@link CategorizeEvalOperator}, picks the row
     * at {@code keepPosition}, then uses {@link CategorizeStateEmitOperator} to append the full
     * shard categorizer state. Returns a page suitable as input to {@link CategorizeStateMergeOperator}:
     * channel 0 = local cat ID of the kept row, channel 1 = serialized categorizer state.
     */
    private Page buildShardPage(DriverContext ctx, BlockFactory blockFactory, List<String> messages, int keepPosition) throws IOException {
        CategorizeEvalOperator[] holder = new CategorizeEvalOperator[1];
        CategorizeEvalOperator evalOp = new CategorizeEvalOperator.Factory(0, CATEGORIZE_DEF, analysisRegistry, holder).get(ctx);
        try {
            try (BytesRefBlock.Builder builder = blockFactory.newBytesRefBlockBuilder(messages.size())) {
                for (String msg : messages) {
                    builder.appendBytesRef(new BytesRef(msg));
                }
                evalOp.addInput(new Page(builder.build()));
            }
            evalOp.finish();
            Page evalPage = evalOp.getOutput();
            assertNotNull(evalPage);

            // Pick the local cat ID for the kept row; release other blocks.
            IntBlock allCatIds = evalPage.getBlock(1);
            evalPage.getBlock(0).close();
            int keptCatId;
            try {
                keptCatId = allCatIds.getInt(allCatIds.getFirstValueIndex(keepPosition));
            } finally {
                allCatIds.close();
            }

            // Emit the full shard state alongside the kept row's local cat ID.
            IntBlock keptBlock = blockFactory.newConstantIntBlockWith(keptCatId, 1);
            CategorizeStateEmitOperator emitOp = new CategorizeStateEmitOperator.Factory(holder).get(ctx);
            emitOp.addInput(new Page(keptBlock));
            emitOp.finish();
            Page emitPage = emitOp.getOutput();
            assertNotNull(emitPage);
            return emitPage;
        } finally {
            evalOp.close();
        }
    }

    /** Multivalued IntBlock positions are remapped element-by-element via the empty state. */
    public void testMultivaluedIdsRemappedViaEmptyState() {
        DriverContext ctx = driverContext();
        BlockFactory blockFactory = ctx.blockFactory();
        try (CategorizeStateMergeOperator op = new CategorizeStateMergeOperator.Factory(0, 1, CATEGORIZE_DEF).get(ctx)) {
            IntBlock catIds;
            try (IntBlock.Builder builder = blockFactory.newIntBlockBuilder(2)) {
                // position 0: single value 1
                builder.appendInt(1);
                // position 1: MV [2, 3]
                builder.beginPositionEntry();
                builder.appendInt(2);
                builder.appendInt(3);
                builder.endPositionEntry();
                catIds = builder.build();
            }
            BytesRefBlock state = blockFactory.newConstantBytesRefBlockWith(EMPTY_STATE, 2);
            op.addInput(new Page(catIds, state));
            op.finish();
            Page result = op.getOutput();
            assertNotNull(result);
            try {
                IntBlock remapped = result.getBlock(0);
                assertThat("position 0 has one value", remapped.getValueCount(0), equalTo(1));
                assertThat("position 0 remapped to NULL_ORD", remapped.getInt(remapped.getFirstValueIndex(0)), equalTo(0));
                assertThat("position 1 retains two values", remapped.getValueCount(1), equalTo(2));
                int first = remapped.getFirstValueIndex(1);
                assertThat("MV first element remapped to NULL_ORD", remapped.getInt(first), equalTo(0));
                assertThat("MV second element remapped to NULL_ORD", remapped.getInt(first + 1), equalTo(0));
            } finally {
                result.releaseBlocks();
            }
        }
    }
}
