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
import org.elasticsearch.compute.aggregation.AggregatorMode;
import org.elasticsearch.compute.aggregation.blockhash.BlockHash;
import org.elasticsearch.compute.aggregation.blockhash.CategorizeBlockHash;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.ElementType;
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
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;

/**
 * Tests for {@link CategorizeMergeOperator}: state-channel removal, ID remapping via empty
 * and real categorizer states, multivalued positions, and end-to-end pipeline correctness.
 */
public class CategorizeMergeOperatorTests extends OperatorTestCase {

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
                    builder.appendInt(i % 3 + 1); // positive IDs; empty state remaps these to NULL_ORD
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
        assertThat(results.isEmpty(), equalTo(false));
        for (Page resultPage : results) {
            // State channel dropped: every result page has exactly one block
            assertThat("state channel dropped", resultPage.getBlockCount(), equalTo(1));
            // Empty state maps all IDs to NULL_ORD (0)
            IntBlock remapped = resultPage.getBlock(0);
            for (int p = 0; p < resultPage.getPositionCount(); p++) {
                assertThat("local ID remapped to NULL_ORD via empty state", remapped.getInt(remapped.getFirstValueIndex(p)), equalTo(0));
            }
        }
    }

    @Override
    protected Operator.OperatorFactory simple(SimpleOptions options) {
        return new CategorizeMergeOperator.Factory(
            0,
            1,
            CATEGORIZE_DEF,
            new GroupedLimitOperator.Factory(100, List.of(0), List.of(ElementType.INT))
        );
    }

    @Override
    protected Matcher<String> expectedDescriptionOfSimple() {
        return equalTo(
            "CategorizeGroupingMergeOperator[catIdChannel=0, stateChannel=1, emitState=false, "
                + "inner=GroupedLimitOperator[limitPerGroup = 100]]"
        );
    }

    @Override
    protected Matcher<String> expectedToStringOfSimple() {
        return equalTo(
            "CategorizeGroupingMergeOperator[catIdChannel=0, stateChannel=1, emitState=false, "
                + "inner=GroupedLimitOperator[limitPerGroup = 100, groupKeys = [0], groups = 0]]"
        );
    }

    @Override
    protected void assertStatus(Map<String, Object> map, List<Page> input, List<Page> output) {
        assertThat(map, nullValue());
    }

    /** NULL_ORD (0) in local IDs maps to NULL_ORD (0) in global IDs regardless of state. */
    public void testNullOrdPassesThroughUnchanged() {
        DriverContext ctx = driverContext();
        BlockFactory blockFactory = ctx.blockFactory();
        try (CategorizeMergeOperator op = mergeFactory().get(ctx)) {
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
        try (CategorizeMergeOperator op = mergeFactory().get(ctx)) {
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
     * A real categorizer state: two distinct messages produce two positive global IDs that differ
     * from each other.
     */
    public void testRealStateRemapsIdsToGlobalIds() {
        DriverContext ctx = driverContext();
        BlockFactory blockFactory = ctx.blockFactory();

        // Build a page with two rows using a real shard categorizer state
        Page shardPage = buildShardPage(blockFactory, List.of("Connection error", "Disconnected"));
        try (CategorizeMergeOperator mergeOp = mergeFactory().get(ctx)) {
            mergeOp.addInput(shardPage);
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
    public void testCrossShardSameLocalOrdinalMappedToDifferentGlobalIds() {
        DriverContext ctx = driverContext();
        BlockFactory blockFactory = ctx.blockFactory();

        // Shard 1: "Error" messages → local cat 1 = "error *"
        Page shard1Row = keepFirstRow(blockFactory, buildShardPage(blockFactory, List.of("Error alpha", "Error beta", "Error gamma")));
        // Shard 2: "Info" messages → also local cat 1 (independent ordinal counter)
        Page shard2Row = keepFirstRow(blockFactory, buildShardPage(blockFactory, List.of("Info alpha", "Info beta", "Info gamma")));

        try (CategorizeMergeOperator mergeOp = mergeFactory().get(ctx)) {
            mergeOp.addInput(shard1Row);
            Page merged1 = mergeOp.getOutput();
            assertNotNull(merged1);

            mergeOp.addInput(shard2Row);
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

    /** Multivalued IntBlock positions are remapped element-by-element via the empty state. */
    public void testMultivaluedIdsRemappedViaEmptyState() {
        DriverContext ctx = driverContext();
        BlockFactory blockFactory = ctx.blockFactory();
        try (CategorizeMergeOperator op = mergeFactory().get(ctx)) {
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

    /**
     * INTERMEDIATE mode ({@code emitState=true}): no output is returned before {@link Operator#finish()}.
     */
    public void testIntermediateModeOutputWithheldUntilFinish() {
        DriverContext ctx = driverContext();
        BlockFactory blockFactory = ctx.blockFactory();
        try (
            CategorizeMergeOperator op = new CategorizeMergeOperator.Factory(
                0,
                1,
                CATEGORIZE_DEF,
                new GroupedLimitOperator.Factory(10, List.of(0), List.of(ElementType.INT)),
                true
            ).get(ctx)
        ) {
            op.addInput(buildShardPage(blockFactory, List.of("Error alpha")));
            assertNull("no output before finish() in INTERMEDIATE mode", op.getOutput());
            assertFalse("not finished before finish()", op.isFinished());
            op.finish();
            Page p = op.getOutput();
            assertNotNull("output available after finish()", p);
            p.releaseBlocks();
        }
    }

    /**
     * Regression for the buffering requirement in INTERMEDIATE mode: every output page must carry the
     * <em>final</em> merged categorizer state. A buggy implementation that serialized state after each
     * drain would produce stale snapshots missing categories seen by later input pages.
     *
     * <p>Two shard pages are fed (different categories, both local cat 1). After merging, the global
     * model has two categories. Every output page must carry that complete two-category state, not a
     * one-category snapshot.
     */
    public void testIntermediateModeEveryOutputPageCarriesFinalMergedState() {
        DriverContext ctx = driverContext();
        BlockFactory blockFactory = ctx.blockFactory();
        try (
            CategorizeMergeOperator op = new CategorizeMergeOperator.Factory(
                0,
                1,
                CATEGORIZE_DEF,
                new GroupedLimitOperator.Factory(Integer.MAX_VALUE, List.of(0), List.of(ElementType.INT)),
                true
            ).get(ctx)
        ) {
            op.addInput(buildShardPage(blockFactory, List.of("Connection refused")));
            op.addInput(buildShardPage(blockFactory, List.of("Disk failure")));
            op.finish();

            List<Page> output = new ArrayList<>();
            Page p;
            while ((p = op.getOutput()) != null) {
                output.add(p);
            }

            assertThat("some output produced", output.isEmpty(), equalTo(false));
            for (Page page : output) {
                // inner GroupedLimitOperator output: 1 block (catId). With state appended: 2 blocks.
                assertThat("INTERMEDIATE mode appends state block to each page", page.getBlockCount(), equalTo(2));
            }

            // All output pages carry the same (final) merged-model bytes
            BytesRef expected = extractState(output.get(0));
            for (Page page : output) {
                assertThat("all output pages carry the final merged state", extractState(page), equalTo(expected));
            }

            output.forEach(Page::releaseBlocks);
        }
    }

    /**
     * Complement to {@link #testCrossShardSameLocalOrdinalMappedToDifferentGlobalIds}: when two
     * shards independently assign local ordinal 1 to the <em>same</em> logical category (same message
     * pattern), state merging must collapse both to a single global ordinal.
     */
    public void testSameLogicalCategoryFromTwoShardsMapsToDifferentLocalButSameGlobalId() {
        DriverContext ctx = driverContext();
        BlockFactory blockFactory = ctx.blockFactory();

        // Identical message lists → identical category patterns → local cat 1 on each shard
        List<String> messages = List.of("Error alpha", "Error beta", "Error gamma");
        Page shard1 = keepFirstRow(blockFactory, buildShardPage(blockFactory, messages));
        Page shard2 = keepFirstRow(blockFactory, buildShardPage(blockFactory, messages));

        try (CategorizeMergeOperator mergeOp = mergeFactory().get(ctx)) {
            mergeOp.addInput(shard1);
            Page out1 = mergeOp.getOutput();
            assertNotNull(out1);

            mergeOp.addInput(shard2);
            mergeOp.finish();
            Page out2 = mergeOp.getOutput();
            assertNotNull(out2);

            try {
                IntBlock ids1 = out1.getBlock(0);
                IntBlock ids2 = out2.getBlock(0);
                int globalId1 = ids1.getInt(ids1.getFirstValueIndex(0));
                int globalId2 = ids2.getInt(ids2.getFirstValueIndex(0));
                assertThat("same logical category from two shards maps to the same global ID", globalId1, equalTo(globalId2));
            } finally {
                out1.releaseBlocks();
                out2.releaseBlocks();
            }
        }
    }

    /**
     * Returns a {@link CategorizeMergeOperator.Factory} in FINAL mode (no state re-emit)
     * wrapping a {@link GroupedLimitOperator} with a large per-group limit.
     */
    private CategorizeMergeOperator.Factory mergeFactory() {
        return new CategorizeMergeOperator.Factory(
            0,
            1,
            CATEGORIZE_DEF,
            new GroupedLimitOperator.Factory(Integer.MAX_VALUE, List.of(0), List.of(ElementType.INT))
        );
    }

    private static BytesRef extractState(Page page) {
        BytesRefBlock stateBlock = page.getBlock(page.getBlockCount() - 1);
        return stateBlock.getBytesRef(stateBlock.getFirstValueIndex(0), new BytesRef());
    }

    /**
     * Categorizes all {@code messages} with a fresh {@link CategorizeBlockHash} and returns a page
     * suitable as input to {@link CategorizeMergeOperator} with {@code catIdChannel=0,
     * stateChannel=1}: one row per message, local catIds at channel 0, constant serialized
     * categorizer state at channel 1.
     */
    private Page buildShardPage(BlockFactory blockFactory, List<String> messages) {
        CategorizeBlockHash hash = new CategorizeBlockHash(blockFactory, 0, AggregatorMode.INITIAL, CATEGORIZE_DEF, analysisRegistry);
        try {
            BytesRefBlock textBlock;
            try (BytesRefBlock.Builder builder = blockFactory.newBytesRefBlockBuilder(messages.size())) {
                for (String msg : messages) {
                    builder.appendBytesRef(new BytesRef(msg));
                }
                textBlock = builder.build();
            }
            IntBlock catIds = hash.categorize(textBlock);
            textBlock.close();
            BytesRef state = hash.serializeCategorizer();
            BytesRefBlock stateBlock = blockFactory.newConstantBytesRefBlockWith(state, catIds.getPositionCount());
            return new Page(catIds, stateBlock);
        } finally {
            hash.close();
        }
    }

    /**
     * Returns a new 1-row page carrying the local cat ID at position 0 of {@code sourcePage} and a
     * copy of the constant categorizer state. Releases {@code sourcePage}.
     */
    private static Page keepFirstRow(BlockFactory blockFactory, Page sourcePage) {
        try {
            IntBlock allCatIds = sourcePage.getBlock(0);
            BytesRefBlock stateBlock = sourcePage.getBlock(1);
            int keptCatId = allCatIds.getInt(allCatIds.getFirstValueIndex(0));
            BytesRef state = BytesRef.deepCopyOf(stateBlock.getBytesRef(stateBlock.getFirstValueIndex(0), new BytesRef()));
            IntBlock keptBlock = blockFactory.newConstantIntBlockWith(keptCatId, 1);
            BytesRefBlock keptState = blockFactory.newConstantBytesRefBlockWith(state, 1);
            return new Page(keptBlock, keptState);
        } finally {
            sourcePage.releaseBlocks();
        }
    }
}
