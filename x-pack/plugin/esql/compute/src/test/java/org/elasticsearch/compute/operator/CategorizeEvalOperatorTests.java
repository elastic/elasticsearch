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
import org.elasticsearch.compute.data.ElementType;
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
import java.util.Locale;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.nullValue;

/**
 * Tests for {@link CategorizeEvalOperator}: buffering, state emission, and single-node passthrough.
 */
public class CategorizeEvalOperatorTests extends OperatorTestCase {

    private static final BlockHash.CategorizeDef CATEGORIZE_DEF = new BlockHash.CategorizeDef(
        null,
        BlockHash.CategorizeDef.OutputFormat.TOKENS,
        70
    );

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
     * Simple factory uses SINGLE mode (no state channel) so the standard driver-based tests
     * work without the finish-before-output buffering constraint of INITIAL mode.
     * Text is at channel 0; {@link CategorizeEvalOperator} appends catId at channel 1.
     */
    @Override
    protected Operator.OperatorFactory simple(SimpleOptions options) {
        // catId is appended at channel 1 (after text at channel 0), so elementTypes must cover
        // both channels: [BYTES_REF for text, INT for catId].
        return new CategorizeEvalOperator.Factory(
            0,
            CATEGORIZE_DEF,
            analysisRegistry,
            new GroupedLimitOperator.Factory(100, List.of(1), List.of(ElementType.BYTES_REF, ElementType.INT)),
            true
        );
    }

    @Override
    protected SourceOperator simpleInput(BlockFactory blockFactory, int size) {
        List<Page> pages = new ArrayList<>();
        String[] templates = { "Error %d timeout", "Info %d success", "Warning %d expired" };
        try (BytesRefBlock.Builder b = blockFactory.newBytesRefBlockBuilder(size)) {
            for (int i = 0; i < size; i++) {
                b.appendBytesRef(new BytesRef(String.format(Locale.ROOT, templates[i % 3], i)));
            }
            pages.add(new Page(b.build()));
        }
        return new CannedSourceOperator(pages.iterator());
    }

    @Override
    protected void assertSimpleOutput(List<Page> input, List<Page> results) {
        assertThat(results.isEmpty(), equalTo(false));
        int totalOutput = results.stream().mapToInt(Page::getPositionCount).sum();
        int totalInput = input.stream().mapToInt(Page::getPositionCount).sum();
        assertThat("output rows do not exceed input rows", totalOutput, lessThanOrEqualTo(totalInput));
        for (Page result : results) {
            // Input: 1-block page (text). Operator appends catId → 2 blocks.
            // SINGLE mode: no state channel appended. Inner GroupedLimitOperator passes through.
            assertThat("SINGLE mode output: text block + catId block", result.getBlockCount(), equalTo(2));
        }
    }

    @Override
    protected Matcher<String> expectedDescriptionOfSimple() {
        return equalTo("CategorizeEvalOperator[channel=0, inner=GroupedLimitOperator[limitPerGroup = 100]]");
    }

    @Override
    protected Matcher<String> expectedToStringOfSimple() {
        return equalTo("CategorizeEvalOperator[channel=0, inner=GroupedLimitOperator[limitPerGroup = 100, groupKeys = [1], groups = 0]]");
    }

    @Override
    protected void assertStatus(Map<String, Object> map, List<Page> input, List<Page> output) {
        assertThat(map, nullValue());
    }

    /** In INITIAL mode ({@code isSingleNode=false}) all output is withheld until {@code finish()} is called. */
    public void testNoOutputBeforeFinish() {
        DriverContext ctx = driverContext();
        BlockFactory blockFactory = ctx.blockFactory();
        try (
            CategorizeEvalOperator op = new CategorizeEvalOperator.Factory(
                0,
                CATEGORIZE_DEF,
                analysisRegistry,
                new GroupedLimitOperator.Factory(10, List.of(1), List.of(ElementType.BYTES_REF, ElementType.INT)),
                false
            ).get(ctx)
        ) {
            op.addInput(singleTextPage(blockFactory, "Error A"));
            assertNull("getOutput() returns null before finish()", op.getOutput());
            assertFalse("isFinished() is false before finish()", op.isFinished());
            op.finish();
            Page p = op.getOutput();
            assertNotNull("output available after finish()", p);
            p.releaseBlocks();
        }
    }

    /** In SINGLE mode ({@code isSingleNode=true}) output pages carry exactly text + catId — no appended state block. */
    public void testSingleNodeNoStateChannel() {
        DriverContext ctx = driverContext();
        BlockFactory blockFactory = ctx.blockFactory();
        try (
            CategorizeEvalOperator op = new CategorizeEvalOperator.Factory(
                0,
                CATEGORIZE_DEF,
                analysisRegistry,
                new GroupedLimitOperator.Factory(10, List.of(1), List.of(ElementType.BYTES_REF, ElementType.INT)),
                true
            ).get(ctx)
        ) {
            op.addInput(singleTextPage(blockFactory, "Error A"));
            op.finish();
            Page p = op.getOutput();
            assertNotNull(p);
            try {
                assertThat("SINGLE mode: no state block appended (text + catId only)", p.getBlockCount(), equalTo(2));
            } finally {
                p.releaseBlocks();
            }
        }
    }

    /**
     * Regression test: in INITIAL mode the categorizer state is serialized once, lazily, after
     * {@code finish()}, so every output page — including those buffered before later input pages
     * were processed — carries the final complete model.
     *
     * <p>Setup: inner {@link GroupedLimitOperator} with {@code limit=1}. Four input pages:
     * <ol>
     *   <li>"Error alpha timeout"   → cat1 (new group, kept)</li>
     *   <li>"Info beta success"     → cat2 (new group, kept)</li>
     *   <li>"Error alpha timeout"   → cat1 (group full, inner rejects all rows, nothing buffered)</li>
     *   <li>"Warning delta expired" → cat3 (new group, kept)</li>
     * </ol>
     * Three output pages are produced (one per kept row). All three must carry identical state
     * bytes. A buggy implementation that snapshotted state eagerly after each drain would produce
     * pages with different (stale) states.
     */
    public void testAllOutputPagesCarryFinalState() {
        DriverContext ctx = driverContext();
        BlockFactory blockFactory = ctx.blockFactory();
        try (
            CategorizeEvalOperator op = new CategorizeEvalOperator.Factory(
                0,
                CATEGORIZE_DEF,
                analysisRegistry,
                new GroupedLimitOperator.Factory(1, List.of(1), List.of(ElementType.BYTES_REF, ElementType.INT)),
                false
            ).get(ctx)
        ) {
            op.addInput(singleTextPage(blockFactory, "Error alpha timeout"));    // → cat1 (kept)
            op.addInput(singleTextPage(blockFactory, "Info beta success"));      // → cat2 (kept)
            op.addInput(singleTextPage(blockFactory, "Error alpha timeout"));    // → cat1 (group full, rejected)
            op.addInput(singleTextPage(blockFactory, "Warning delta expired"));  // → cat3 (kept)
            op.finish();

            List<Page> output = new ArrayList<>();
            Page p;
            while ((p = op.getOutput()) != null) {
                output.add(p);
            }

            assertThat("one output page per kept row (3 total)", output.size(), equalTo(3));

            // All output pages must carry the same (final) serialized state
            BytesRef expected = extractState(output.get(0));
            for (Page page : output) {
                assertThat("all output pages carry the final categorizer state", extractState(page), equalTo(expected));
            }

            output.forEach(Page::releaseBlocks);
        }
    }

    private static Page singleTextPage(BlockFactory blockFactory, String message) {
        try (BytesRefBlock.Builder b = blockFactory.newBytesRefBlockBuilder(1)) {
            b.appendBytesRef(new BytesRef(message));
            return new Page(b.build());
        }
    }

    private static BytesRef extractState(Page page) {
        BytesRefBlock stateBlock = page.getBlock(page.getBlockCount() - 1);
        return stateBlock.getBytesRef(stateBlock.getFirstValueIndex(0), new BytesRef());
    }
}
